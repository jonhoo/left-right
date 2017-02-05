use super::Operation;
use inner::Inner;
use read::ReadHandle;

use std::sync;
use std::sync::atomic;
use std::hash::{Hash, BuildHasher};
use std::collections::hash_map::RandomState;

/// A handle that may be used to modify the eventually consistent map.
///
/// Note that any changes made to the map will not be made visible to readers until `refresh()` is
/// called.
///
/// # Examples
/// ```
/// let x = ('x', 42);
///
/// let (r, mut w) = evmap::new();
///
/// // the map is uninitialized, so all lookups should return None
/// assert_eq!(r.get_and(&x.0, |rs| rs.len()), None);
///
/// w.refresh();
///
/// // after the first refresh, it is empty, but ready
/// assert_eq!(r.get_and(&x.0, |rs| rs.len()), None);
///
/// w.insert(x.0, x);
///
/// // it is empty even after an add (we haven't refresh yet)
/// assert_eq!(r.get_and(&x.0, |rs| rs.len()), None);
///
/// w.refresh();
///
/// // but after the swap, the record is there!
/// assert_eq!(r.get_and(&x.0, |rs| rs.len()), Some(1));
/// assert_eq!(r.get_and(&x.0, |rs| rs.iter().any(|v| v.0 == x.0 && v.1 == x.1)), Some(true));
/// ```
pub struct WriteHandle<K, V, M = (), S = RandomState>
    where K: Eq + Hash,
          S: BuildHasher
{
    w_handle: Option<Box<sync::Arc<Inner<K, V, M, S>>>>,
    oplog: Vec<Operation<K, V>>,
    r_handle: ReadHandle<K, V, M, S>,
    first: bool,
}

pub fn new<K, V, M, S>(w_handle: Inner<K, V, M, S>,
                       r_handle: ReadHandle<K, V, M, S>)
                       -> WriteHandle<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    WriteHandle {
        w_handle: Some(Box::new(sync::Arc::new(w_handle))),
        oplog: Vec::new(),
        r_handle: r_handle,
        first: true,
    }
}

impl<K, V, M, S> WriteHandle<K, V, M, S>
    where K: Eq + Hash + Clone,
          S: BuildHasher + Clone,
          V: Eq + Clone,
          M: 'static + Clone
{
    /// Refresh the handle used by readers so that pending writes are made visible.
    ///
    /// This method needs to wait for all readers to move to the new handle so that it can replay
    /// the operational log onto the stale map copy the readers used to use. This can take some
    /// time, especially if readers are executing slow operations, or if there are many of them.
    pub fn refresh(&mut self) {
        use std::thread;

        // at this point, we have exclusive access to w_handle, and it is up-to-date with all writes
        // r_handle is accessed by readers through a sync::Weak upgrade, and has old data
        // w_log contains all the changes that are in w_handle, but not in r_handle
        //
        // we're going to do the following:
        //
        //  - atomically swap in a weak pointer to the current w_handle into the BufferedStore,
        //    letting readers see new and updated state
        //  - store r_handle as our new w_handle
        //  - wait until we have exclusive access to this new w_handle
        //  - replay w_log onto w_handle

        // prepare w_handle
        let w_handle = self.w_handle.take().unwrap();
        let meta = w_handle.meta.clone();
        let w_handle_clone = if self.first {
            // this is the *first* swap, clone current w_handle instead of insterting all the rows
            // one-by-one
            self.first = false;
            Some(w_handle.data.clone())
        } else {
            None
        };
        let w_handle: *mut sync::Arc<_> = Box::into_raw(w_handle);

        // swap in our w_handle, and get r_handle in return
        let r_handle = self.r_handle.inner.swap(w_handle, atomic::Ordering::SeqCst);
        self.w_handle = Some(unsafe { Box::from_raw(r_handle) });

        // let readers go so they will be done with the old read Arc
        thread::yield_now();

        // now, wait for all existing readers to go away
        loop {
            if let Some(w_handle) = sync::Arc::get_mut(&mut *self.w_handle.as_mut().unwrap()) {
                // they're all gone
                // OR ARE THEY?
                // some poor reader could have *read* the pointer right before we swapped it, *but
                // not yet cloned the Arc*. we then check that there's only one strong reference,
                // *which there is*. *then* that reader upgrades their Arc => Uh-oh. *however*,
                // because of the second pointer read in readers, we know that a reader will detect
                // this case, and simply refuse to use the Arc it cloned. therefore, it is safe for
                // us to start mutating here

                // put in all the updates the read store hasn't seen
                if let Some(old_w_handle) = w_handle_clone {
                    w_handle.data = old_w_handle;
                } else {
                    for op in self.oplog.drain(..) {
                        Self::apply_op(w_handle, op);
                    }
                }
                w_handle.meta = meta;
                w_handle.mark_ready();

                // w_handle (the old r_handle) is now fully up to date!
                break;
            } else {
                thread::yield_now();
            }
        }
    }

    /// Set the metadata.
    ///
    /// Will only be visible to readers after the next call to `refresh()`.
    pub fn set_meta(&mut self, mut meta: M) -> M {
        self.with_mut(move |inner| {
            use std::mem;
            mem::swap(&mut inner.meta, &mut meta);
            meta
        })
    }

    fn add_op(&mut self, op: Operation<K, V>) {
        if self.first {
            // before the first swap, we'd rather just clone the entire map,
            // so no need to maintain an oplog (or to clone op)
            self.with_mut(|inner| { Self::apply_op(inner, op); });
        } else {
            self.with_mut(|inner| { Self::apply_op(inner, op.clone()); });
            // and also log it to later apply to the reads
            self.oplog.push(op);
        }
    }

    /// Add the given value to the value-set of the given key.
    ///
    /// The updated value-set will only be visible to readers after the next call to `refresh()`.
    pub fn insert(&mut self, k: K, v: V) {
        self.add_op(Operation::Add(k, v));
    }

    /// Replace the value-set of the given key with the given value.
    ///
    /// The new value will only be visible to readers after the next call to `refresh()`.
    pub fn update(&mut self, k: K, v: V) {
        self.add_op(Operation::Replace(k, v));
    }

    /// Remove the given value from the value-set of the given key.
    ///
    /// The updated value-set will only be visible to readers after the next call to `refresh()`.
    pub fn remove(&mut self, k: K, v: V) {
        self.add_op(Operation::Remove(k, v));
    }

    /// Remove the value-set for the given key.
    ///
    /// The value-set will only disappear from readers after the next call to `refresh()`.
    pub fn empty(&mut self, k: K) {
        self.add_op(Operation::Empty(k));
    }

    fn with_mut<F, T: 'static>(&mut self, f: F) -> T
        where F: FnOnce(&mut Inner<K, V, M, S>) -> T
    {
        let arc: &mut sync::Arc<_> = &mut *self.w_handle.as_mut().unwrap();
        loop {
            if let Some(s) = sync::Arc::get_mut(arc) {
                return f(s);
            } else {
                // writer should always be sole owner outside of swap
                // *however*, there may have been a reader who read the arc pointer before the
                // atomic pointer swap, and cloned *after* the Arc::get_mut call in swap(), so we
                // could still end up here. we know that the reader will detect its mistake and
                // drop that Arc (without using it), so we eventually end up with a unique Arc. In
                // fact, because we know the reader will never use the Arc in that case, we *could*
                // just start mutating straight away, but unfortunately Arc doesn't provide an API
                // to force this.
                use std::thread;
                thread::yield_now();
            }
        }
    }

    fn apply_op(inner: &mut Inner<K, V, M, S>, op: Operation<K, V>) {
        match op {
            Operation::Replace(key, value) => {
                let mut v = inner.data.entry(key).or_insert_with(Vec::new);
                v.clear();
                v.push(value);
            }
            Operation::Add(key, value) => {
                inner.data.entry(key).or_insert_with(Vec::new).push(value);
            }
            Operation::Empty(key) => {
                inner.data.remove(&key);
            }
            Operation::Remove(key, value) => {
                let mut now_empty = false;
                if let Some(mut e) = inner.data.get_mut(&key) {
                    // find the first entry that matches all fields
                    if let Some(i) = e.iter().position(|v| v == &value) {
                        e.swap_remove(i);
                        now_empty = e.is_empty();
                    }
                }
                if now_empty {
                    // no more entries for this key -- free up some space in the map
                    inner.data.remove(&key);
                }
            }
        }
    }
}

impl<K, V, M, S> Extend<(K, V)> for WriteHandle<K, V, M, S>
    where K: Eq + Hash + Clone,
          S: BuildHasher + Clone,
          V: Eq + Clone,
          M: 'static + Clone
{
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

// allow using write handle for reads
use std::ops::Deref;
impl<K, V, M, S> Deref for WriteHandle<K, V, M, S>
    where K: Eq + Hash + Clone,
          S: BuildHasher + Clone,
          V: Eq + Clone,
          M: 'static + Clone
{
    type Target = ReadHandle<K, V, M, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
