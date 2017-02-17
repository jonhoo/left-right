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
    swap_index: usize,
    r_handle: ReadHandle<K, V, M, S>,
    meta: M,
    first: bool,
    second: bool,
}

pub fn new<K, V, M, S>(w_handle: Inner<K, V, M, S>,
                       r_handle: ReadHandle<K, V, M, S>)
                       -> WriteHandle<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher,
          M: 'static + Clone
{
    let m = w_handle.meta.clone();
    WriteHandle {
        w_handle: Some(Box::new(sync::Arc::new(w_handle))),
        oplog: Vec::new(),
        swap_index: 0,
        r_handle: r_handle,
        meta: m,
        first: true,
        second: false,
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

        // first, wait for all readers on our write map to go away
        // (there still may be some present since we did the previous refresh)
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

                if self.second {
                    use std::mem;
                    // before the first refresh, all writes went directly to w_handle. then, at the
                    // first refresh, r_handle and w_handle were swapped. thus, the w_handle we
                    // have now is empty, *and* none of the writes in r_handle are in the oplog.
                    // we therefore have to first clone the entire state of the current r_handle
                    // and make that w_handle, and *then* replay the oplog (which holds writes
                    // following the first refresh).
                    //
                    // this may seem unnecessarily complex, but it has the major advantage that it
                    // is relatively efficient to do lots of writes to the evmap at startup to
                    // populate it, and then refresh().
                    let r_handle = unsafe {
                        Box::from_raw(self.r_handle.inner.load(atomic::Ordering::SeqCst))
                    };
                    w_handle.data = r_handle.data.clone();
                    mem::forget(r_handle);
                }

                // the w_handle map has not seen any of the writes in the oplog
                // the r_handle map has not seen any of the writes following swap_index
                if self.swap_index != 0 {
                    // we can drain out the operations that only the w_handle map needs
                    // NOTE: the if above is because drain(0..0) would remove 0
                    for op in self.oplog.drain(0..self.swap_index) {
                        Self::apply_op(w_handle, op);
                    }
                }
                // the rest have to be cloned because they'll also be needed by the r_handle map
                for op in self.oplog.iter().cloned() {
                    Self::apply_op(w_handle, op);
                }
                // the w_handle map is about to become the r_handle, and can ignore the oplog
                self.swap_index = self.oplog.len();
                // ensure meta-information is up to date
                w_handle.meta = self.meta.clone();
                w_handle.mark_ready();

                // w_handle (the old r_handle) is now fully up to date!
                break;
            } else {
                thread::yield_now();
            }
        }

        // at this point, we have exclusive access to w_handle, and it is up-to-date with all
        // writes. the stale r_handle is accessed by readers through an Arc clone of atomic pointer
        // inside the ReadHandle. oplog contains all the changes that are in w_handle, but not in
        // r_handle.
        //
        // it's now time for us to swap the maps so that readers see up-to-date results from
        // w_handle.

        // prepare w_handle
        let w_handle = self.w_handle.take().unwrap();
        let w_handle: *mut sync::Arc<_> = Box::into_raw(w_handle);

        // swap in our w_handle, and get r_handle in return
        let r_handle = self.r_handle.inner.swap(w_handle, atomic::Ordering::SeqCst);
        self.w_handle = Some(unsafe { Box::from_raw(r_handle) });

        // NOTE: at this point, there are likely still readers using the w_handle we got
        self.second = self.first;
        self.first = false;
    }

    /// Set the metadata.
    ///
    /// Will only be visible to readers after the next call to `refresh()`.
    pub fn set_meta(&mut self, mut meta: M) -> M {
        use std::mem;
        mem::swap(&mut self.meta, &mut meta);
        meta
    }

    fn add_op(&mut self, op: Operation<K, V>) {
        if self.first == false {
            self.oplog.push(op);
        } else {
            // we know there are no outstanding w_handle readers, so we can modify it directly!
            let arc: &mut sync::Arc<_> = &mut *self.w_handle.as_mut().unwrap();
            let inner = sync::Arc::get_mut(arc).expect("before first refresh there are no readers");
            Self::apply_op(inner, op);
            // NOTE: since we didn't record this in the oplog, r_handle *must* clone w_handle
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
