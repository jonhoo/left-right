use super::Operation;
use inner::Inner;
use read::ReadHandle;

use std::sync::atomic;
use std::hash::{BuildHasher, Hash};
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
where
    K: Eq + Hash,
    S: BuildHasher,
{
    w_handle: Option<Box<Inner<K, V, M, S>>>,
    oplog: Vec<Operation<K, V>>,
    swap_index: usize,
    r_handle: ReadHandle<K, V, M, S>,
    last_epochs: Vec<usize>,
    epochs_checked: Vec<bool>,
    meta: M,
    first: bool,
    second: bool,
}

pub fn new<K, V, M, S>(
    w_handle: Inner<K, V, M, S>,
    r_handle: ReadHandle<K, V, M, S>,
) -> WriteHandle<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
    M: 'static + Clone,
{
    let m = w_handle.meta.clone();
    WriteHandle {
        w_handle: Some(Box::new(w_handle)),
        oplog: Vec::new(),
        swap_index: 0,
        r_handle: r_handle,
        last_epochs: Vec::new(),
        epochs_checked: Vec::new(),
        meta: m,
        first: true,
        second: false,
    }
}

impl<K, V, M, S> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Clone,
    M: 'static + Clone,
{
    /// Refresh the handle used by readers so that pending writes are made visible.
    ///
    /// This method needs to wait for all readers to move to the new handle so that it can replay
    /// the operational log onto the stale map copy the readers used to use. This can take some
    /// time, especially if readers are executing slow operations, or if there are many of them.
    pub fn refresh(&mut self) {
        use std::thread;
        use std::mem;

        // we need to wait until all epochs have changed since the swaps *or* until a "finished"
        // flag has been observed to be on for two subsequent iterations (there still may be some
        // readers present since we did the previous refresh)
        //
        // NOTE: it is safe for us to hold the lock for the entire duration of the swap. we will
        // only block on pre-existing readers, and they are never waiting to push onto epochs
        // unless they have finished reading.
        let epochs = self.w_handle.as_ref().unwrap().epochs.clone();
        let epochs = epochs.lock().unwrap();
        let mut first = false;
        let high_bit = 1usize << (mem::size_of::<usize>() * 8 - 1);
        loop {
            let last_epochs = &self.last_epochs;

            let mut all_left = false;
            if first {
                // fast path -- read all and see if all have changed (which is likely)
                let epochs_checked = &mut self.epochs_checked;
                all_left = last_epochs.iter().enumerate().all(|(i, &e)| {
                    let now = epochs[i].load(atomic::Ordering::Acquire);
                    epochs_checked[i] = now == 0 || now != e || now & high_bit != 0;
                    epochs_checked[i]
                });
                first = false;
            }

            if !all_left {
                // slow path -- only some have changed
                all_left = self.epochs_checked
                    .iter_mut()
                    .enumerate()
                    .all(|(i, epoch)| {
                        if *epoch {
                            return true;
                        }

                        let last = last_epochs[i];
                        let now = epochs[i].load(atomic::Ordering::Acquire);
                        if last != now {
                            // epoch increased or high bit now set, both indicate reader has left.
                            // may have entered again, but if so it must have read new pointer.
                            *epoch = true;
                            return true;
                        }

                        if now == 0 {
                            // reader has never done a read
                            *epoch = true;
                            return true;
                        }

                        if now & high_bit != 0 {
                            // high bit set, and previous value was the same, so reader has indeed
                            // left.
                            *epoch = true;
                            return true;
                        }

                        false
                    });
            }

            if all_left {
                // all the readers have left!
                // we can safely bring the w_handle up to date.
                let w_handle = self.w_handle.as_mut().unwrap();

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
                        Box::from_raw(self.r_handle.inner.load(atomic::Ordering::Relaxed))
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
        let w_handle = Box::into_raw(w_handle);

        // swap in our w_handle, and get r_handle in return
        // note that this *technically* only needs to be Ordering::Release, but we make it SeqCst
        // to ensure that the subsequent epoch reads aren't re-ordered to before the swap.
        let r_handle = self.r_handle.inner.swap(w_handle, atomic::Ordering::SeqCst);
        let r_handle = unsafe { Box::from_raw(r_handle) };

        self.last_epochs.clear();
        self.epochs_checked.clear();
        for e in epochs.iter() {
            self.last_epochs.push(e.load(atomic::Ordering::Acquire));
            self.epochs_checked.push(false);
        }

        // NOTE: at this point, there are likely still readers using the w_handle we got
        self.w_handle = Some(r_handle);
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
        if !self.first {
            self.oplog.push(op);
        } else {
            // we know there are no outstanding w_handle readers, so we can modify it directly!
            let inner = self.w_handle.as_mut().unwrap();
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

    /// Clear the value-set of the given key, without removing it.
    ///
    /// The new value will only be visible to readers after the next call to `refresh()`.
    pub fn clear(&mut self, k: K) {
        self.add_op(Operation::Clear(k));
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
            Operation::Clear(key) => {
                let mut v = inner.data.entry(key).or_insert_with(Vec::new);
                v.clear();
            }
            Operation::Add(key, value) => {
                inner.data.entry(key).or_insert_with(Vec::new).push(value);
            }
            Operation::Empty(key) => {
                inner.data.remove(&key);
            }
            Operation::Remove(key, value) => {
                if let Some(mut e) = inner.data.get_mut(&key) {
                    // find the first entry that matches all fields
                    if let Some(i) = e.iter().position(|v| v == &value) {
                        e.swap_remove(i);
                    }
                }
            }
        }
    }
}

impl<K, V, M, S> Extend<(K, V)> for WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Clone,
    M: 'static + Clone,
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
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Clone,
    M: 'static + Clone,
{
    type Target = ReadHandle<K, V, M, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
