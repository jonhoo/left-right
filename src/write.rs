use super::{Operation, ShallowCopy};
use inner::Inner;
use read::ReadHandle;

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::atomic;

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
    K: Eq + Hash + Clone,
    S: BuildHasher + Default,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    w_handle: Option<Box<Inner<K, V, M, S>>>,
    oplog: Vec<Operation<K, V>>,
    swap_index: usize,
    r_handle: ReadHandle<K, V, M, S>,
    last_epochs: Vec<usize>,
    meta: M,
    first: bool,
    second: bool,
}

pub(crate) fn new<K, V, M, S>(
    w_handle: Inner<K, V, M, S>,
    r_handle: ReadHandle<K, V, M, S>,
) -> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Default,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    let m = w_handle.meta.clone();
    WriteHandle {
        w_handle: Some(Box::new(w_handle)),
        oplog: Vec::new(),
        swap_index: 0,
        r_handle: r_handle,
        last_epochs: Vec::new(),
        meta: m,
        first: true,
        second: false,
    }
}

impl<K, V, M, S> Drop for WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Default,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    fn drop(&mut self) {
        // eventually, the last Arc<Inner> will be dropped, and that will cause the entire read map
        // to be dropped. this will, in turn, call the destructors for the values that are there.
        // we need to make sure that we don't end up dropping some aliased values twice, and that
        // we don't forget to drop any values.
        //
        // we *could* laboriously walk through .data and .oplog to figure out exactly what
        // destructors we do or don't need to call to ensure exactly-once dropping. but, instead,
        // we'll just let refresh do the work for us.
        if !self.oplog.is_empty() {
            // applies oplog entries that are in r_handle to w_handle and applies new oplog entries
            // to w_handle, then swaps. oplog is *not* guaranteed to be empty, but *is* guaranteed
            // to only have ops that have been applied to exactly one map.
            self.refresh();
        }
        if !self.oplog.is_empty() {
            // applies oplog entries that are in (old) w_handle to (old) r_handle. this leaves
            // oplog empty, and the two data maps identical.
            self.refresh();
        }
        assert!(self.oplog.is_empty());

        // since the two maps are exactly equal, we need to make sure that we *don't* call the
        // destructors of any of the values that are in our map, as they'll all be called when the
        // last read handle goes out of scope.
        use std::mem;
        for (_, mut vs) in self.w_handle.as_mut().unwrap().data.drain() {
            for v in vs.drain(..) {
                mem::forget(v);
            }
        }
    }
}

impl<K, V, M, S> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Default,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    /// Refresh the handle used by readers so that pending writes are made visible.
    ///
    /// This method needs to wait for all readers to move to the new handle so that it can replay
    /// the operational log onto the stale map copy the readers used to use. This can take some
    /// time, especially if readers are executing slow operations, or if there are many of them.
    pub fn refresh(&mut self) {
        use std::mem;
        use std::thread;

        // we need to wait until all epochs have changed since the swaps *or* until a "finished"
        // flag has been observed to be on for two subsequent iterations (there still may be some
        // readers present since we did the previous refresh)
        //
        // NOTE: it is safe for us to hold the lock for the entire duration of the swap. we will
        // only block on pre-existing readers, and they are never waiting to push onto epochs
        // unless they have finished reading.
        let epochs = Arc::clone(&self.w_handle.as_ref().unwrap().epochs);
        let epochs = epochs.lock().unwrap();
        self.last_epochs.resize(epochs.len(), 0);

        let mut iter = 0;
        let mut starti = 0;
        let high_bit = 1usize << (mem::size_of::<usize>() * 8 - 1);
        'retry: loop {
            // read all and see if all have changed (which is likely)
            for (i, epoch) in epochs.iter().enumerate().skip(starti) {
                if self.last_epochs[i] & high_bit != 0 {
                    // reader was not active right after last swap
                    // and therefore *must* only see new pointer
                    continue;
                }

                let now = epoch.load(atomic::Ordering::Acquire);
                if (now != self.last_epochs[i]) | (now & high_bit != 0) | (now == 0) {
                    // reader must have seen last swap
                } else {
                    // reader may not have seen swap
                    // continue from this reader's epoch
                    starti = i;

                    // how eagerly should we retry?
                    if iter != 20 {
                        iter += 1;
                    } else {
                        thread::yield_now();
                    }

                    continue 'retry;
                }
            }

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
                let mut r_handle =
                    unsafe { Box::from_raw(self.r_handle.inner.load(atomic::Ordering::Relaxed)) };
                // XXX: it really is too bad that we can't just .clone() the data here and save
                // ourselves a lot of re-hashing, re-bucketization, etc.
                w_handle.data = r_handle
                    .data
                    .iter_mut()
                    .map(|(k, vs)| {
                        (
                            k.clone(),
                            vs.iter_mut().map(|v| unsafe { v.shallow_copy() }).collect(),
                        )
                    })
                    .collect();
                mem::forget(r_handle);
            }

            // the w_handle map has not seen any of the writes in the oplog
            // the r_handle map has not seen any of the writes following swap_index
            if self.swap_index != 0 {
                // we can drain out the operations that only the w_handle map needs
                //
                // NOTE: the if above is because drain(0..0) would remove 0
                //
                // NOTE: the distinction between apply_first and apply_second is the reason why our
                // use of shallow_copy is safe. we apply each op in the oplog twice, first with
                // apply_first, and then with apply_second. on apply_first, no destructors are
                // called for removed values (since those values all still exist in the other map),
                // and all new values are shallow copied in (since we need the original for the
                // other map). on apply_second, we call the destructor for anything that's been
                // removed (since those removals have already happened on the other map, and
                // without calling their destructor).
                for op in self.oplog.drain(0..self.swap_index) {
                    Self::apply_second(w_handle, op);
                }
            }
            // the rest have to be cloned because they'll also be needed by the r_handle map
            for op in self.oplog.iter_mut() {
                Self::apply_first(w_handle, op);
            }
            // the w_handle map is about to become the r_handle, and can ignore the oplog
            self.swap_index = self.oplog.len();
            // ensure meta-information is up to date
            w_handle.meta = self.meta.clone();
            w_handle.mark_ready();

            // w_handle (the old r_handle) is now fully up to date!
            break;
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
        let r_handle = self.r_handle
            .inner
            .swap(w_handle, atomic::Ordering::Release);
        let r_handle = unsafe { Box::from_raw(r_handle) };

        // ensure that the subsequent epoch reads aren't re-ordered to before the swap
        atomic::fence(atomic::Ordering::SeqCst);

        for (i, epoch) in epochs.iter().enumerate() {
            self.last_epochs[i] = epoch.load(atomic::Ordering::Acquire);
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
            Self::apply_second(inner, op);
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
    /// This will allocate an empty value-set for the key if it does not already exist.
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

    fn apply_first(inner: &mut Inner<K, V, M, S>, op: &mut Operation<K, V>) {
        use std::mem;
        match *op {
            Operation::Replace(ref key, ref mut value) => {
                let vs = inner.data.entry(key.clone()).or_insert_with(Vec::new);
                // don't run destructors yet -- still in use by other map
                for v in vs.drain(..) {
                    mem::forget(v);
                }
                vs.push(unsafe { value.shallow_copy() });
            }
            Operation::Clear(ref key) => {
                // don't run destructors yet -- still in use by other map
                for v in inner
                    .data
                    .entry(key.clone())
                    .or_insert_with(Vec::new)
                    .drain(..)
                {
                    mem::forget(v);
                }
            }
            Operation::Add(ref key, ref mut value) => {
                inner
                    .data
                    .entry(key.clone())
                    .or_insert_with(Vec::new)
                    .push(unsafe { value.shallow_copy() });
            }
            Operation::Empty(ref key) => {
                if let Some(mut vs) = inner.data.remove(key) {
                    // don't run destructors yet -- still in use by other map
                    for v in vs.drain(..) {
                        mem::forget(v);
                    }
                }
            }
            Operation::Remove(ref key, ref value) => {
                if let Some(e) = inner.data.get_mut(key) {
                    // find the first entry that matches all fields
                    if let Some(i) = e.iter().position(|v| v == value) {
                        let v = e.swap_remove(i);
                        // don't run destructor yet -- still in use by other map
                        mem::forget(v);
                    }
                }
            }
        }
    }

    fn apply_second(inner: &mut Inner<K, V, M, S>, op: Operation<K, V>) {
        match op {
            Operation::Replace(key, value) => {
                let v = inner.data.entry(key).or_insert_with(Vec::new);
                v.clear();
                v.push(value);
            }
            Operation::Clear(key) => {
                let v = inner.data.entry(key).or_insert_with(Vec::new);
                v.clear();
            }
            Operation::Add(key, value) => {
                inner.data.entry(key).or_insert_with(Vec::new).push(value);
            }
            Operation::Empty(key) => {
                inner.data.remove(&key);
            }
            Operation::Remove(key, value) => {
                if let Some(e) = inner.data.get_mut(&key) {
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
    S: BuildHasher + Default,
    V: Eq + ShallowCopy,
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
    S: BuildHasher + Default,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    type Target = ReadHandle<K, V, M, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
