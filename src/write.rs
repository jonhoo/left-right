use super::ShallowCopy;
use inner::Inner;
use op::{MapOperation, MarkedOperation, Operation, Predicate, ValueOperation};
use read::ReadHandle;

use std::borrow::Cow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic;
use std::sync::{Arc, MutexGuard};
use std::{mem, ptr, thread};

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
    S: BuildHasher + Clone,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    w_handle: Option<Box<Inner<K, V, M, S>>>,
    oplog: Vec<MarkedOperation<K, V>>,
    swap_index: usize,
    r_handle: ReadHandle<K, V, M, S>,
    last_epochs: Vec<usize>,
    meta: M,
    first: bool,
    second: bool,

    drop_dont_refresh: bool,
}

pub(crate) fn new<K, V, M, S>(
    w_handle: Inner<K, V, M, S>,
    r_handle: ReadHandle<K, V, M, S>,
) -> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
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

        drop_dont_refresh: false,
    }
}

impl<K, V, M, S> Drop for WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    fn drop(&mut self) {
        if !self.drop_dont_refresh {
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
        }
        assert!(self.oplog.is_empty());

        // since the two maps are exactly equal, we need to make sure that we *don't* call the
        // destructors of any of the values that are in our map, as they'll all be called when the
        // last read handle goes out of scope.
        for (_, mut vs) in self.w_handle.as_mut().unwrap().data.drain() {
            unsafe {
                // truncate value-set without dropping values
                vs.set_len(0);
            }
        }
    }
}

impl<K, V, M, S> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    fn wait(&mut self, epochs: &mut MutexGuard<Vec<Arc<atomic::AtomicUsize>>>) {
        let mut iter = 0;
        let mut starti = 0;
        let high_bit = 1usize << (mem::size_of::<usize>() * 8 - 1);
        self.last_epochs.resize(epochs.len(), 0);
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
            break;
        }
    }

    /// Refresh the handle used by readers so that pending writes are made visible.
    ///
    /// This method needs to wait for all readers to move to the new handle so that it can replay
    /// the operational log onto the stale map copy the readers used to use. This can take some
    /// time, especially if readers are executing slow operations, or if there are many of them.
    pub fn refresh(&mut self) -> &mut Self {
        // we need to wait until all epochs have changed since the swaps *or* until a "finished"
        // flag has been observed to be on for two subsequent iterations (there still may be some
        // readers present since we did the previous refresh)
        //
        // NOTE: it is safe for us to hold the lock for the entire duration of the swap. we will
        // only block on pre-existing readers, and they are never waiting to push onto epochs
        // unless they have finished reading.
        let epochs = Arc::clone(&self.w_handle.as_ref().unwrap().epochs);

        let mut epochs = epochs.lock().unwrap();

        self.wait(&mut epochs);

        {
            // all the readers have left!
            // we can safely bring the w_handle up to date.
            let w_handle = self.w_handle.as_mut().unwrap();

            if self.second {
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
                    self.r_handle
                        .inner
                        .load(atomic::Ordering::Relaxed)
                        .as_mut()
                        .unwrap()
                };

                // XXX: it really is too bad that we can't just .clone() the data here and save
                // ourselves a lot of re-hashing, re-bucketization, etc.
                w_handle
                    .data
                    .extend(r_handle.data.iter_mut().map(|(k, vs)| {
                        (
                            k.clone(),
                            vs.iter_mut().map(|v| unsafe { v.shallow_copy() }).collect(),
                        )
                    }));
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
                Self::apply_second(w_handle, &self.oplog[..self.swap_index]);

                debug_assert_eq!(
                    self.oplog
                        .iter()
                        .take(self.swap_index)
                        .filter(|op| !op.consumed())
                        .count(),
                    0
                );

                self.oplog.drain(..self.swap_index);
            }

            // the rest have to be shallow-cloned because
            // they'll also be needed by the r_handle map
            Self::apply_first(w_handle, &self.oplog);

            // the w_handle map is about to become the r_handle, and can ignore the oplog
            self.swap_index = self.oplog.len();

            // ensure meta-information is up to date
            w_handle.meta = self.meta.clone();
            w_handle.mark_ready();

            // w_handle (the old r_handle) is now fully up to date!
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
        let r_handle = self
            .r_handle
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

        self
    }

    /// Drop this map without preserving one side for reads.
    ///
    /// Normally, the maps are not deallocated until all readers have also gone away.
    /// When this method is called, the map is immediately (but safely) taken away from all
    /// readers, causing all future lookups to return `None`.
    ///
    /// ```
    /// use std::thread;
    /// let (r, mut w) = evmap::new();
    ///
    /// // start some readers
    /// let readers: Vec<_> = (0..4).map(|_| {
    ///     let r = r.clone();
    ///     thread::spawn(move || {
    ///         loop {
    ///             let l = r.len();
    ///             if l == 0 {
    ///                 if r.is_destroyed() {
    ///                     // the writer destroyed the map!
    ///                     break;
    ///                 }
    ///                 thread::yield_now();
    ///             } else {
    ///                 // the reader will either see all the reviews,
    ///                 // or none of them, since refresh() is atomic.
    ///                 assert_eq!(l, 4);
    ///             }
    ///         }
    ///     })
    /// }).collect();
    ///
    /// // do some writes
    /// w.insert(0, String::from("foo"))
    ///  .insert(1, String::from("bar"))
    ///  .insert(2, String::from("baz"))
    ///  .insert(3, String::from("qux"));
    ///
    /// // expose the writes
    /// w.refresh();
    /// assert_eq!(r.len(), 4);
    ///
    /// // refresh a few times to exercise the readers
    /// for _ in 0..10 {
    ///     w.refresh();
    /// }
    ///
    /// // now blow the map away
    /// w.destroy();
    ///
    /// // all the threads should eventually see that the map was destroyed
    /// for r in readers.into_iter() {
    ///     assert!(r.join().is_ok());
    /// }
    /// ```
    pub fn destroy(mut self) {
        use std::ptr;

        // first, ensure both maps are up to date
        // (otherwise safely dropping deduplicated rows is a pain)
        // see also impl Drop
        self.refresh();
        self.refresh();

        // next, grab the read handle and set it to NULL
        let r_handle = self
            .r_handle
            .inner
            .swap(ptr::null_mut(), atomic::Ordering::Release);
        let r_handle = unsafe { Box::from_raw(r_handle) };

        // now, wait for all readers to depart
        let epochs = Arc::clone(&self.w_handle.as_ref().unwrap().epochs);

        let mut epochs = epochs.lock().unwrap();

        self.wait(&mut epochs);

        // ensure that the subsequent epoch reads aren't re-ordered to before the swap
        atomic::fence(atomic::Ordering::SeqCst);

        // all readers have now observed the NULL, so we own both handles.
        // all records are duplicated between w_handle and r_handle.
        // we should *only* call the destructor for each record once!
        // first, we drop self, which will forget all the records in w_handle
        self.drop_dont_refresh = true;
        drop(self);
        // then we drop r_handle, which will free all the records
        drop(r_handle);
    }

    /// Gives the sequence of operations that have not yet been applied.
    ///
    /// Note that until the *first* call to `refresh`, the sequence of operations is always empty.
    ///
    /// ```
    /// # use evmap::{Operation, op::ValueOperation};
    /// let x = ('x', 42);
    ///
    /// let (r, mut w) = evmap::new();
    ///
    /// // before the first refresh, no oplog is kept
    /// w.refresh();
    ///
    /// assert_eq!(w.pending().count(), 0);
    ///
    /// w.insert(x.0, x);
    /// assert_eq!(
    ///     w.pending().next(),
    ///     Some(&Operation::Value {
    ///         key: x.0,
    ///         op: ValueOperation::Add(x)
    ///     })
    /// );
    ///
    /// w.refresh();
    ///
    /// w.remove(x.0, x);
    /// assert_eq!(
    ///     w.pending().next(),
    ///     Some(&Operation::Value {
    ///         key: x.0,
    ///         op: ValueOperation::Remove(x)
    ///     })
    /// );
    ///
    /// w.refresh();
    /// assert_eq!(w.pending().count(), 0);
    /// ```
    pub fn pending(&self) -> impl Iterator<Item = &Operation<K, V>> {
        self.oplog.iter().filter_map(|marked_op| marked_op.as_ref())
    }

    /// Refresh as necessary to ensure that all operations are visible to readers.
    ///
    /// `WriteHandle::refresh` will *always* wait for old readers to depart and swap the maps.
    /// This method will only do so if there are pending operations.
    pub fn flush(&mut self) -> &mut Self {
        if !self.oplog.is_empty() {
            self.refresh();
        }

        self
    }

    /// Set the metadata.
    ///
    /// Will only be visible to readers after the next call to `refresh()`.
    pub fn set_meta(&mut self, mut meta: M) -> M {
        mem::swap(&mut self.meta, &mut meta);
        meta
    }

    fn add_op(&mut self, op: Operation<K, V>) -> &mut Self {
        if !self.first {
            self.oplog.push(MarkedOperation::new(op));
        } else {
            // we know there are no outstanding w_handle readers, so we can modify it directly!
            let inner = self.w_handle.as_mut().unwrap();

            match op {
                Operation::Map { op } => op.apply_second(inner),
                Operation::Value { key, op } => {
                    let raw_entry = inner.data.raw_entry_mut().from_key(&key);

                    op.apply_second(Cow::Owned(key), raw_entry);
                }
            }

            // NOTE: since we didn't record this in the oplog, r_handle *must* clone w_handle
        }

        self
    }

    #[inline(always)]
    fn add_value_op(&mut self, key: K, op: ValueOperation<V>) -> &mut Self {
        self.add_op(Operation::Value { key, op })
    }

    /// Add the given value to the value-set of the given key.
    ///
    /// The updated value-set will only be visible to readers after the next call to `refresh()`.
    pub fn insert(&mut self, k: K, v: V) -> &mut Self {
        self.add_value_op(k, ValueOperation::Add(v))
    }

    /// Replace the value-set of the given key with the given value.
    ///
    /// Replacing the value will automatically deallocate any heap storage
    /// and place the new value back into the `SmallVec` inline storage.
    /// This can improve cache locality for common cases where the
    /// value-set is only ever a single element.
    ///
    /// See [the doc section on this](./index.html#small-vector-optimization) for more information.
    ///
    /// The new value will only be visible to readers after the next call to `refresh()`.
    pub fn update(&mut self, k: K, v: V) -> &mut Self {
        self.add_value_op(k, ValueOperation::Replace(v))
    }

    /// Clear the value-set of the given key, without removing it.
    ///
    /// If a value-set already exists, this will clear it but leave the
    /// allocated memory intact for reuse, or if no associated value-set exists
    /// an empty value-set will be created for the given key.
    ///
    /// The new value will only be visible to readers after the next call to `refresh()`.
    pub fn clear(&mut self, k: K) -> &mut Self {
        self.add_value_op(k, ValueOperation::Clear)
    }

    /// Remove the given value from the value-set of the given key, by swapping it with the last
    /// value and popping it off the value-set. This does not preserve element order in the value-set.
    ///
    /// To preserve order when removing, consider using `remove_stable`.
    ///
    /// The updated value-set will only be visible to readers after the next call to `refresh()`.
    pub fn remove(&mut self, k: K, value: V) -> &mut Self {
        self.add_value_op(k, ValueOperation::Remove(value))
    }

    /// Remove the given value from the value-set of the given key, preserving the order
    /// of elements in the value-set.
    ///
    /// The updated value-set will only be visible to readers after the next call to `refresh()`.
    #[inline]
    pub fn remove_stable(&mut self, k: K, value: V) -> &mut Self {
        self.add_value_op(k, ValueOperation::RemoveStable(value))
    }

    /// Remove the value-set for the given key.
    ///
    /// The value-set will only disappear from readers after the next call to `refresh()`.
    pub fn empty(&mut self, k: K) -> &mut Self {
        self.add_value_op(k, ValueOperation::Empty)
    }

    /// Retain elements for the given key using the provided predicate function.
    ///
    /// The remaining value-set will only be visible to readers after the next call to `refresh()`
    pub fn retain<F>(&mut self, k: K, f: F) -> &mut Self
    where
        F: Fn(&V) -> bool + 'static + Send + Sync,
    {
        self.add_value_op(k, ValueOperation::Retain(Predicate(Arc::new(f))))
    }

    /// Shrinks a value-set to it's minimum necessary size, freeing memory
    /// and potentially improving cache locality.
    ///
    /// The optimized value-set will only be visible to readers after the next call to `refresh()`
    pub fn fit(&mut self, k: K) -> &mut Self {
        self.add_value_op(k, ValueOperation::Fit)
    }

    /// Like [`WriteHandle::fit`](#method.fit), but shrinks <b>all</b> value-sets in the map.
    ///
    /// The optimized value-sets will only be visible to readers after the next call to `refresh()`
    pub fn fit_all(&mut self) -> &mut Self {
        self.add_op(Operation::Map {
            op: MapOperation::FitAll,
        })
    }

    /// Reserves capacity for some number of additional elements in a value-set,
    /// or creates an empty value-set for this key with the given capacity if
    /// it doesn't already exist.
    ///
    /// Readers are unaffected by this operation, but it can improve performance
    /// by pre-allocating space for large value-sets.
    pub fn reserve(&mut self, k: K, additional: usize) -> &mut Self {
        self.add_value_op(k, ValueOperation::Reserve(additional))
    }

    /// Apply ops in such a way that no values are dropped, only forgotten
    fn apply_first(inner: &mut Inner<K, V, M, S>, ops: &[MarkedOperation<K, V>]) {
        for (i, op) in ops.iter().enumerate() {
            match op.take_first() {
                None => {}
                Some(Operation::Map { ref mut op }) => op.apply_first(inner),
                Some(Operation::Value {
                    ref key,
                    ref mut op,
                }) => {
                    // cache the key hash
                    let hash = {
                        let mut hasher = inner.data.hasher().build_hasher();

                        key.hash(&mut hasher);

                        hasher.finish()
                    };

                    // apply the current operation
                    {
                        let raw_entry = inner.data.raw_entry_mut();

                        op.apply_first(
                            Cow::Borrowed(key),
                            raw_entry.from_key_hashed_nocheck(hash, key),
                        );
                    }

                    'lookahead: for next_marked_op in &ops[i + 1..] {
                        match next_marked_op.as_mut() {
                            Some(Operation::Map { .. }) => break 'lookahead,
                            Some(Operation::Value {
                                key: ref next_key,
                                op: ref mut next_op,
                            }) if next_key == key => {
                                next_marked_op.mark_first();

                                let raw_entry = inner.data.raw_entry_mut();

                                next_op.apply_first(
                                    Cow::Borrowed(key),
                                    raw_entry.from_key_hashed_nocheck(hash, key),
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    /// Apply operations while allowing dropping of values
    fn apply_second(inner: &mut Inner<K, V, M, S>, ops: &[MarkedOperation<K, V>]) {
        for (i, op) in ops.iter().enumerate() {
            match op.take_second() {
                None => {}

                // Apply whole-map operations now
                Some(Operation::Map { op }) => op.apply_second(inner),

                // If the op was a value operation
                Some(Operation::Value { key, op }) => {
                    // cache the key hash
                    let hash = {
                        let mut hasher = inner.data.hasher().build_hasher();

                        key.hash(&mut hasher);

                        hasher.finish()
                    };

                    // apply the current operation
                    {
                        let raw_entry = inner.data.raw_entry_mut();

                        op.apply_second(
                            Cow::Borrowed(&key),
                            raw_entry.from_key_hashed_nocheck(hash, &key),
                        );
                    }

                    // from here, look ahead for value operations
                    // with the same key.
                    //
                    // This done in such a way as to avoid repeated matching,
                    // by using unsafe pointers and references
                    'lookahead: for next_marked_op in &ops[i + 1..] {
                        match next_marked_op.as_mut() {
                            Some(Operation::Map { .. }) => break 'lookahead,
                            Some(Operation::Value {
                                key: ref next_key,
                                op: ref mut next_op,
                            }) if next_key == &key => {
                                // mark this op as being consumed
                                next_marked_op.mark_second();

                                // read the value from the reference we have
                                let op = unsafe { ptr::read(next_op) };

                                let raw_entry = inner.data.raw_entry_mut();

                                op.apply_second(
                                    Cow::Borrowed(&key),
                                    raw_entry.from_key_hashed_nocheck(hash, &key),
                                );
                            }
                            _ => {}
                        }
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
    S: BuildHasher + Clone,
    V: Eq + ShallowCopy,
    M: 'static + Clone,
{
    type Target = ReadHandle<K, V, M, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
