use super::{Operation, Predicate, ShallowCopy};
use crate::inner::Inner;
use crate::read::ReadHandle;
use crate::values::Values;

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::mem::ManuallyDrop;
use std::sync::atomic;
use std::sync::{Arc, MutexGuard};
use std::{fmt, mem, thread};

#[cfg(feature = "indexed")]
use indexmap::map::Entry;
#[cfg(not(feature = "indexed"))]
use std::collections::hash_map::Entry;

/// A handle that may be used to modify the eventually consistent map.
///
/// Note that any changes made to the map will not be made visible to readers until `refresh()` is
/// called.
///
/// When the `WriteHandle` is dropped, the map is immediately (but safely) taken away from all
/// readers, causing all future lookups to return `None`.
///
/// # Examples
/// ```
/// let x = ('x', 42);
///
/// let (r, mut w) = evmap::new();
///
/// // the map is uninitialized, so all lookups should return None
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
///
/// w.refresh();
///
/// // after the first refresh, it is empty, but ready
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
///
/// w.insert(x.0, x);
///
/// // it is empty even after an add (we haven't refresh yet)
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), None);
///
/// w.refresh();
///
/// // but after the swap, the record is there!
/// assert_eq!(r.get(&x.0).map(|rs| rs.len()), Some(1));
/// assert_eq!(r.get(&x.0).map(|rs| rs.iter().any(|v| v.0 == x.0 && v.1 == x.1)), Some(true));
/// ```
pub struct WriteHandle<K, V, M = (), S = RandomState>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Hash + ShallowCopy,
    M: 'static + Clone,
{
    epochs: crate::Epochs,
    w_handle: Option<Box<Inner<K, ManuallyDrop<V>, M, S>>>,
    oplog: Vec<Operation<K, V>>,
    swap_index: usize,
    r_handle: ReadHandle<K, V, M, S>,
    last_epochs: Vec<usize>,
    meta: M,
    first: bool,
    second: bool,
}

impl<K, V, M, S> fmt::Debug for WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone + fmt::Debug,
    S: BuildHasher + Clone,
    V: Eq + Hash + ShallowCopy + fmt::Debug,
    M: 'static + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("epochs", &self.epochs)
            .field("w_handle", &self.w_handle)
            .field("oplog", &self.oplog)
            .field("swap_index", &self.swap_index)
            .field("r_handle", &self.r_handle)
            .field("meta", &self.meta)
            .field("first", &self.first)
            .field("second", &self.second)
            .finish()
    }
}

pub(crate) fn new<K, V, M, S>(
    w_handle: Inner<K, ManuallyDrop<V>, M, S>,
    epochs: crate::Epochs,
    r_handle: ReadHandle<K, V, M, S>,
) -> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Hash + ShallowCopy,
    M: 'static + Clone,
{
    let m = w_handle.meta.clone();
    WriteHandle {
        epochs,
        w_handle: Some(Box::new(w_handle)),
        oplog: Vec::new(),
        swap_index: 0,
        r_handle,
        last_epochs: Vec::new(),
        meta: m,
        first: true,
        second: false,
    }
}

impl<K, V, M, S> Drop for WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Hash + ShallowCopy,
    M: 'static + Clone,
{
    fn drop(&mut self) {
        use std::ptr;

        // first, ensure both maps are up to date
        // (otherwise safely dropping deduplicated rows is a pain)
        if !self.oplog.is_empty() {
            self.refresh();
        }
        if !self.oplog.is_empty() {
            self.refresh();
        }
        assert!(self.oplog.is_empty());

        // next, grab the read handle and set it to NULL
        let r_handle = self
            .r_handle
            .inner
            .swap(ptr::null_mut(), atomic::Ordering::Release);

        // now, wait for all readers to depart
        let epochs = Arc::clone(&self.epochs);
        let mut epochs = epochs.lock().unwrap();
        self.wait(&mut epochs);

        // ensure that the subsequent epoch reads aren't re-ordered to before the swap
        atomic::fence(atomic::Ordering::SeqCst);

        let w_handle = &mut self.w_handle.as_mut().unwrap().data;

        // all readers have now observed the NULL, so we own both handles.
        // all records are duplicated between w_handle and r_handle.
        // since the two maps are exactly equal, we need to make sure that we *don't* call the
        // destructors of any of the values that are in our map, as they'll all be called when the
        // last read handle goes out of scope. to do so, we first clear w_handle, which won't drop
        // any elements since its values are kept as ManuallyDrop:
        w_handle.clear();

        // then we transmute r_handle to remove the ManuallyDrop, and then drop it, which will free
        // all the records. this is safe, since we know that no readers are using this pointer
        // anymore (due to the .wait() following swapping the pointer with NULL).
        drop(unsafe { Box::from_raw(r_handle as *mut Inner<K, V, M, S>) });
    }
}

impl<K, V, M, S> WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Hash + ShallowCopy,
    M: 'static + Clone,
{
    fn wait(&mut self, epochs: &mut MutexGuard<'_, slab::Slab<Arc<atomic::AtomicUsize>>>) {
        let mut iter = 0;
        let mut starti = 0;
        let high_bit = 1usize << (mem::size_of::<usize>() * 8 - 1);
        // we're over-estimating here, but slab doesn't expose its max index
        self.last_epochs.resize(epochs.capacity(), 0);
        'retry: loop {
            // read all and see if all have changed (which is likely)
            for (ii, (ri, epoch)) in epochs.iter().enumerate().skip(starti) {
                // note that `ri` _may_ have been re-used since we last read into last_epochs.
                // this is okay though, as a change still implies that the new reader must have
                // arrived _after_ we did the atomic swap, and thus must also have seen the new
                // pointer.
                if self.last_epochs[ri] & high_bit != 0 {
                    // reader was not active right after last swap
                    // and therefore *must* only see new pointer
                    continue;
                }

                let now = epoch.load(atomic::Ordering::Acquire);
                if (now != self.last_epochs[ri]) | (now & high_bit != 0) | (now == 0) {
                    // reader must have seen last swap
                } else {
                    // reader may not have seen swap
                    // continue from this reader's epoch
                    starti = ii;

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
        let epochs = Arc::clone(&self.epochs);
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
                w_handle.data.extend(r_handle.data.iter().map(|(k, vs)| {
                    (
                        k.clone(),
                        Values::from_iter(
                            vs.iter().map(|v| unsafe { (&**v).shallow_copy() }),
                            r_handle.data.hasher(),
                        ),
                    )
                }));
            }

            // safety: we will not swap while we hold this reference
            let r_hasher = unsafe { self.r_handle.hasher() };

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
                    // because we are applying second, we _do_ want to perform drops
                    Self::apply_second(unsafe { w_handle.do_drop() }, op, r_hasher);
                }
            }
            // the rest have to be cloned because they'll also be needed by the r_handle map
            for op in self.oplog.iter_mut() {
                Self::apply_first(w_handle, op, r_hasher);
            }
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

        for (ri, epoch) in epochs.iter() {
            self.last_epochs[ri] = epoch.load(atomic::Ordering::Acquire);
        }

        // NOTE: at this point, there are likely still readers using the w_handle we got
        self.w_handle = Some(r_handle);
        self.second = self.first;
        self.first = false;

        self
    }

    /// Gives the sequence of operations that have not yet been applied.
    ///
    /// Note that until the *first* call to `refresh`, the sequence of operations is always empty.
    ///
    /// ```
    /// # use evmap::Operation;
    /// let x = ('x', 42);
    ///
    /// let (r, mut w) = evmap::new();
    ///
    /// // before the first refresh, no oplog is kept
    /// w.refresh();
    ///
    /// assert_eq!(w.pending(), &[]);
    /// w.insert(x.0, x);
    /// assert_eq!(w.pending(), &[Operation::Add(x.0, x)]);
    /// w.refresh();
    /// w.remove(x.0, x);
    /// assert_eq!(w.pending(), &[Operation::Remove(x.0, x)]);
    /// w.refresh();
    /// assert_eq!(w.pending(), &[]);
    /// ```
    pub fn pending(&self) -> &[Operation<K, V>] {
        &self.oplog[self.swap_index..]
    }

    /// Refresh as necessary to ensure that all operations are visible to readers.
    ///
    /// `WriteHandle::refresh` will *always* wait for old readers to depart and swap the maps.
    /// This method will only do so if there are pending operations.
    pub fn flush(&mut self) -> &mut Self {
        if !self.pending().is_empty() {
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
            self.oplog.push(op);
        } else {
            // we know there are no outstanding w_handle readers, so we can modify it directly!
            let w_inner = self.w_handle.as_mut().unwrap();
            let r_hasher = unsafe { self.r_handle.hasher() };
            // because we are applying second, we _do_ want to perform drops
            Self::apply_second(unsafe { w_inner.do_drop() }, op, r_hasher);
            // NOTE: since we didn't record this in the oplog, r_handle *must* clone w_handle
        }

        self
    }

    /// Add the given value to the value-bag of the given key.
    ///
    /// The updated value-bag will only be visible to readers after the next call to `refresh()`.
    pub fn insert(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::Add(k, v))
    }

    /// Replace the value-bag of the given key with the given value.
    ///
    /// Replacing the value will automatically deallocate any heap storage and place the new value
    /// back into the `SmallVec` inline storage. This can improve cache locality for common
    /// cases where the value-bag is only ever a single element.
    ///
    /// See [the doc section on this](./index.html#small-vector-optimization) for more information.
    ///
    /// The new value will only be visible to readers after the next call to `refresh()`.
    pub fn update(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::Replace(k, v))
    }

    /// Clear the value-bag of the given key, without removing it.
    ///
    /// If a value-bag already exists, this will clear it but leave the
    /// allocated memory intact for reuse, or if no associated value-bag exists
    /// an empty value-bag will be created for the given key.
    ///
    /// The new value will only be visible to readers after the next call to `refresh()`.
    pub fn clear(&mut self, k: K) -> &mut Self {
        self.add_op(Operation::Clear(k))
    }

    /// Remove the given value from the value-bag of the given key.
    ///
    /// The updated value-bag will only be visible to readers after the next call to `refresh()`.
    pub fn remove(&mut self, k: K, v: V) -> &mut Self {
        self.add_op(Operation::Remove(k, v))
    }

    /// Remove the value-bag for the given key.
    ///
    /// The value-bag will only disappear from readers after the next call to `refresh()`.
    pub fn empty(&mut self, k: K) -> &mut Self {
        self.add_op(Operation::Empty(k))
    }

    /// Purge all value-bags from the map.
    ///
    /// The map will only appear empty to readers after the next call to `refresh()`.
    ///
    /// Note that this will iterate once over all the keys internally.
    pub fn purge(&mut self) -> &mut Self {
        self.add_op(Operation::Purge)
    }

    /// Retain elements for the given key using the provided predicate function.
    ///
    /// The remaining value-bag will only be visible to readers after the next call to `refresh()`
    ///
    /// # Safety
    ///
    /// The given closure is called _twice_ for each element, once when called, and once
    /// on swap. It _must_ retain the same elements each time, otherwise a value may exist in one
    /// map, but not the other, leaving the two maps permanently out-of-sync. This is _really_ bad,
    /// as values are aliased between the maps, and are assumed safe to free when they leave the
    /// map during a `refresh`. Returning `true` when `retain` is first called for a value, and
    /// `false` the second time would free the value, but leave an aliased pointer to it in the
    /// other side of the map.
    ///
    /// The arguments to the predicate function are the current value in the value-bag, and `true`
    /// if this is the first value in the value-bag on the second map, or `false` otherwise. Use
    /// the second argument to know when to reset any closure-local state to ensure deterministic
    /// operation.
    ///
    /// So, stated plainly, the given closure _must_ return the same order of true/false for each
    /// of the two iterations over the value-bag. That is, the sequence of returned booleans before
    /// the second argument is true must be exactly equal to the sequence of returned booleans
    /// at and beyond when the second argument is true.
    pub unsafe fn retain<F>(&mut self, k: K, f: F) -> &mut Self
    where
        F: FnMut(&V, bool) -> bool + 'static + Send,
    {
        self.add_op(Operation::Retain(k, Predicate(Box::new(f))))
    }

    /// Shrinks a value-bag to it's minimum necessary size, freeing memory
    /// and potentially improving cache locality by switching to inline storage.
    ///
    /// The optimized value-bag will only be visible to readers after the next call to `refresh()`
    pub fn fit(&mut self, k: K) -> &mut Self {
        self.add_op(Operation::Fit(Some(k)))
    }

    /// Like [`WriteHandle::fit`](#method.fit), but shrinks <b>all</b> value-bags in the map.
    ///
    /// The optimized value-bags will only be visible to readers after the next call to `refresh()`
    pub fn fit_all(&mut self) -> &mut Self {
        self.add_op(Operation::Fit(None))
    }

    /// Reserves capacity for some number of additional elements in a value-bag,
    /// or creates an empty value-bag for this key with the given capacity if
    /// it doesn't already exist.
    ///
    /// Readers are unaffected by this operation, but it can improve performance
    /// by pre-allocating space for large value-bags.
    pub fn reserve(&mut self, k: K, additional: usize) -> &mut Self {
        self.add_op(Operation::Reserve(k, additional))
    }

    #[cfg(feature = "eviction")]
    /// Remove the value-bag for `n` randomly chosen keys.
    ///
    /// This method immediately calls `refresh()` to ensure that the keys and values it returns
    /// match the elements that will be emptied on the next call to `refresh()`. The value-bags
    /// will only disappear from readers after the next call to `refresh()`.
    pub fn empty_random<'a>(
        &'a mut self,
        rng: &mut impl rand::Rng,
        n: usize,
    ) -> impl ExactSizeIterator<Item = (&'a K, &'a Values<V, S>)> {
        // force a refresh so that our view into self.r_handle matches the indices we choose.
        // if we didn't do this, the `i`th element of r_handle may be a completely different
        // element than the one that _will_ be evicted when `EmptyAt([.. i ..])` is applied.
        // this would be bad since we are telling the caller which elements we are evicting!
        // note also that we _must_ use `r_handle`, not `w_handle`, since `w_handle` may have
        // pending operations even after a refresh!
        self.refresh();

        let inner = self.r_handle.inner.load(atomic::Ordering::SeqCst);
        let inner: &'a _ = unsafe { &(*inner).data };

        // let's pick some (distinct) indices to evict!
        let n = n.min(inner.len());
        let indices = rand::seq::index::sample(rng, inner.len(), n);

        // we need to sort the indices so that, later, we can make sure to swap remove from last to
        // first (and so not accidentally remove the wrong index).
        let mut to_remove = indices.clone().into_vec();
        to_remove.sort();
        self.add_op(Operation::EmptyAt(to_remove));

        indices.into_iter().map(move |i| {
            let (k, vs) = inner.get_index(i).expect("in-range");
            (k, vs.user_friendly())
        })
    }

    /// Apply ops in such a way that no values are dropped, only forgotten
    fn apply_first(
        inner: &mut Inner<K, ManuallyDrop<V>, M, S>,
        op: &mut Operation<K, V>,
        hasher: &S,
    ) {
        match *op {
            Operation::Replace(ref key, ref mut value) => {
                let vs = inner.data.entry(key.clone()).or_insert_with(Values::new);

                // truncate vector
                vs.clear();

                // implicit shrink_to_fit on replace op
                // so it will switch back to inline allocation for the subsequent push.
                vs.shrink_to_fit();

                vs.push(unsafe { value.shallow_copy() }, hasher);
            }
            Operation::Clear(ref key) => {
                inner
                    .data
                    .entry(key.clone())
                    .or_insert_with(Values::new)
                    .clear();
            }
            Operation::Add(ref key, ref mut value) => {
                inner
                    .data
                    .entry(key.clone())
                    .or_insert_with(Values::new)
                    .push(unsafe { value.shallow_copy() }, hasher);
            }
            Operation::Empty(ref key) => {
                #[cfg(not(feature = "indexed"))]
                inner.data.remove(key);
                #[cfg(feature = "indexed")]
                inner.data.swap_remove(key);
            }
            Operation::Purge => {
                inner.data.clear();
            }
            #[cfg(feature = "eviction")]
            Operation::EmptyAt(ref indices) => {
                for &index in indices.iter().rev() {
                    inner.data.swap_remove_index(index);
                }
            }
            Operation::Remove(ref key, ref value) => {
                if let Some(e) = inner.data.get_mut(key) {
                    // remove a matching value from the value set
                    // safety: this is fine
                    e.swap_remove(unsafe { &*(value as *const _ as *const ManuallyDrop<V>) });
                }
            }
            Operation::Retain(ref key, ref mut predicate) => {
                if let Some(e) = inner.data.get_mut(key) {
                    let mut first = true;
                    e.retain(move |v| {
                        let retain = predicate.eval(v, first);
                        first = false;
                        retain
                    });
                }
            }
            Operation::Fit(ref key) => match key {
                Some(ref key) => {
                    if let Some(e) = inner.data.get_mut(key) {
                        e.shrink_to_fit();
                    }
                }
                None => {
                    for value_set in inner.data.values_mut() {
                        value_set.shrink_to_fit();
                    }
                }
            },
            Operation::Reserve(ref key, additional) => match inner.data.entry(key.clone()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().reserve(additional, hasher);
                }
                Entry::Vacant(entry) => {
                    entry.insert(Values::with_capacity_and_hasher(additional, hasher));
                }
            },
        }
    }

    /// Apply operations while allowing dropping of values
    fn apply_second(inner: &mut Inner<K, V, M, S>, op: Operation<K, V>, hasher: &S) {
        match op {
            Operation::Replace(key, value) => {
                let v = inner.data.entry(key).or_insert_with(Values::new);

                // we are going second, so we should drop!
                v.clear();

                v.shrink_to_fit();

                v.push(value, hasher);
            }
            Operation::Clear(key) => {
                inner.data.entry(key).or_insert_with(Values::new).clear();
            }
            Operation::Add(key, value) => {
                inner
                    .data
                    .entry(key)
                    .or_insert_with(Values::new)
                    .push(value, hasher);
            }
            Operation::Empty(key) => {
                #[cfg(not(feature = "indexed"))]
                inner.data.remove(&key);
                #[cfg(feature = "indexed")]
                inner.data.swap_remove(&key);
            }
            Operation::Purge => {
                inner.data.clear();
            }
            #[cfg(feature = "eviction")]
            Operation::EmptyAt(indices) => {
                for &index in indices.iter().rev() {
                    inner.data.swap_remove_index(index);
                }
            }
            Operation::Remove(key, value) => {
                if let Some(e) = inner.data.get_mut(&key) {
                    // find the first entry that matches all fields
                    e.swap_remove(&value);
                }
            }
            Operation::Retain(key, mut predicate) => {
                if let Some(e) = inner.data.get_mut(&key) {
                    let mut first = true;
                    e.retain(move |v| {
                        let retain = predicate.eval(v, first);
                        first = false;
                        retain
                    });
                }
            }
            Operation::Fit(key) => match key {
                Some(ref key) => {
                    if let Some(e) = inner.data.get_mut(key) {
                        e.shrink_to_fit();
                    }
                }
                None => {
                    for value_set in inner.data.values_mut() {
                        value_set.shrink_to_fit();
                    }
                }
            },
            Operation::Reserve(key, additional) => match inner.data.entry(key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().reserve(additional, hasher);
                }
                Entry::Vacant(entry) => {
                    entry.insert(Values::with_capacity_and_hasher(additional, hasher));
                }
            },
        }
    }
}

impl<K, V, M, S> Extend<(K, V)> for WriteHandle<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    V: Eq + Hash + ShallowCopy,
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
    V: Eq + Hash + ShallowCopy,
    M: 'static + Clone,
{
    type Target = ReadHandle<K, V, M, S>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
