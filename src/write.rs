use crate::epochs::{Epoch, Epochs};
use crate::read::ReadHandle;
use crate::sync::{fence, Arc, Ordering};
use crate::Absorb;
use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;
use core::ops::DerefMut;
use core::ptr::NonNull;

#[cfg(test)]
use core::sync::atomic::AtomicBool;

/// A writer handle to a left-right guarded data structure.
///
/// All operations on the underlying data should be enqueued as operations of type `O` using
/// [`append`](Self::append). The effect of this operations are only exposed to readers once
/// [`publish`](Self::publish) is called.
///
/// # Reading through a `WriteHandle`
///
/// `WriteHandle` allows access to a [`ReadHandle`] through `Deref<Target = ReadHandle>`. Note that
/// since the reads go through a [`ReadHandle`], those reads are subject to the same visibility
/// restrictions as reads that do not go through the `WriteHandle`: they only see the effects of
/// operations prior to the last call to [`publish`](Self::publish).
pub struct WriteHandle<T, O>
where
    T: Absorb<O>,
{
    epochs: Epochs,
    w_handle: NonNull<T>,
    oplog: VecDeque<O>,
    swap_index: usize,
    r_handle: ReadHandle<T>,
    epochs_and_last: Vec<LastEpoch>,
    #[cfg(test)]
    refreshes: usize,
    #[cfg(test)]
    is_waiting: Arc<AtomicBool>,
    /// Write directly to the write handle map, since no publish has happened.
    first: bool,
    /// A publish has happened, but the two copies have not been synchronized yet.
    second: bool,
    /// If we call `Self::take` the drop needs to be different.
    taken: bool,
    thread_yield: fn(),
}

/// [`Epoch`] shared with corresponding [`ReadHandle`].
///
/// [`ReadHandle`] just drops their copy of the [`Epoch`] and [`WriteHandle`] removes this entry on
/// [`Arc::strong_count`]` == 1`.
struct LastEpoch {
    /// [`Epoch`] copy shared with corresponding [`ReadHandle`].
    epoch: Epoch,

    /// Last seen value of [`Epoch`] after the swap.
    last: usize,
}

// safety: if a `WriteHandle` is sent across a thread boundary, we need to be able to take
// ownership of both Ts and Os across that thread boundary. since `WriteHandle` holds a
// `ReadHandle`, we also need to respect its Send requirements.
unsafe impl<T, O> Send for WriteHandle<T, O>
where
    T: Absorb<O>,
    T: Send,
    O: Send,
    ReadHandle<T>: Send,
{
}

impl<T, O> fmt::Debug for WriteHandle<T, O>
where
    T: Absorb<O> + fmt::Debug,
    O: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("epochs", &self.epochs)
            .field("w_handle", &self.w_handle)
            .field("oplog", &self.oplog)
            .field("swap_index", &self.swap_index)
            .field("r_handle", &self.r_handle)
            .field("first", &self.first)
            .field("second", &self.second)
            .finish()
    }
}

/// A **smart pointer** to an owned backing data structure. This makes sure that the
/// data is dropped correctly (using [`Absorb::drop_second`]).
///
/// Additionally it allows for unsafely getting the inner data out using [`into_box()`](Taken::into_box).
pub struct Taken<T: Absorb<O>, O> {
    inner: Option<Box<T>>,
    _marker: PhantomData<O>,
}

impl<T: Absorb<O> + fmt::Debug, O> fmt::Debug for Taken<T, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Taken")
            .field(
                "inner",
                self.inner
                    .as_ref()
                    .expect("inner is only taken in `into_box` which drops self"),
            )
            .finish()
    }
}

impl<T: Absorb<O>, O> Deref for Taken<T, O> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("inner is only taken in `into_box` which drops self")
    }
}

impl<T: Absorb<O>, O> DerefMut for Taken<T, O> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_mut()
            .expect("inner is only taken in `into_box` which drops self")
    }
}

impl<T: Absorb<O>, O> Taken<T, O> {
    /// # Safety
    ///
    /// You must call [`Absorb::drop_second`] in case just dropping `T` is not safe and sufficient.
    ///
    /// If you used the default implementation of [`Absorb::drop_second`] (which just calls
    /// [`drop`](Drop::drop)) you don't need to call [`Absorb::drop_second`].
    pub unsafe fn into_box(mut self) -> Box<T> {
        self.inner
            .take()
            .expect("inner is only taken here then self is dropped")
    }
}

impl<T: Absorb<O>, O> Drop for Taken<T, O> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            T::drop_second(inner);
        }
    }
}

impl<T, O> WriteHandle<T, O>
where
    T: Absorb<O>,
{
    /// Takes out the inner backing data structure if it hasn't been taken yet. Otherwise returns `None`.
    ///
    /// Makes sure that all the pending operations are applied and waits till all the read handles
    /// have departed. Then it uses [`Absorb::drop_first`] to drop one of the copies of the data and
    /// returns the other copy as a [`Taken`] smart pointer.
    fn take_inner(&mut self) -> Option<Taken<T, O>> {
        use core::ptr;
        // Can only take inner once.
        if self.taken {
            return None;
        }

        // Disallow taking again.
        self.taken = true;

        // first, ensure both copies are up to date
        // (otherwise safely dropping the possibly duplicated w_handle data is a pain)
        if self.first || !self.oplog.is_empty() {
            self.publish();
        }
        if !self.oplog.is_empty() {
            self.publish();
        }
        assert!(self.oplog.is_empty());

        // next, grab the read handle and set it to NULL
        let r_handle = self.r_handle.inner.swap(ptr::null_mut(), Ordering::Release);

        // now, wait for all readers to depart
        self.wait();

        // ensure that the subsequent epoch reads aren't re-ordered to before the swap
        fence(Ordering::SeqCst);

        // all readers have now observed the NULL, so we own both handles.
        // all operations have been applied to both w_handle and r_handle.
        // give the underlying data structure an opportunity to handle the one copy differently:
        //
        // safety: w_handle was initially crated from a `Box`, and is no longer aliased.
        Absorb::drop_first(unsafe { Box::from_raw(self.w_handle.as_ptr()) });

        // next we take the r_handle and return it as a boxed value.
        //
        // this is safe, since we know that no readers are using this pointer
        // anymore (due to the .wait() following swapping the pointer with NULL).
        //
        // safety: r_handle was initially crated from a `Box`, and is no longer aliased.
        let boxed_r_handle = unsafe { Box::from_raw(r_handle) };

        Some(Taken {
            inner: Some(boxed_r_handle),
            _marker: PhantomData,
        })
    }
}

impl<T, O> Drop for WriteHandle<T, O>
where
    T: Absorb<O>,
{
    fn drop(&mut self) {
        if let Some(inner) = self.take_inner() {
            drop(inner);
        }
    }
}

impl<T, O> WriteHandle<T, O>
where
    T: Absorb<O>,
{
    pub(crate) fn new(
        w_handle: T,
        epochs: Epochs,
        r_handle: ReadHandle<T>,
        thread_yield: fn(),
    ) -> Self {
        Self {
            epochs,
            // safety: Box<T> is not null and covariant.
            w_handle: unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(w_handle))) },
            oplog: VecDeque::new(),
            swap_index: 0,
            r_handle,
            epochs_and_last: Vec::new(),
            #[cfg(test)]
            is_waiting: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            refreshes: 0,
            first: true,
            second: true,
            taken: false,
            thread_yield,
        }
    }

    fn wait(&mut self) {
        let mut iter = 0;
        let mut starti = 0;

        #[cfg(test)]
        {
            self.is_waiting.store(true, Ordering::Relaxed);
        }

        'retry: loop {
            // read all and see if all have changed (which is likely)
            for (ii, e) in self.epochs_and_last.iter_mut().enumerate().skip(starti) {
                // if the reader's epoch was even last we read it (which was _after_ the swap),
                // then they either do not have the pointer, or must have read the pointer strictly
                // after the swap. in either case, they cannot be using the old pointer value (what
                // is now w_handle).
                //
                // note that this holds even with wrap-around since std::u{N}::MAX == 2 ^ N - 1,
                // which is odd, and std::u{N}::MAX + 1 == 0 is even.
                //
                // note also that `ri` _may_ have been re-used since we last read into last_epochs.
                // this is okay though, as a change still implies that the new reader must have
                // arrived _after_ we did the atomic swap, and thus must also have seen the new
                // pointer.
                if e.last % 2 == 0 {
                    continue;
                }

                let now = e.epoch.load(Ordering::Acquire);
                if now != e.last {
                    // reader must have seen the last swap, since they have done at least one
                    // operation since we last looked at their epoch, which _must_ mean that they
                    // are no longer using the old pointer value.
                } else {
                    // reader may not have seen swap
                    // continue from this reader's epoch
                    starti = ii;

                    if !cfg!(loom) {
                        // how eagerly should we retry?
                        if iter != 20 {
                            iter += 1;
                        } else {
                            (self.thread_yield)();
                        }
                    }

                    #[cfg(loom)]
                    loom::thread::yield_now();

                    continue 'retry;
                }
            }
            break;
        }
        #[cfg(test)]
        {
            self.is_waiting.store(false, Ordering::Relaxed);
        }
    }

    /// Try to publish once without waiting.
    ///
    /// This performs a single, non-blocking check of reader epochs. If all current readers have
    /// advanced since the last swap, it performs a publish and returns `true`. If any reader may
    /// still be accessing the old copy, it does nothing and returns `false`.
    ///
    /// Unlike [`publish`](Self::publish), this never spins or waits. Use it on latency-sensitive
    /// paths where skipping a publish is preferable to blocking; call again later or fall back to
    /// [`publish`](Self::publish) if you must ensure visibility.
    ///
    /// Returns `true` if a publish occurred, `false` otherwise.
    pub fn try_publish(&mut self) -> bool {
        for e in &mut self.epochs_and_last {
            if e.last % 2 == 0 {
                continue;
            }

            let now = e.epoch.load(Ordering::Acquire);
            if now != e.last {
                continue;
            } else {
                return false;
            }
        }
        #[cfg(test)]
        {
            self.is_waiting.store(false, Ordering::Relaxed);
        }
        self.update_and_swap();

        true
    }

    /// Publish all operations append to the log to reads.
    ///
    /// This method needs to wait for all readers to move to the "other" copy of the data so that
    /// it can replay the operational log onto the stale copy the readers used to use. This can
    /// take some time, especially if readers are executing slow operations, or if there are many
    /// of them.
    pub fn publish(&mut self) -> &mut Self {
        self.wait();
        self.update_and_swap()
    }

    /// Brings `w_handle` up to date with the oplog, then swaps `r_handle` and `w_handle`.
    ///
    /// This method must only be called when all readers have exited `w_handle` (e.g., after
    /// `wait`).
    fn update_and_swap(&mut self) -> &mut Self {
        if !self.first {
            // all the readers have left!
            // safety: we haven't freed the Box, and no readers are accessing the w_handle
            let w_handle = unsafe { self.w_handle.as_mut() };

            // safety: we will not swap while we hold this reference
            let r_handle = unsafe {
                self.r_handle
                    .inner
                    .load(Ordering::Acquire)
                    .as_ref()
                    .unwrap()
            };

            if self.second {
                Absorb::sync_with(w_handle, r_handle);
                self.second = false
            }

            // the w_handle copy has not seen any of the writes in the oplog
            // the r_handle copy has not seen any of the writes following swap_index
            if self.swap_index != 0 {
                // we can drain out the operations that only the w_handle copy needs
                //
                // NOTE: the if above is because drain(0..0) would remove 0
                for op in self.oplog.drain(0..self.swap_index) {
                    T::absorb_second(w_handle, op, r_handle);
                }
            }
            // we cannot give owned operations to absorb_first
            // since they'll also be needed by the r_handle copy
            for op in self.oplog.iter_mut() {
                T::absorb_first(w_handle, op, r_handle);
            }
            // the w_handle copy is about to become the r_handle, and can ignore the oplog
            self.swap_index = self.oplog.len();

        // w_handle (the old r_handle) is now fully up to date!
        } else {
            self.first = false
        }

        // at this point, we have exclusive access to w_handle, and it is up-to-date with all
        // writes. the stale r_handle is accessed by readers through an Arc clone of atomic pointer
        // inside the ReadHandle. oplog contains all the changes that are in w_handle, but not in
        // r_handle.
        //
        // it's now time for us to swap the copies so that readers see up-to-date results from
        // w_handle.

        // swap in our w_handle, and get r_handle in return
        let r_handle = self
            .r_handle
            .inner
            .swap(self.w_handle.as_ptr(), Ordering::Release);

        // NOTE: at this point, there are likely still readers using r_handle.
        // safety: r_handle was also created from a Box, so it is not null and is covariant.
        self.w_handle = unsafe { NonNull::new_unchecked(r_handle) };

        // ensure that the subsequent epoch reads aren't re-ordered to before the swap
        fence(Ordering::SeqCst);

        // subsequent readers will be reading from swapped copy of the data
        let new_epochs = self.epochs.take_all();
        self.epochs_and_last.retain_mut(|e| {
            // strong count is 1 => reader is dropped and we have the only copy of Arc
            if Arc::strong_count(&e.epoch) == 1 {
                false
            } else {
                e.last = e.epoch.load(Ordering::Acquire);
                true
            }
        });
        self.epochs_and_last.extend(new_epochs.filter_map(|e| {
            (Arc::strong_count(&e) != 1).then(|| LastEpoch {
                last: e.load(Ordering::Acquire),
                epoch: e,
            })
        }));

        #[cfg(test)]
        {
            self.refreshes += 1;
        }

        self
    }

    /// Publish as necessary to ensure that all operations are visible to readers.
    ///
    /// `WriteHandle::publish` will *always* wait for old readers to depart and swap the maps.
    /// This method will only do so if there are pending operations.
    pub fn flush(&mut self) {
        if self.has_pending_operations() {
            self.publish();
        } else {
            // even if there are no pending updates, dropped readers should be cleaned
            self.epochs_and_last
                .retain(|e| Arc::strong_count(&e.epoch) != 1);
            self.epochs_and_last
                .extend(self.epochs.take_all().filter_map(|epoch| {
                    (Arc::strong_count(&epoch) != 1).then_some(LastEpoch {
                        epoch,
                        // reader joined after the latest swap => no need to wait for its departure
                        last: 0,
                    })
                }))
        }
    }

    /// Returns true if there are operations in the operational log that have not yet been exposed
    /// to readers.
    pub fn has_pending_operations(&self) -> bool {
        // NOTE: we don't use self.oplog.is_empty() here because it's not really that important if
        // there are operations that have not yet been applied to the _write_ handle.
        self.swap_index < self.oplog.len()
    }

    /// Append the given operation to the operational log.
    ///
    /// Its effects will not be exposed to readers until you call [`publish`](Self::publish).
    pub fn append(&mut self, op: O) -> &mut Self {
        self.extend(core::iter::once(op));
        self
    }

    /// Returns a raw pointer to the write copy of the data (the one readers are _not_ accessing).
    ///
    /// Note that it is only safe to mutate through this pointer if you _know_ that there are no
    /// readers still present in this copy. This is not normally something you know; even after
    /// calling `publish`, readers may still be in the write copy for some time. In general, the
    /// only time you know this is okay is before the first call to `publish` (since no readers
    /// ever entered the write copy).
    // TODO: Make this return `Option<&mut T>`,
    // and only `Some` if there are indeed to readers in the write copy.
    pub fn raw_write_handle(&mut self) -> NonNull<T> {
        self.w_handle
    }

    /// Returns the backing data structure.
    ///
    /// Makes sure that all the pending operations are applied and waits till all the read handles
    /// have departed. Then it uses [`Absorb::drop_first`] to drop one of the copies of the data and
    /// returns the other copy as a [`Taken`] smart pointer.
    pub fn take(mut self) -> Taken<T, O> {
        // It is always safe to `expect` here because `take_inner` is private
        // and it is only called here and in the drop impl. Since we have an owned
        // `self` we know the drop has not yet been called. And every first call of
        // `take_inner` returns `Some`
        self.take_inner()
            .expect("inner is only taken here then self is dropped")
    }
}

// allow using write handle for reads
use core::ops::Deref;
impl<T, O> Deref for WriteHandle<T, O>
where
    T: Absorb<O>,
{
    type Target = ReadHandle<T>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}

impl<T, O> Extend<O> for WriteHandle<T, O>
where
    T: Absorb<O>,
{
    /// Add multiple operations to the operational log.
    ///
    /// Their effects will not be exposed to readers until you call [`publish`](Self::publish)
    fn extend<I>(&mut self, ops: I)
    where
        I: IntoIterator<Item = O>,
    {
        if self.first {
            // Safety: we know there are no outstanding w_handle readers, since we haven't
            // refreshed ever before, so we can modify it directly!
            let mut w_inner = self.raw_write_handle();
            let w_inner = unsafe { w_inner.as_mut() };
            let r_handle = self.enter().expect("map has not yet been destroyed");
            // Because we are operating directly on the map, and nothing is aliased, we do want
            // to perform drops, so we invoke absorb_second.
            for op in ops {
                Absorb::absorb_second(w_inner, op, &*r_handle);
            }
        } else {
            self.oplog.extend(ops);
        }
    }
}

/// `WriteHandle` can be sent across thread boundaries:
///
/// ```
/// use left_right::WriteHandle;
///
/// struct Data;
/// impl left_right::Absorb<()> for Data {
///     fn absorb_first(&mut self, _: &mut (), _: &Self) {}
///     fn sync_with(&mut self, _: &Self) {}
/// }
///
/// fn is_send<T: Send>() {
///   // dummy function just used for its parameterized type bound
/// }
///
/// is_send::<WriteHandle<Data, ()>>()
/// ```
///
/// As long as the inner types allow that of course.
/// Namely, the data type has to be `Send`:
///
/// ```compile_fail
/// use left_right::WriteHandle;
/// use alloc::rc::Rc;
///
/// struct Data(Rc<()>);
/// impl left_right::Absorb<()> for Data {
///     fn absorb_first(&mut self, _: &mut (), _: &Self) {}
/// }
///
/// fn is_send<T: Send>() {
///   // dummy function just used for its parameterized type bound
/// }
///
/// is_send::<WriteHandle<Data, ()>>()
/// ```
///
/// .. the operation type has to be `Send`:
///
/// ```compile_fail
/// use left_right::WriteHandle;
/// use alloc::rc::Rc;
///
/// struct Data;
/// impl left_right::Absorb<Rc<()>> for Data {
///     fn absorb_first(&mut self, _: &mut Rc<()>, _: &Self) {}
/// }
///
/// fn is_send<T: Send>() {
///   // dummy function just used for its parameterized type bound
/// }
///
/// is_send::<WriteHandle<Data, Rc<()>>>()
/// ```
///
/// .. and the data type has to be `Sync` so it's still okay to read through `ReadHandle`s:
///
/// ```compile_fail
/// use left_right::WriteHandle;
/// use alloc::cell::Cell;
///
/// struct Data(Cell<()>);
/// impl left_right::Absorb<()> for Data {
///     fn absorb_first(&mut self, _: &mut (), _: &Self) {}
/// }
///
/// fn is_send<T: Send>() {
///   // dummy function just used for its parameterized type bound
/// }
///
/// is_send::<WriteHandle<Data, ()>>()
/// ```
#[allow(dead_code)]
struct CheckWriteHandleSend;

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::LastEpoch;
    use crate::sync::{Arc, AtomicUsize, Ordering};
    use crate::Absorb;
    use crossbeam_utils::CachePadded;
    include!("./utilities.rs");

    #[test]
    fn append_test() {
        let (mut w, _r) = crate::new::<i32, _>();
        assert_eq!(w.first, true);
        w.append(CounterAddOp(1));
        assert_eq!(w.oplog.len(), 0);
        assert_eq!(w.first, true);
        w.publish();
        assert_eq!(w.first, false);
        w.append(CounterAddOp(2));
        w.append(CounterAddOp(3));
        assert_eq!(w.oplog.len(), 2);
    }

    #[test]
    fn take_test() {
        // publish twice then take with no pending operations
        let (mut w, _r) = crate::new_from_empty::<i32, _>(2);
        w.append(CounterAddOp(1));
        w.publish();
        w.append(CounterAddOp(1));
        w.publish();
        assert_eq!(*w.take(), 4);

        // publish twice then pending operation published by take
        let (mut w, _r) = crate::new_from_empty::<i32, _>(2);
        w.append(CounterAddOp(1));
        w.publish();
        w.append(CounterAddOp(1));
        w.publish();
        w.append(CounterAddOp(2));
        assert_eq!(*w.take(), 6);

        // normal publish then pending operations published by take
        let (mut w, _r) = crate::new_from_empty::<i32, _>(2);
        w.append(CounterAddOp(1));
        w.publish();
        w.append(CounterAddOp(1));
        assert_eq!(*w.take(), 4);

        // pending operations published by take
        let (mut w, _r) = crate::new_from_empty::<i32, _>(2);
        w.append(CounterAddOp(1));
        assert_eq!(*w.take(), 3);

        // emptry op queue
        let (mut w, _r) = crate::new_from_empty::<i32, _>(2);
        w.append(CounterAddOp(1));
        w.publish();
        assert_eq!(*w.take(), 3);

        // no operations
        let (w, _r) = crate::new_from_empty::<i32, _>(2);
        assert_eq!(*w.take(), 2);
    }

    #[test]
    fn wait_test() {
        use std::sync::Barrier;
        use std::thread;
        let (mut w, _r) = crate::new::<i32, _>();

        // Case 1: no tracked readers — wait returns immediately.
        w.epochs_and_last.clear();
        w.wait();

        // Case 2: one reader is still in a read (odd, unchanged); wait must block
        // until that reader's epoch advances.
        let finished_epoch_1 = Arc::new(CachePadded::new(AtomicUsize::new(2)));
        let finished_epoch_2 = Arc::new(CachePadded::new(AtomicUsize::new(2)));
        let held_epoch = Arc::new(CachePadded::new(AtomicUsize::new(1)));
        w.epochs_and_last = vec![
            LastEpoch {
                epoch: Arc::clone(&finished_epoch_1),
                last: 2,
            },
            LastEpoch {
                epoch: Arc::clone(&finished_epoch_2),
                last: 2,
            },
            LastEpoch {
                epoch: Arc::clone(&held_epoch),
                last: 1,
            },
        ];

        let barrier = Arc::new(Barrier::new(2));
        let is_waiting = Arc::clone(&w.is_waiting);

        // check writer's waiting state before calling wait.
        assert!(!is_waiting.load(Ordering::Relaxed));

        let barrier2 = Arc::clone(&barrier);
        let wait_handle = thread::spawn(move || {
            barrier2.wait();
            w.wait();
        });

        barrier.wait();

        // make sure wait() entered its spin loop before we release the held epoch.
        while !is_waiting.load(Ordering::Relaxed) {
            thread::yield_now();
        }

        held_epoch.fetch_add(1, Ordering::SeqCst);

        // wait must return after the held epoch advances.
        wait_handle.join().unwrap();
    }

    #[test]
    fn flush_noblock() {
        let (mut w, r) = crate::new::<i32, _>();
        w.append(CounterAddOp(42));
        w.publish();
        assert_eq!(*r.enter().unwrap(), 42);

        // pin the epoch
        let _count = r.enter();
        // refresh would hang here
        assert_eq!(w.oplog.iter().skip(w.swap_index).count(), 0);
        assert!(!w.has_pending_operations());
    }

    #[test]
    fn flush_no_refresh() {
        let (mut w, _) = crate::new::<i32, _>();

        // Until we refresh, writes are written directly instead of going to the
        // oplog (because there can't be any readers on the w_handle table).
        assert!(!w.has_pending_operations());
        w.publish();
        assert!(!w.has_pending_operations());
        assert_eq!(w.refreshes, 1);

        w.append(CounterAddOp(42));
        assert!(w.has_pending_operations());
        w.publish();
        assert!(!w.has_pending_operations());
        assert_eq!(w.refreshes, 2);

        w.append(CounterAddOp(42));
        assert!(w.has_pending_operations());
        w.publish();
        assert!(!w.has_pending_operations());
        assert_eq!(w.refreshes, 3);

        // Sanity check that a refresh would have been visible
        assert!(!w.has_pending_operations());
        w.publish();
        assert_eq!(w.refreshes, 4);
    }

    #[test]
    fn try_publish() {
        let (mut w, _r) = crate::new::<i32, _>();

        // Case 1: a reader has not advanced (odd and unchanged) -> returns false.
        let held_epoch = Arc::new(CachePadded::new(AtomicUsize::new(1)));
        w.epochs_and_last = vec![LastEpoch {
            epoch: Arc::clone(&held_epoch),
            last: 1,
        }];
        assert!(!w.try_publish());

        // Case 2: a reader is even (was not in a read at the last swap) -> skipped, publish proceeds.
        let finished_epoch = Arc::new(CachePadded::new(AtomicUsize::new(2)));
        w.epochs_and_last = vec![LastEpoch {
            epoch: Arc::clone(&finished_epoch),
            last: 2,
        }];
        let before = w.refreshes;
        assert!(w.try_publish());
        assert_eq!(w.refreshes, before + 1);

        // Case 3: a reader was odd at the last swap but has since advanced -> safe, publish proceeds.
        let finished_epoch = Arc::new(CachePadded::new(AtomicUsize::new(2)));
        w.epochs_and_last = vec![LastEpoch {
            epoch: Arc::clone(&finished_epoch), // advanced from 1 -> 2
            last: 1,
        }];
        let before = w.refreshes;
        assert!(w.try_publish());
        assert_eq!(w.refreshes, before + 1);
    }

    #[test]
    fn dropped_readers_removed_on_flush() {
        let (mut w, r1) = crate::new::<i32, _>();

        // initial state
        assert_eq!(w.epochs_and_last.len(), 0);

        w.flush();
        // inner writer's reader + external reader
        assert_eq!(w.epochs_and_last.len(), 2);

        let r2 = r1.clone();
        let r3 = r1.clone();
        // inner writer's reader + external reader + 2 readers inside Epochs
        assert_eq!(w.epochs_and_last.len(), 2);

        w.flush();
        // inner writer's reader + 3 external readers
        assert_eq!(w.epochs_and_last.len(), 4);

        // flush removes readers that were created and dropped since the last flush
        let r4 = r1.clone();
        drop(r4);
        w.flush();
        // inner writer's reader + 3 external readers
        assert_eq!(w.epochs_and_last.len(), 4);

        drop((r1, r2, r3));
        w.flush();
        // inner writer's reader + external reader
        assert_eq!(w.epochs_and_last.len(), 1);
    }
}
