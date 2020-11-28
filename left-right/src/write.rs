use super::Absorb;
use crate::read::ReadHandle;

use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::sync::atomic;
use std::sync::{Arc, MutexGuard};
use std::{fmt, mem, thread};

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
    epochs: crate::Epochs,
    w_handle: Option<ManuallyDrop<Box<T>>>,
    oplog: Vec<O>,
    swap_index: usize,
    r_handle: ReadHandle<T>,
    last_epochs: Vec<u64>,
    #[cfg(test)]
    refreshes: usize,
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
            .finish()
    }
}

impl<T, O> Drop for WriteHandle<T, O>
where
    T: Absorb<O>,
{
    fn drop(&mut self) {
        use std::ptr;

        // first, ensure both copies are up to date
        // (otherwise safely dropping deduplicated data is a pain)
        if !self.oplog.is_empty() {
            self.publish();
        }
        if !self.oplog.is_empty() {
            self.publish();
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

        let w_handle = self.w_handle.take().unwrap();

        // all readers have now observed the NULL, so we own both handles.
        // all operations have been applied to both w_handle and r_handle.
        // give the underlying data structure an opportunity to handle the first copy differently:
        Absorb::drop_first(ManuallyDrop::into_inner(w_handle));

        // next we transmute r_handle to remove the ManuallyDrop, and then drop it.
        // this is safe, since we know that no readers are using this pointer
        // anymore (due to the .wait() following swapping the pointer with NULL).
        drop(unsafe { Box::from_raw(r_handle as *mut T) });
    }
}

impl<T, O> WriteHandle<T, O>
where
    T: Absorb<O>,
{
    pub(crate) fn new(w_handle: T, epochs: crate::Epochs, r_handle: ReadHandle<T>) -> Self {
        Self {
            epochs,
            w_handle: Some(ManuallyDrop::new(Box::new(w_handle))),
            oplog: Vec::new(),
            swap_index: 0,
            r_handle,
            last_epochs: Vec::new(),
            #[cfg(test)]
            refreshes: 0,
        }
    }

    fn wait(&mut self, epochs: &mut MutexGuard<'_, slab::Slab<Arc<atomic::AtomicU64>>>) {
        let mut iter = 0;
        let mut starti = 0;

        // We want a 1 in the type of the epochs, without naming that type here.
        #[allow(unused_assignments)]
        let mut epoch_unit = self.last_epochs.get(0).copied().unwrap_or(0);
        epoch_unit = 1;
        let high_bit = epoch_unit << (mem::size_of_val(&epoch_unit) * 8 - 1);

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

    /// Publish all operations append to the log to reads.
    ///
    /// This method needs to wait for all readers to move to the "other" copy of the data so that
    /// it can replay the operational log onto the stale copy the readers used to use. This can
    /// take some time, especially if readers are executing slow operations, or if there are many
    /// of them.
    pub fn publish(&mut self) -> &mut Self {
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
            let w_handle = self.w_handle.as_deref_mut().unwrap();

            // safety: we will not swap while we hold this reference
            let r_handle = unsafe {
                self.r_handle
                    .inner
                    .load(atomic::Ordering::Acquire)
                    .as_ref()
                    .unwrap()
            };

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
        }

        // at this point, we have exclusive access to w_handle, and it is up-to-date with all
        // writes. the stale r_handle is accessed by readers through an Arc clone of atomic pointer
        // inside the ReadHandle. oplog contains all the changes that are in w_handle, but not in
        // r_handle.
        //
        // it's now time for us to swap the copies so that readers see up-to-date results from
        // w_handle.

        // prepare w_handle
        let w_handle = self.w_handle.take().unwrap();
        let w_handle = Box::into_raw(ManuallyDrop::into_inner(w_handle));

        // swap in our w_handle, and get r_handle in return
        let r_handle = self
            .r_handle
            .inner
            .swap(w_handle as *mut T, atomic::Ordering::Release);
        let r_handle = ManuallyDrop::new(unsafe { Box::from_raw(r_handle as *mut T) });

        // ensure that the subsequent epoch reads aren't re-ordered to before the swap
        atomic::fence(atomic::Ordering::SeqCst);

        for (ri, epoch) in epochs.iter() {
            self.last_epochs[ri] = epoch.load(atomic::Ordering::Acquire);
        }

        // NOTE: at this point, there are likely still readers using the w_handle we got
        self.w_handle = Some(r_handle);

        #[cfg(test)]
        {
            self.refreshes += 1;
        }

        self
    }

    /// Returns true if there are operations in the operational log that have not yet been exposed
    /// to readers.
    pub fn has_pending_operations(&self) -> bool {
        self.swap_index < self.oplog.len()
    }

    /// Append the given operation to the operational log.
    ///
    /// Its effects will not be exposed to readers until you call [`publish`](Self::publish).
    pub fn append(&mut self, op: O) -> &mut Self {
        self.oplog.push(op);
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
        let ref_mut_t = &mut ***self
            .w_handle
            .as_mut()
            .expect("write handle is only null after drop");
        // Safety: Box is not null and covariant.
        unsafe { NonNull::new_unchecked(ref_mut_t) }
    }
}

// allow using write handle for reads
use std::ops::Deref;
impl<T, O> Deref for WriteHandle<T, O>
where
    T: Absorb<O>,
{
    type Target = ReadHandle<T>;
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}

#[cfg(test)]
mod tests {
    use crate::CounterAddOp;

    #[test]
    fn flush_noblock() {
        let (mut w, r) = crate::new::<i32, _>();
        w.append(CounterAddOp(42));
        w.publish();
        assert_eq!(*r.enter().unwrap(), 42);

        // pin the epoch
        let _count = r.enter();
        // refresh would hang here
        assert!(w.oplog[w.swap_index..].is_empty());
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
}
