use super::Absorb;
use crate::read::ReadHandle;

use std::mem::ManuallyDrop;
use std::sync::atomic;
use std::sync::{Arc, MutexGuard};
use std::{fmt, mem, thread};

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

        let w_handle = self.w_handle.take().unwrap();

        // all readers have now observed the NULL, so we own both handles.
        // all records are duplicated between w_handle and r_handle.
        // since the two maps are exactly equal, we need to make sure that we *don't* call the
        // destructors of any of the values that are in our map, as they'll all be called when the
        // last read handle goes out of scope. to do so, we first clear w_handle, which won't drop
        // any elements since its values are kept as ManuallyDrop:
        Absorb::drop_first(ManuallyDrop::into_inner(w_handle));

        // then we transmute r_handle to remove the ManuallyDrop, and then drop it, which will free
        // all the records. this is safe, since we know that no readers are using this pointer
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
            let w_handle = self.w_handle.as_deref_mut().unwrap();

            // safety: we will not swap while we hold this reference
            let r_handle = unsafe {
                self.r_handle
                    .inner
                    .load(atomic::Ordering::Acquire)
                    .as_ref()
                    .unwrap()
            };

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
                    T::absorb_second(w_handle, op, r_handle);
                }
            }
            // the rest have to be cloned because they'll also be needed by the r_handle map
            for op in self.oplog.iter_mut() {
                T::absorb_first(w_handle, op, r_handle);
            }
            // the w_handle map is about to become the r_handle, and can ignore the oplog
            self.swap_index = self.oplog.len();

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

    /// Refresh as necessary to ensure that all operations are visible to readers.
    ///
    /// `WriteHandle::refresh` will *always* wait for old readers to depart and swap the maps.
    /// This method will only do so if there are pending operations.
    pub fn flush(&mut self) -> &mut Self {
        if self.swap_index < self.oplog.len() {
            self.refresh();
        }

        self
    }

    pub fn append_op(&mut self, op: O) -> &mut Self {
        self.oplog.push(op);
        self
    }

    pub fn raw_write_handle(&mut self) -> *mut T {
        &mut ***self
            .w_handle
            .as_mut()
            .expect("write handle is only null after drop")
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
        let (r, mut w) = crate::new::<i32, _>();
        w.append_op(CounterAddOp(42));
        w.refresh();
        assert_eq!(*r.enter().unwrap(), 42);

        // pin the epoch
        let _count = r.enter();
        // refresh would hang here, but flush won't
        assert!(w.oplog[w.swap_index..].is_empty());
        w.flush();
    }

    #[test]
    fn flush_no_refresh() {
        let (_, mut w) = crate::new::<i32, _>();

        // Until we refresh, writes are written directly instead of going to the
        // oplog (because there can't be any readers on the w_handle table).
        w.refresh();

        assert_eq!(w.refreshes, 1);

        w.flush();

        // No refresh because there are no operations
        assert_eq!(w.refreshes, 1);

        w.append_op(CounterAddOp(42));
        w.flush();

        // A refresh happened!
        assert_eq!(w.refreshes, 2);

        w.flush();

        // Subsequent flushes don't refresh until there are more ops!
        assert_eq!(w.refreshes, 2);

        w.append_op(CounterAddOp(42));
        w.flush();

        assert_eq!(w.refreshes, 3);

        // Sanity check that a refresh would have been visible
        w.refresh();
        assert_eq!(w.refreshes, 4);
    }
}
