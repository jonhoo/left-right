use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;
use std::sync::{self, Arc};

// To make [`WriteHandle`] and friends work.
#[cfg(doc)]
use crate::WriteHandle;

mod guard;
pub use guard::ReadGuard;

mod factory;
pub use factory::ReadHandleFactory;

/// A read handle to a left-right guarded data structure.
///
/// To use a handle, first call [`enter`](Self::enter) to acquire a [`ReadGuard`]. This is similar
/// to acquiring a `Mutex`, except that no exclusive lock is taken. All reads of the underlying
/// data structure can then happen through the [`ReadGuard`] (which implements `Deref<Target =
/// T>`).
///
/// Reads through a `ReadHandle` only see the changes up until the last time
/// [`WriteHandle::publish`] was called. That is, even if a writer performs a number of
/// modifications to the underlying data, those changes are not visible to reads until the writer
/// calls [`publish`](crate::WriteHandle::publish).
///
/// `ReadHandle` is not `Sync`, which means that you cannot share a `ReadHandle` across many
/// threads. This is because the coordination necessary to do so would significantly hamper the
/// scalability of reads. If you had many reads go through one `ReadHandle`, they would need to
/// coordinate among themselves for every read, which would lead to core contention and poor
/// multi-core performance. By having `ReadHandle` not be `Sync`, you are forced to keep a
/// `ReadHandle` per reader, which guarantees that you do not accidentally ruin your performance.
///
/// You can create a new, independent `ReadHandle` either by cloning an existing handle or by using
/// a [`ReadHandleFactory`]. Note, however, that creating a new handle through either of these
/// mechanisms _does_ take a lock, and may therefore become a bottleneck if you do it frequently.
pub struct ReadHandle<T> {
    pub(crate) inner: sync::Arc<AtomicPtr<T>>,
    pub(crate) epochs: crate::Epochs,
    epoch: sync::Arc<sync::atomic::AtomicUsize>,
    epoch_i: usize,
    enters: Cell<usize>,

    // `ReadHandle` is _only_ Send if T is Sync. If T is !Sync, then it's not okay for us to expose
    // references to it to other threads! Since negative impls are not available on stable, we pull
    // this little hack to make the type not auto-impl Send, and then explicitly add the impl when
    // appropriate.
    _unimpl_send: PhantomData<*const T>,
}
unsafe impl<T> Send for ReadHandle<T> where T: Sync {}

impl<T> Drop for ReadHandle<T> {
    fn drop(&mut self) {
        // epoch must already be even for us to have &mut self,
        // so okay to lock since we're not holding up the epoch anyway.
        let e = self.epochs.lock().unwrap().remove(self.epoch_i);
        assert!(Arc::ptr_eq(&e, &self.epoch));
        assert_eq!(self.enters.get(), 0);
    }
}

impl<T> fmt::Debug for ReadHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandle")
            .field("epochs", &self.epochs)
            .field("epoch", &self.epoch)
            .finish()
    }
}

impl<T> Clone for ReadHandle<T> {
    fn clone(&self) -> Self {
        ReadHandle::new_with_arc(
            sync::Arc::clone(&self.inner),
            sync::Arc::clone(&self.epochs),
        )
    }
}

impl<T> ReadHandle<T> {
    pub(crate) fn new(inner: T, epochs: crate::Epochs) -> Self {
        let store = Box::into_raw(Box::new(inner));
        let inner = sync::Arc::new(AtomicPtr::new(store));
        Self::new_with_arc(inner, epochs)
    }

    fn new_with_arc(inner: Arc<AtomicPtr<T>>, epochs: crate::Epochs) -> Self {
        // tell writer about our epoch tracker
        let epoch = sync::Arc::new(atomic::AtomicUsize::new(0));
        // okay to lock, since we're not holding up the epoch
        let epoch_i = epochs.lock().unwrap().insert(Arc::clone(&epoch));

        Self {
            epochs,
            epoch,
            epoch_i,
            enters: Cell::new(0),
            inner,
            _unimpl_send: PhantomData,
        }
    }
}

impl<T> ReadHandle<T> {
    /// Take out a guarded live reference to the read copy of the `T`.
    ///
    /// While the guard lives, the [`WriteHandle`] cannot proceed with a call to
    /// [`WriteHandle::publish`], so no queued operations will become visible to _any_ reader.
    ///
    /// If the `WriteHandle` has been dropped, this function returns `None`.
    pub fn enter(&self) -> Option<ReadGuard<'_, T>> {
        let enters = self.enters.get();
        if enters != 0 {
            // We have already locked the epoch.
            // Just give out another guard.
            let r_handle = self.inner.load(atomic::Ordering::Acquire);
            // since we previously bumped our epoch, this pointer will remain valid until we bump
            // it again, which only happens when the last ReadGuard is dropped.
            let r_handle = unsafe { r_handle.as_ref() };

            return if let Some(r_handle) = r_handle {
                self.enters.set(enters + 1);
                Some(ReadGuard {
                    handle: guard::ReadHandleState::from(self),
                    t: r_handle,
                })
            } else {
                unreachable!("if pointer is null, no ReadGuard should have been issued");
            };
        }

        // once we update our epoch, the writer can no longer do a swap until we set the MSB to
        // indicate that we've finished our read. however, we still need to deal with the case of a
        // race between when the writer reads our epoch and when they decide to make the swap.
        //
        // assume that there is a concurrent writer. it just swapped the atomic pointer from A to
        // B. the writer wants to modify A, and needs to know if that is safe. we can be in any of
        // the following cases when we atomically swap out our epoch:
        //
        //  1. the writer has read our previous epoch twice
        //  2. the writer has already read our previous epoch once
        //  3. the writer has not yet read our previous epoch
        //
        // let's discuss each of these in turn.
        //
        //  1. since writers assume they are free to proceed if they read an epoch with MSB set
        //     twice in a row, this is equivalent to case (2) below.
        //  2. the writer will see our epoch change, and so will assume that we have read B. it
        //     will therefore feel free to modify A. note that *another* pointer swap can happen,
        //     back to A, but then the writer would be block on our epoch, and so cannot modify
        //     A *or* B. consequently, using a pointer we read *after* the epoch swap is definitely
        //     safe here.
        //  3. the writer will read our epoch, notice that MSB is not set, and will keep reading,
        //     continuing to observe that it is still not set until we finish our read. thus,
        //     neither A nor B are being modified, and we can safely use either.
        //
        // in all cases, using a pointer we read *after* updating our epoch is safe.

        // so, update our epoch tracker.
        self.epoch.fetch_add(1, atomic::Ordering::AcqRel);

        // ensure that the pointer read happens strictly after updating the epoch
        atomic::fence(atomic::Ordering::SeqCst);

        // then, atomically read pointer, and use the copy being pointed to
        let r_handle = self.inner.load(atomic::Ordering::Acquire);

        // since we bumped our epoch, this pointer will remain valid until we bump it again
        let r_handle = unsafe { r_handle.as_ref() };

        if let Some(r_handle) = r_handle {
            // add a guard to ensure we restore read parity even if we panic
            let enters = self.enters.get() + 1;
            self.enters.set(enters);
            Some(ReadGuard {
                handle: guard::ReadHandleState::from(self),
                t: r_handle,
            })
        } else {
            // the writehandle has been dropped, and so has both copies,
            // so restore parity and return None
            self.epoch.fetch_add(1, atomic::Ordering::AcqRel);
            None
        }
    }

    /// Returns true if the [`WriteHandle`] has been dropped.
    pub fn was_dropped(&self) -> bool {
        self.inner.load(atomic::Ordering::Acquire).is_null()
    }

    /// Returns a raw pointer to the read copy of the data.
    ///
    /// Note that it is only safe to read through this pointer if you _know_ that the writer will
    /// not start writing into it. This is most likely only the case if you are calling this method
    /// from inside a method that holds `&mut WriteHandle`.
    ///
    /// Casting this pointer to `&mut` is never safe.
    pub fn raw_handle(&self) -> Option<NonNull<T>> {
        NonNull::new(self.inner.load(atomic::Ordering::Acquire))
    }
}

/// `ReadHandle` cannot be shared across threads:
///
/// ```compile_fail
/// use left_right::ReadHandle;
///
/// fn is_sync<T: Sync>() {
///   // dummy function just used for its parameterized type bound
/// }
///
/// // the line below will not compile as ReadHandle does not implement Sync
///
/// is_sync::<ReadHandle<u64>>()
/// ```
///
/// But, it can be sent across threads:
///
/// ```
/// use left_right::ReadHandle;
///
/// fn is_send<T: Send>() {
///   // dummy function just used for its parameterized type bound
/// }
///
/// is_send::<ReadHandle<u64>>()
/// ```
///
/// As long as the wrapped type is `Sync` that is.
///
/// ```compile_fail
/// use left_right::ReadHandle;
///
/// fn is_send<T: Send>() {}
///
/// is_send::<ReadHandle<std::cell::Cell<u64>>>()
/// ```
#[allow(dead_code)]
struct CheckReadHandleSendNotSync;
