use super::ReadHandle;
use crate::sync::{Arc, AtomicPtr};
use std::fmt;
use std::marker::PhantomData;

/// A type that lets you produce new [`ReadHandle`] instances.
///
/// This serves as a handy way to distribute read handles across many threads without requiring
/// additional external locking to synchronize access to the non-`Sync` [`ReadHandle`] type. Note
/// that this _internally_ takes a lock whenever you call [`ReadHandleFactory::handle`], so
/// you should not expect producing new handles rapidly to scale well. The factory is `Send` and
/// `Sync` when `T` is `Sync`.
pub struct ReadHandleFactory<T> {
    pub(super) inner: Arc<AtomicPtr<T>>,
    pub(super) epochs: crate::Epochs,
    pub(super) _unimpl_send_sync: PhantomData<*const T>,
}

// Safety: the factory only gives out shared references to T through the handles it creates, so it
// can cross or be shared across thread boundaries exactly when T is Sync.
unsafe impl<T> Send for ReadHandleFactory<T> where T: Sync {}
unsafe impl<T> Sync for ReadHandleFactory<T> where T: Sync {}

impl<T> fmt::Debug for ReadHandleFactory<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandleFactory")
            .field("epochs", &self.epochs)
            .finish()
    }
}

impl<T> Clone for ReadHandleFactory<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            epochs: Arc::clone(&self.epochs),
            _unimpl_send_sync: PhantomData,
        }
    }
}

impl<T> ReadHandleFactory<T> {
    /// Produce a new [`ReadHandle`] to the same left-right data structure as this factory was
    /// originally produced from.
    pub fn handle(&self) -> ReadHandle<T> {
        ReadHandle::new_with_arc(Arc::clone(&self.inner), Arc::clone(&self.epochs))
    }
}

/// `ReadHandleFactory` can be sent and shared across threads when `T` is `Sync`:
///
/// ```
/// use left_right::ReadHandleFactory;
///
/// fn is_send_sync<T: Send + Sync>() {}
/// is_send_sync::<ReadHandleFactory<u64>>();
/// ```
///
/// A factory cannot cross thread boundaries when `T` is not `Sync`:
///
/// ```compile_fail
/// use left_right::ReadHandleFactory;
/// use std::cell::Cell;
///
/// fn is_send_sync<T: Send + Sync>() {}
/// is_send_sync::<ReadHandleFactory<Cell<u64>>>();
/// ```
#[allow(dead_code)]
struct CheckReadHandleFactorySendSync;
