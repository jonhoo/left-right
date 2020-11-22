use std::cell::Cell;
use std::mem;
use std::sync;
use std::sync::atomic;

#[derive(Debug, Copy, Clone)]
pub(super) struct ReadHandleState<'rh> {
    pub(super) shared_epoch: &'rh sync::atomic::AtomicU64,
    pub(super) own_epoch: &'rh Cell<u64>,
    pub(super) enters: &'rh Cell<usize>,
}

impl<'rh, T> From<&'rh super::ReadHandle<T>> for ReadHandleState<'rh> {
    fn from(rh: &'rh super::ReadHandle<T>) -> Self {
        Self {
            shared_epoch: &rh.epoch,
            own_epoch: &rh.my_epoch,
            enters: &rh.enters,
        }
    }
}

/// A guard wrapping a live reference into a [`LeftRight`].
///
/// As long as this guard lives, the `T` being read cannot change, and if a writer attempts to
/// call [`WriteHandle::refresh`], that call will block until this guard is dropped.
#[derive(Debug)]
pub struct ReadGuard<'rh, T: ?Sized> {
    // NOTE: _technically_ this is more like &'self.
    // the reference is valid until the guard is dropped.
    pub(super) t: &'rh T,
    pub(super) epoch: u64,
    pub(super) handle: ReadHandleState<'rh>,
}

impl<'rh, T: ?Sized> ReadGuard<'rh, T> {
    pub fn map<F, U: ?Sized>(orig: Self, f: F) -> ReadGuard<'rh, U>
    where
        F: for<'a> FnOnce(&'a T) -> &'a U,
    {
        let rg = ReadGuard {
            t: f(orig.t),
            epoch: orig.epoch,
            handle: orig.handle,
        };
        mem::forget(orig);
        rg
    }

    pub fn try_map<F, U: ?Sized>(orig: Self, f: F) -> Option<ReadGuard<'rh, U>>
    where
        F: for<'a> FnOnce(&'a T) -> Option<&'a U>,
    {
        let rg = ReadGuard {
            t: f(orig.t)?,
            epoch: orig.epoch,
            handle: orig.handle,
        };
        mem::forget(orig);
        Some(rg)
    }
}

impl<'rh, T: ?Sized> AsRef<T> for ReadGuard<'rh, T> {
    fn as_ref(&self) -> &T {
        self.t
    }
}

impl<'rh, T: ?Sized> std::ops::Deref for ReadGuard<'rh, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.t
    }
}

impl<'rh, T: ?Sized> Drop for ReadGuard<'rh, T> {
    fn drop(&mut self) {
        let enters = self.handle.enters.get() - 1;
        self.handle.enters.set(enters);
        if enters == 0 {
            // We are the last guard to be dropped -- now release our epoch.
            let epoch = self.handle.own_epoch.get();
            self.handle.shared_epoch.store(
                epoch | 1 << (mem::size_of_val(&epoch) * 8 - 1),
                atomic::Ordering::Release,
            );
        }
    }
}
