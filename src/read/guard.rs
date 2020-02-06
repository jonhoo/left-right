use std::mem;
use std::sync;
use std::sync::atomic;

/// A guard wrapping a live reference into an evmap.
///
/// As long as this guard lives, the map being read cannot change, and if a writer attempts to
/// call [`WriteHandle::refresh`], that call will block until this guard is dropped.
#[derive(Debug)]
pub struct ReadGuard<'rh, T: ?Sized> {
    // NOTE: _technically_ this is more like &'self.
    // the reference is valid until the guard is dropped.
    pub(super) t: &'rh T,
    pub(super) epoch: usize,
    pub(super) handle: &'rh sync::atomic::AtomicUsize,
}

impl<'rh, T: ?Sized> ReadGuard<'rh, T> {
    pub(super) fn map_ref<F, U: ?Sized>(self, f: F) -> ReadGuard<'rh, U>
    where
        F: for<'a> FnOnce(&'a T) -> &'a U,
    {
        let rg = ReadGuard {
            t: f(self.t),
            epoch: self.epoch,
            handle: self.handle,
        };
        mem::forget(self);
        rg
    }

    pub(super) fn map_opt<F, U: ?Sized>(self, f: F) -> Option<ReadGuard<'rh, U>>
    where
        F: for<'a> FnOnce(&'a T) -> Option<&'a U>,
    {
        let rg = Some(ReadGuard {
            t: f(self.t)?,
            epoch: self.epoch,
            handle: self.handle,
        });
        mem::forget(self);
        rg
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
        self.handle.store(
            (self.epoch + 1) | 1usize << (mem::size_of::<usize>() * 8 - 1),
            atomic::Ordering::Release,
        );
    }
}
