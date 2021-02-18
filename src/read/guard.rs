use crate::sync::{AtomicUsize, Ordering};
use std::cell::Cell;
use std::mem;

#[derive(Debug, Copy, Clone)]
pub(super) struct ReadHandleState<'rh> {
    pub(super) epoch: &'rh AtomicUsize,
    pub(super) enters: &'rh Cell<usize>,
}

impl<'rh, T> From<&'rh super::ReadHandle<T>> for ReadHandleState<'rh> {
    fn from(rh: &'rh super::ReadHandle<T>) -> Self {
        Self {
            epoch: &rh.epoch,
            enters: &rh.enters,
        }
    }
}

/// A guard wrapping a live reference into a left-right protected `T`.
///
/// As long as this guard lives, the `T` being read cannot change. If a writer attempts to call
/// [`WriteHandle::publish`](crate::WriteHandle::publish), that call will block until this guard is
/// dropped.
///
/// To scope the guard to a subset of the data in `T`, use [`map`](Self::map) and
/// [`try_map`](Self::try_map).
#[derive(Debug)]
pub struct ReadGuard<'rh, T: ?Sized> {
    // NOTE: _technically_ this is more like &'self.
    // the reference is valid until the guard is dropped.
    pub(super) t: &'rh T,
    pub(super) handle: ReadHandleState<'rh>,
}

impl<'rh, T: ?Sized> ReadGuard<'rh, T> {
    /// Makes a new `ReadGuard` for a component of the borrowed data.
    ///
    /// This is an associated function that needs to be used as `ReadGuard::map(...)`, since
    /// a method would interfere with methods of the same name on the contents of a `Readguard`
    /// used through `Deref`.
    ///
    /// # Examples
    ///
    /// ```
    /// use left_right::{ReadGuard, ReadHandle};
    ///
    /// fn get_str(handle: &ReadHandle<Vec<(String, i32)>>, i: usize) -> Option<ReadGuard<'_, str>> {
    ///     handle.enter().map(|guard| {
    ///         ReadGuard::map(guard, |t| {
    ///             &*t[i].0
    ///         })
    ///     })
    /// }
    /// ```
    pub fn map<F, U: ?Sized>(orig: Self, f: F) -> ReadGuard<'rh, U>
    where
        F: for<'a> FnOnce(&'a T) -> &'a U,
    {
        let rg = ReadGuard {
            t: f(orig.t),
            handle: orig.handle,
        };
        mem::forget(orig);
        rg
    }

    /// Makes a new `ReadGuard` for a component of the borrowed data that may not exist.
    ///
    /// This method differs from [`map`](Self::map) in that it drops the guard if the closure maps
    /// to `None`. This allows you to "lift" a `ReadGuard<Option<T>>` into an
    /// `Option<ReadGuard<T>>`.
    ///
    /// This is an associated function that needs to be used as `ReadGuard::try_map(...)`, since
    /// a method would interfere with methods of the same name on the contents of a `Readguard`
    /// used through `Deref`.
    ///
    /// # Examples
    ///
    /// ```
    /// use left_right::{ReadGuard, ReadHandle};
    ///
    /// fn try_get_str(handle: &ReadHandle<Vec<(String, i32)>>, i: usize) -> Option<ReadGuard<'_, str>> {
    ///     handle.enter().and_then(|guard| {
    ///         ReadGuard::try_map(guard, |t| {
    ///             t.get(i).map(|v| &*v.0)
    ///         })
    ///     })
    /// }
    /// ```
    pub fn try_map<F, U: ?Sized>(orig: Self, f: F) -> Option<ReadGuard<'rh, U>>
    where
        F: for<'a> FnOnce(&'a T) -> Option<&'a U>,
    {
        let rg = ReadGuard {
            t: f(orig.t)?,
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
            self.handle.epoch.fetch_add(1, Ordering::AcqRel);
        }
    }
}
