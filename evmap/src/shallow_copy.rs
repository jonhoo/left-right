//! Types that can be cheaply aliased.

use std::cell::Cell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;

// Aliasing Box<T> is not okay:
// https://github.com/rust-lang/unsafe-code-guidelines/issues/258
// https://github.com/jonhoo/rust-evmap/issues/74
//
// Also, this cases from a non-mutable pointer to a mutable one, which is never okay.

// Only public since it appears in the (doc-hidden) variants of `values::ValuesIter`.
#[doc(hidden)]
pub struct ForwardThroughAliased<T> {
    aliased: MaybeUninit<T>,

    // We cannot implement Send just because T is Send since we're aliasing it.
    _no_auto_send: PhantomData<*const T>,
}

impl<T> From<T> for ForwardThroughAliased<T> {
    fn from(t: T) -> Self {
        Self {
            aliased: MaybeUninit::new(t),
            _no_auto_send: PhantomData,
        }
    }
}

impl<T> ForwardThroughAliased<T> {
    pub(crate) fn alias(&self) -> Self {
        // safety:
        //   We are aliasing T here, but it is okay because:
        //    a) the T is behind a MaybeUninit, and so will cannot be accessed safely; and
        //    b) we only expose _either_ &T while aliased, or &mut after the aliasing ends.
        ForwardThroughAliased {
            aliased: unsafe { std::ptr::read(&self.aliased) },
            _no_auto_send: PhantomData,
        }
    }
}

// ForwardThroughAliased gives &T across threads if shared or sent across thread boundaries.
// ForwardThroughAliased gives &mut T across threads (for drop) if sent across thread boundaries.
// This implies that we are only Send if T is Send+Sync, and Sync if T is Sync.
//
// Note that these bounds are stricter than what the compiler would auto-generate for the type.
unsafe impl<T> Send for ForwardThroughAliased<T> where T: Send + Sync {}
unsafe impl<T> Sync for ForwardThroughAliased<T> where T: Sync {}

// XXX: Is this a problem if people start nesting evmaps?
// I feel like the answer is yes.
thread_local! {
    static DROP_FOR_REAL: Cell<bool> = Cell::new(false);
}

/// Make any dropped `ForwardThroughAliased` actually drop their inner `T`.
///
/// When the return value is dropped, dropping `ForwardThroughAliased` will have no effect again.
///
/// # Safety
///
/// Only set this when any following `ForwardThroughAliased` that are dropped are no longer
/// aliased.
pub(crate) unsafe fn drop_copies() -> impl Drop {
    struct DropGuard;
    impl Drop for DropGuard {
        fn drop(&mut self) {
            DROP_FOR_REAL.with(|dfr| dfr.set(false));
        }
    }
    let guard = DropGuard;
    DROP_FOR_REAL.with(|dfr| dfr.set(true));
    guard
}

impl<T> Drop for ForwardThroughAliased<T> {
    fn drop(&mut self) {
        DROP_FOR_REAL.with(move |drop_for_real| {
            if drop_for_real.get() {
                // safety:
                //   MaybeUninit<T> was created from a valid T.
                //   That T has not been dropped (drop_copies is unsafe).
                //   T is no longer aliased (drop_copies is unsafe),
                //   so we are allowed to re-take ownership of the T.
                unsafe { std::ptr::drop_in_place(self.aliased.as_mut_ptr()) }
            }
        })
    }
}

impl<T> AsRef<T> for ForwardThroughAliased<T> {
    fn as_ref(&self) -> &T {
        // safety:
        //   MaybeUninit<T> was created from a valid T.
        //   That T has not been dropped (drop_copies is unsafe).
        //   All we have done to T is alias it. But, since we only give out &T
        //   (which should be legal anyway), we're fine.
        unsafe { &*self.aliased.as_ptr() }
    }
}

impl<T> Deref for ForwardThroughAliased<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

use std::hash::{Hash, Hasher};
impl<T> Hash for ForwardThroughAliased<T>
where
    T: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_ref().hash(state)
    }
}

use std::fmt;
impl<T> fmt::Debug for ForwardThroughAliased<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<T> PartialEq for ForwardThroughAliased<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }

    fn ne(&self, other: &Self) -> bool {
        self.as_ref().ne(other.as_ref())
    }
}

impl<T> Eq for ForwardThroughAliased<T> where T: Eq {}

impl<T> PartialOrd for ForwardThroughAliased<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }

    fn lt(&self, other: &Self) -> bool {
        self.as_ref().lt(other.as_ref())
    }

    fn le(&self, other: &Self) -> bool {
        self.as_ref().le(other.as_ref())
    }

    fn gt(&self, other: &Self) -> bool {
        self.as_ref().gt(other.as_ref())
    }

    fn ge(&self, other: &Self) -> bool {
        self.as_ref().ge(other.as_ref())
    }
}

impl<T> Ord for ForwardThroughAliased<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

use std::borrow::Borrow;
impl<T> Borrow<T> for ForwardThroughAliased<T> {
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
// What we _really_ want here is:
// ```
// impl<T, U> Borrow<U> for ForwardThroughAliased<T>
// where
//     T: Borrow<U>,
// {
//     fn borrow(&self) -> &U {
//         self.as_ref().borrow()
//     }
// }
// ```
// But unfortunately that won't work due to trait coherence.
// Instead, we manually write the nice Borrow impls from std.
// This won't help with custom Borrow impls, but gets you pretty far.
impl Borrow<str> for ForwardThroughAliased<String> {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}
impl Borrow<std::path::Path> for ForwardThroughAliased<std::path::PathBuf> {
    fn borrow(&self) -> &std::path::Path {
        self.as_ref()
    }
}
impl<T> Borrow<[T]> for ForwardThroughAliased<Vec<T>> {
    fn borrow(&self) -> &[T] {
        self.as_ref()
    }
}
impl<T> Borrow<T> for ForwardThroughAliased<Box<T>>
where
    T: ?Sized,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
impl<T> Borrow<T> for ForwardThroughAliased<std::sync::Arc<T>>
where
    T: ?Sized,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
impl<T> Borrow<T> for ForwardThroughAliased<std::rc::Rc<T>>
where
    T: ?Sized,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
