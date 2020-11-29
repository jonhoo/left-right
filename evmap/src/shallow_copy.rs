//! Types that can be cheaply aliased.

use std::cell::Cell;
use std::mem;
use std::ops::{Deref, DerefMut};

/// Types that implement this trait can be cheaply copied by (potentially) aliasing the data they
/// contain. Only the _last_ shallow copy will be dropped -- all others will be silently leaked
/// (with `mem::forget`).
///
/// To implement this trait for your own `Copy` type, write:
///
/// ```rust
/// # use evmap::ShallowCopy;
/// use std::mem::ManuallyDrop;
///
/// #[derive(Copy, Clone)]
/// struct T;
///
/// impl ShallowCopy for T {
///     unsafe fn shallow_copy(&self) -> ManuallyDrop<Self> {
///         ManuallyDrop::new(*self)
///     }
/// }
/// ```
///
/// If you have a non-`Copy` type, the value returned by `shallow_copy` should point to the same
/// data as the `&mut self`, and it should be safe to `mem::forget` either of the copies as long as
/// the other is dropped normally afterwards.
///
/// For complex, non-`Copy` types, you can place the type behind a wrapper that implements
/// `ShallowCopy` such as `Arc`.
/// Alternatively, if your type is made up of types that all implement `ShallowCopy`, consider
/// using the `evmap-derive` crate, which contains a derive macro for `ShallowCopy`.
/// See that crate's documentation for details.
///
/// Send + Sync + Hash + Eq etc. must translate (think Borrow)
pub trait ShallowCopy {
    type SplitType;
    type Target: ?Sized;

    /// Perform an aliasing copy of this value.
    ///
    /// # Safety
    ///
    /// The use of this method is *only* safe if the values involved are never mutated, and only
    /// one of the copies is dropped; the remaining copies must be forgotten with `mem::forget`.
    fn split(self) -> (Self::SplitType, Self::SplitType);

    fn deref_self(&self) -> &Self::Target;

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target;

    unsafe fn drop(split: &mut Self::SplitType);
}

pub(crate) enum MaybeShallowCopied<T>
where
    T: ShallowCopy,
{
    Owned(T),
    Copied(T::SplitType),
    Swapping,
}

impl<T> MaybeShallowCopied<T>
where
    T: ShallowCopy,
{
    // unsafe because shallow copy must remain dereferencable for lifetime of return value.
    pub(crate) unsafe fn shallow_copy_first(&mut self) -> ForwardThroughShallowCopy<T> {
        match mem::replace(self, MaybeShallowCopied::Swapping) {
            MaybeShallowCopied::Owned(t) => {
                let (a, b) = t.split();
                *self = MaybeShallowCopied::Copied(a);
                ForwardThroughShallowCopy::Split(b)
            }
            MaybeShallowCopied::Copied(_) => unreachable!(),
            MaybeShallowCopied::Swapping => unreachable!(),
        }
    }

    // unsafe because shallow copy must remain dereferencable for lifetime of return value.
    pub(crate) unsafe fn shallow_copy_second(self) -> ForwardThroughShallowCopy<T> {
        match self {
            MaybeShallowCopied::Copied(split) => ForwardThroughShallowCopy::Split(split),
            MaybeShallowCopied::Owned(_) => unreachable!(),
            MaybeShallowCopied::Swapping => unreachable!(),
        }
    }
}

impl<T> fmt::Debug for MaybeShallowCopied<T>
where
    T: ShallowCopy + fmt::Debug,
    T::Target: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MaybeShallowCopied::Owned(ref t) => t.fmt(f),
            MaybeShallowCopied::Copied(ref split) => unsafe { T::deref(split) }.fmt(f),
            MaybeShallowCopied::Swapping => unreachable!(),
        }
    }
}

use std::sync::Arc;
impl<T> ShallowCopy for Arc<T>
where
    T: ?Sized,
{
    type SplitType = *const T;
    type Target = T;

    fn split(self) -> (Self::SplitType, Self::SplitType) {
        let ptr = Arc::into_raw(self);
        (ptr, ptr)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        &**split
    }

    unsafe fn drop(split: &mut Self::SplitType) {
        Arc::from_raw(*split);
    }
}

use std::rc::Rc;
impl<T> ShallowCopy for Rc<T>
where
    T: ?Sized,
{
    type SplitType = *const T;
    type Target = T;

    fn split(self) -> (Self::SplitType, Self::SplitType) {
        let ptr = Rc::into_raw(self);
        (ptr, ptr)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        &**split
    }

    unsafe fn drop(split: &mut Self::SplitType) {
        Rc::from_raw(*split);
    }
}

// Aliasing Box<T> is not okay:
// https://github.com/rust-lang/unsafe-code-guidelines/issues/258
// https://github.com/jonhoo/rust-evmap/issues/74
//
// Also, this cases from a non-mutable pointer to a mutable one, which is never okay.

impl<T> ShallowCopy for Box<T>
where
    T: ?Sized,
{
    type SplitType = *mut T;
    type Target = T;

    fn split(self) -> (Self::SplitType, Self::SplitType) {
        let ptr = Box::into_raw(self);
        (ptr, ptr)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        &**split
    }

    unsafe fn drop(split: &mut Self::SplitType) {
        Box::from_raw(split);
    }
}

// We need GAT for Option to implement ShallowCopy. Bleh...
// The reason is that we need Target to be Target<'a> where the 'a is assigned in deref.
// Otherwise, Option's deref has to return &Option<T::Target>, which it obviously cannot.
//
// impl<T> ShallowCopy for Option<T>
// where
//     T: ShallowCopy,
//     T::Target: Sized,
// {
//     type SplitType = Option<T::SplitType>;
//     type Target = Option<T::Target>;
//
//     fn split(self) -> (Self::SplitType, Self::SplitType) {
//         if let Some(this) = self {
//             let (a, b) = this.split();
//             (Some(a), Some(b))
//         } else {
//             (None, None)
//         }
//     }
//
//     unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
//         split.map(|st| unsafe { T::deref(st) })
//     }
//
//     unsafe fn drop(split: &mut Self::SplitType) {
//         split.map(|st| unsafe { T::drop(st) });
//     }
// }

impl ShallowCopy for String {
    type SplitType = (*mut u8, usize, usize);
    type Target = str;

    fn split(mut self) -> (Self::SplitType, Self::SplitType) {
        let len = self.len();
        let cap = self.capacity();
        // safety: safe because we will not mutate the string.
        let buf = unsafe { self.as_bytes_mut() }.as_mut_ptr();
        let split = (buf, len, cap);
        (split, split)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        let u8s = std::slice::from_raw_parts(split.0, split.1);
        std::str::from_utf8_unchecked(u8s)
    }

    unsafe fn drop(split: &mut Self::SplitType) {
        String::from_raw_parts(split.0, split.1, split.2);
    }
}

impl<T> ShallowCopy for Vec<T> {
    type SplitType = (*mut T, usize, usize);
    type Target = [T];

    fn split(mut self) -> (Self::SplitType, Self::SplitType) {
        let buf = self.as_mut_ptr();
        let len = self.len();
        let cap = self.capacity();
        let split = (buf, len, cap);
        (split, split)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        std::slice::from_raw_parts(split.0, split.1)
    }

    unsafe fn drop(split: &mut Self::SplitType) {
        Vec::from_raw_parts(split.0, split.1, split.2);
    }
}

#[cfg(feature = "bytes")]
impl ShallowCopy for bytes::Bytes {
    unsafe fn shallow_copy(&self) -> ManuallyDrop<Self> {
        let len = self.len();
        let buf: &'static [u8] = std::slice::from_raw_parts(self.as_ptr(), len);
        ManuallyDrop::new(bytes::Bytes::from_static(buf))
    }
}

impl<'a, T> ShallowCopy for &'a T
where
    T: ?Sized,
{
    type SplitType = *const T;
    type Target = T;

    fn split(self) -> (Self::SplitType, Self::SplitType) {
        let ptr: Self::SplitType = self;
        (ptr, ptr)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        &**split
    }

    unsafe fn drop(_: &mut Self::SplitType) {}
}

/// If you are willing to have your values be copied between the two views of the `evmap`,
/// wrap them in this type.
///
/// This is effectively a way to bypass the `ShallowCopy` optimization.
/// Note that you do not need this wrapper for most `Copy` primitives.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Default)]
#[repr(transparent)]
pub struct CopyValue<T>(T);

impl<T: Copy> From<T> for CopyValue<T> {
    fn from(t: T) -> Self {
        CopyValue(t)
    }
}

impl<T> Deref for CopyValue<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CopyValue<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> ShallowCopy for CopyValue<T>
where
    T: Copy,
{
    type SplitType = T;
    type Target = T;

    fn split(self) -> (Self::SplitType, Self::SplitType) {
        (self.0, self.0)
    }

    fn deref_self(&self) -> &Self::Target {
        &*self
    }

    unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
        split
    }

    unsafe fn drop(_: &mut Self::SplitType) {}
}

macro_rules! impl_shallow_copy_for_copy_primitives {
    ($($t:ty)*) => ($(
        impl ShallowCopy for $t {
            type SplitType = $t;
            type Target = $t;

            fn split(self) -> (Self::SplitType, Self::SplitType) {
                (self, self)
            }

            fn deref_self(&self) -> &Self::Target {
                &*self
            }

            unsafe fn deref(split: &Self::SplitType) -> &Self::Target {
                split
            }

            unsafe fn drop(_: &mut Self::SplitType) {}
        }
    )*)
}

impl_shallow_copy_for_copy_primitives!(() bool char usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64);

// Only public since it appears in the (doc-hidden) variants of `values::ValuesIter`.
#[doc(hidden)]
pub enum ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
{
    Split(T::SplitType),
    Ref(*const T::Target),
}

// safety: ShallowCopy requires that the split preserves Send + Sync.
// since we only ever give out &T, it is okay to Send+Sync us as long as T is Sync
unsafe impl<T> Send for ForwardThroughShallowCopy<T> where T: ShallowCopy + Sync {}
unsafe impl<T> Sync for ForwardThroughShallowCopy<T> where T: ShallowCopy + Sync {}

impl<T> ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
{
    // unsafe because return value must not be used after the lifetime of the reference ends.
    pub(crate) unsafe fn from_ref(t: &T::Target) -> Self {
        ForwardThroughShallowCopy::Ref(t)
    }
}

thread_local! {
    static DROP_FOR_REAL: Cell<bool> = Cell::new(false);
}

pub(crate) unsafe fn drop_copies(yes: bool) {
    DROP_FOR_REAL.with(|dfr| dfr.set(yes))
}

impl<T> Drop for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
{
    fn drop(&mut self) {
        DROP_FOR_REAL.with(move |drop_for_real| {
            if drop_for_real.get() {
                if let ForwardThroughShallowCopy::Split(s) = self {
                    unsafe { T::drop(s) };
                }
            }
        })
    }
}

impl<T> Deref for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
{
    type Target = T::Target;
    fn deref(&self) -> &Self::Target {
        match self {
            ForwardThroughShallowCopy::Split(s) => unsafe { T::deref(s) },
            ForwardThroughShallowCopy::Ref(r) => unsafe { &**r },
        }
    }
}

impl<T> AsRef<T::Target> for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
{
    fn as_ref(&self) -> &T::Target {
        match self {
            ForwardThroughShallowCopy::Split(s) => unsafe { T::deref(s) },
            ForwardThroughShallowCopy::Ref(r) => unsafe { &**r },
        }
    }
}

use std::hash::{Hash, Hasher};
impl<T> Hash for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
    T::Target: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_ref().hash(state)
    }
}

use std::fmt;
impl<T> fmt::Debug for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
    T::Target: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<T> PartialEq for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
    T::Target: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }

    fn ne(&self, other: &Self) -> bool {
        self.as_ref().ne(other.as_ref())
    }
}

impl<T> Eq for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
    T::Target: Eq,
{
}

impl<T> PartialOrd for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
    T::Target: PartialOrd,
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

impl<T> Ord for ForwardThroughShallowCopy<T>
where
    T: ShallowCopy,
    T::Target: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}
