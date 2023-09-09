//! Types that aid in aliasing values across the two left-right data copies.
//!
//! This module primarily revolves around the [`Aliased`] type, and its associated [`DropBehavior`]
//! trait. The basic flow of using it is going to go as follows.
//!
//! In general, each value in your data structure should be stored wrapped in an `Aliased`, with an
//! associated type `D` that has `DropBehavior::DO_DROP` set to `false`. In
//! [`Absorb::absorb_first`], you then simply drop any removed `Aliased<T, D>` as normal. The
//! backing `T` will not be dropped.
//!
//! In [`Absorb::absorb_second`], you first cast your datastructure from
//!
//! ```rust,ignore
//! &mut DataStructure<Aliased<T, D>>
//! ```
//!
//! to
//!
//! ```rust,ignore
//! &mut DataStructure<Aliased<T, D2>>
//! ```
//!
//! where `<D2 as DropBehavior>::DO_DROP` is `true`. This time, any `Aliased<T>` that you drop
//! _will_ drop the inner `T`, but this should be safe since the only other alias was dropped in
//! `absorb_first`. This is where the invariant that `absorb_*` is deterministic becomes extremely
//! important!
//!
//! Sounds nice enough, right? Well, you have to be _really_ careful when working with this type.
//! There are two primary things to watch out for:
//!
//! ## Mismatched dropping
//!
//! If `absorb_first` and `absorb_second` do not drop _exactly_ the same aliased values for a given
//! operation from the oplog, unsoundness ensues. Specifically, what will happen is that
//! `absorb_first` does _not_ drop some aliased `t: T`, but `absorb_second` _does_. Since
//! `absorb_second` _assumes_ that `t` no longer has any alises (it expects that `absorb_first` got
//! rid of such an alias), it will drop the `T`. But that `T` is still in the "other" data copy,
//! and may still get accessed by readers, who will then be accessing a dropped value, which is
//! unsound.
//!
//! While it might seem like it is easy to ensure that `absorb_first` and `absorb_second` do the
//! same thing, it is not. A good example of this is non-deterministic (likely malicious)
//! implementations of traits that you'd _expect_ to be deterministic like `Hash` or `Eq`. Imagine
//! someone writes an implementation like:
//!
//! ```rust
//! use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
//! static SNEAKY: AtomicBool = AtomicBool::new(false);
//!
//! #[derive(Eq, Hash)]
//! struct Sneaky(Vec<u8>);
//! impl PartialEq for Sneaky {
//!     fn eq(&self, other: &Self) -> bool {
//!         if SNEAKY.swap(false, SeqCst) {
//!             false
//!         } else {
//!             self.0 == other.0
//!         }
//!     }
//! }
//! ```
//!
//! Will your `absorb_*` calls still do the same thing? If the answer is no, then your
//! datastructure is unsound.
//!
//! Every datastructure is different, so it is difficult to give good advice on how to achieve
//! determinism. My general advice is to never call user-defined methods in `absorb_second`. Call
//! them in `absorb_first`, and use the `&mut O` to stash the results in the oplog itself. That
//! way, in `absorb_second`, you can use those cached values instead. This may be hard to pull off
//! for complicated datastructures, but it does tend to guarantee determinism.
//!
//! If that is unrealistic, mark the constructor for your data structure as `unsafe`, with a safety
//! comment that says that the inner types must have deterministic implementations of certain
//! traits. It's not ideal, but hopefully consumers _know_ what types they are using your
//! datastructures with, and will be able to check that their implementations are indeed not
//! malicious.
//!
//! ## Unsafe casting
//!
//! The instructions above say to cast from
//!
//! ```rust,ignore
//! &mut DataStructure<Aliased<T, D>>
//! ```
//!
//! to
//!
//! ```rust,ignore
//! &mut DataStructure<Aliased<T, D2>>
//! ```
//!
//! That cast is unsafe, and rightly so! While it is _likely_ that the cast is safe, that is far
//! from obvious, and it's worth spending some time on why, since it has implications for how you
//! use `Aliased` in your own crate.
//!
//! The cast is only sound if the two types are laid out exactly the same in memory, but that is
//! harder to guarantee than you might expect. The Rust compiler does not guarantee that
//! `A<Aliased<T>>` and `A<T>` are laid out the same in memory for any arbitrary `A`, _even_ if
//! both `A` and `Aliased` are `#[repr(transparent)]`. The _primary_ reason for this is associated
//! types. Imagine that I write this code:
//!
//! ```rust,ignore
//! trait Wonky { type Weird; }
//! struct A<T: Wonky>(T, T::Weird);
//!
//! impl<T> Wonky for Aliased<T> { type Weird = u32; }
//! impl<T> Wonky for T { type Weird = u16; }
//! ```
//!
//! Clearly, these types will end up with different memory layouts, since one will contain a
//! `u32` and the other a `u16` (let's ignore the fact that this particular example requires
//! specialization). This, in turn, means that it is _not_ generally safe to transmute between a
//! wrapper around one type and that same wrapper around a different type with the same layout! You
//! can see this discussed in far more detail here if you're curious:
//!
//! <https://github.com/jonhoo/rust-evmap/pull/83#issuecomment-735504638>
//!
//! Now, if we can find a way to _guarantee_ that the types have the same layout, this problem
//! changes, but how might we go about this? Our saving grace is that we are casting between
//! `A<Aliased<T, D>>` and `A<Aliased<T, D2>>` where we control both `D` and `D2`. If we ensure
//! that both those types are private, there is no way for any code external to your crate can
//! implement a trait for one type but not the other. And thus there's no way (that I know of) for
//! making it unsound to cast between the types!
//!
//! Now, I only say that this is likely sound because the language does not actually give
//! this as a _guarantee_ at the moment. Though wiser minds [seem to suggest that this might
//! be okay](https://github.com/rust-lang/unsafe-code-guidelines/issues/35#issuecomment-735858397).
//!
//! But this warrants repeating: **your `D` types for `Aliased` _must_ be private**.

use alloc::{boxed::Box, string::String, vec::Vec};
use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::ops::Deref;

// Just to make the doc comment linking work.
#[allow(unused_imports)]
use crate::Absorb;

/// Dictates the dropping behavior for the implementing type when used with [`Aliased`].
pub trait DropBehavior {
    /// An [`Aliased<T, D>`](Aliased) will drop its inner `T` if and only if `D::DO_DROP` is `true`
    const DO_DROP: bool;
}

/// A `T` that is aliased.
///
/// You should be able to mostly ignore this type, as it can generally be treated exactly like a
/// `&T`. However, there are some minor exceptions around forwarding traits -- since `Aliased` is a
/// wrapper type around `T`, it cannot automatically forward traits it does not know about to `&T`.
/// This means that if your `&T` implements, say, `Serialize` or some custom `Borrow<Q>`,
/// `Aliased<T>` will not implement that same trait. You can work around this either by
/// implementing your trait specifically for `Aliased<T>` where possible, or by manually
/// dereferencing to get the `&T` before using the trait.
#[repr(transparent)]
pub struct Aliased<T, D>
where
    D: DropBehavior,
{
    aliased: MaybeUninit<T>,

    drop_behavior: PhantomData<D>,

    // We cannot implement Send just because T is Send since we're aliasing it.
    _no_auto_send: PhantomData<*const T>,
}

impl<T, D> Aliased<T, D>
where
    D: DropBehavior,
{
    /// Create an alias of the inner `T`.
    ///
    /// # Safety
    ///
    /// This method is only safe to call as long as you ensure that the alias is never used after
    /// an `Aliased<T, D>` of `self` where `D::DO_DROP` is `true` is dropped, **and** as long
    /// as no `&mut T` is ever given out while some `Aliased<T>` may still be used. The returned
    /// type assumes that it is always safe to dereference into `&T`, which would not be true if
    /// either of those invariants were broken.
    pub unsafe fn alias(&self) -> Self {
        // safety:
        //   We are aliasing T here, but it is okay because:
        //    a) the T is behind a MaybeUninit, and so will cannot be accessed safely; and
        //    b) we only expose _either_ &T while aliased, or &mut after the aliasing ends.
        Aliased {
            aliased: core::ptr::read(&self.aliased),
            drop_behavior: PhantomData,
            _no_auto_send: PhantomData,
        }
    }

    /// Construct an aliased value around a `T`.
    ///
    /// This method is safe since it effectively leaks `T`. Note that we do not implement `From<T>`
    /// because we do not want users to construct `Aliased<T>`s on their own. If they did, they
    /// would almost certain end up with incorrect drop behavior.
    pub fn from(t: T) -> Self {
        Self {
            aliased: MaybeUninit::new(t),
            drop_behavior: PhantomData,
            _no_auto_send: PhantomData,
        }
    }

    /// Turn this aliased `T` into one with a different drop behavior.
    ///
    /// # Safety
    ///
    /// It is always safe to change an `Aliased` from a dropping `D` to a non-dropping `D`. Going
    /// the other way around is only safe if `self` is the last alias for the `T`.
    pub unsafe fn change_drop<D2: DropBehavior>(self) -> Aliased<T, D2> {
        Aliased {
            // safety:
            aliased: core::ptr::read(&self.aliased),
            drop_behavior: PhantomData,
            _no_auto_send: PhantomData,
        }
    }
}

// Aliased gives &T across threads if shared or sent across thread boundaries.
// Aliased gives &mut T across threads (for drop) if sent across thread boundaries.
// This implies that we are only Send if T is Send+Sync, and Sync if T is Sync.
//
// Note that these bounds are stricter than what the compiler would auto-generate for the type.
unsafe impl<T, D> Send for Aliased<T, D>
where
    D: DropBehavior,
    T: Send + Sync,
{
}
unsafe impl<T, D> Sync for Aliased<T, D>
where
    D: DropBehavior,
    T: Sync,
{
}

impl<T, D> Drop for Aliased<T, D>
where
    D: DropBehavior,
{
    fn drop(&mut self) {
        if D::DO_DROP {
            // safety:
            //   MaybeUninit<T> was created from a valid T.
            //   That T has not been dropped (getting a Aliased<T, DoDrop> is unsafe).
            //   T is no longer aliased (by the safety assumption of getting a Aliased<T, DoDrop>),
            //   so we are allowed to re-take ownership of the T.
            unsafe { core::ptr::drop_in_place(self.aliased.as_mut_ptr()) }
        }
    }
}

impl<T, D> AsRef<T> for Aliased<T, D>
where
    D: DropBehavior,
{
    fn as_ref(&self) -> &T {
        // safety:
        //   MaybeUninit<T> was created from a valid T.
        //   That T has not been dropped (getting a Aliased<T, DoDrop> is unsafe).
        //   All we have done to T is alias it. But, since we only give out &T
        //   (which should be legal anyway), we're fine.
        unsafe { &*self.aliased.as_ptr() }
    }
}

impl<T, D> Deref for Aliased<T, D>
where
    D: DropBehavior,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

use core::hash::{Hash, Hasher};
impl<T, D> Hash for Aliased<T, D>
where
    D: DropBehavior,
    T: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_ref().hash(state)
    }
}

use core::fmt;
impl<T, D> fmt::Debug for Aliased<T, D>
where
    D: DropBehavior,
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl<T, D> PartialEq for Aliased<T, D>
where
    D: DropBehavior,
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<T, D> Eq for Aliased<T, D>
where
    D: DropBehavior,
    T: Eq,
{
}

impl<T, D> PartialOrd for Aliased<T, D>
where
    D: DropBehavior,
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
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

impl<T, D> Ord for Aliased<T, D>
where
    D: DropBehavior,
    T: Ord,
{
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

use core::borrow::Borrow;
impl<T, D> Borrow<T> for Aliased<T, D>
where
    D: DropBehavior,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
// What we _really_ want here is:
// ```
// impl<T, D> Borrow<D> for Aliased<T>
// where
//     T: Borrow<D>,
// {
//     fn borrow(&self) -> &U {
//         self.as_ref().borrow()
//     }
// }
// ```
// But unfortunately that won't work due to trait coherence.
// Instead, we manually write the nice Borrow impls from std.
// This won't help with custom Borrow impls, but gets you pretty far.
impl<D> Borrow<str> for Aliased<String, D>
where
    D: DropBehavior,
{
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

#[cfg(feature = "std")]
impl<D> Borrow<std::path::Path> for Aliased<std::path::PathBuf, D>
where
    D: DropBehavior,
{
    fn borrow(&self) -> &std::path::Path {
        self.as_ref()
    }
}
impl<T, D> Borrow<[T]> for Aliased<Vec<T>, D>
where
    D: DropBehavior,
{
    fn borrow(&self) -> &[T] {
        self.as_ref()
    }
}
impl<T, D> Borrow<T> for Aliased<Box<T>, D>
where
    T: ?Sized,
    D: DropBehavior,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
impl<T, D> Borrow<T> for Aliased<alloc::sync::Arc<T>, D>
where
    T: ?Sized,
    D: DropBehavior,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
impl<T, D> Borrow<T> for Aliased<alloc::rc::Rc<T>, D>
where
    T: ?Sized,
    D: DropBehavior,
{
    fn borrow(&self) -> &T {
        self.as_ref()
    }
}
