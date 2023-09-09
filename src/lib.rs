//! A concurrency primitive for high concurrency reads over a single-writer data structure.
//!
//! The primitive keeps two copies of the backing data structure, one that is accessed by readers,
//! and one that is accessed by the (single) writer. This enables all reads to proceed in parallel
//! with minimal coordination, and shifts the coordination overhead to the writer. In the absence
//! of writes, reads scale linearly with the number of cores.
//!
//! When the writer wishes to expose new changes to the datastructure (see
//! [`WriteHandle::publish`]), it "flips" the two copies so that subsequent reads go to the old
//! "write side", and future writers go to the old "read side". This process does cause two cache
//! line invalidations for the readers, but does not stop them from making progress (i.e., reads
//! are wait-free).
//!
//! In order to keep both copies up to date, left-right keeps an operational log ("oplog") of all
//! the modifications to the data structure, which it uses to bring the old read data up to date
//! with the latest writes on a flip. Since there are two copies of the data, each oplog entry is
//! applied twice: once to the write copy and again to the (stale) read copy.
//!
//! # Trade-offs
//!
//! Few concurrency wins come for free, and this one is no exception. The drawbacks of this
//! primitive are:
//!
//!  - **Increased memory use**: since we keep two copies of the backing data structure, we are
//!  effectively doubling the memory use of the underlying data. With some clever de-duplication,
//!  this cost can be ameliorated to some degree, but it's something to be aware of. Furthermore,
//!  if writers only call `publish` infrequently despite adding many writes to the operational log,
//!  the operational log itself may grow quite large, which adds additional overhead.
//!  - **Deterministic operations**: as the entries in the operational log are applied twice, once
//!  to each copy of the data, it is essential that the operations are deterministic. If they are
//!  not, the two copies will no longer mirror one another, and will continue to diverge over time.
//!  - **Single writer**: left-right only supports a single writer. To have multiple writers, you
//!  need to ensure exclusive access to the [`WriteHandle`] through something like a
//!  [`Mutex`](std::sync::Mutex).
//!  - **Slow writes**: Writes through left-right are slower than they would be directly against
//!  the backing datastructure. This is both because they have to go through the operational log,
//!  and because they must each be applied twice.
//!
//! # How does it work?
//!
//! Take a look at [this YouTube video](https://www.youtube.com/watch?v=eLNAMEoKAAc) which goes
//! through the basic concurrency algorithm, as well as the initial development of this library.
//! Alternatively, there's a shorter (but also less complete) description in [this
//! talk](https://www.youtube.com/watch?v=s19G6n0UjsM&t=1994).
//!
//! At a glance, left-right is implemented using two regular `T`s, an operational log, epoch
//! counting, and some pointer magic. There is a single pointer through which all readers go. It
//! points to a `T` that the readers access in order to read data. Every time a read has accessed
//! the pointer, they increment a local epoch counter, and they update it again when they have
//! finished the read. When a write occurs, the writer updates the other `T` (for which there are
//! no readers), and also stores a copy of the change in a log. When [`WriteHandle::publish`] is
//! called, the writer, atomically swaps the reader pointer to point to the other `T`. It then
//! waits for the epochs of all current readers to change, and then replays the operational log to
//! bring the stale copy up to date.
//!
//! The design resembles this [left-right concurrency
//! scheme](https://hal.archives-ouvertes.fr/hal-01207881/document) from 2015, though I am not
//! aware of any follow-up to that work.
//!
//! # How do I use it?
//!
//! If you just want a data structure for fast reads, you likely want to use a crate that _uses_
//! this crate, like [`evmap`](https://docs.rs/evmap/). If you want to develop such a crate
//! yourself, here's what you do:
//!
//! ```rust
//! use left_right::{Absorb, ReadHandle, WriteHandle};
//!
//! // First, define an operational log type.
//! // For most real-world use-cases, this will be an `enum`, but we'll keep it simple:
//! struct CounterAddOp(i32);
//!
//! // Then, implement the unsafe `Absorb` trait for your data structure type,
//! // and provide the oplog type as the generic argument.
//! // You can read this as "`i32` can absorb changes of type `CounterAddOp`".
//! impl Absorb<CounterAddOp> for i32 {
//!     // See the documentation of `Absorb::absorb_first`.
//!     //
//!     // Essentially, this is where you define what applying
//!     // the oplog type to the datastructure does.
//!     fn absorb_first(&mut self, operation: &mut CounterAddOp, _: &Self) {
//!         *self += operation.0;
//!     }
//!
//!     // See the documentation of `Absorb::absorb_second`.
//!     //
//!     // This may or may not be the same as `absorb_first`,
//!     // depending on whether or not you de-duplicate values
//!     // across the two copies of your data structure.
//!     fn absorb_second(&mut self, operation: CounterAddOp, _: &Self) {
//!         *self += operation.0;
//!     }
//!
//!     // See the documentation of `Absorb::drop_first`.
//!     fn drop_first(self: Box<Self>) {}
//!
//!     fn sync_with(&mut self, first: &Self) {
//!         *self = *first
//!     }
//! }
//!
//! // Now, you can construct a new left-right over an instance of your data structure.
//! // This will give you a `WriteHandle` that accepts writes in the form of oplog entries,
//! // and a (cloneable) `ReadHandle` that gives you `&` access to the data structure.
//! let (write, read) = left_right::new::<i32, CounterAddOp>();
//!
//! // You will likely want to embed these handles in your own types so that you can
//! // provide more ergonomic methods for performing operations on your type.
//! struct Counter(WriteHandle<i32, CounterAddOp>);
//! impl Counter {
//!     // The methods on you write handle type will likely all just add to the operational log.
//!     pub fn add(&mut self, i: i32) {
//!         self.0.append(CounterAddOp(i));
//!     }
//!
//!     // You should also provide a method for exposing the results of any pending operations.
//!     //
//!     // Until this is called, any writes made since the last call to `publish` will not be
//!     // visible to readers. See `WriteHandle::publish` for more details. Make sure to call
//!     // this out in _your_ documentation as well, so that your users will be aware of this
//!     // "weird" behavior.
//!     pub fn publish(&mut self) {
//!         self.0.publish();
//!     }
//! }
//!
//! // Similarly, for reads:
//! #[derive(Clone)]
//! struct CountReader(ReadHandle<i32>);
//! impl CountReader {
//!     pub fn get(&self) -> i32 {
//!         // The `ReadHandle` itself does not allow you to access the underlying data.
//!         // Instead, you must first "enter" the data structure. This is similar to
//!         // taking a `Mutex`, except that no lock is actually taken. When you enter,
//!         // you are given back a guard, which gives you shared access (through the
//!         // `Deref` trait) to the "read copy" of the data structure.
//!         //
//!         // Note that `enter` may yield `None`, which implies that the `WriteHandle`
//!         // was dropped, and took the backing data down with it.
//!         //
//!         // Note also that for as long as the guard lives, a writer that tries to
//!         // call `WriteHandle::publish` will be blocked from making progress.
//!         self.0.enter().map(|guard| *guard).unwrap_or(0)
//!     }
//! }
//!
//! // These wrapper types are likely what you'll give out to your consumers.
//! let (mut w, r) = (Counter(write), CountReader(read));
//!
//! // They can then use the type fairly ergonomically:
//! assert_eq!(r.get(), 0);
//! w.add(1);
//! // no call to publish, so read side remains the same:
//! assert_eq!(r.get(), 0);
//! w.publish();
//! assert_eq!(r.get(), 1);
//! drop(w);
//! // writer dropped data, so reads yield fallback value:
//! assert_eq!(r.get(), 0);
//! ```
//!
//! One additional noteworthy detail: much like with `Mutex`, `RwLock`, and `RefCell` from the
//! standard library, the values you dereference out of a `ReadGuard` are tied to the lifetime of
//! that `ReadGuard`. This can make it awkward to write ergonomic methods on the read handle that
//! return references into the underlying data, and may tempt you to clone the data out or take a
//! closure instead. Instead, consider using [`ReadGuard::map`] and [`ReadGuard::try_map`], which
//! (like `RefCell`'s [`Ref::map`](std::cell::Ref::map)) allow you to provide a guarded reference
//! deeper into your data structure.
#![warn(
    missing_docs,
    rust_2018_idioms,
    missing_debug_implementations,
    broken_intra_doc_links
)]
#![allow(clippy::type_complexity)]

mod sync;

use crate::sync::{Arc, AtomicUsize, Mutex};

type Epochs = Arc<Mutex<slab::Slab<Arc<AtomicUsize>>>>;

mod write;
pub use crate::write::Taken;
pub use crate::write::WriteHandle;

mod read;
pub use crate::read::{ReadGuard, ReadHandle, ReadHandleFactory};

pub mod aliasing;

/// The result of calling [`Absorb::try_compress`](Absorb::try_compress).
#[derive(Debug)]
pub enum TryCompressResult<O> {
    /// Returned when [`try_compress`](Absorb::try_compress) was successful.
    ///
    /// The expectation is that the `prev` argument to `try_compress` now represents the combined operation after consuming `next`.
    ///
    /// Compression will continue by attempting to combine the new `prev` with its predecessors.
    Compressed,
    /// The two operations passed to [`try_compress`](Absorb::try_compress) were not combined, but commute.
    ///
    /// Since `prev` and `next` commute, compression will continue attempting to merge `next` with operations that precede `prev` in the oplog, potentially changing the relative ordering of `prev` and `next`.
    ///
    /// Returns ownership of `next` so that compression can continue (or `next` can be restored).
    Independent(O),
    /// The two operations passed to [`try_compress`](Absorb::try_compress) were not combined, and do not commute.
    ///
    /// Since `prev` and `next` do not commute, `next` cannot be moved past `prev`, and must thus be left in its current location.
    ///
    /// Returns ownership of `next` so that it can be put back in the oplog (after `prev`).
    Dependent(O),
}

/// Types that can incorporate operations of type `O`.
///
/// This trait allows `left-right` to keep the two copies of the underlying data structure (see the
/// [crate-level documentation](crate)) the same over time. Each write operation to the data
/// structure is logged as an operation of type `O` in an _operational log_ (oplog), and is applied
/// once to each copy of the data.
///
/// Implementations should ensure that the absorbption of each `O` is deterministic. That is, if
/// two instances of the implementing type are initially equal, and then absorb the same `O`,
/// they should remain equal afterwards. If this is not the case, the two copies will drift apart
/// over time, and hold different values.
///
/// The trait provides separate methods for the first and second absorption of each `O`. For many
/// implementations, these will be the same (which is why `absorb_second` defaults to calling
/// `absorb_first`), but not all. In particular, some implementations may need to modify the `O` to
/// ensure deterministic results when it is applied to the second copy. Or, they may need to
/// ensure that removed values in the data structure are only dropped when they are removed from
/// _both_ copies, in case they alias the backing data to save memory.
///
/// For the same reason, `Absorb` allows implementors to define `drop_first`, which is used to drop
/// the first of the two copies. In this case, de-duplicating implementations may need to forget
/// values rather than drop them so that they are not dropped twice when the second copy is
/// dropped.
pub trait Absorb<O> {
    /// Apply `O` to the first of the two copies.
    ///
    /// `other` is a reference to the other copy of the data, which has seen all operations up
    /// until the previous call to [`WriteHandle::publish`]. That is, `other` is one "publish
    /// cycle" behind.
    fn absorb_first(&mut self, operation: &mut O, other: &Self);

    /// Apply `O` to the second of the two copies.
    ///
    /// `other` is a reference to the other copy of the data, which has seen all operations up to
    /// the call to [`WriteHandle::publish`] that initially exposed this `O`. That is, `other` is
    /// one "publish cycle" ahead.
    ///
    /// Note that this method should modify the underlying data in _exactly_ the same way as
    /// `O` modified `other`, otherwise the two copies will drift apart. Be particularly mindful of
    /// non-deterministic implementations of traits that are often assumed to be deterministic
    /// (like `Eq` and `Hash`), and of "hidden states" that subtly affect results like the
    /// `RandomState` of a `HashMap` which can change iteration order.
    ///
    /// Defaults to calling `absorb_first`.
    fn absorb_second(&mut self, mut operation: O, other: &Self) {
        Self::absorb_first(self, &mut operation, other)
    }

    /// Drop the first of the two copies.
    ///
    /// Defaults to calling `Self::drop`.
    #[allow(clippy::boxed_local)]
    fn drop_first(self: Box<Self>) {}

    /// Drop the second of the two copies.
    ///
    /// Defaults to calling `Self::drop`.
    #[allow(clippy::boxed_local)]
    fn drop_second(self: Box<Self>) {}

    /// Sync the data from `first` into `self`.
    ///
    /// To improve initialization performance, before the first call to `publish` changes aren't
    /// added to the internal oplog, but applied to the first copy directly using `absorb_second`.
    /// The first `publish` then calls `sync_with` instead of `absorb_second`.
    ///
    /// `sync_with` should ensure that `self`'s state exactly matches that of `first` after it
    /// returns. Be particularly mindful of non-deterministic implementations of traits that are
    /// often assumed to be deterministic (like `Eq` and `Hash`), and of "hidden states" that
    /// subtly affect results like the `RandomState` of a `HashMap` which can change iteration
    /// order.
    fn sync_with(&mut self, first: &Self);

    /// Range at which [`WriteHandle`] tries to compress the oplog, reset each time a compression succeeds.
    ///
    /// Can be used to avoid having insertion into the oplog be O(oplog.len * ops.len) if it is filled with mainly independent ops.
    ///
    /// Defaults to `0`, which disables compression and allows the usage of an efficient fallback.
    const MAX_COMPRESS_RANGE: usize = 0;

    /// Try to compress two ops into a single op and return a [`TryCompressResult`].
    ///
    /// Used to optimize the oplog while extending it, `prev` is the target inside the oplog, `next` is the op being inserted.
    ///
    /// Defaults to [`TryCompressResult::Dependent`], which sub-optimally disables compression.
    /// Setting [`Self::MAX_COMPRESS_RANGE`](Absorb::MAX_COMPRESS_RANGE) to or leaving it at it's default of `0` is vastly more efficient for that.
    fn try_compress(prev: &mut O, next: O) -> TryCompressResult<O> {
        // yes, unnecessary, but: makes it so that prev is not an unused variable
        // and really matches the mental model of 'all ops are dependent'.
        match prev {
            _ => TryCompressResult::Dependent(next),
        }
    }
}

/// Construct a new write and read handle pair from an empty data structure.
///
/// The type must implement `Clone` so we can construct the second copy from the first.
pub fn new_from_empty<T, O>(t: T) -> (WriteHandle<T, O>, ReadHandle<T>)
where
    T: Absorb<O> + Clone,
{
    let epochs = Default::default();

    let r = ReadHandle::new(t.clone(), Arc::clone(&epochs));
    let w = WriteHandle::new(t, epochs, r.clone());
    (w, r)
}

/// Construct a new write and read handle pair from the data structure default.
///
/// The type must implement `Default` so we can construct two empty instances. You must ensure that
/// the trait's `Default` implementation is deterministic and idempotent - that is to say, two
/// instances created by it must behave _exactly_ the same. An example of where this is problematic
/// is `HashMap` - due to `RandomState`, two instances returned by `Default` may have a different
/// iteration order.
///
/// If your type's `Default` implementation does not guarantee this, you can use `new_from_empty`,
/// which relies on `Clone` instead of `Default`.
pub fn new<T, O>() -> (WriteHandle<T, O>, ReadHandle<T>)
where
    T: Absorb<O> + Default,
{
    let epochs = Default::default();

    let r = ReadHandle::new(T::default(), Arc::clone(&epochs));
    let w = WriteHandle::new(T::default(), epochs, r.clone());
    (w, r)
}
