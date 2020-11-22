#![warn(
    // missing_docs,
    rust_2018_idioms,
    missing_debug_implementations,
    broken_intra_doc_links
)]
#![allow(clippy::type_complexity)]

use std::sync::{atomic, Arc, Mutex};

pub(crate) type Epochs = Arc<Mutex<slab::Slab<Arc<atomic::AtomicU64>>>>;

mod write;
pub use crate::write::WriteHandle;

mod read;
pub use crate::read::{ReadGuard, ReadHandle};

/// NOTE: Document that operations must be deterministic.
pub unsafe trait Absorb<O> {
    /// Apply ops in such a way that no values are dropped, only forgotten
    fn absorb_first(&mut self, operation: &mut O, other: &Self);

    /// Apply operations while allowing dropping of values
    fn absorb_second(&mut self, operation: O, other: &Self);

    /// Drop `Self`, but do not drop any shared values.
    fn drop_first(self: Box<Self>);
}

unsafe impl<T, O> Absorb<O> for Box<T>
where
    T: Absorb<O>,
{
    fn absorb_first(&mut self, operation: &mut O, other: &Self) {
        T::absorb_first(self, operation, other)
    }

    fn absorb_second(&mut self, operation: O, other: &Self) {
        T::absorb_second(self, operation, other)
    }

    fn drop_first(self: Box<Self>) {
        T::drop_first(*self)
    }
}

/// NOTE: The `T` must be empty, as otherwise `apply_first` would forget, not drop, initial values
/// from one of the two halves.
#[allow(clippy::type_complexity)]
pub fn new_from_empty<T, O>(t: T) -> (ReadHandle<T>, WriteHandle<T, O>)
where
    T: Absorb<O> + Clone,
{
    let epochs = Default::default();

    let r = ReadHandle::new(t.clone(), Arc::clone(&epochs));
    let w = WriteHandle::new(t, epochs, r.clone());
    (r, w)
}

#[allow(clippy::type_complexity)]
pub fn new<T, O>() -> (ReadHandle<T>, WriteHandle<T, O>)
where
    T: Absorb<O> + Default + Clone,
{
    new_from_empty(T::default())
}

#[cfg(test)]
struct CounterAddOp(i32);

#[cfg(test)]
unsafe impl Absorb<CounterAddOp> for i32 {
    fn absorb_first(&mut self, operation: &mut CounterAddOp, _: &Self) {
        *self += operation.0;
    }

    fn absorb_second(&mut self, operation: CounterAddOp, _: &Self) {
        *self += operation.0;
    }

    fn drop_first(self: Box<Self>) {}
}
