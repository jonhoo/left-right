//! Operations on maps and value-sets

use std::borrow::Cow;
use std::cell::{Cell, UnsafeCell};
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::{fmt, mem, ptr};

use hashbrown::hash_map::RawEntryMut;

#[cfg(feature = "smallvec")]
use smallvec;

use super::ShallowCopy;
use inner::{Inner, Values};

/// Unary predicate used to retain elements
pub struct Predicate<V>(pub(crate) Arc<Fn(&V) -> bool + Send + Sync>);

impl<V> Predicate<V> {
    /// Evaluate the predicate for the given element
    #[inline]
    pub fn eval(&self, value: &V) -> bool {
        (*self.0)(value)
    }
}

impl<V> Clone for Predicate<V> {
    #[inline]
    fn clone(&self) -> Self {
        Predicate(self.0.clone())
    }
}

impl<V> PartialEq for Predicate<V> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<V> Eq for Predicate<V> {}

impl<V> fmt::Debug for Predicate<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Predicate")
            .field(&format_args!("{:p}", &*self.0 as *const _))
            .finish()
    }
}

/// Closure that will modify an individual value-set
pub struct Modifier<V>(pub(crate) Arc<for<'a> Fn(&mut Modify<'a, V>) + Send + Sync>);

impl<V> Modifier<V> {
    #[inline]
    fn modify(&self, value: &mut Modify<V>) {
        (*self.0)(value)
    }
}

impl<V> Clone for Modifier<V> {
    #[inline]
    fn clone(&self) -> Self {
        Modifier(self.0.clone())
    }
}

impl<V> PartialEq for Modifier<V> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<V> Eq for Modifier<V> {}

impl<V> fmt::Debug for Modifier<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Modifier")
            .field(&format_args!("{:p}", &*self.0 as *const _))
            .finish()
    }
}

/// A pending operation on a single value-set in the map
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ValueOperation<V> {
    /// Replace the set of entries for this key with this value.
    Replace(V),
    /// Add this value to the set of entries for this key.
    Add(V),
    /// Remove this value from the set of entries for this key, without preserving the
    /// order of elements in the value-set
    Remove(V),

    /// Remove this value from the set of entries for this key, preserving order of the
    /// elements in the value-set
    RemoveStable(V),

    /// Remove all values in the value set for this key.
    Clear,

    /// Remove the value set for this key.
    Empty,

    /// Retains all values matching the given predicate.
    Retain(Predicate<V>),
    /// Shrinks a value-set to it's minimum necessary size, freeing memory
    /// and potentially improving cache locality.
    ///
    /// If no key is given, all value-sets will shrink to fit.
    Fit,
    /// Reserves capacity for some number of additional elements in a value-set,
    /// or creates an empty value-set for this key with the given capacity if
    /// it doesn't already exist.
    ///
    /// This can improve performance by pre-allocating space for large value-sets.
    Reserve(usize),

    /// Arbitrary modification
    Modify(Modifier<V>),
}

/// A pending global map operation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MapOperation<V> {
    /// Applies `Fit` to all value-sets
    FitAll,

    /// Removes empty value-sets from the map
    Prune,

    /// Arbitrary modification for every value-set in the map
    ForEach(Modifier<V>),
}

/// A pending map operation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Operation<K, V> {
    /// A pending operation on a single value-set in the map
    Value {
        /// Key of the value-set in the map
        key: K,
        /// Operation to apply to that value-set
        op: ValueOperation<V>,
    },

    /// A pending global map operation.
    Map {
        /// Operation to apply to the entire map
        op: MapOperation<V>,
    },
}

/// The concept for this is that the "first" version can only borrow the data,
/// while the "second" version can consume it permenenantly.
///
/// However, they have to work together and sometimes overlap. Therefore,
/// it must be possible to do both, in any order.
///
/// It does this through two flags, where the first simply mutably borrows the
/// inner operation, while the second consumes it and renders it unreadable again.
pub(crate) struct MarkedOperation<K, V> {
    pub op: mem::ManuallyDrop<UnsafeCell<Operation<K, V>>>,
    pub flag: Cell<u8>,
}

const NONE_FLAG: u8 = 0; // value has not been used at all
const FIRST_FLAG: u8 = 1; // value has been used without consuming
const SECOND_FLAG: u8 = 2; // value has been consumed

impl<K, V> Drop for MarkedOperation<K, V> {
    fn drop(&mut self) {
        match self.flag.get() {
            // If the operation hasn't been consumed, drop it
            NONE_FLAG | FIRST_FLAG => unsafe {
                mem::ManuallyDrop::drop(&mut self.op);
            },
            _ => {}
        }
    }
}

impl<K, V> MarkedOperation<K, V> {
    #[inline]
    pub fn new(op: Operation<K, V>) -> Self {
        MarkedOperation {
            op: mem::ManuallyDrop::new(UnsafeCell::new(op)),
            flag: Cell::new(NONE_FLAG),
        }
    }

    /// Mark the inner value as having been read/written to in the `apply_first` scope,
    /// and therefore should not be read/written to again.
    #[inline(always)]
    pub fn mark_first(&self) {
        self.flag.set(FIRST_FLAG);
    }

    /// Mark the inner value as consumed. It can no longer be read/written to,
    /// and will not be dropped here.
    #[inline(always)]
    pub fn mark_second(&self) {
        self.flag.set(SECOND_FLAG);
    }

    #[inline(always)]
    pub fn consumed(&self) -> bool {
        self.flag.get() == SECOND_FLAG
    }

    #[inline(always)]
    pub fn as_ref(&self) -> Option<&Operation<K, V>> {
        match self.flag.get() {
            // If nothing has touched the inner value, it's safe to read
            NONE_FLAG => Some(unsafe { &*self.op.get() }),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_mut(&self) -> Option<&mut Operation<K, V>> {
        match self.flag.get() {
            // If nothing has touched the inner value, it's safe to write
            NONE_FLAG => Some(unsafe { &mut *self.op.get() }),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn take_first(&self) -> Option<&mut Operation<K, V>> {
        match self.flag.get() {
            // If nothing has touched the inner value, it's safe to read/write
            NONE_FLAG => {
                // Mark it as having been read/written to in `apply_first`
                self.mark_first();

                Some(unsafe { &mut *self.op.get() })
            }
            _ => None,
        }
    }

    #[inline(always)]
    pub fn take_second(&self) -> Option<Operation<K, V>> {
        match self.flag.get() {
            // If the inner value has been consumed, it's NOT safe to read/write to,
            // so `None` is returned
            SECOND_FLAG => None,

            // Otherwise, where it is guaranteed to not be used within the
            // same scope as "first" operations, we can consume the value and
            // mark it as being moved out.
            _ => {
                // Mark it as having been consumed
                self.mark_second();

                // "Moves" the value out of the inner storage.
                //
                // This is safe so long as the old value is never accessed again.
                Some(unsafe { ptr::read(self.op.get()) })
            }
        }
    }
}

impl<V> MapOperation<V>
where
    V: PartialEq + ShallowCopy,
{
    pub(crate) fn apply_first<K, M, S>(&mut self, inner: &mut Inner<K, V, M, S>)
    where
        K: Eq + Hash,
        S: BuildHasher,
    {
        match *self {
            MapOperation::FitAll => {
                for vs in inner.data.values_mut() {
                    vs.shrink_to_fit();
                }
            }
            MapOperation::Prune => {
                inner.data.retain(|_, vs| !vs.is_empty());
            }
            MapOperation::ForEach(ref modifier) => {
                for vs in inner.data.values_mut() {
                    let mut modify = Modify::new(vs, true);

                    modifier.modify(&mut modify);

                    modify.apply_first();
                }
            }
        }
    }

    pub(crate) fn apply_second<K, M, S>(self, inner: &mut Inner<K, V, M, S>)
    where
        K: Eq + Hash,
        S: BuildHasher,
    {
        match self {
            MapOperation::FitAll => {
                for vs in inner.data.values_mut() {
                    vs.shrink_to_fit();
                }
            }
            MapOperation::Prune => {
                inner.data.retain(|_, vs| !vs.is_empty());
            }
            MapOperation::ForEach(modifier) => {
                for vs in inner.data.values_mut() {
                    let mut modify = Modify::new(vs, false);

                    modifier.modify(&mut modify);

                    modify.apply_second();
                }
            }
        }
    }
}

/// Allows for quick and efficient application of operations on a single value-set
pub struct Modify<'a, V> {
    #[cfg(feature = "smallvec")]
    ops: smallvec::SmallVec<[ValueOperation<V>; 1]>,

    #[cfg(not(feature = "smallvec"))]
    ops: Vec<ValueOperation<V>>,

    values: &'a mut Values<V>,
    first: bool,
}

impl<'a, V> Modify<'a, V>
where
    V: PartialEq + ShallowCopy,
{
    fn new(values: &'a mut Values<V>, first: bool) -> Self {
        Modify {
            ops: Default::default(),
            values,
            first,
        }
    }

    /// Returns a slice to the most up-to-date version of the value set
    ///
    /// <b>WARNING</b>: This data may be aliased, and should never be cloned.
    #[inline]
    pub unsafe fn get(&self) -> &[V] {
        &self.values
    }

    fn apply_first(&mut self) {
        let Modify {
            ref mut ops,
            values,
            ..
        } = self;

        for op in ops {
            match *op {
                ValueOperation::Replace(ref mut value) => {
                    unsafe {
                        values.set_len(0);
                    }

                    values.shrink_to_fit();
                    values.push(unsafe { value.shallow_copy() });
                }
                ValueOperation::Clear => unsafe { values.set_len(0) },
                ValueOperation::Add(ref mut value) => {
                    values.push(unsafe { value.shallow_copy() });
                }
                ValueOperation::Remove(ref value) => {
                    if let Some(i) = values.iter().position(|v| v == value) {
                        mem::forget(values.swap_remove(i));
                    }
                }
                ValueOperation::RemoveStable(ref value) => {
                    if let Some(i) = values.iter().position(|v| v == value) {
                        mem::forget(values.remove(i));
                    }
                }
                ValueOperation::Retain(ref predicate) => {
                    let mut del = 0;
                    let len = values.len();

                    // See the comments on the primary implementation for more info on this
                    for i in 0..len {
                        if !predicate.eval(unsafe { values.get_unchecked(i) }) {
                            del += 1;
                        } else {
                            values.swap(i - del, i);
                        }
                    }

                    unsafe {
                        values.set_len(len - del);
                    }
                }
                ValueOperation::Fit => values.shrink_to_fit(),
                ValueOperation::Reserve(additional) => values.reserve(additional),
                ValueOperation::Modify(ref modifier) => {
                    let mut modify = Modify::new(values, true);

                    modifier.modify(&mut modify);

                    modify.apply_first();
                }

                // Not even allowed to be added
                ValueOperation::Empty => unimplemented!(),
            }
        }
    }

    fn apply_second(&mut self) {
        let Modify { ops, values, .. } = self;

        #[cfg(feature = "smallvec")]
        let ops = ops.drain();

        #[cfg(not(feature = "smallvec"))]
        let ops = ops.drain(..);

        for op in ops {
            match op {
                ValueOperation::Replace(value) => {
                    values.clear();
                    values.shrink_to_fit();
                    values.push(value);
                }
                ValueOperation::Clear => values.clear(),
                ValueOperation::Add(value) => values.push(value),
                ValueOperation::Remove(value) => {
                    if let Some(i) = values.iter().position(|v| v == &value) {
                        mem::drop(values.swap_remove(i));
                    }
                }
                ValueOperation::RemoveStable(value) => {
                    if let Some(i) = values.iter().position(|v| v == &value) {
                        mem::drop(values.remove(i));
                    }
                }
                ValueOperation::Retain(predicate) => values.retain(|v| predicate.eval(v)),
                ValueOperation::Fit => values.shrink_to_fit(),
                ValueOperation::Reserve(additional) => values.reserve(additional),
                ValueOperation::Modify(modifier) => {
                    let mut modify = Modify::new(values, false);

                    modifier.modify(&mut modify);

                    modify.apply_second();
                }

                // Not even allowed to be added
                ValueOperation::Empty => unimplemented!(),
            }
        }
    }

    /// Apply all pending operations to this value-set
    ///
    /// This will update the value-set in-place, allowing the modifier to
    /// read it immediately with the changes applied.
    pub fn refresh(&mut self) -> &mut Self {
        if self.first {
            self.apply_first();
        } else {
            self.apply_second();
        }

        self
    }

    #[inline]
    fn add_op(&mut self, op: ValueOperation<V>) -> &mut Self {
        self.ops.push(op);
        self
    }

    /// Replace the value-set of the given key with the given value.
    #[inline]
    pub fn update(&mut self, value: V) -> &mut Self {
        self.add_op(ValueOperation::Replace(value))
    }

    /// Clear the value-set of the given key, without removing it.
    #[inline]
    pub fn clear(&mut self) -> &mut Self {
        self.add_op(ValueOperation::Clear)
    }

    /// Add the given value to the value-set of the given key.
    #[inline]
    pub fn insert(&mut self, value: V) -> &mut Self {
        self.add_op(ValueOperation::Add(value))
    }

    /// Remove the given value from the value-set of the given key, by swapping it with the last
    /// value and popping it off the value-set. This does not preserve element order in the value-set.
    ///
    /// To preserve order when removing, consider using `remove_stable`.
    #[inline]
    pub fn remove(&mut self, value: V) -> &mut Self {
        self.add_op(ValueOperation::Remove(value))
    }

    /// Remove the given value from the value-set of the given key, preserving the order
    /// of elements in the value-set.
    #[inline]
    pub fn remove_stable(&mut self, value: V) -> &mut Self {
        self.add_op(ValueOperation::RemoveStable(value))
    }

    /// Retain elements for the given key using the provided predicate function.
    pub fn retain<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn(&V) -> bool + 'static + Send + Sync,
    {
        self.add_op(ValueOperation::Retain(Predicate(Arc::new(f))))
    }

    /// Shrinks a value-set to it's minimum necessary size, freeing memory
    /// and potentially improving cache locality.
    #[inline]
    pub fn fit(&mut self) -> &mut Self {
        self.add_op(ValueOperation::Fit)
    }

    /// Reserves capacity for some number of additional elements in a value-set
    #[inline]
    pub fn reserve(&mut self, additional: usize) -> &mut Self {
        self.add_op(ValueOperation::Reserve(additional))
    }

    /// Perform additional arbitrary operations on this value-set
    pub fn modify<F>(&mut self, f: F) -> &mut Self
    where
        F: for<'b> Fn(&mut Modify<'b, V>) + 'static + Send + Sync,
    {
        self.add_op(ValueOperation::Modify(Modifier(Arc::new(f))))
    }
}

impl<V> ValueOperation<V>
where
    V: Eq + ShallowCopy,
{
    pub(crate) fn apply_first<K, S>(&mut self, key: Cow<K>, entry: RawEntryMut<K, Values<V>, S>)
    where
        K: Hash + Clone,
        S: BuildHasher,
    {
        match *self {
            ValueOperation::Remove(ref value) => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    let values = entry.get_mut();

                    if let Some(i) = values.iter().position(|v| v == value) {
                        mem::forget(values.swap_remove(i));
                    }
                }
            }
            ValueOperation::RemoveStable(ref value) => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    let values = entry.get_mut();

                    if let Some(i) = values.iter().position(|v| v == value) {
                        mem::forget(values.remove(i));
                    }
                }
            }
            ValueOperation::Empty => {
                if let RawEntryMut::Occupied(entry) = entry {
                    entry.remove();
                }
            }
            ValueOperation::Retain(ref predicate) => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    let e = entry.get_mut();

                    let mut del = 0;
                    let len = e.len();

                    // "bubble up" the values we wish to remove, so they can be truncated.
                    //
                    // See https://github.com/servo/rust-smallvec/blob/a775b5f74cce0d3c7218608fd9f6fd721bb0f461/lib.rs#L881-L897
                    // for SmallVec's implementation of `retain`, the only difference being that we
                    // cannot drop values here, so they are truncated with `set_len`
                    for i in 0..len {
                        if !predicate.eval(unsafe { e.get_unchecked(i) }) {
                            del += 1;
                        } else {
                            e.swap(i - del, i);
                        }
                    }

                    unsafe {
                        // truncate vector without dropping values
                        e.set_len(len - del);
                    }
                }
            }
            ValueOperation::Fit => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    entry.get_mut().shrink_to_fit();
                }
            }
            ValueOperation::Reserve(additional) => match entry {
                RawEntryMut::Occupied(mut entry) => {
                    entry.get_mut().reserve(additional);
                }
                RawEntryMut::Vacant(entry) => {
                    entry.insert(key.into_owned(), Values::with_capacity(additional));
                }
            },
            // Fall through to operations that can use a single `or_insert_with`
            ValueOperation::Replace(_)
            | ValueOperation::Clear
            | ValueOperation::Add(_)
            | ValueOperation::Modify(_) => {
                let (_, v) = entry.or_insert_with(|| (key.into_owned(), Values::new()));

                match self {
                    ValueOperation::Replace(ref mut value) => {
                        unsafe {
                            // truncate vector without dropping values
                            v.set_len(0);
                        }

                        v.shrink_to_fit();
                        v.push(unsafe { value.shallow_copy() });
                    }
                    ValueOperation::Clear => unsafe {
                        // truncate vector without dropping values
                        v.set_len(0);
                    },
                    ValueOperation::Add(ref mut value) => {
                        v.push(unsafe { value.shallow_copy() });
                    }
                    ValueOperation::Modify(modifier) => {
                        let mut modify = Modify::new(v, true);

                        modifier.modify(&mut modify);

                        modify.apply_first();
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    pub(crate) fn apply_second<K, S>(self, key: Cow<K>, entry: RawEntryMut<K, Values<V>, S>)
    where
        K: Hash + Clone,
        S: BuildHasher,
    {
        match self {
            ValueOperation::Remove(value) => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    let values = entry.get_mut();

                    if let Some(i) = values.iter().position(|v| v == &value) {
                        mem::drop(values.swap_remove(i));
                    }
                }
            }
            ValueOperation::RemoveStable(value) => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    let values = entry.get_mut();

                    if let Some(i) = values.iter().position(|v| v == &value) {
                        mem::drop(values.remove(i));
                    }
                }
            }
            ValueOperation::Empty => {
                if let RawEntryMut::Occupied(entry) = entry {
                    entry.remove();
                }
            }
            ValueOperation::Retain(predicate) => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    entry.get_mut().retain(|v| predicate.eval(v));
                }
            }
            ValueOperation::Fit => {
                if let RawEntryMut::Occupied(mut entry) = entry {
                    entry.get_mut().shrink_to_fit();
                }
            }
            ValueOperation::Reserve(additional) => match entry {
                RawEntryMut::Occupied(mut entry) => {
                    entry.get_mut().reserve(additional);
                }
                RawEntryMut::Vacant(entry) => {
                    entry.insert(key.into_owned(), Values::with_capacity(additional));
                }
            },
            // Fall through to operations that can use a single `or_insert_with`
            ValueOperation::Replace(_)
            | ValueOperation::Clear
            | ValueOperation::Add(_)
            | ValueOperation::Modify(_) => {
                let (_, v) = entry.or_insert_with(|| (key.into_owned(), Values::new()));

                match self {
                    ValueOperation::Replace(value) => {
                        v.clear();
                        v.shrink_to_fit();
                        v.push(value);
                    }
                    ValueOperation::Clear => {
                        v.clear();
                    }
                    ValueOperation::Add(value) => {
                        v.push(value);
                    }
                    ValueOperation::Modify(modifier) => {
                        let mut modify = Modify::new(v, false);

                        modifier.modify(&mut modify);

                        modify.apply_second();
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
