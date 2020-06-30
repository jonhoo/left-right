use std::borrow::Borrow;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::mem::ManuallyDrop;

/// This value determines when a value-set is promoted from a list to a HashBag.
const BAG_THRESHOLD: usize = 32;

/// A bag of values for a given key in the evmap.
#[repr(transparent)]
pub struct Values<T, S = std::collections::hash_map::RandomState>(ValuesInner<T, S>);

impl<T, S> Default for Values<T, S> {
    fn default() -> Self {
        Values(ValuesInner::Short(Default::default()))
    }
}

impl<T, S> fmt::Debug for Values<T, S>
where
    T: fmt::Debug,
    S: BuildHasher,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_set().entries(self.iter()).finish()
    }
}

enum ValuesInner<T, S> {
    Short(smallvec::SmallVec<[T; 1]>),
    Long(hashbag::HashBag<T, S>),
}

impl<T, S> Values<ManuallyDrop<T>, S> {
    pub(crate) fn user_friendly(&self) -> &Values<T, S> {
        unsafe { &*(self as *const Self as *const Values<T, S>) }
    }
}

impl<T, S> Values<T, S> {
    /// Returns the number of values.
    pub fn len(&self) -> usize {
        match self.0 {
            ValuesInner::Short(ref v) => v.len(),
            ValuesInner::Long(ref v) => v.len(),
        }
    }

    /// Returns true if the bag holds no values.
    pub fn is_empty(&self) -> bool {
        match self.0 {
            ValuesInner::Short(ref v) => v.is_empty(),
            ValuesInner::Long(ref v) => v.is_empty(),
        }
    }

    /// Returns the number of values that can be held without reallocating.
    pub fn capacity(&self) -> usize {
        match self.0 {
            ValuesInner::Short(ref v) => v.capacity(),
            ValuesInner::Long(ref v) => v.capacity(),
        }
    }

    /// An iterator visiting all elements in arbitrary order.
    ///
    /// The iterator element type is &'a T.
    pub fn iter(&self) -> ValuesIter<'_, T, S> {
        match self.0 {
            ValuesInner::Short(ref v) => ValuesIter::Short(v.iter()),
            ValuesInner::Long(ref v) => ValuesIter::Long(v.iter()),
        }
    }

    /// Returns a guarded reference to _one_ value corresponding to the key.
    ///
    /// This is mostly intended for use when you are working with no more than one value per key.
    /// If there are multiple values stored for this key, there are no guarantees to which element
    /// is returned.
    pub fn get_one(&self) -> Option<&T> {
        match self.0 {
            ValuesInner::Short(ref v) => v.get(0),
            ValuesInner::Long(ref v) => v.iter().next(),
        }
    }

    /// Returns true if a value matching `value` is among the stored values.
    ///
    /// The value may be any borrowed form of `T`, but [`Hash`] and [`Eq`] on the borrowed form
    /// *must* match those for the value type.
    pub fn contains<Q: ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Eq + Hash,
        T: Eq + Hash,
        S: BuildHasher,
    {
        match self.0 {
            ValuesInner::Short(ref v) => v.iter().any(|v| v.borrow() == value),
            ValuesInner::Long(ref v) => v.contains(value) != 0,
        }
    }

    #[cfg(test)]
    fn is_short(&self) -> bool {
        matches!(self.0, ValuesInner::Short(_))
    }
}

impl<'a, T, S> IntoIterator for &'a Values<T, S> {
    type IntoIter = ValuesIter<'a, T, S>;
    type Item = &'a T;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub enum ValuesIter<'a, T, S> {
    #[doc(hidden)]
    Short(<&'a smallvec::SmallVec<[T; 1]> as IntoIterator>::IntoIter),
    #[doc(hidden)]
    Long(<&'a hashbag::HashBag<T, S> as IntoIterator>::IntoIter),
}

impl<'a, T, S> Iterator for ValuesIter<'a, T, S> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        match *self {
            Self::Short(ref mut it) => it.next(),
            Self::Long(ref mut it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Short(it) => it.size_hint(),
            Self::Long(it) => it.size_hint(),
        }
    }
}

impl<'a, T, S> ExactSizeIterator for ValuesIter<'a, T, S>
where
    <&'a smallvec::SmallVec<[T; 1]> as IntoIterator>::IntoIter: ExactSizeIterator,
    <&'a hashbag::HashBag<T, S> as IntoIterator>::IntoIter: ExactSizeIterator,
{
}

impl<'a, T, S> std::iter::FusedIterator for ValuesIter<'a, T, S>
where
    <&'a smallvec::SmallVec<[T; 1]> as IntoIterator>::IntoIter: std::iter::FusedIterator,
    <&'a hashbag::HashBag<T, S> as IntoIterator>::IntoIter: std::iter::FusedIterator,
{
}

impl<T, S> Values<T, S>
where
    T: Eq + Hash,
    S: BuildHasher + Clone,
{
    pub(crate) fn new() -> Self {
        Self(ValuesInner::Short(smallvec::SmallVec::new()))
    }

    pub(crate) fn with_capacity_and_hasher(capacity: usize, hasher: &S) -> Self {
        if capacity > BAG_THRESHOLD {
            Self(ValuesInner::Long(
                hashbag::HashBag::with_capacity_and_hasher(capacity, hasher.clone()),
            ))
        } else {
            Self(ValuesInner::Short(smallvec::SmallVec::with_capacity(
                capacity,
            )))
        }
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        match self.0 {
            ValuesInner::Short(ref mut v) => v.shrink_to_fit(),
            ValuesInner::Long(ref mut v) => {
                // here, we actually want to be clever
                // we want to potentially "downgrade" from a Long to a Short
                //
                // NOTE: there may be more than one instance of row in the bag. if there is, we do
                // not move to a SmallVec. The reason is simple: if we did, we would need to
                // duplicate those rows again. But, how would we do so safely? If we clone into
                // both the left and the right map (that is, on both first and second apply), then
                // we would only free one of them. If we shallow_copy the one we have in the
                // hashbag, then once any instance gets remove from both sides, it'll be freed,
                // which will invalidate the remaining references.
                if v.len() < BAG_THRESHOLD && v.len() == v.set_len() {
                    let mut short = smallvec::SmallVec::with_capacity(v.len());
                    // NOTE: this drain _must_ have a deterministic iteration order.
                    // that is, the items must be yielded in the same order regardless of whether
                    // we are iterating on the left or right map. otherwise, this happens;
                    //
                    //   1. after shrink_to_fit, left value is AA*B*, right is B*AA*.
                    //      X* elements are shallow copies of each other
                    //   2. swap_remove B (1st); left is AA*B*, right is now A*A
                    //   3. swap_remove B (2nd); left drops B* and is now AA*, right is A*A
                    //   4. swap_remove A (1st); left is now A*, right is A*A
                    //   5. swap_remove A (2nd); left is A*, right drops A* and is now A
                    //      right dropped A* while A still has it -- no okay!
                    for (row, n) in v.drain() {
                        assert_eq!(n, 1);
                        short.push(row);
                    }
                    self.0 = ValuesInner::Short(short);
                } else {
                    v.shrink_to_fit();
                }
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        // NOTE: we do _not_ downgrade to Short here -- shrink is for that
        match self.0 {
            ValuesInner::Short(ref mut v) => v.clear(),
            ValuesInner::Long(ref mut v) => v.clear(),
        }
    }

    pub(crate) fn swap_remove(&mut self, value: &T) {
        match self.0 {
            ValuesInner::Short(ref mut v) => {
                if let Some(i) = v.iter().position(|v| v == value) {
                    v.swap_remove(i);
                }
            }
            ValuesInner::Long(ref mut v) => {
                v.remove(value);
            }
        }
    }

    fn baggify(&mut self, capacity: usize, hasher: &S) {
        if let ValuesInner::Short(ref mut v) = self.0 {
            let mut long = hashbag::HashBag::with_capacity_and_hasher(capacity, hasher.clone());

            // NOTE: this _may_ drop some values since the bag does not keep duplicates.
            // that should be fine -- if we drop for the first time, we're dropping
            // ManuallyDrop, which won't actually drop the shallow copies. when we drop for
            // the second time, we do the actual dropping. since second application has the
            // exact same original state, this change from short/long should occur exactly
            // the same.
            long.extend(v.drain(..));
            self.0 = ValuesInner::Long(long);
        }
    }

    pub(crate) fn reserve(&mut self, additional: usize, hasher: &S) {
        match self.0 {
            ValuesInner::Short(ref mut v) => {
                let n = v.len() + additional;
                if n >= BAG_THRESHOLD {
                    self.baggify(n, hasher);
                } else {
                    v.reserve(additional)
                }
            }
            ValuesInner::Long(ref mut v) => v.reserve(additional),
        }
    }

    pub(crate) fn push(&mut self, value: T, hasher: &S) {
        match self.0 {
            ValuesInner::Short(ref mut v) => {
                // we may want to upgrade to a Long..
                let n = v.len() + 1;
                if n >= BAG_THRESHOLD {
                    self.baggify(n, hasher);
                    if let ValuesInner::Long(ref mut v) = self.0 {
                        v.insert(value);
                    } else {
                        unreachable!();
                    }
                } else {
                    v.push(value);
                }
            }
            ValuesInner::Long(ref mut v) => {
                v.insert(value);
            }
        }
    }

    pub(crate) fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        match self.0 {
            ValuesInner::Short(ref mut v) => v.retain(|v| f(&*v)),
            ValuesInner::Long(ref mut v) => v.retain(|v, n| if f(v) { n } else { 0 }),
        }
    }

    pub(crate) fn from_iter<I>(iter: I, hasher: &S) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let iter = iter.into_iter();
        if iter.size_hint().0 > BAG_THRESHOLD {
            let mut long = hashbag::HashBag::with_hasher(hasher.clone());
            long.extend(iter);
            Self(ValuesInner::Long(long))
        } else {
            use std::iter::FromIterator;
            Self(ValuesInner::Short(smallvec::SmallVec::from_iter(iter)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::RandomState;

    macro_rules! assert_empty {
        ($x:expr) => {
            assert_eq!($x.len(), 0);
            assert!($x.is_empty());
            assert_eq!($x.iter().count(), 0);
            assert_eq!($x.into_iter().count(), 0);
            assert_eq!($x.get_one(), None);
        };
    }

    macro_rules! assert_len {
        ($x:expr, $n:expr) => {
            assert_eq!($x.len(), $n);
            assert!(!$x.is_empty());
            assert_eq!($x.iter().count(), $n);
            assert_eq!($x.into_iter().count(), $n);
        };
    }

    #[test]
    fn sensible_default() {
        let v: Values<i32> = Values::default();
        assert!(v.is_short());
        assert_eq!(v.capacity(), 1);
        assert_empty!(v);
    }

    #[test]
    fn short_values() {
        let hasher = RandomState::default();
        let mut v = Values::new();

        let values = 0..BAG_THRESHOLD - 1;
        let len = values.clone().count();
        for i in values.clone() {
            v.push(i, &hasher);
        }

        for i in values.clone() {
            assert!(v.contains(&i));
        }
        assert!(v.is_short());
        assert_len!(v, len);
        assert_eq!(v.get_one(), Some(&0));

        v.clear();

        assert_empty!(v);

        // clear() should not affect capacity or value type!
        assert!(v.capacity() > 1);
        assert!(v.is_short());

        v.shrink_to_fit();

        assert_eq!(v.capacity(), 1);
    }

    #[test]
    fn long_values() {
        let hasher = RandomState::default();
        let mut v = Values::new();

        let values = 0..BAG_THRESHOLD;
        let len = values.clone().count();
        for i in values.clone() {
            v.push(i, &hasher);
        }

        for i in values.clone() {
            assert!(v.contains(&i));
        }
        assert!(!v.is_short());
        assert_len!(v, len);
        assert!(values.contains(v.get_one().unwrap()));

        v.clear();

        assert_empty!(v);

        // clear() should not affect capacity or value type!
        assert!(v.capacity() > 1);
        assert!(!v.is_short());

        v.shrink_to_fit();

        // Now we have short values!
        assert!(v.is_short());
        assert_eq!(v.capacity(), 1);
    }
}
