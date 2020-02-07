use std::borrow::Borrow;
use std::hash::Hash;
use std::mem::ManuallyDrop;

const BAG_THRESHOLD: usize = 32;

/// A bag of values for a given key in the evmap.
#[derive(Debug)]
#[repr(transparent)]
pub struct Values<T>(ValuesInner<T>);

#[derive(Debug)]
enum ValuesInner<T> {
    Short(smallvec::SmallVec<[T; 1]>),
    Long(hashbag::HashBag<T>),
}

impl<T> Values<ManuallyDrop<T>> {
    pub(crate) fn user_friendly(&self) -> &Values<T> {
        unsafe { std::mem::transmute(self) }
    }
}

impl<T> Values<T> {
    /// Returns the number of values.
    pub fn len(&self) -> usize {
        match self.0 {
            ValuesInner::Short(ref v) => v.len(),
            ValuesInner::Long(ref v) => v.len(),
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
    pub fn iter(&self) -> ValuesIter<'_, T> {
        match self.0 {
            ValuesInner::Short(ref v) => ValuesIter::Short(v.iter()),
            ValuesInner::Long(ref v) => ValuesIter::Long(v.iter()),
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
    {
        match self.0 {
            ValuesInner::Short(ref v) => v.iter().any(|v| v.borrow() == value),
            ValuesInner::Long(ref v) => v.contains(value) != 0,
        }
    }
}

impl<'a, T> IntoIterator for &'a Values<T> {
    type IntoIter = ValuesIter<'a, T>;
    type Item = &'a T;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
pub enum ValuesIter<'a, T> {
    #[doc(hidden)]
    Short(<&'a smallvec::SmallVec<[T; 1]> as IntoIterator>::IntoIter),
    #[doc(hidden)]
    Long(<&'a hashbag::HashBag<T> as IntoIterator>::IntoIter),
}

impl<'a, T> Iterator for ValuesIter<'a, T> {
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

impl<'a, T> ExactSizeIterator for ValuesIter<'a, T>
where
    <&'a smallvec::SmallVec<[T; 1]> as IntoIterator>::IntoIter: ExactSizeIterator,
    <&'a hashbag::HashBag<T> as IntoIterator>::IntoIter: ExactSizeIterator,
{
}

impl<'a, T> std::iter::FusedIterator for ValuesIter<'a, T>
where
    <&'a smallvec::SmallVec<[T; 1]> as IntoIterator>::IntoIter: std::iter::FusedIterator,
    <&'a hashbag::HashBag<T> as IntoIterator>::IntoIter: std::iter::FusedIterator,
{
}

impl<T> Values<T>
where
    T: Eq + Hash + Clone,
{
    pub(crate) fn new() -> Self {
        Self(ValuesInner::Short(smallvec::SmallVec::new()))
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        if capacity > BAG_THRESHOLD {
            Self(ValuesInner::Long(hashbag::HashBag::with_capacity(capacity)))
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
                if v.len() < BAG_THRESHOLD {
                    let mut short = smallvec::SmallVec::with_capacity(v.len());
                    for (row, n) in v.drain() {
                        // there may be more than one instance of row in the bag. if there is, we
                        // need to clone them before inserting them into the smallvec. if we did
                        // not (if we instead did a shallow copy), then dropping the second
                        // occurrence of a duplicated element on second apply() would be a
                        // double-free. this is definitely a little unfortunate, as it means not
                        // only does T: Clone, but _also_ we have to clone a value that technically
                        // the user has already given us a clone of (when they initially pushed
                        // it!).
                        for _ in 1..n {
                            short.push(row.clone());
                        }
                        short.push(row);
                    }
                    std::mem::replace(&mut self.0, ValuesInner::Short(short));
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

    fn to_long(&mut self, capacity: usize) {
        if let ValuesInner::Short(ref mut v) = self.0 {
            let mut long = hashbag::HashBag::with_capacity(capacity);
            // NOTE: this _may_ drop some values since the bag does not keep duplicates.
            // that should be fine -- if we drop for the first time, we're dropping
            // ManuallyDrop, which won't actually drop the shallow copies. when we drop for
            // the second time, we do the actual dropping. since second application has the
            // exact same original state, this change from short/long should occur exactly
            // the same.
            long.extend(v.drain(..));
            std::mem::replace(&mut self.0, ValuesInner::Long(long));
        }
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        match self.0 {
            ValuesInner::Short(ref mut v) => {
                let n = v.len() + additional;
                if n >= BAG_THRESHOLD {
                    self.to_long(n);
                } else {
                    v.reserve(additional)
                }
            }
            ValuesInner::Long(ref mut v) => v.reserve(additional),
        }
    }

    pub(crate) fn push(&mut self, value: T) {
        match self.0 {
            ValuesInner::Short(ref mut v) => {
                // we may want to upgrade to a Long..
                let n = v.len() + 1;
                if n >= BAG_THRESHOLD {
                    self.to_long(n);
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
}

impl<A> std::iter::FromIterator<A> for Values<A>
where
    A: Hash + Eq,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        let iter = iter.into_iter();
        if iter.size_hint().0 > BAG_THRESHOLD {
            Self(ValuesInner::Long(hashbag::HashBag::from_iter(iter)))
        } else {
            Self(ValuesInner::Short(smallvec::SmallVec::from_iter(iter)))
        }
    }
}
