use super::ReadGuard;
use crate::inner::Inner;

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash};

/// A live reference into the read half of an evmap.
///
/// As long as this lives, the map being read cannot change, and if a writer attempts to
/// call [`WriteHandle::refresh`], that call will block until this is dropped.
///
/// Since the map remains immutable while this lives, the methods on this type all give you
/// unguarded references to types contained in the map.
#[derive(Debug)]
pub struct MapReadRef<'rh, K, V, M, S>
where
    K: Hash + Eq,
    S: BuildHasher,
{
    pub(super) guard: Option<ReadGuard<'rh, Inner<K, V, M, S>>>,
}

impl<'rh, K, V, M, S> MapReadRef<'rh, K, V, M, S>
where
    K: Hash + Eq,
    S: BuildHasher,
{
    /// Iterate over all key + valuesets in the map.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// refresh will block waiting on this reader to finish.
    pub fn iter<'a>(&'a self) -> ReadGuardIter<'a, K, V, S> {
        ReadGuardIter {
            iter: self.guard.as_ref().map(|rg| rg.data.iter()),
        }
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        self.guard
            .as_ref()
            .map(|inner| inner.data.len())
            .unwrap_or(0)
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.guard
            .as_ref()
            .map(|inner| inner.data.is_empty())
            .unwrap_or(true)
    }

    /// Get the current meta value.
    pub fn meta(&self) -> Option<&M> {
        self.guard.as_ref().map(|inner| &inner.meta)
    }

    /// Returns a reference to the value-set corresponding to the key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, or the map has been destroyed, this
    /// function returns `None`.
    pub fn get<'a, Q: ?Sized>(&'a self, key: &'_ Q) -> Option<&'a [V]>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = self.guard.as_ref()?;
        if !inner.is_ready() {
            return None;
        }
        inner.data.get(key).map(|v| &v[..])
    }

    /// Returns true if the writer has destroyed this map.
    ///
    /// See [`WriteHandle::destroy`].
    pub fn is_destroyed(&self) -> bool {
        self.guard.is_none()
    }

    /// Returns true if the map contains any values for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = self.guard.as_ref();
        match inner {
            None => false,
            Some(ref inner) if !inner.is_ready() => false,
            Some(ref inner) => inner.data.contains_key(key),
        }
    }
}

impl<'rh, K, Q, V, M, S> std::ops::Index<&'_ Q> for MapReadRef<'rh, K, V, M, S>
where
    K: Eq + Hash + Borrow<Q>,
    Q: Eq + Hash + ?Sized,
    S: BuildHasher,
{
    type Output = [V];
    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).unwrap()
    }
}

impl<'rg, 'rh, K, V, M, S> IntoIterator for &'rg MapReadRef<'rh, K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'rg K, &'rg [V]);
    type IntoIter = ReadGuardIter<'rg, K, V, S>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An [`Iterator`] over keys and value-sets in the evmap.
#[derive(Debug)]
pub struct ReadGuardIter<'rg, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    iter: Option<
        <&'rg crate::inner::MapImpl<K, crate::inner::Values<V>, S> as IntoIterator>::IntoIter,
    >,
}

impl<'rg, K, V, S> Iterator for ReadGuardIter<'rg, K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    type Item = (&'rg K, &'rg [V]);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .as_mut()
            .and_then(|iter| iter.next().map(|(k, v)| (k, &v[..])))
    }
}
