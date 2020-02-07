use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::mem::ManuallyDrop;

#[cfg(feature = "indexed")]
pub(crate) use indexmap::IndexMap as MapImpl;
#[cfg(not(feature = "indexed"))]
pub(crate) use std::collections::HashMap as MapImpl;

use crate::values::Values;

pub(crate) struct Inner<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) data: MapImpl<K, Values<V>, S>,
    pub(crate) meta: M,
    ready: bool,
}

impl<K, V, M, S> Inner<K, ManuallyDrop<V>, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) unsafe fn do_drop(&mut self) -> &mut Inner<K, V, M, S> {
        std::mem::transmute(self)
    }
}

impl<K, V, M, S> fmt::Debug for Inner<K, V, M, S>
where
    K: Eq + Hash + fmt::Debug,
    S: BuildHasher,
    V: fmt::Debug,
    M: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner")
            .field("data", &self.data)
            .field("meta", &self.meta)
            .field("ready", &self.ready)
            .finish()
    }
}

impl<K, V, M, S> Clone for Inner<K, V, M, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher + Clone,
    M: Clone,
{
    fn clone(&self) -> Self {
        assert!(self.data.is_empty());
        Inner {
            data: MapImpl::with_capacity_and_hasher(
                self.data.capacity(),
                self.data.hasher().clone(),
            ),
            meta: self.meta.clone(),
            ready: self.ready,
        }
    }
}

impl<K, V, M, S> Inner<K, ManuallyDrop<V>, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn with_hasher(m: M, hash_builder: S) -> Self {
        Inner {
            data: MapImpl::with_hasher(hash_builder),
            meta: m,
            ready: false,
        }
    }

    pub fn with_capacity_and_hasher(m: M, capacity: usize, hash_builder: S) -> Self {
        Inner {
            data: MapImpl::with_capacity_and_hasher(capacity, hash_builder),
            meta: m,
            ready: false,
        }
    }
}

impl<K, V, M, S> Inner<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn mark_ready(&mut self) {
        self.ready = true;
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }
}
