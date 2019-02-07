use std::hash::{BuildHasher, Hash};
use std::sync::{atomic, Arc, Mutex};

use hashbrown::HashMap;

pub(crate) type Values<T> = smallvec::SmallVec<[T; 1]>;

pub(crate) struct Inner<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) data: HashMap<K, Values<V>, S>,
    pub(crate) epochs: Arc<Mutex<Vec<Arc<atomic::AtomicUsize>>>>,
    pub(crate) meta: M,
    ready: bool,
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
            data: HashMap::with_capacity_and_hasher(
                self.data.capacity(),
                self.data.hasher().clone(),
            ),
            epochs: Arc::clone(&self.epochs),
            meta: self.meta.clone(),
            ready: self.ready,
        }
    }
}

impl<K, V, M, S> Inner<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub fn with_hasher(m: M, hash_builder: S) -> Self {
        Inner {
            data: HashMap::with_hasher(hash_builder),
            epochs: Default::default(),
            meta: m,
            ready: false,
        }
    }

    pub fn with_capacity_and_hasher(m: M, capacity: usize, hash_builder: S) -> Self {
        Inner {
            data: HashMap::with_capacity_and_hasher(capacity, hash_builder),
            epochs: Default::default(),
            meta: m,
            ready: false,
        }
    }

    pub fn mark_ready(&mut self) {
        self.ready = true;
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }
}
