use std::hash::{Hash, BuildHasher};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic};

pub struct Inner<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    pub data: HashMap<K, Vec<V>, S>,
    pub epochs: Arc<Mutex<Vec<Arc<atomic::AtomicUsize>>>>,
    pub meta: M,
    ready: bool,
}

impl<K, V, M, S> Clone for Inner<K, V, M, S>
    where K: Eq + Hash + Clone,
          S: BuildHasher + Clone,
          V: Clone,
          M: Clone
{
    fn clone(&self) -> Self {
        Inner {
            data: self.data.clone(),
            epochs: self.epochs.clone(),
            meta: self.meta.clone(),
            ready: self.ready,
        }
    }
}

impl<K, V, M, S> Inner<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher
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

    pub fn register_epoch(&self, epoch: &Arc<atomic::AtomicUsize>) {
        self.epochs
            .lock()
            .unwrap()
            .push(epoch.clone());
    }
}
