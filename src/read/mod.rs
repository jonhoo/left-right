use crate::inner::Inner;
use crate::values::Values;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;
use std::sync::{self, Arc};
use std::{cell, fmt, mem};

mod guard;
pub use guard::ReadGuard;

mod factory;
pub use factory::ReadHandleFactory;

mod read_ref;
pub use read_ref::{MapReadRef, ReadGuardIter};

/// A handle that may be used to read from the eventually consistent map.
///
/// Note that any changes made to the map will not be made visible until the writer calls
/// `refresh()`. In other words, all operations performed on a `ReadHandle` will *only* see writes
/// to the map that preceeded the last call to `refresh()`.
pub struct ReadHandle<K, V, M = (), S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    pub(crate) inner: sync::Arc<AtomicPtr<Inner<K, ManuallyDrop<V>, M, S>>>,
    pub(crate) epochs: crate::Epochs,
    epoch: sync::Arc<sync::atomic::AtomicUsize>,
    my_epoch: sync::atomic::AtomicUsize,

    // Since a `ReadHandle` keeps track of its own epoch, it is not safe for multiple threads to
    // call `with_handle` at the same time. We *could* keep it `Sync` and make `with_handle`
    // require `&mut self`, but that seems overly excessive. It would also mean that all other
    // methods on `ReadHandle` would now take `&mut self`, *and* that `ReadHandle` can no longer be
    // `Clone`. Since optin_builtin_traits is still an unstable feature, we use this hack to make
    // `ReadHandle` be marked as `!Sync` (since it contains an `Cell` which is `!Sync`).
    _not_sync_no_feature: PhantomData<cell::Cell<()>>,
}

impl<K, V, M, S> fmt::Debug for ReadHandle<K, V, M, S>
where
    K: Eq + Hash + fmt::Debug,
    S: BuildHasher,
    M: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandle")
            .field("epochs", &self.epochs)
            .field("epoch", &self.epoch)
            .field("my_epoch", &self.my_epoch)
            .finish()
    }
}

impl<K, V, M, S> Clone for ReadHandle<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn clone(&self) -> Self {
        ReadHandle::new(
            sync::Arc::clone(&self.inner),
            sync::Arc::clone(&self.epochs),
        )
    }
}

pub(crate) fn new<K, V, M, S>(
    inner: Inner<K, ManuallyDrop<V>, M, S>,
    epochs: crate::Epochs,
) -> ReadHandle<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    let store = Box::into_raw(Box::new(inner));
    ReadHandle::new(sync::Arc::new(AtomicPtr::new(store)), epochs)
}

impl<K, V, M, S> ReadHandle<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn new(
        inner: sync::Arc<AtomicPtr<Inner<K, ManuallyDrop<V>, M, S>>>,
        epochs: crate::Epochs,
    ) -> Self {
        // tell writer about our epoch tracker
        let epoch = sync::Arc::new(atomic::AtomicUsize::new(0));
        epochs.lock().unwrap().push(Arc::clone(&epoch));

        Self {
            epochs,
            epoch,
            my_epoch: atomic::AtomicUsize::new(0),
            inner,
            _not_sync_no_feature: PhantomData,
        }
    }

    /// Create a new `Sync` type that can produce additional `ReadHandle`s for use in other
    /// threads.
    pub fn factory(&self) -> ReadHandleFactory<K, V, M, S> {
        ReadHandleFactory {
            inner: sync::Arc::clone(&self.inner),
            epochs: sync::Arc::clone(&self.epochs),
        }
    }
}

impl<K, V, M, S> ReadHandle<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
    M: Clone,
{
    fn handle(&self) -> Option<ReadGuard<'_, Inner<K, ManuallyDrop<V>, M, S>>> {
        // once we update our epoch, the writer can no longer do a swap until we set the MSB to
        // indicate that we've finished our read. however, we still need to deal with the case of a
        // race between when the writer reads our epoch and when they decide to make the swap.
        //
        // assume that there is a concurrent writer. it just swapped the atomic pointer from A to
        // B. the writer wants to modify A, and needs to know if that is safe. we can be in any of
        // the following cases when we atomically swap out our epoch:
        //
        //  1. the writer has read our previous epoch twice
        //  2. the writer has already read our previous epoch once
        //  3. the writer has not yet read our previous epoch
        //
        // let's discuss each of these in turn.
        //
        //  1. since writers assume they are free to proceed if they read an epoch with MSB set
        //     twice in a row, this is equivalent to case (2) below.
        //  2. the writer will see our epoch change, and so will assume that we have read B. it
        //     will therefore feel free to modify A. note that *another* pointer swap can happen,
        //     back to A, but then the writer would be block on our epoch, and so cannot modify
        //     A *or* B. consequently, using a pointer we read *after* the epoch swap is definitely
        //     safe here.
        //  3. the writer will read our epoch, notice that MSB is not set, and will keep reading,
        //     continuing to observe that it is still not set until we finish our read. thus,
        //     neither A nor B are being modified, and we can safely use either.
        //
        // in all cases, using a pointer we read *after* updating our epoch is safe.

        // so, update our epoch tracker.
        let epoch = self.my_epoch.fetch_add(1, atomic::Ordering::Relaxed);
        self.epoch.store(epoch + 1, atomic::Ordering::Release);

        // ensure that the pointer read happens strictly after updating the epoch
        atomic::fence(atomic::Ordering::SeqCst);

        // then, atomically read pointer, and use the map being pointed to
        let r_handle = self.inner.load(atomic::Ordering::Acquire);

        // since we bumped our epoch, this pointer will remain valid until we bump it again
        let r_handle = unsafe { r_handle.as_ref() };

        if let Some(r_handle) = r_handle {
            // add a guard to ensure we restore read parity even if we panic
            Some(ReadGuard {
                handle: &self.epoch,
                epoch,
                t: r_handle,
            })
        } else {
            // the map has not yet been initialized, so restore parity and return None
            self.epoch.store(
                (epoch + 1) | 1usize << (mem::size_of::<usize>() * 8 - 1),
                atomic::Ordering::Release,
            );
            None
        }
    }

    /// Take out a guarded live reference to the read side of the map.
    ///
    /// This lets you perform more complex read operations on the map.
    ///
    /// While the reference lives, the map cannot be refreshed.
    ///
    /// See [`MapReadRef`].
    pub fn read(&self) -> MapReadRef<'_, K, V, M, S> {
        MapReadRef {
            guard: self.handle(),
        }
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Get the current meta value.
    pub fn meta(&self) -> Option<ReadGuard<'_, M>> {
        Some(self.handle()?.map_ref(|inner| &inner.meta))
    }

    /// Internal version of `get_and`
    fn get_raw<Q: ?Sized>(&self, key: &Q) -> Option<ReadGuard<'_, Values<ManuallyDrop<V>>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = self.handle()?;
        if !inner.is_ready() {
            return None;
        }
        inner.map_opt(|inner| inner.data.get(key))
    }

    /// Returns a guarded reference to the values corresponding to the key.
    ///
    /// While the guard lives, the map cannot be refreshed.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form must match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, or the map has been destroyed, this
    /// function returns `None`.
    #[inline]
    pub fn get<'rh, Q: ?Sized>(&'rh self, key: &'_ Q) -> Option<ReadGuard<'rh, Values<V>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        // call `borrow` here to monomorphize `get_raw` fewer times
        Some(self.get_raw(key.borrow())?.map_ref(Values::user_friendly))
    }

    /// Returns a guarded reference to the values corresponding to the key along with the map
    /// meta.
    ///
    /// While the guard lives, the map cannot be refreshed.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, or the map has been destroyed, this
    /// function returns `None`.
    ///
    /// If no values exist for the given key, `Some(None, _)` is returned.
    pub fn meta_get<Q: ?Sized>(&self, key: &Q) -> Option<(Option<ReadGuard<'_, Values<V>>>, M)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let inner = self.handle()?;
        if !inner.is_ready() {
            return None;
        }
        let meta = inner.meta.clone();
        let res = inner
            .map_opt(|inner| inner.data.get(key))
            .map(|r| r.map_ref(Values::user_friendly));
        Some((res, meta))
    }

    /// Returns true if the writer has destroyed this map.
    ///
    /// See [`WriteHandle::destroy`].
    pub fn is_destroyed(&self) -> bool {
        self.read().is_destroyed()
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
        self.read().contains_key(key)
    }

    /// Read all values in the map, and transform them into a new collection.
    pub fn map_into<Map, Collector, Target>(&self, mut f: Map) -> Collector
    where
        Map: FnMut(&K, &Values<V>) -> Target,
        Collector: FromIterator<Target>,
    {
        Collector::from_iter(self.read().iter().map(|(k, v)| f(k, v)))
    }
}

#[cfg(test)]
mod test {
    use crate::new;

    // the idea of this test is to allocate 64 elements, and only use 17. The vector will
    // probably try to fit either exactly the length, to the next highest power of 2 from
    // the length, or something else entirely, E.g. 17, 32, etc.,
    // but it should always end up being smaller than the original 64 elements reserved.
    #[test]
    fn reserve_and_fit() {
        const MIN: usize = (1 << 4) + 1;
        const MAX: usize = (1 << 6);

        let (r, mut w) = new();

        w.reserve(0, MAX).refresh();

        assert!(r.get_raw(&0).unwrap().capacity() >= MAX);

        for i in 0..MIN {
            w.insert(0, i);
        }

        w.fit_all().refresh();

        assert!(r.get_raw(&0).unwrap().capacity() < MAX);
    }
}
