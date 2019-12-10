use crate::inner::{Inner, Values};

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::iter::{self, FromIterator};
use std::marker::PhantomData;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;
use std::sync::{self, Arc};
use std::{cell, fmt, mem};

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
    pub(crate) inner: sync::Arc<AtomicPtr<Inner<K, V, M, S>>>,
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

/// A type that is both `Sync` and `Send` and lets you produce new [`ReadHandle`] instances.
///
/// This serves as a handy way to distribute read handles across many threads without requiring
/// additional external locking to synchronize access to the non-`Sync` `ReadHandle` type. Note
/// that this _internally_ takes a lock whenever you call [`ReadHandleFactory::handle`], so
/// you should not expect producing new handles rapidly to scale well.
pub struct ReadHandleFactory<K, V, M = (), S = RandomState>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    inner: sync::Arc<AtomicPtr<Inner<K, V, M, S>>>,
    epochs: crate::Epochs,
}

impl<K, V, M, S> fmt::Debug for ReadHandleFactory<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandleFactory")
            .field("epochs", &self.epochs)
            .finish()
    }
}

impl<K, V, M, S> Clone for ReadHandleFactory<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    fn clone(&self) -> Self {
        Self {
            inner: sync::Arc::clone(&self.inner),
            epochs: sync::Arc::clone(&self.epochs),
        }
    }
}

impl<K, V, M, S> ReadHandleFactory<K, V, M, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    /// Produce a new [`ReadHandle`] to the same map as this factory was originally produced from.
    pub fn handle(&self) -> ReadHandle<K, V, M, S> {
        ReadHandle::new(
            sync::Arc::clone(&self.inner),
            sync::Arc::clone(&self.epochs),
        )
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
    inner: Inner<K, V, M, S>,
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
    fn new(inner: sync::Arc<AtomicPtr<Inner<K, V, M, S>>>, epochs: crate::Epochs) -> Self {
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
    fn with_handle<F, T>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&Inner<K, V, M, S>) -> T,
    {
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

        // add a guard to ensure we restore read parity even if we panic
        struct RestoreParity<'a>(&'a sync::Arc<sync::atomic::AtomicUsize>, usize);
        impl<'a> Drop for RestoreParity<'a> {
            fn drop(&mut self) {
                self.0.store(
                    (self.1 + 1) | 1usize << (mem::size_of::<usize>() * 8 - 1),
                    atomic::Ordering::Release,
                );
            }
        }
        let _guard = RestoreParity(&self.epoch, epoch);

        // and finally, call the provided handler function
        unsafe { r_handle.as_ref().map(f) }
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        self.with_handle(|inner| inner.data.len()).unwrap_or(0)
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.with_handle(|inner| inner.data.is_empty())
            .unwrap_or(true)
    }

    /// Get the current meta value.
    pub fn meta(&self) -> Option<M> {
        self.with_handle(|inner| inner.meta.clone())
    }

    /// Internal version of `get_and`
    fn get_raw<Q: ?Sized, F, T>(&self, key: &Q, then: F) -> Option<T>
    where
        F: FnOnce(&Values<V>) -> T,
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.with_handle(move |inner| {
            if !inner.is_ready() {
                None
            } else {
                inner.data.get(key).map(then)
            }
        })
        .unwrap_or(None)
    }

    /// Applies a function to the values corresponding to the key, and returns the result.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, this function returns `None`.
    ///
    /// If no values exist for the given key, no refresh has happened, or the map has been
    /// destroyed, `then` will not be called, and `None` will be returned.
    #[inline]
    pub fn get_and<Q: ?Sized, F, T>(&self, key: &Q, then: F) -> Option<T>
    where
        F: FnOnce(&[V]) -> T,
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        // call `borrow` here to monomorphize `get_raw` fewer times
        self.get_raw(key.borrow(), |values| then(&**values))
    }

    /// Applies a function to the values corresponding to the key, and returns the result alongside
    /// the meta information.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, or if the map has been closed by the
    /// writer, this function returns `None`.
    ///
    /// If no values exist for the given key, `then` will not be called, and `Some(None, _)` is
    /// returned.
    pub fn meta_get_and<Q: ?Sized, F, T>(&self, key: &Q, then: F) -> Option<(Option<T>, M)>
    where
        F: FnOnce(&[V]) -> T,
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.with_handle(move |inner| {
            if !inner.is_ready() {
                None
            } else {
                let res = inner.data.get(key).map(move |v| then(&**v));
                let res = (res, inner.meta.clone());
                Some(res)
            }
        })
        .unwrap_or(None)
    }

    /// If the writer has destroyed this map, this method will return true.
    ///
    /// See `WriteHandle::destroy`.
    pub fn is_destroyed(&self) -> bool {
        self.with_handle(|_| ()).is_none()
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
        self.with_handle(move |inner| inner.data.contains_key(key))
            .unwrap_or(false)
    }

    /// Read all values in the map, and transform them into a new collection.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// refresh will block waiting on this reader to finish.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&K, &[V]),
    {
        self.with_handle(move |inner| {
            for (k, vs) in &inner.data {
                f(k, &vs[..])
            }
        });
    }

    /// Read all values in the map, and transform them into a new collection.
    pub fn map_into<Map, Collector, Target>(&self, mut f: Map) -> Collector
    where
        Map: FnMut(&K, &[V]) -> Target,
        Collector: FromIterator<Target>,
    {
        self.with_handle(move |inner| {
            Collector::from_iter(inner.data.iter().map(|(k, vs)| f(k, &vs[..])))
        })
        .unwrap_or_else(|| Collector::from_iter(iter::empty()))
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

        r.get_raw(&0, |vs| assert_eq!(vs.capacity(), MAX)).unwrap();

        for i in 0..MIN {
            w.insert(0, i);
        }

        w.fit_all().refresh();

        r.get_raw(&0, |vs| assert!(vs.capacity() < MAX)).unwrap();
    }
}
