use inner::Inner;

use std::sync;
use std::sync::atomic;
use std::sync::atomic::AtomicPtr;
use std::hash::{Hash, BuildHasher};
use std::collections::hash_map::RandomState;
use std::borrow::Borrow;
use std::iter::FromIterator;

/// A handle that may be used to read from the eventually consistent map.
///
/// Note that any changes made to the map will not be made visible until the writer calls
/// `refresh()`. In other words, all operations performed on a `ReadHandle` will *only* see writes
/// to the map that preceeded the last call to `refresh()`.
pub struct ReadHandle<K, V, M = (), S = RandomState>
    where K: Eq + Hash,
          S: BuildHasher
{
    pub(crate) inner: sync::Arc<AtomicPtr<Inner<K, V, M, S>>>,
    epoch: sync::Arc<sync::atomic::AtomicUsize>,
}

impl<K, V, M, S> Clone for ReadHandle<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher,
          M: Clone
{
    fn clone(&self) -> Self {
        let epoch = sync::Arc::new(atomic::AtomicUsize::new(0));
        self.with_handle(|inner| { inner.register_epoch(&epoch); });
        ReadHandle {
            epoch: epoch,
            inner: self.inner.clone(),
        }
    }
}

pub fn new<K, V, M, S>(inner: Inner<K, V, M, S>) -> ReadHandle<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    // tell writer about our epoch tracker
    let epoch = sync::Arc::new(atomic::AtomicUsize::new(0));
    inner.register_epoch(&epoch);

    let store = Box::into_raw(Box::new(inner));
    ReadHandle {
        epoch: epoch,
        inner: sync::Arc::new(AtomicPtr::new(store)),
    }
}

impl<K, V, M, S> ReadHandle<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher,
          M: Clone
{
    fn with_handle<F, T>(&self, f: F) -> T
        where F: FnOnce(&Inner<K, V, M, S>) -> T
    {
        use std::mem;
        let r_handle = self.inner.load(atomic::Ordering::SeqCst);

        // update our epoch tracker (the bit shifts clear MSB).
        let epoch = self.epoch.load(atomic::Ordering::Relaxed) << 1 >> 1;
        self.epoch.store(epoch + 1, atomic::Ordering::Release);

        // we need to read again, because writer *could* have read our epoch between us reading the
        // pointer and updating our epoch, and would then have seen finished twice in a row!
        let r_handle_again = self.inner.load(atomic::Ordering::SeqCst);

        let res = if r_handle != r_handle_again {
            // a swap happened under us.
            //
            // let's first figure out where the writer can possibly be at this point. since we have
            // updated our epoch, and a pointer swap has happened, we must be in one of the
            // following cases:
            //
            //  (a) we read and updated, *then* a writer swapped, checked epochs, and now blocks
            //  (b) we read, then a writer swapped, checked epochs, and continued
            //
            // in (a), we know that the writer that did the pointer swap is waiting on our epoch.
            // we also know that we *ought* to be using a clone of the *new* pointer to read from.
            // since the writer won't do anything until we mark our epoch as finished, we can
            // freely just use the second pointer we read.
            //
            // in (b), we know that there is a writer that thinks it owns r_handle, and so it is
            // not safe to use it. how about r_handle_again? the only way that would be unsafe to
            // use would be if a writer has gone all the way through the pointer swap and epoch
            // check test *after* we read r_handle_again. can that have happened? no. if a
            // second writer came along after we read r_handle_again, and wanted to swap, it would
            // get r_handle from its atomic pointer swap. since we updated our epoch in between,
            // and haven't marked it as finished, it would fail the epoch test, and therefore block
            // at that point. thus, we know that no writer is currently modifying r_handle_again
            // (and won't be for as long as we don't update our epoch).
            let rs = unsafe { Box::from_raw(r_handle_again) };
            let res = f(&*rs);
            mem::forget(rs); // don't free the Box!
            res
        } else {
            // are we actually safe in this case? could there not have been two swaps in a row,
            // making the value r_handle -> r_handle_again -> r_handle? well, there could, but that
            // implies that r_handle is safe to use. why? well, for this to have happened, we must
            // have read the pointer, then a writer went runs swap() (potentially multiple times).
            // then, at some point, we updated our epoch. then, we read r_handle_again to be equal
            // to r_handle.
            //
            // the moment we update our epoch, we immediately prevent any writer from taking
            // ownership of r_handle. however, what if the current writer thinks that it owns
            // r_handle? well, we read r_handle_again == r_handle, which means that at some point
            // *after the clone*, a writer made r_handle read owned. since no writer can take
            // ownership of r_handle after we update our epoch, we know that r_handle must still be
            // owned by readers.
            let rs = unsafe { Box::from_raw(r_handle) };
            let res = f(&*rs);
            mem::forget(rs); // don't free the Box!
            res
        };

        // we've finished reading -- let the writer know
        self.epoch.store((epoch + 1) | 1usize << (mem::size_of::<usize>() * 8 - 1),
                         atomic::Ordering::Release);

        res
    }

    /// Returns the number of non-empty keys present in the map.
    pub fn len(&self) -> usize {
        self.with_handle(|inner| inner.data.len())
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.with_handle(|inner| inner.data.is_empty())
    }

    /// Get the current meta value.
    pub fn meta(&self) -> M {
        self.with_handle(|inner| inner.meta.clone())
    }

    /// Applies a function to the values corresponding to the key, and returns the result.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, this function returns `None`.
    ///
    /// If no values exist for the given key, the function will not be called, and `None` will be
    /// returned.
    pub fn get_and<Q: ?Sized, F, T>(&self, key: &Q, then: F) -> Option<T>
        where F: FnOnce(&[V]) -> T,
              K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.with_handle(move |inner| if !inner.is_ready() {
                             None
                         } else {
                             inner.data.get(key).map(move |v| then(&**v))
                         })
    }

    /// Applies a function to the values corresponding to the key, and returns the result alongside
    /// the meta information.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    ///
    /// Note that not all writes will be included with this read -- only those that have been
    /// refreshed by the writer. If no refresh has happened, this function returns `None`.
    ///
    /// If no values exist for the given key, the function will not be called, and `Some(None, _)`
    /// will be returned.
    pub fn meta_get_and<Q: ?Sized, F, T>(&self, key: &Q, then: F) -> Option<(Option<T>, M)>
        where F: FnOnce(&[V]) -> T,
              K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.with_handle(move |inner| if !inner.is_ready() {
                             None
                         } else {
                             let res = inner.data.get(key).map(move |v| then(&**v));
                             let res = (res, inner.meta.clone());
                             Some(res)
                         })
    }

    /// Returns true if the map contains any values for the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but `Hash` and `Eq` on the borrowed
    /// form *must* match those for the key type.
    pub fn contains_key<Q: ?Sized>(&self, key: &Q) -> bool
        where K: Borrow<Q>,
              Q: Hash + Eq
    {
        self.with_handle(move |inner| inner.data.contains_key(key))
    }

    /// Read all values in the map, and transform them into a new collection.
    ///
    /// Be careful with this function! While the iteration is ongoing, any writer that tries to
    /// refresh will block waiting on this reader to finish.
    pub fn for_each<F>(&self, mut f: F)
        where F: FnMut(&K, &[V])
    {
        self.with_handle(move |inner| for (k, vs) in &inner.data {
                             f(k, &vs[..])
                         })
    }

    /// Read all values in the map, and transform them into a new collection.
    pub fn map_into<Map, Collector, Target>(&self, mut f: Map) -> Collector
        where Map: FnMut(&K, &[V]) -> Target,
              Collector: FromIterator<Target>
    {
        self.with_handle(move |inner| {
                             Collector::from_iter(inner.data.iter().map(|(k, vs)| f(k, &vs[..])))
                         })
    }
}
