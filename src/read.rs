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
#[derive(Clone)]
pub struct ReadHandle<K, V, M = (), S = RandomState>
    where K: Eq + Hash,
          S: BuildHasher
{
    pub(crate) inner: sync::Arc<AtomicPtr<sync::Arc<Inner<K, V, M, S>>>>,
}

pub fn new<K, V, M, S>(inner: Inner<K, V, M, S>) -> ReadHandle<K, V, M, S>
    where K: Eq + Hash,
          S: BuildHasher
{
    let store = Box::into_raw(Box::new(sync::Arc::new(inner)));
    ReadHandle { inner: sync::Arc::new(AtomicPtr::new(store)) }
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
        let r_handle = unsafe { Box::from_raw(self.inner.load(atomic::Ordering::SeqCst)) };
        let rs: sync::Arc<_> = (&*r_handle).clone();

        let r_handle_again = unsafe { Box::from_raw(self.inner.load(atomic::Ordering::SeqCst)) };
        #[allow(collapsible_if)]
        let res = if !sync::Arc::ptr_eq(&*r_handle, &*r_handle_again) {
            // a swap happened under us.
            //
            // let's first figure out where the writer can possibly be at this point. since we *do*
            // have a clone of an Arc, and a pointer swap has happened, we must be in one of the
            // following cases:
            //
            //  (a) we read and cloned, *then* a writer swapped, checked for uniqueness, and blocks
            //  (b) we read, then a writer swapped, checked for uniqueness, and continued
            //
            // in (a), we know that the writer that did the pointer swap is waiting on our Arc (rs)
            // we also know that we *ought* to be using a clone of the *new* pointer to read from.
            // since the writer won't do anything until we release rs, we can just do a new clone,
            // and *then* release the old Arc.
            //
            // in (b), we know that there is a writer that thinks it owns rs, and so it is not safe
            // to use rs. how about Arc(r_handle_again)? the only way that would be unsafe to use
            // would be if a writer has gone all the way through the pointer swap and Arc
            // uniqueness test *after* we read r_handle_again. can that have happened? no. if a
            // second writer came along after we read r_handle_again, and wanted to swap, it would
            // get r_handle from its atomic pointer swap. since we're still holding an
            // Arc(r_handle), it would fail the uniqueness test, and therefore block at that point.
            // thus, we know that no writer is currently modifying r_handle_again (and won't be for
            // as long as we hold rs).
            let rs2: sync::Arc<_> = (&*r_handle_again).clone();
            let res = f(&*rs2);
            drop(rs2);
            drop(rs);
            res
        } else {
            // are we actually safe in this case? could there not have been two swaps in a row,
            // making the value r_handle -> r_handle_again -> r_handle? well, there could, but that
            // implies that r_handle is safe to use. why? well, for this to have happened, we must
            // have read the pointer, then a writer went runs swap() (potentially multiple times).
            // then, at some point, we cloned(). then, we read r_handle_again to be equal to
            // r_handle.
            //
            // the moment we clone, we immediately prevent any writer from taking ownership of
            // r_handle. however, what if the current writer thinks that it owns r_handle? well, we
            // read r_handle_again == r_handle, which means that at some point *after the clone*, a
            // writer made r_handle read owned. since no writer can take ownership of r_handle
            // after we cloned it, we know that r_handle must still be owned by readers.
            let res = f(&*rs);
            drop(rs);
            res
        };
        mem::forget(r_handle); // don't free the Box!
        mem::forget(r_handle_again); // don't free the Box!
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
