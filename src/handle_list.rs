use core::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
    sync::atomic::AtomicBool,
};

use crate::sync::{Arc, AtomicPtr, AtomicUsize, Ordering};
use alloc::boxed::Box;

// TODO
// * For now I'm just using Ordering::SeqCst, because I havent really looked into what exactly we
// need for the Ordering, so this should probably be made more accurate in the Future

/// A Lock-Free List of Handles
pub struct HandleList {
    inner: Arc<InnerList>,
}

struct InnerList {
    // The Head of the List
    head: AtomicPtr<ListEntry>,
}

/// A Snapshot of the HandleList
///
/// Iterating over this Snapshot only yields the Entries that were present when this Snapshot was taken
pub struct ListSnapshot {
    // The Head-Ptr at the time of creation
    head: *const ListEntry,

    // This entry exists to make sure that we keep the inner List alive and it wont be freed from under us
    _list: Arc<InnerList>,
}

/// An Iterator over the Entries in a Snapshot
pub struct SnapshotIter<'s> {
    // A Pointer to the next Entry that will be yielded
    current: *const ListEntry,
    _marker: PhantomData<&'s ()>,
}

struct ListEntry {
    data: Arc<AtomicUsize>,
    used: AtomicBool,
    // Stores the number of following entries in the list
    followers: usize,
    // We can use a normal Ptr here because we never append or remove Entries and only add new Entries
    // by changing the Head, so we never modify this Ptr and therefore dont need an AtomicPtr
    next: *const Self,
}

/// The EntryHandle is needed to allow for reuse of entries, after a handle is dropped
#[derive(Debug)]
pub struct EntryHandle {
    counter: Arc<AtomicUsize>,
    elem: *const ListEntry,
}

impl HandleList {
    /// Creates a new empty HandleList
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerList {
                head: AtomicPtr::new(core::ptr::null_mut()),
            }),
        }
    }

    fn len(&self) -> usize {
        let head = self.inner.head.load(Ordering::SeqCst);
        if head.is_null() {
            return 0;
        }

        // Safety
        // The prt is not null and as entries are never deallocated, so the ptr should always be
        // valid
        unsafe { (*head).followers + 1 }
    }

    /// Obtains a new Entry
    pub fn get_entry(&self) -> EntryHandle {
        if let Some(entry) = self.try_acquire() {
            return entry;
        }

        self.new_entry()
    }

    fn try_acquire(&self) -> Option<EntryHandle> {
        let mut current: *const ListEntry = self.inner.head.load(Ordering::SeqCst);
        while !current.is_null() {
            // Safety
            // The ptr is not null and entries are never deallocated
            let current_entry = unsafe { &*current };

            if !current_entry.used.load(Ordering::SeqCst) {
                if current_entry
                    .used
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Some(EntryHandle {
                        counter: current_entry.data.clone(),
                        elem: current,
                    });
                }
            }

            current = current_entry.next;
        }

        None
    }

    /// Adds a new Entry to the List and returns the Counter for the Entry
    fn new_entry(&self) -> EntryHandle {
        let count = Arc::new(AtomicUsize::new(0));

        self.add_counter(count)
    }

    /// Adds a new Counter to the List of Entries, increasing the size of the List
    fn add_counter(&self, count: Arc<AtomicUsize>) -> EntryHandle {
        let counter = count.clone();

        let n_node = Box::new(ListEntry {
            data: count,
            used: AtomicBool::new(true),
            followers: 0,
            next: core::ptr::null(),
        });
        let n_node_ptr = Box::into_raw(n_node);

        let mut current_head = self.inner.head.load(Ordering::SeqCst);
        loop {
            // Safety
            // This is save, because we have not stored the Ptr elsewhere so we have exclusive
            // access.
            // The Ptr is also still valid, because we never free Entries on the List
            unsafe { (*n_node_ptr).next = current_head };

            // Update the follower count of the new entry
            if !current_head.is_null() {
                // Safety
                // This is save, because we know the Ptr is not null and we know that
                // Entries will never be deallocated, so the ptr still refers to a valid
                // entry.
                unsafe { (*n_node_ptr).followers = (*current_head).followers + 1 };
            }

            // Attempt to add the Entry to the List by setting it as the new Head
            match self.inner.head.compare_exchange(
                current_head,
                n_node_ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return EntryHandle {
                        counter,
                        elem: n_node_ptr,
                    }
                }
                Err(n_head) => {
                    // Store the found Head-Ptr to avoid an extra load at the start of every loop
                    current_head = n_head;
                }
            }
        }
    }

    /// Creates a new Snapshot of the List at this Point in Time
    pub fn snapshot(&self) -> ListSnapshot {
        ListSnapshot {
            head: self.inner.head.load(Ordering::SeqCst),
            _list: self.inner.clone(),
        }
    }

    /// Inserts the Items of the Iterator, but in reverse order
    #[cfg(test)]
    pub fn extend<I>(&self, iter: I)
    where
        I: IntoIterator<Item = Arc<AtomicUsize>>,
    {
        for item in iter.into_iter() {
            self.add_counter(item);
        }
    }
}

impl Default for HandleList {
    fn default() -> Self {
        Self::new()
    }
}
impl Debug for HandleList {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        // TODO
        // Figure out how exactly we want the Debug output to look
        write!(f, "HandleList")
    }
}
impl Clone for HandleList {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ListSnapshot {
    /// Obtain an iterator over the Entries in this Snapshot
    pub fn iter(&self) -> SnapshotIter<'_> {
        SnapshotIter {
            current: self.head,
            _marker: PhantomData {},
        }
    }

    /// Get the Length of the current List of entries
    pub fn len(&self) -> usize {
        if self.head.is_null() {
            return 0;
        }

        unsafe { (*self.head).followers + 1 }
    }
}

impl<'s> Iterator for SnapshotIter<'s> {
    // TODO
    // Maybe don't return an owned Value here
    type Item = &'s AtomicUsize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }

        // # Safety
        // The Ptr is not null, because of the previous if-statement.
        //
        // The Data is also not freed yet, because we know that at least one snapshot is still
        // alive (because we bind to it through the lifetime) and as long as at least one
        // snapshot exists, the InnerList will not be freed or dropped. This means that the entries
        // in the List are also not yet freed and therefore its safe to still access them
        let entry = unsafe { &*self.current };

        self.current = entry.next;

        Some(&entry.data)
    }
}

impl Drop for InnerList {
    fn drop(&mut self) {
        // We iterate over all the Entries of the List and free every Entry of the List
        let mut current = *self.head.get_mut();
        while !current.is_null() {
            // # Safety
            // This is safe, because we only enter the loop body if the Pointer is not null and we
            // also know that the Entry is not yet freed because we only free them once we are dropped
            // and because we are now in Drop, noone before us has freed any Entry on the List
            let next = unsafe { &*current }.next as *mut ListEntry;

            // # Safety
            // 1. We know that the Pointer was allocated using Box::new
            // 2. We are the only ones to convert it back into a Box again, because we only ever do
            // this when the InnerList is dropped (now) and then also free all the Entries so there
            // is no chance of one entry surviving or still being stored somewhere for later use.
            // 3. There is also no other reference to the Element, because otherwise the InnerList
            // could not be dropped and we would not be in this section
            let entry = unsafe { Box::from_raw(current) };
            drop(entry);

            current = next;
        }
    }
}

impl EntryHandle {
    pub fn counter(&self) -> &AtomicUsize {
        &self.counter
    }
}
impl Drop for EntryHandle {
    fn drop(&mut self) {
        let elem = unsafe { &*self.elem };
        elem.used.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_list() {
        let list = HandleList::new();
        drop(list);
    }

    #[test]
    fn empty_snapshot() {
        let list = HandleList::new();

        let snapshot = list.snapshot();

        // Assert that the Iterator over the Snapshot is empty
        assert_eq!(0, snapshot.iter().count());
    }

    #[test]
    fn snapshots_and_entries() {
        let list = HandleList::new();

        let empty_snapshot = list.snapshot();
        assert_eq!(0, empty_snapshot.iter().count());

        let entry = list.new_entry();
        entry.counter().store(1, Ordering::SeqCst);

        // Make sure that the Snapshot we got before adding a new Entry is still empty
        assert_eq!(0, empty_snapshot.iter().count());

        let second_snapshot = list.snapshot();
        assert_eq!(1, second_snapshot.iter().count());

        let snapshot_entry = second_snapshot.iter().next().unwrap();
        assert_eq!(
            entry.counter().load(Ordering::SeqCst),
            snapshot_entry.load(Ordering::SeqCst)
        );
    }

    #[test]
    fn entry_reuse() {
        let list = HandleList::new();

        assert_eq!(0, list.len());

        let entry1 = list.get_entry();
        assert_eq!(1, list.len());
        drop(entry1);

        let entry2 = list.get_entry();
        assert_eq!(1, list.len());
    }
}
