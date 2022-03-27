use core::fmt::{Debug, Formatter};

use crate::sync::{Arc, AtomicPtr, AtomicUsize, Ordering};
use alloc::boxed::Box;

// TODO
// * For now I'm just using Ordering::SeqCst, because I havent really looked into what exactly we
// need for the Ordering, so this should probably be made more accurate in the Future

/// A Lock-Free List of Handles
pub struct HandleList {
    // The Head of the List
    head: AtomicPtr<ListEntry>,
}

/// A Snapshot of the HandleList
///
/// Iterating over this Snapshot only yields the Entries that were present when this Snapshot was taken
pub struct ListSnapshot {
    // The Head-Ptr at the time of creation
    head: *const ListEntry,
}

/// An Iterator over the Entries in a Snapshot
pub struct SnapshotIter {
    // A Pointer to the next Entry that will be yielded
    current: *const ListEntry,
}

struct ListEntry {
    data: Arc<AtomicUsize>,
    // We can use a normal Ptr here because we never append or remove Entries and only add new Entries
    // by changing the Head, so we never modify this Ptr and therefore dont need an AtomicPtr
    next: *const Self,
}

impl HandleList {
    /// Creates a new empty HandleList
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(core::ptr::null_mut()),
        }
    }

    /// Adds a new Entry to the List and returns the Counter for the Entry
    pub fn new_entry(&self) -> Arc<AtomicUsize> {
        let count = Arc::new(AtomicUsize::new(0));

        self.add_counter(count.clone());
        count
    }
    fn add_counter(&self, count: Arc<AtomicUsize>) {
        let n_node = Box::new(ListEntry {
            data: count,
            next: core::ptr::null(),
        });
        let n_node_ptr = Box::into_raw(n_node);

        let mut current_head = self.head.load(Ordering::SeqCst);
        loop {
            // Safety
            // This is save, because we have not stored the Ptr elsewhere so we have exclusive
            // access.
            // The Ptr is also still valid, because we never free Entries on the List
            unsafe { (*n_node_ptr).next = current_head };

            // Attempt to add the Entry to the List by setting it as the new Head
            match self.head.compare_exchange(
                current_head,
                n_node_ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
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
            head: self.head.load(Ordering::SeqCst),
        }
    }

    /// Inserts the Items of the Iterator, but in reverse order
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

impl ListSnapshot {
    /// Obtain an iterator over the Entries in this Snapshot
    pub fn iter(&self) -> SnapshotIter {
        SnapshotIter { current: self.head }
    }
}

impl Iterator for SnapshotIter {
    // TODO
    // Maybe don't return an owned Value here
    type Item = Arc<AtomicUsize>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }

        // Safety
        // The Ptr is not null, because of the previous if-statement.
        // The Data is also not freed, because we never free Entries on the List.
        // We also have no one mutating Entries on the List and therefore we can access this without
        // any extra synchronization needed.
        let entry = unsafe { &*self.current };

        self.current = entry.next;

        Some(entry.data.clone())
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
        entry.store(1, Ordering::SeqCst);

        // Make sure that the Snapshot we got before adding a new Entry is still empty
        assert_eq!(0, empty_snapshot.iter().count());

        let second_snapshot = list.snapshot();
        assert_eq!(1, second_snapshot.iter().count());

        let snapshot_entry = second_snapshot.iter().next().unwrap();
        assert_eq!(
            entry.load(Ordering::SeqCst),
            snapshot_entry.load(Ordering::SeqCst)
        );
    }
}
