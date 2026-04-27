use alloc::boxed::Box;
use core::{
    pin::Pin,
    ptr::{addr_of_mut, NonNull},
};

use cordyceps::{stack::Links, Linked, Stack, TransferStack};
use crossbeam_utils::CachePadded;

use crate::sync::{Arc, AtomicUsize};

pub(crate) type Epoch = Arc<CachePadded<AtomicUsize>>;

/// Passes copies of [`ReadHandle`]'s [`Epoch`]s to writer.
///
/// [`ReadHandle`]s don't clean up anything on [`drop`], so there is opportunity for an unbounded
/// memory growth in case [`WriteHandle`] does nothing and new [`ReadHandle`]s are created and
/// dropped.
///
/// [`WriteHandle`] cleans up [`Epoch`]s with [`Arc::strong_count`]` == 1` on [`publish`]/successful
/// [`try_publish`]/[`flush`].
///
/// [`ReadHandle`]: crate::ReadHandle
/// [`WriteHandle`]: crate::WriteHandle
/// [`publish`]: crate::WriteHandle::publish
/// [`try_publish`]: crate::WriteHandle::try_publish
/// [`flush`]: crate::WriteHandle::flush
#[derive(Clone, Debug, Default)]
pub(crate) struct Epochs(Arc<TransferStack<Entry>>);

impl Epochs {
    /// Pushes [`Epoch`] for [`WriteHandle`] to take all of at once with [`Self::take_all`].
    ///
    /// [`WriteHandle`]: crate::WriteHandle
    pub(crate) fn push(&self, epoch: Epoch) {
        self.0.push(Box::pin(Entry {
            epoch,
            links: Links::new(),
        }));
    }

    /// Takes all [`Epoch`]s and returns [`Iterator`] over them.
    pub(crate) fn take_all(&self) -> EpochIter {
        EpochIter(self.0.take_all())
    }
}

/// Entry for an intrusive [`TransferStack`] that stores both [`Epoch`] and a link to the next
/// [`Entry`].
struct Entry {
    // TODO: AFAIK it would to UB to implement `Linked` directly for `Handle` of
    //       `Pin<Arc<struct Entry { epoch: CachePadded<AtomicUsize>, links: Links<Self> }>>`,
    //       as we would be able to obtain `&Entry` while mutating through `Linked::links()`
    //       impl that obtains `&mut Links`, this should be an instant UB.
    //       But! I think it would be safe to implement custom `Arc` that has `Deref` impl
    //       directly into `&CachePadded<AtomicUsize>` and leave mutable access only through
    //       `Linked::links()`. This would eliminate 1 allocation on reader init.
    epoch: Epoch,
    links: Links<Self>,
}

// SAFETY: impl taken directly from `cordyceps` tests.
unsafe impl Linked<Links<Self>> for Entry {
    type Handle = Pin<Box<Self>>;

    fn into_ptr(r: Self::Handle) -> NonNull<Self> {
        unsafe { NonNull::new_unchecked(Box::leak(Pin::into_inner_unchecked(r))) }
    }

    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        Pin::new_unchecked(Box::from_raw(ptr.as_ptr()))
    }

    unsafe fn links(ptr: NonNull<Self>) -> NonNull<Links<Self>> {
        let links = addr_of_mut!((*ptr.as_ptr()).links);
        NonNull::new_unchecked(links)
    }
}

pub(crate) struct EpochIter(Stack<Entry>);

impl Iterator for EpochIter {
    type Item = Epoch;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop().map(|e|
                // SAFETY: data under the `Pin` is never moved.
                unsafe { Pin::into_inner_unchecked(e) }.epoch)
    }
}
