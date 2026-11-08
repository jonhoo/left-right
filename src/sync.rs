#[cfg(loom)]
pub(crate) use loom::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Mutex, MutexGuard};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Mutex, MutexGuard};
