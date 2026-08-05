use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::DAG;

/// A thread-safe wrapper around `DAG` using `Arc<RwLock<...>>`.
///
/// `ConcurrentDAG` is a thin convenience type for Rust consumers who need
/// shared ownership of a DAG across multiple threads. Multiple readers can
/// access the DAG concurrently; writers get exclusive access.
///
/// For Python consumers, the GIL already serializes access, so `PyDAG`
/// does not wrap `ConcurrentDAG`. Use `snapshot()` instead for isolation.
pub struct ConcurrentDAG<P = ()> {
    inner: Arc<RwLock<DAG<P>>>,
}

impl<P> ConcurrentDAG<P> {
    /// Create a new `ConcurrentDAG` wrapping an empty `DAG`.
    pub fn new() -> Self {
        ConcurrentDAG {
            inner: Arc::new(RwLock::new(DAG::new())),
        }
    }

    /// Wrap an existing `DAG` in a `ConcurrentDAG`.
    pub fn from_dag(dag: DAG<P>) -> Self {
        ConcurrentDAG {
            inner: Arc::new(RwLock::new(dag)),
        }
    }

    /// Acquire a read lock on the inner DAG.
    pub fn read(&self) -> RwLockReadGuard<'_, DAG<P>> {
        self.inner.read().unwrap()
    }

    /// Acquire a write lock on the inner DAG.
    pub fn write(&self) -> RwLockWriteGuard<'_, DAG<P>> {
        self.inner.write().unwrap()
    }

    /// Consume this `ConcurrentDAG` and return the inner `DAG`,
    /// if this is the sole owner. Returns `None` if other clones exist.
    pub fn into_inner(self) -> Option<DAG<P>> {
        Arc::try_unwrap(self.inner)
            .ok()
            .map(|rwlock| rwlock.into_inner().unwrap())
    }
}

impl<P: Clone> ConcurrentDAG<P> {
    /// Take a snapshot of the DAG under a read lock.
    pub fn snapshot(&self) -> DAG<P> {
        self.read().snapshot()
    }
}

impl<P> Clone for ConcurrentDAG<P> {
    fn clone(&self) -> Self {
        ConcurrentDAG {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<P> Default for ConcurrentDAG<P> {
    fn default() -> Self {
        Self::new()
    }
}
