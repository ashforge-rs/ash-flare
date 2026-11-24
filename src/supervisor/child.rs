//! Child management - handles both worker and supervisor children

use super::handle::SupervisorHandle;
use super::spec::SupervisorSpec;
use crate::restart::RestartPolicy;
use crate::types::ChildType;
use crate::worker::{WorkerProcess, deadline_after};
use std::collections::HashMap;
use std::sync::Arc;

/// Represents either a worker or a nested supervisor in the supervision tree
pub(crate) enum Child {
    Worker(WorkerProcess),
    Supervisor {
        handle: SupervisorHandle,
        spec: Arc<SupervisorSpec>,
    },
}

impl Child {
    #[inline]
    pub fn id(&self) -> &str {
        match self {
            Child::Worker(w) => &w.spec.id,
            Child::Supervisor { spec, .. } => &spec.name,
        }
    }

    /// The child's id as a shared string, used as its key in [`Children`].
    #[inline]
    fn key(&self) -> &Arc<str> {
        match self {
            Child::Worker(w) => &w.spec.id,
            Child::Supervisor { handle, .. } => &handle.name,
        }
    }

    #[inline]
    pub fn child_type(&self) -> ChildType {
        match self {
            Child::Worker(_) => ChildType::Worker,
            Child::Supervisor { .. } => ChildType::Supervisor,
        }
    }

    /// The child's restart policy: `None` for nested supervisors, which have no
    /// policy of their own and are always treated as permanent.
    #[inline]
    pub fn restart_policy(&self) -> Option<RestartPolicy> {
        match self {
            Child::Worker(w) => Some(w.spec.restart_policy),
            Child::Supervisor { .. } => None,
        }
    }

    /// Stops the child, allowing `timeout` for a worker to wind down
    /// cooperatively before its task is aborted. A nested supervisor is waited
    /// on until its own children have stopped.
    pub async fn shutdown(&mut self, timeout: std::time::Duration) {
        match self {
            Child::Worker(w) => {
                let _graceful = w.stop(timeout).await;
            }
            Child::Supervisor { handle, .. } => {
                let _shutdown_result = handle.shutdown().await;
            }
        }
    }
}

/// Stops every child in `children` at the same time.
///
/// All workers are signalled and all nested supervisors told to shut down
/// before anything is waited on, and the workers share one deadline. Stopping
/// them one after another would make the total time the *sum* of every child's
/// cleanup (up to `timeout` per uncooperative worker) instead of the slowest.
pub(crate) async fn stop_all(children: &mut [Child], timeout: std::time::Duration) {
    let mut nested = Vec::new();
    for child in children.iter_mut() {
        match child {
            Child::Worker(worker) => worker.request_stop(),
            Child::Supervisor { handle, .. } => nested.push(handle.begin_shutdown()),
        }
    }

    let deadline = deadline_after(timeout);
    for child in children.iter_mut() {
        if let Child::Worker(worker) = child {
            let _graceful = if timeout.is_zero() {
                worker.abort().await
            } else {
                worker.stop_by(deadline).await
            };
        }
    }

    // Nested supervisors apply their own shutdown timeout to their children.
    for done in nested.into_iter().flatten() {
        let _stopped = done.await;
    }
}

/// A supervisor's children in start order, indexed by id.
///
/// Termination, restart and `terminate_child` look a child up by id; the index
/// makes that O(1) instead of a scan over every sibling. Positions only change
/// when a child is removed, which is when the index is rebuilt. Keys are the
/// children's own shared ids, so rebuilding never allocates strings.
pub(crate) struct Children {
    list: Vec<Child>,
    index: HashMap<Arc<str>, usize>,
}

impl Children {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            list: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Position of the child with this id.
    pub fn position(&self, id: &str) -> Option<usize> {
        self.index.get(id).copied()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.index.contains_key(id)
    }

    pub fn push(&mut self, child: Child) {
        self.index.insert(Arc::clone(child.key()), self.list.len());
        self.list.push(child);
    }

    pub fn remove(&mut self, position: usize) -> Child {
        let child = self.list.remove(position);
        self.reindex();
        child
    }

    /// Keeps only the children for which `keep` returns true.
    pub fn retain(&mut self, keep: impl FnMut(&Child) -> bool) {
        self.list.retain(keep);
        self.reindex();
    }

    pub fn clear(&mut self) {
        self.list.clear();
        self.index.clear();
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Child> {
        self.list.iter()
    }

    /// The children as a slice, to stop or respawn them in place. Replacing a
    /// child with one of the same id keeps the index valid.
    pub fn as_mut_slice(&mut self) -> &mut [Child] {
        &mut self.list
    }

    fn reindex(&mut self) {
        self.index.clear();
        for (position, child) in self.list.iter().enumerate() {
            self.index.insert(Arc::clone(child.key()), position);
        }
    }
}

impl std::ops::Index<usize> for Children {
    type Output = Child;

    #[allow(clippy::indexing_slicing)]
    fn index(&self, position: usize) -> &Child {
        &self.list[position]
    }
}

impl std::ops::IndexMut<usize> for Children {
    #[allow(clippy::indexing_slicing)]
    fn index_mut(&mut self, position: usize) -> &mut Child {
        &mut self.list[position]
    }
}
