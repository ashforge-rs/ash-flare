//! Child management - handles both worker and supervisor children

use super::handle::SupervisorHandle;
use super::spec::SupervisorSpec;
use crate::restart::RestartPolicy;
use crate::types::ChildType;
use crate::worker::{WorkerProcess, deadline_after};
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
