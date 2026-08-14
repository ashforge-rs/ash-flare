//! Child management - handles both worker and supervisor children

use super::handle::SupervisorHandle;
use super::spec::SupervisorSpec;
use crate::restart::RestartPolicy;
use crate::types::ChildType;
use crate::worker::{Worker, WorkerProcess, WorkerSpec};
use std::sync::Arc;

/// Represents either a worker or a nested supervisor in the supervision tree
pub(crate) enum Child<W: Worker> {
    Worker(WorkerProcess<W>),
    Supervisor {
        handle: SupervisorHandle<W>,
        spec: Arc<SupervisorSpec<W>>,
    },
}

impl<W: Worker> Child<W> {
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

    #[inline]
    #[allow(clippy::unnecessary_wraps)]
    pub fn restart_policy(&self) -> Option<RestartPolicy> {
        match self {
            Child::Worker(w) => Some(w.spec.restart_policy),
            Child::Supervisor { .. } => Some(RestartPolicy::Permanent),
        }
    }

    /// Policy as seen by the shared restart decision: `None` for nested
    /// supervisors, which are always treated as permanent.
    #[inline]
    pub fn restart_policy_for_decision(&self) -> Option<RestartPolicy> {
        match self {
            Child::Worker(w) => Some(w.spec.restart_policy),
            Child::Supervisor { .. } => None,
        }
    }

    /// Stops the child, allowing `timeout` for a worker to wind down
    /// cooperatively before its task is aborted.
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

/// Holds information needed to restart a child after termination
pub(crate) enum RestartInfo<W: Worker> {
    Worker(WorkerSpec<W>),
    Supervisor(Arc<SupervisorSpec<W>>),
}
