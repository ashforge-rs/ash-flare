//! Child management - handles both worker and supervisor children

use super::handle::SupervisorHandle;
use super::spec::{Shutdown, ShutdownOrder, SupervisorSpec};
use crate::restart::RestartPolicy;
use crate::types::ChildType;
use crate::worker::{WorkerProcess, deadline_after};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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

    /// Identifies this run of the child; a restart gets a new one.
    #[inline]
    pub fn instance(&self) -> u64 {
        match self {
            Child::Worker(w) => w.instance,
            Child::Supervisor { handle, .. } => handle.instance,
        }
    }

    /// The child's id as a shared string, used as its key in [`Children`].
    #[inline]
    pub fn key(&self) -> &Arc<str> {
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

    /// The policy restart decisions use. Nested supervisors carry their own,
    /// set with `SupervisorSpec::with_restart_policy`.
    #[inline]
    pub fn policy(&self) -> RestartPolicy {
        match self {
            Child::Worker(w) => w.spec.restart_policy,
            Child::Supervisor { spec, .. } => spec.restart_policy,
        }
    }

    /// Whether the child counts towards its supervisor's auto shutdown.
    #[inline]
    pub fn is_significant(&self) -> bool {
        match self {
            Child::Worker(w) => w.spec.significant,
            Child::Supervisor { .. } => false,
        }
    }

    /// How this child is stopped: its own [`Shutdown`] setting, or the
    /// supervisor's timeout. Nested supervisors are always waited on, since
    /// they apply their own children's settings.
    fn shutdown_mode(&self, default_timeout: Duration) -> Shutdown {
        match self {
            Child::Worker(w) => match w.spec.shutdown {
                Some(Shutdown::Timeout(timeout)) if timeout.is_zero() => Shutdown::BrutalKill,
                Some(mode) => mode,
                None if default_timeout.is_zero() => Shutdown::BrutalKill,
                None => Shutdown::Timeout(default_timeout),
            },
            Child::Supervisor { .. } => Shutdown::Infinity,
        }
    }

    /// Asks the child to stop without waiting for it. Returns what to wait on.
    fn begin_stop(&mut self, default_timeout: Duration) -> PendingStop {
        let mode = self.shutdown_mode(default_timeout);
        match self {
            Child::Worker(worker) => {
                // Checked before asking: a worker that already finished has
                // reported its own exit, which must not be overwritten.
                let was_running = worker.is_running();
                let wait = match mode {
                    Shutdown::BrutalKill => Wait::Abort,
                    Shutdown::Timeout(timeout) => {
                        worker.request_stop();
                        Wait::Worker(deadline_after(timeout))
                    }
                    Shutdown::Infinity => {
                        worker.request_stop();
                        Wait::Worker(None)
                    }
                };
                PendingStop { was_running, wait }
            }
            Child::Supervisor { handle, .. } => PendingStop {
                was_running: !handle.is_stopped(),
                wait: Wait::Supervisor(handle.begin_shutdown()),
            },
        }
    }

    /// Waits for a stop started with [`begin_stop`](Self::begin_stop).
    async fn finish_stop(&mut self, pending: PendingStop) -> StopOutcome {
        let stopped_itself = match (self, pending.wait) {
            (Child::Worker(worker), Wait::Abort) => worker.abort().await,
            (Child::Worker(worker), Wait::Worker(deadline)) => worker.stop_by(deadline).await,
            (_, Wait::Supervisor(Some(done))) => {
                let _stopped = done.await;
                true
            }
            _ => true,
        };

        if !pending.was_running {
            StopOutcome::AlreadyExited
        } else if stopped_itself {
            StopOutcome::Stopped
        } else {
            StopOutcome::Killed
        }
    }

    /// Stops the child according to its shutdown setting and waits for it.
    pub async fn shutdown(&mut self, default_timeout: Duration) -> StopOutcome {
        let pending = self.begin_stop(default_timeout);
        self.finish_stop(pending).await
    }
}

/// How a stop requested by the supervisor ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopOutcome {
    /// The child had already exited on its own; its exit report says why.
    AlreadyExited,
    /// The child stopped cooperatively. It still sends an exit report, which
    /// the supervisor recognises as its own doing.
    Stopped,
    /// The child's task was aborted; it sends no exit report.
    Killed,
}

/// A stop that has been requested but not yet waited for.
struct PendingStop {
    was_running: bool,
    wait: Wait,
}

enum Wait {
    /// Abort the worker outright.
    Abort,
    /// Wait for the signalled worker until this deadline (`None`: forever).
    Worker(Option<tokio::time::Instant>),
    /// Wait for the nested supervisor to report it has stopped.
    Supervisor(Option<tokio::sync::oneshot::Receiver<()>>),
}

/// Stops every child in `children`, each according to its own shutdown
/// setting.
///
/// With [`ShutdownOrder::Reverse`] children are stopped one at a time, last
/// started first, so nothing is stopped while a later child that may depend on
/// it is still running. With [`ShutdownOrder::Concurrent`] every child is asked
/// first and then waited for, so the total time is the slowest child's rather
/// than the sum.
///
/// Returns each child's [`StopOutcome`], in the same order as `children`.
pub(crate) async fn stop_all(
    children: &mut [Child],
    default_timeout: Duration,
    order: ShutdownOrder,
) -> Vec<StopOutcome> {
    match order {
        ShutdownOrder::Reverse => {
            let mut outcomes = vec![StopOutcome::AlreadyExited; children.len()];
            for (child, outcome) in children.iter_mut().zip(outcomes.iter_mut()).rev() {
                *outcome = child.shutdown(default_timeout).await;
            }
            outcomes
        }
        ShutdownOrder::Concurrent => {
            let pending: Vec<PendingStop> = children
                .iter_mut()
                .map(|child| child.begin_stop(default_timeout))
                .collect();
            let mut outcomes = Vec::with_capacity(children.len());
            for (child, stop) in children.iter_mut().zip(pending) {
                outcomes.push(child.finish_stop(stop).await);
            }
            outcomes
        }
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
