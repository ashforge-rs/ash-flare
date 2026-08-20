//! Supervisor runtime - internal state machine

use super::child::Child;
use super::error::SupervisorError;
use super::handle::SupervisorHandle;
use super::spec::{ChildSpec, SupervisorSpec};
use crate::restart::{BackoffTracker, RestartPolicy, RestartStrategy, RestartTracker};
use crate::supervisor_common::{RestartDecision, decide_restart};
use crate::types::{ChildExitReason, ChildId, ChildInfo};
use crate::worker::{Worker, WorkerProcess, WorkerSpec, WorkerTermination};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Internal commands sent to supervisor runtime
pub(crate) enum SupervisorCommand<W: Worker> {
    StartChild {
        spec: WorkerSpec<W>,
        respond_to: oneshot::Sender<Result<ChildId, SupervisorError>>,
    },
    StartChildLinked {
        spec: WorkerSpec<W>,
        timeout: std::time::Duration,
        respond_to: oneshot::Sender<Result<ChildId, SupervisorError>>,
    },
    TerminateChild {
        id: ChildId,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    WhichChildren {
        respond_to: oneshot::Sender<Result<Vec<ChildInfo>, SupervisorError>>,
    },
    GetRestartStrategy {
        respond_to: oneshot::Sender<RestartStrategy>,
    },
    GetUptime {
        respond_to: oneshot::Sender<u64>,
    },
    ChildTerminated {
        id: ChildId,
        reason: ChildExitReason,
    },
    /// Fires once a restart backoff has elapsed. Restarts are deferred through
    /// the command queue so the supervisor keeps serving commands while a child
    /// is backing off.
    RestartChild {
        id: ChildId,
        strategy: RestartStrategy,
    },
    Shutdown,
}

impl<W: Worker> From<WorkerTermination> for SupervisorCommand<W> {
    fn from(term: WorkerTermination) -> Self {
        SupervisorCommand::ChildTerminated {
            id: term.id,
            reason: term.reason,
        }
    }
}

/// Whether the runtime loop should keep serving commands or terminate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeControl {
    /// Keep processing commands.
    Continue,
    /// Terminate the supervisor task.
    Stop,
}

/// Internal state machine that manages supervisor lifecycle and child processes
pub(crate) struct SupervisorRuntime<W: Worker> {
    name: String,
    children: Vec<Child<W>>,
    control_rx: mpsc::UnboundedReceiver<SupervisorCommand<W>>,
    control_tx: mpsc::UnboundedSender<SupervisorCommand<W>>,
    restart_strategy: RestartStrategy,
    restart_tracker: RestartTracker,
    backoff_tracker: BackoffTracker,
    shutdown_timeout: std::time::Duration,
    created_at: std::time::Instant,
}

impl<W: Worker> SupervisorRuntime<W> {
    pub(crate) fn new(
        spec: SupervisorSpec<W>,
        control_rx: mpsc::UnboundedReceiver<SupervisorCommand<W>>,
        control_tx: mpsc::UnboundedSender<SupervisorCommand<W>>,
    ) -> Self {
        let mut children = Vec::with_capacity(spec.children.len());

        for child_spec in spec.children {
            match child_spec {
                ChildSpec::Worker(worker_spec) => {
                    let worker =
                        WorkerProcess::spawn(worker_spec, spec.name.clone(), control_tx.clone());
                    children.push(Child::Worker(worker));
                }
                ChildSpec::Supervisor(supervisor_spec) => {
                    let supervisor = SupervisorHandle::start_supervised(
                        (*supervisor_spec).clone(),
                        control_tx.clone(),
                    );
                    children.push(Child::Supervisor {
                        handle: supervisor,
                        spec: Arc::clone(&supervisor_spec),
                    });
                }
            }
        }

        Self {
            name: spec.name,
            children,
            control_rx,
            control_tx,
            restart_strategy: spec.restart_strategy,
            restart_tracker: RestartTracker::new(spec.restart_intensity),
            backoff_tracker: BackoffTracker::new(spec.restart_backoff),
            shutdown_timeout: spec.shutdown_timeout,
            created_at: std::time::Instant::now(),
        }
    }

    /// Serves commands until the supervisor terminates, and reports *why* it
    /// terminated so a parent supervisor can apply its own policy.
    pub(crate) async fn run(mut self) -> ChildExitReason {
        while let Some(command) = self.control_rx.recv().await {
            match command {
                SupervisorCommand::StartChild { spec, respond_to } => {
                    let result = self.handle_start_child(spec);
                    let _send = respond_to.send(result);
                }
                SupervisorCommand::StartChildLinked {
                    spec,
                    timeout,
                    respond_to,
                } => {
                    let result = self.handle_start_child_linked(spec, timeout).await;
                    let _send = respond_to.send(result);
                }
                SupervisorCommand::TerminateChild { id, respond_to } => {
                    let result = self.handle_terminate_child(&id).await;
                    let _send = respond_to.send(result);
                }
                SupervisorCommand::WhichChildren { respond_to } => {
                    let result = self.handle_which_children();
                    let _send = respond_to.send(result);
                }
                SupervisorCommand::GetRestartStrategy { respond_to } => {
                    let _send = respond_to.send(self.restart_strategy);
                }
                SupervisorCommand::GetUptime { respond_to } => {
                    let uptime = self.created_at.elapsed().as_secs();
                    let _send = respond_to.send(uptime);
                }
                SupervisorCommand::ChildTerminated { id, reason } => {
                    if self.handle_child_terminated(id, reason).await == RuntimeControl::Stop {
                        // Restart intensity exceeded: children are already shut
                        // down. Terminate the supervisor itself, reporting an
                        // abnormal exit so a parent supervisor observes the
                        // failure and applies its own policy, as OTP does.
                        return ChildExitReason::Abnormal;
                    }
                }
                SupervisorCommand::RestartChild { id, strategy } => {
                    self.handle_restart_child(&id, strategy).await;
                }
                SupervisorCommand::Shutdown => {
                    self.shutdown_children().await;
                    return ChildExitReason::Shutdown;
                }
            }
        }

        self.shutdown_children().await;
        ChildExitReason::Shutdown
    }

    fn handle_start_child(&mut self, spec: WorkerSpec<W>) -> Result<ChildId, SupervisorError> {
        // Check if child with same ID already exists
        if self.children.iter().any(|c| c.id() == spec.id) {
            return Err(SupervisorError::ChildAlreadyExists(spec.id.clone()));
        }

        let id = spec.id.clone();
        let worker = WorkerProcess::spawn(spec, self.name.clone(), self.control_tx.clone());

        self.children.push(Child::Worker(worker));
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            "dynamically started child"
        );

        Ok(id)
    }

    async fn handle_start_child_linked(
        &mut self,
        spec: WorkerSpec<W>,
        timeout: std::time::Duration,
    ) -> Result<ChildId, SupervisorError> {
        // Check if child with same ID already exists
        if self.children.iter().any(|c| c.id() == spec.id) {
            return Err(SupervisorError::ChildAlreadyExists(spec.id.clone()));
        }

        let id = spec.id.clone();
        let (init_tx, init_rx) = oneshot::channel();

        let worker = WorkerProcess::spawn_with_link(
            spec,
            self.name.clone(),
            self.control_tx.clone(),
            init_tx,
        );

        // Wait for initialization with timeout
        let init_result = tokio::time::timeout(timeout, init_rx).await;

        match init_result {
            Ok(Ok(Ok(()))) => {
                // Initialization succeeded
                self.children.push(Child::Worker(worker));
                tracing::debug!(
                    supervisor = %self.name,
                    child = %id,
                    "linked child started successfully"
                );
                Ok(id)
            }
            Ok(Ok(Err(reason))) => {
                // Initialization failed - worker sent error
                tracing::error!(
                    supervisor = %self.name,
                    child = %id,
                    reason = %reason,
                    "linked child initialization failed"
                );
                // Note: init failures do NOT trigger restart policies
                Err(SupervisorError::InitializationFailed {
                    child_id: id,
                    reason,
                })
            }
            Ok(Err(_)) => {
                // Channel closed - worker panicked before sending result
                tracing::error!(
                    supervisor = %self.name,
                    child = %id,
                    "linked child panicked during initialization"
                );
                Err(SupervisorError::InitializationFailed {
                    child_id: id,
                    reason: "worker panicked during initialization".to_owned(),
                })
            }
            Err(_) => {
                // Timeout
                tracing::error!(
                    supervisor = %self.name,
                    child = %id,
                    timeout_secs = ?timeout.as_secs(),
                    "linked child initialization timed out"
                );
                Err(SupervisorError::InitializationTimeout {
                    child_id: id,
                    timeout,
                })
            }
        }
    }

    async fn handle_terminate_child(&mut self, id: &str) -> Result<(), SupervisorError> {
        let position = self
            .children
            .iter()
            .position(|c| c.id() == id)
            .ok_or_else(|| SupervisorError::ChildNotFound(id.to_owned()))?;

        let mut child = self.children.remove(position);
        child.shutdown(self.shutdown_timeout).await;
        self.backoff_tracker.reset_child(id);

        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            "terminated child"
        );
        Ok(())
    }

    #[allow(clippy::unnecessary_wraps)]
    fn handle_which_children(&self) -> Result<Vec<ChildInfo>, SupervisorError> {
        let info = self
            .children
            .iter()
            .map(|child| ChildInfo {
                id: child.id().to_owned(),
                child_type: child.child_type(),
                restart_policy: child.restart_policy(),
            })
            .collect();

        Ok(info)
    }

    #[allow(clippy::indexing_slicing)]
    async fn handle_child_terminated(
        &mut self,
        id: ChildId,
        reason: ChildExitReason,
    ) -> RuntimeControl {
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            reason = ?reason,
            "child terminated"
        );

        let Some(position) = self.children.iter().position(|c| c.id() == id) else {
            tracing::warn!(
                supervisor = %self.name,
                child = %id,
                "terminated child not found in list"
            );
            return RuntimeControl::Continue;
        };

        // Supervision semantics live in one shared place; this runtime only
        // carries out the decision.
        let decision = decide_restart(
            self.children[position].restart_policy_for_decision(),
            reason,
            self.restart_strategy,
            &mut self.restart_tracker,
            &mut self.backoff_tracker,
            &id,
        );

        match decision {
            RestartDecision::Ignore => {
                // The supervisor stopped this child itself, as part of a restart
                // round or a shutdown. Nothing to do; the child list already
                // reflects whatever that operation intended.
                RuntimeControl::Continue
            }
            RestartDecision::Drop => {
                tracing::debug!(
                    supervisor = %self.name,
                    child = %id,
                    policy = ?self.children[position].restart_policy(),
                    reason = ?reason,
                    "not restarting child"
                );
                self.children.remove(position);
                RuntimeControl::Continue
            }
            RestartDecision::Escalate => {
                tracing::error!(
                    supervisor = %self.name,
                    "restart intensity exceeded, shutting down"
                );
                self.shutdown_children().await;
                RuntimeControl::Stop
            }
            RestartDecision::Restart { delay, strategy } => {
                if delay.is_zero() {
                    self.apply_restart(position, strategy).await;
                } else {
                    self.defer_restart(id, delay, strategy);
                }

                RuntimeControl::Continue
            }
        }
    }

    /// Schedules a restart for after `delay` without blocking the command loop.
    ///
    /// Sleeping inline would stall `Shutdown`, `which_children` and every other
    /// command for the whole backoff - up to 30s with the default settings.
    fn defer_restart(&self, id: ChildId, delay: std::time::Duration, strategy: RestartStrategy) {
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            delay_ms = %delay.as_millis(),
            "waiting before restart"
        );

        let control_tx = self.control_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _send = control_tx.send(SupervisorCommand::RestartChild { id, strategy });
        });
    }

    /// Carries out a restart whose backoff has elapsed.
    async fn handle_restart_child(&mut self, id: &str, strategy: RestartStrategy) {
        let Some(position) = self.children.iter().position(|c| c.id() == id) else {
            // Terminated or dropped while it was backing off.
            tracing::debug!(
                supervisor = %self.name,
                child = %id,
                "child is no longer supervised, skipping deferred restart"
            );
            return;
        };

        self.apply_restart(position, strategy).await;
    }

    async fn apply_restart(&mut self, position: usize, strategy: RestartStrategy) {
        match strategy {
            RestartStrategy::OneForOne => self.restart_child(position).await,
            RestartStrategy::OneForAll => self.restart_all_children().await,
            RestartStrategy::RestForOne => self.restart_from(position).await,
        }
    }

    #[allow(clippy::indexing_slicing)]
    async fn restart_child(&mut self, position: usize) {
        self.children[position]
            .shutdown(self.shutdown_timeout)
            .await;

        let id = Self::respawn(&mut self.children[position], &self.name, &self.control_tx);
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            "child restarted"
        );
    }

    async fn restart_all_children(&mut self) {
        tracing::debug!(
            supervisor = %self.name,
            "restarting all children (one_for_all)"
        );

        // Shutdown all children
        for child in &mut self.children {
            child.shutdown(self.shutdown_timeout).await;
        }

        let mut kept = Vec::with_capacity(self.children.len());
        for mut child in self.children.drain(..) {
            if Self::leaves_supervision(&child) {
                tracing::debug!(
                    supervisor = %self.name,
                    child = %child.id(),
                    "temporary child not restarted, dropping from supervision"
                );
                self.backoff_tracker.reset_child(child.id());
                continue;
            }

            // Nested supervisors were just shut down too, so skipping them would
            // strand a dead handle in the children list.
            let id = Self::respawn(&mut child, &self.name, &self.control_tx);
            tracing::debug!(
                supervisor = %self.name,
                child = %id,
                "child restarted"
            );
            kept.push(child);
        }
        self.children = kept;
    }

    /// Whether a child stopped as part of a restart round is gone for good.
    ///
    /// A `Temporary` child is never restarted, not even when it was a sibling's
    /// failure that stopped it, so it leaves supervision here.
    fn leaves_supervision(child: &Child<W>) -> bool {
        child.restart_policy() == Some(RestartPolicy::Temporary)
    }

    /// Replaces a child that has been shut down with a freshly started one,
    /// returning its id.
    fn respawn(
        child: &mut Child<W>,
        supervisor_name: &str,
        control_tx: &mpsc::UnboundedSender<SupervisorCommand<W>>,
    ) -> ChildId {
        match child {
            Child::Worker(worker) => {
                let spec = worker.spec.clone();
                let id = spec.id.clone();
                *child = Child::Worker(WorkerProcess::spawn(
                    spec,
                    supervisor_name.to_owned(),
                    control_tx.clone(),
                ));
                id
            }
            Child::Supervisor { spec, .. } => {
                let spec = Arc::clone(spec);
                let id = spec.name.clone();
                let handle =
                    SupervisorHandle::start_supervised((*spec).clone(), control_tx.clone());
                *child = Child::Supervisor { handle, spec };
                id
            }
        }
    }

    #[allow(clippy::indexing_slicing)]
    async fn restart_from(&mut self, position: usize) {
        tracing::debug!(
            supervisor = %self.name,
            position = %position,
            "restarting from position (rest_for_one)"
        );

        let mut kept = Vec::new();
        for mut child in self.children.split_off(position) {
            child.shutdown(self.shutdown_timeout).await;

            if Self::leaves_supervision(&child) {
                tracing::debug!(
                    supervisor = %self.name,
                    child = %child.id(),
                    "temporary child not restarted, dropping from supervision"
                );
                self.backoff_tracker.reset_child(child.id());
                continue;
            }

            let id = Self::respawn(&mut child, &self.name, &self.control_tx);
            tracing::debug!(
                supervisor = %self.name,
                child = %id,
                "child restarted"
            );
            kept.push(child);
        }
        self.children.append(&mut kept);
    }

    async fn shutdown_children(&mut self) {
        for mut child in self.children.drain(..) {
            let id = child.id().to_owned();
            child.shutdown(self.shutdown_timeout).await;
            tracing::debug!(
                supervisor = %self.name,
                child = %id,
                "shut down child"
            );
        }
    }
}
