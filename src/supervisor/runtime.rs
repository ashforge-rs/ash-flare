//! Supervisor runtime - internal state machine

use super::child::{Child, RestartInfo};
use super::error::SupervisorError;
use super::handle::SupervisorHandle;
use super::spec::{ChildSpec, SupervisorSpec};
use crate::restart::{RestartPolicy, RestartStrategy, RestartTracker};
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

/// Internal state machine that manages supervisor lifecycle and child processes
pub(crate) struct SupervisorRuntime<W: Worker> {
    name: String,
    children: Vec<Child<W>>,
    control_rx: mpsc::UnboundedReceiver<SupervisorCommand<W>>,
    control_tx: mpsc::UnboundedSender<SupervisorCommand<W>>,
    restart_strategy: RestartStrategy,
    restart_tracker: RestartTracker,
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
                    let supervisor = SupervisorHandle::start((*supervisor_spec).clone());
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
            created_at: std::time::Instant::now(),
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(command) = self.control_rx.recv().await {
            match command {
                SupervisorCommand::StartChild { spec, respond_to } => {
                    let result = self.handle_start_child(spec).await;
                    let _ = respond_to.send(result);
                }
                SupervisorCommand::StartChildLinked {
                    spec,
                    timeout,
                    respond_to,
                } => {
                    let result = self.handle_start_child_linked(spec, timeout).await;
                    let _ = respond_to.send(result);
                }
                SupervisorCommand::TerminateChild { id, respond_to } => {
                    let result = self.handle_terminate_child(&id).await;
                    let _ = respond_to.send(result);
                }
                SupervisorCommand::WhichChildren { respond_to } => {
                    let result = self.handle_which_children();
                    let _ = respond_to.send(result);
                }
                SupervisorCommand::GetRestartStrategy { respond_to } => {
                    let _ = respond_to.send(self.restart_strategy);
                }
                SupervisorCommand::GetUptime { respond_to } => {
                    let uptime = self.created_at.elapsed().as_secs();
                    let _ = respond_to.send(uptime);
                }
                SupervisorCommand::ChildTerminated { id, reason } => {
                    self.handle_child_terminated(id, reason).await;
                }
                SupervisorCommand::Shutdown => {
                    self.shutdown_children().await;
                    return;
                }
            }
        }

        self.shutdown_children().await;
    }

    async fn handle_start_child(
        &mut self,
        spec: WorkerSpec<W>,
    ) -> Result<ChildId, SupervisorError> {
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
        child.shutdown().await;

        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            "terminated child"
        );
        Ok(())
    }

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

    async fn handle_child_terminated(&mut self, id: ChildId, reason: ChildExitReason) {
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            reason = ?reason,
            "child terminated"
        );

        let position = match self.children.iter().position(|c| c.id() == id) {
            Some(pos) => pos,
            None => {
                tracing::warn!(
                    supervisor = %self.name,
                    child = %id,
                    "terminated child not found in list"
                );
                return;
            }
        };

        // Determine if we should restart based on policy and reason
        let should_restart = match &self.children[position] {
            Child::Worker(w) => match w.spec.restart_policy {
                RestartPolicy::Permanent => true,
                RestartPolicy::Temporary => false,
                RestartPolicy::Transient => reason == ChildExitReason::Abnormal,
            },
            Child::Supervisor { .. } => true, // Supervisors are always permanent
        };

        if !should_restart {
            tracing::debug!(
                supervisor = %self.name,
                child = %id,
                policy = ?self.children[position].restart_policy(),
                reason = ?reason,
                "not restarting child"
            );
            self.children.remove(position);
            return;
        }

        // Check restart intensity
        if self.restart_tracker.record_restart() {
            tracing::error!(
                supervisor = %self.name,
                "restart intensity exceeded, shutting down"
            );
            self.shutdown_children().await;
            return;
        }

        // Apply restart strategy
        match self.restart_strategy {
            RestartStrategy::OneForOne => {
                self.restart_child(position).await;
            }
            RestartStrategy::OneForAll => {
                self.restart_all_children().await;
            }
            RestartStrategy::RestForOne => {
                self.restart_from(position).await;
            }
        }
    }

    async fn restart_child(&mut self, position: usize) {
        // Extract spec info before shutdown
        let restart_info = match &self.children[position] {
            Child::Worker(worker) => RestartInfo::Worker(worker.spec.clone()),
            Child::Supervisor { spec, .. } => RestartInfo::Supervisor(Arc::clone(spec)),
        };

        // Shutdown old child
        self.children[position].shutdown().await;

        // Restart based on type
        match restart_info {
            RestartInfo::Worker(spec) => {
                tracing::debug!(
                    supervisor = %self.name,
                    worker = %spec.id,
                    "restarting worker"
                );
                let new_worker =
                    WorkerProcess::spawn(spec.clone(), self.name.clone(), self.control_tx.clone());
                self.children[position] = Child::Worker(new_worker);
                tracing::debug!(
                    supervisor = %self.name,
                    worker = %spec.id,
                    "worker restarted"
                );
            }
            RestartInfo::Supervisor(spec) => {
                let name = spec.name.clone();
                tracing::debug!(
                    supervisor = %self.name,
                    child_supervisor = %name,
                    "restarting supervisor"
                );
                let new_handle = SupervisorHandle::start((*spec).clone());
                self.children[position] = Child::Supervisor {
                    handle: new_handle,
                    spec,
                };
                tracing::debug!(
                    supervisor = %self.name,
                    child_supervisor = %name,
                    "supervisor restarted"
                );
            }
        }
    }

    async fn restart_all_children(&mut self) {
        tracing::debug!(
            supervisor = %self.name,
            "restarting all children (one_for_all)"
        );

        // Shutdown all children
        for child in &mut self.children {
            child.shutdown().await;
        }

        // Restart all worker children
        for child in &mut self.children {
            if let Child::Worker(worker) = child {
                let spec = worker.spec.clone();
                let new_worker =
                    WorkerProcess::spawn(spec.clone(), self.name.clone(), self.control_tx.clone());
                *child = Child::Worker(new_worker);
                tracing::debug!(
                    supervisor = %self.name,
                    child = %spec.id,
                    "child restarted"
                );
            }
        }
    }

    async fn restart_from(&mut self, position: usize) {
        tracing::debug!(
            supervisor = %self.name,
            position = %position,
            "restarting from position (rest_for_one)"
        );

        for i in position..self.children.len() {
            self.children[i].shutdown().await;

            if let Child::Worker(worker) = &self.children[i] {
                let spec = worker.spec.clone();
                let new_worker =
                    WorkerProcess::spawn(spec.clone(), self.name.clone(), self.control_tx.clone());
                self.children[i] = Child::Worker(new_worker);
                tracing::debug!(
                    supervisor = %self.name,
                    child = %spec.id,
                    "child restarted"
                );
            }
        }
    }

    async fn shutdown_children(&mut self) {
        for child in self.children.drain(..) {
            let id = child.id().to_owned();
            let mut child = child;
            child.shutdown().await;
            tracing::debug!(
                supervisor = %self.name,
                child = %id,
                "shut down child"
            );
        }
    }
}
