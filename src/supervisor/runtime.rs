//! Supervisor runtime - internal state machine

use super::child::{Child, Children, StopOutcome, stop_all};
use super::error::SupervisorError;
use super::handle::SupervisorHandle;
use super::spec::{AutoShutdown, ChildDef, ShutdownOrder, SupervisorSpec};
use crate::events::{EventSender, SupervisorEvent, emit};
use crate::restart::{BackoffTracker, RestartPolicy, RestartStrategy, RestartTracker};
use crate::stall::StallWatch;
use crate::supervisor_common::{RestartDecision, decide_restart};
use crate::types::{ChildExitReason, ChildId, ChildInfo, ChildType};
use crate::worker::{SpawnContext, WorkerProcess, WorkerSpec};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Internal commands sent to supervisor runtime
pub(crate) enum SupervisorCommand {
    StartChild {
        spec: WorkerSpec,
        respond_to: oneshot::Sender<Result<ChildId, SupervisorError>>,
    },
    /// Starts a child under a fresh id made from the spec's id as a prefix.
    StartUniqueChild {
        spec: WorkerSpec,
        respond_to: oneshot::Sender<ChildId>,
    },
    StartChildLinked {
        spec: WorkerSpec,
        timeout: Duration,
        respond_to: oneshot::Sender<Result<ChildId, SupervisorError>>,
    },
    TerminateChild {
        id: ChildId,
        respond_to: oneshot::Sender<Result<(), SupervisorError>>,
    },
    /// Kills a child so that its restart policy applies.
    KillChild {
        id: ChildId,
        respond_to: oneshot::Sender<Result<bool, SupervisorError>>,
    },
    WhichChildren {
        respond_to: oneshot::Sender<Vec<ChildInfo>>,
    },
    GetRestartStrategy {
        respond_to: oneshot::Sender<RestartStrategy>,
    },
    GetUptime {
        respond_to: oneshot::Sender<Duration>,
    },
    /// Everything a status report needs, in one round trip.
    GetStatus {
        respond_to: oneshot::Sender<StatusSnapshot>,
    },
    /// A child's exit report. `instance` tells reports from a child that has
    /// since been replaced apart from reports about the current one.
    ChildTerminated {
        id: Arc<str>,
        instance: u64,
        reason: ChildExitReason,
    },
    /// Fires once a restart backoff has elapsed. Restarts are deferred through
    /// the command queue so the supervisor keeps serving commands while a child
    /// is backing off.
    RestartChild {
        id: Arc<str>,
        strategy: RestartStrategy,
    },
    /// Stops every child, then the supervisor itself. `respond_to` is told
    /// once the children are down.
    Shutdown {
        respond_to: Option<oneshot::Sender<()>>,
    },
}

/// A point-in-time summary of a supervisor, answered by a single command.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StatusSnapshot {
    pub children_count: usize,
    pub restart_strategy: RestartStrategy,
    pub uptime: Duration,
}

/// Whether the runtime loop should keep serving commands or terminate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeControl {
    /// Keep processing commands.
    Continue,
    /// Terminate the supervisor, reporting this reason to its parent. Its
    /// children have already been shut down.
    Exit(ChildExitReason),
}

/// Internal state machine that manages supervisor lifecycle and child processes
pub(crate) struct SupervisorRuntime {
    /// Shared with the handle and every worker task, so spawning and
    /// restarting a worker never copies it.
    name: Arc<str>,
    /// Handed to every worker this supervisor starts.
    spawn: SpawnContext,
    children: Children,
    control_rx: mpsc::UnboundedReceiver<SupervisorCommand>,
    control_tx: mpsc::UnboundedSender<SupervisorCommand>,
    restart_strategy: RestartStrategy,
    restart_tracker: RestartTracker,
    backoff_tracker: BackoffTracker,
    shutdown_timeout: Duration,
    shutdown_order: ShutdownOrder,
    auto_shutdown: AutoShutdown,
    /// Next number to try when generating a child id from a prefix.
    next_unique: u64,
    created_at: std::time::Instant,
    events: EventSender,
    /// Instances this supervisor stopped and already reported as terminated;
    /// their own exit reports are not reported again.
    silenced: HashSet<u64>,
}

impl SupervisorRuntime {
    pub(crate) fn new(
        spec: SupervisorSpec,
        name: Arc<str>,
        control_rx: mpsc::UnboundedReceiver<SupervisorCommand>,
        control_tx: mpsc::UnboundedSender<SupervisorCommand>,
        events: EventSender,
    ) -> Self {
        let spawn = SpawnContext {
            supervisor_name: Arc::clone(&name),
            control_tx: control_tx.clone(),
            stall: spec.stall_threshold.map(|threshold| StallWatch {
                threshold,
                events: events.clone(),
            }),
        };
        let mut children = Children::with_capacity(spec.children.len());

        for child_spec in spec.children {
            match child_spec {
                ChildDef::Worker(worker_spec) => {
                    let worker = WorkerProcess::spawn(worker_spec, &spawn);
                    children.push(Child::Worker(worker));
                }
                ChildDef::Supervisor(supervisor_spec) => {
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

        for child in children.iter() {
            emit(
                &events,
                SupervisorEvent::ChildStarted {
                    id: Arc::clone(child.key()),
                },
            );
        }

        Self {
            name,
            spawn,
            children,
            control_rx,
            control_tx,
            restart_strategy: spec.restart_strategy,
            restart_tracker: RestartTracker::new(spec.restart_intensity),
            backoff_tracker: BackoffTracker::new(spec.restart_backoff),
            shutdown_timeout: spec.shutdown_timeout,
            shutdown_order: spec.shutdown_order,
            auto_shutdown: spec.auto_shutdown,
            next_unique: 0,
            created_at: std::time::Instant::now(),
            events,
            silenced: HashSet::new(),
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
                SupervisorCommand::StartUniqueChild {
                    mut spec,
                    respond_to,
                } => {
                    spec.id = self.unique_id(&spec.id);
                    // The id was just checked to be free, so this cannot fail.
                    let result = self.handle_start_child(spec);
                    if let Ok(id) = result {
                        let _send = respond_to.send(id);
                    }
                }
                SupervisorCommand::StartChildLinked {
                    spec,
                    timeout,
                    respond_to,
                } => {
                    let result = self.handle_start_child_linked(spec, timeout).await;
                    let _send = respond_to.send(result);
                }
                SupervisorCommand::KillChild { id, respond_to } => {
                    let (result, control) = self.handle_kill_child(&id).await;
                    let _send = respond_to.send(result);
                    if let RuntimeControl::Exit(exit) = control {
                        return exit;
                    }
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
                SupervisorCommand::GetStatus { respond_to } => {
                    let _send = respond_to.send(StatusSnapshot {
                        children_count: self.children.len(),
                        restart_strategy: self.restart_strategy,
                        uptime: self.created_at.elapsed(),
                    });
                }
                SupervisorCommand::GetUptime { respond_to } => {
                    let uptime = self.created_at.elapsed();
                    let _send = respond_to.send(uptime);
                }
                SupervisorCommand::ChildTerminated {
                    id,
                    instance,
                    reason,
                } => {
                    if let RuntimeControl::Exit(exit) =
                        self.handle_child_terminated(id, instance, reason).await
                    {
                        // Children are already shut down: either restart
                        // intensity was exceeded (abnormal, so the parent applies
                        // its own policy, as OTP does) or significant children
                        // ended (shutdown).
                        return exit;
                    }
                }
                SupervisorCommand::RestartChild { id, strategy } => {
                    self.handle_restart_child(&id, strategy).await;
                }
                SupervisorCommand::Shutdown { respond_to } => {
                    self.shutdown_children().await;
                    // Close before acknowledging, so a caller whose `shutdown`
                    // has returned already sees the handle as stopped.
                    self.control_rx.close();
                    if let Some(tx) = respond_to {
                        let _send = tx.send(());
                    }
                    return ChildExitReason::Shutdown;
                }
            }
        }

        self.shutdown_children().await;
        ChildExitReason::Shutdown
    }

    /// A child id of the form `{prefix}-{n}` not used by any current child.
    fn unique_id(&mut self, prefix: &str) -> Arc<str> {
        loop {
            let candidate = format!("{prefix}-{}", self.next_unique);
            self.next_unique = self.next_unique.wrapping_add(1);
            if !self.children.contains(&candidate) {
                return Arc::from(candidate);
            }
        }
    }

    fn handle_start_child(&mut self, spec: WorkerSpec) -> Result<ChildId, SupervisorError> {
        if self.children.contains(&spec.id) {
            return Err(SupervisorError::ChildAlreadyExists(spec.id.to_string()));
        }

        let id = spec.id.to_string();
        let worker = WorkerProcess::spawn(spec, &self.spawn);

        self.push_started(Child::Worker(worker));
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            "dynamically started child"
        );

        Ok(id)
    }

    async fn handle_start_child_linked(
        &mut self,
        spec: WorkerSpec,
        timeout: Duration,
    ) -> Result<ChildId, SupervisorError> {
        if self.children.contains(&spec.id) {
            return Err(SupervisorError::ChildAlreadyExists(spec.id.to_string()));
        }

        let id = spec.id.to_string();
        let (init_tx, init_rx) = oneshot::channel();

        let worker = WorkerProcess::spawn_with_link(spec, &self.spawn, init_tx);

        // Wait for initialization with timeout
        let init_result = tokio::time::timeout(timeout, init_rx).await;

        match init_result {
            Ok(Ok(Ok(()))) => {
                // Initialization succeeded
                self.push_started(Child::Worker(worker));
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
            .position(id)
            .ok_or_else(|| SupervisorError::ChildNotFound(id.to_owned()))?;

        let mut child = self.children.remove(position);
        let outcome = child.shutdown(self.shutdown_timeout).await;
        self.record_stop(&child, outcome);
        self.backoff_tracker.reset_child(id);

        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            "terminated child"
        );
        Ok(())
    }

    /// Kills a child's current instance and handles it as an exit with
    /// [`ChildExitReason::Killed`], so its restart policy applies.
    async fn handle_kill_child(
        &mut self,
        id: &str,
    ) -> (Result<bool, SupervisorError>, RuntimeControl) {
        let Some(position) = self.children.position(id) else {
            return (
                Err(SupervisorError::ChildNotFound(id.to_owned())),
                RuntimeControl::Continue,
            );
        };

        let instance = self.children[position].instance();
        let killed = match &mut self.children[position] {
            Child::Worker(worker) => worker.kill().await,
            Child::Supervisor { handle, .. } => {
                if handle.is_stopped() {
                    false
                } else {
                    if let Some(done) = handle.begin_shutdown() {
                        let _stopped = done.await;
                    }
                    true
                }
            }
        };
        if !killed {
            // It had already exited; its own report decides what happens.
            return (Ok(false), RuntimeControl::Continue);
        }
        // A nested supervisor still reports its stop; that report is ours.
        if self.children[position].child_type() == ChildType::Supervisor {
            self.silenced.insert(instance);
        }

        let key = Arc::clone(self.children[position].key());
        tracing::debug!(supervisor = %self.name, child = %key, "killed child");
        emit(
            &self.events,
            SupervisorEvent::ChildTerminated {
                id: Arc::clone(&key),
                reason: ChildExitReason::Killed,
            },
        );
        let control = self
            .on_child_exit(position, key, ChildExitReason::Killed)
            .await;
        (Ok(true), control)
    }

    fn handle_which_children(&self) -> Vec<ChildInfo> {
        self.children
            .iter()
            .map(|child| ChildInfo {
                id: child.id().to_owned(),
                child_type: child.child_type(),
                restart_policy: child.restart_policy(),
            })
            .collect()
    }

    async fn handle_child_terminated(
        &mut self,
        id: Arc<str>,
        instance: u64,
        reason: ChildExitReason,
    ) -> RuntimeControl {
        tracing::debug!(
            supervisor = %self.name,
            child = %id,
            reason = ?reason,
            "child terminated"
        );

        let current = self
            .children
            .position(&id)
            .filter(|&position| self.children[position].instance() == instance);
        let Some(position) = current else {
            // The report is about an instance that has since been stopped,
            // replaced or removed. It must not trigger a restart of whatever
            // now holds the id; it is only reported if nobody has yet.
            if !self.silenced.remove(&instance) {
                emit(
                    &self.events,
                    SupervisorEvent::ChildTerminated { id, reason },
                );
            }
            tracing::debug!(
                supervisor = %self.name,
                instance,
                "exit report from a replaced or removed child instance"
            );
            return RuntimeControl::Continue;
        };

        emit(
            &self.events,
            SupervisorEvent::ChildTerminated {
                id: Arc::clone(&id),
                reason,
            },
        );

        self.on_child_exit(position, id, reason).await
    }

    /// Applies the restart decision for the current instance of the child at
    /// `position`, which has terminated with `reason`.
    async fn on_child_exit(
        &mut self,
        position: usize,
        id: Arc<str>,
        reason: ChildExitReason,
    ) -> RuntimeControl {
        // A nested supervisor reporting `Shutdown` about its current instance
        // was not stopped by this supervisor (that would make the report
        // stale): it shut itself down through its auto_shutdown, which counts
        // as ending normally.
        let effective = if self.children[position].child_type() == ChildType::Supervisor
            && reason == ChildExitReason::Shutdown
        {
            ChildExitReason::Normal
        } else {
            reason
        };

        // Supervision semantics live in one shared place; this runtime only
        // carries out the decision.
        let decision = decide_restart(
            Some(self.children[position].policy()),
            effective,
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
                    policy = ?self.children[position].policy(),
                    reason = ?reason,
                    "not restarting child"
                );
                let ended = self.children.remove(position);
                if self.auto_shutdown_due(&ended) {
                    tracing::debug!(
                        supervisor = %self.name,
                        child = %id,
                        "significant child ended, shutting down"
                    );
                    self.shutdown_children().await;
                    return RuntimeControl::Exit(ChildExitReason::Shutdown);
                }
                RuntimeControl::Continue
            }
            RestartDecision::Escalate => {
                tracing::error!(
                    supervisor = %self.name,
                    "restart intensity exceeded, shutting down"
                );
                emit(&self.events, SupervisorEvent::RestartIntensityExceeded);
                self.shutdown_children().await;
                RuntimeControl::Exit(ChildExitReason::Abnormal)
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

    /// Whether `ended`, a child that just terminated on its own and is not
    /// being restarted, makes this supervisor shut itself down.
    fn auto_shutdown_due(&self, ended: &Child) -> bool {
        if !ended.is_significant() {
            return false;
        }
        match self.auto_shutdown {
            AutoShutdown::Never => false,
            AutoShutdown::AnySignificant => true,
            AutoShutdown::AllSignificant => !self.children.iter().any(Child::is_significant),
        }
    }

    /// Schedules a restart for after `delay` without blocking the command loop.
    ///
    /// Sleeping inline would stall `Shutdown`, `which_children` and every other
    /// command for the whole backoff - up to 30s with the default settings.
    fn defer_restart(&self, id: Arc<str>, delay: Duration, strategy: RestartStrategy) {
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
        let Some(position) = self.children.position(id) else {
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

    async fn restart_child(&mut self, position: usize) {
        let outcome = self.children[position]
            .shutdown(self.shutdown_timeout)
            .await;
        let stopped = &self.children[position];
        Self::record_stop_into(&mut self.silenced, &self.events, stopped, outcome);

        let child = &mut self.children[position];
        Self::respawn(child, &self.spawn, &self.events);
        tracing::debug!(
            supervisor = %self.name,
            child = %child.id(),
            "child restarted"
        );
    }

    async fn restart_all_children(&mut self) {
        tracing::debug!(
            supervisor = %self.name,
            "restarting all children (one_for_all)"
        );

        // Nested supervisors are stopped and respawned too; skipping them
        // would strand a dead handle in the children list.
        self.restart_range(0).await;
    }

    /// Whether a child stopped as part of a restart round is gone for good.
    ///
    /// A `Temporary` child is never restarted, not even when it was a sibling's
    /// failure that stopped it, so it leaves supervision here.
    fn leaves_supervision(child: &Child) -> bool {
        child.policy() == RestartPolicy::Temporary
    }

    /// Replaces a child that has been shut down with a freshly started one of
    /// the same id, in place, so its position and index entry stay valid.
    fn respawn(child: &mut Child, spawn: &SpawnContext, events: &EventSender) {
        emit(
            events,
            SupervisorEvent::ChildRestarted {
                id: Arc::clone(child.key()),
            },
        );
        match child {
            Child::Worker(worker) => {
                let spec = worker.spec.clone();
                *child = Child::Worker(WorkerProcess::spawn(spec, spawn));
            }
            Child::Supervisor { spec, .. } => {
                let spec = Arc::clone(spec);
                let handle =
                    SupervisorHandle::start_supervised((*spec).clone(), spawn.control_tx.clone());
                *child = Child::Supervisor { handle, spec };
            }
        }
    }

    async fn restart_from(&mut self, position: usize) {
        tracing::debug!(
            supervisor = %self.name,
            position = %position,
            "restarting from position (rest_for_one)"
        );

        self.restart_range(position).await;
    }

    /// The children from `position` on, which a restart round affects.
    fn affected_from(children: &mut Children, position: usize) -> &mut [Child] {
        children
            .as_mut_slice()
            .get_mut(position..)
            .unwrap_or_default()
    }

    /// Restarts every child from `position` on: stops them together, then
    /// starts them again in their original order, in place. `Temporary`
    /// children leave supervision instead of being restarted.
    async fn restart_range(&mut self, position: usize) {
        let (timeout, order) = (self.shutdown_timeout, self.shutdown_order);
        let outcomes = stop_all(
            Self::affected_from(&mut self.children, position),
            timeout,
            order,
        )
        .await;

        let affected = Self::affected_from(&mut self.children, position);
        Self::record_stops(&mut self.silenced, &self.events, affected, &outcomes, order);

        let mut dropped_any = false;
        for child in affected {
            if Self::leaves_supervision(child) {
                tracing::debug!(
                    supervisor = %self.name,
                    child = %child.id(),
                    "temporary child not restarted, dropping from supervision"
                );
                self.backoff_tracker.reset_child(child.id());
                dropped_any = true;
                continue;
            }

            Self::respawn(child, &self.spawn, &self.events);
            tracing::debug!(
                supervisor = %self.name,
                child = %child.id(),
                "child restarted"
            );
        }

        // Only removing a child moves the others, so only then is the index
        // rebuilt. A stopped `Temporary` child has already been shut down.
        if dropped_any {
            self.children
                .retain(|child| !Self::leaves_supervision(child));
        }
    }

    async fn shutdown_children(&mut self) {
        let outcomes = stop_all(
            self.children.as_mut_slice(),
            self.shutdown_timeout,
            self.shutdown_order,
        )
        .await;
        let order = self.shutdown_order;
        Self::record_stops(
            &mut self.silenced,
            &self.events,
            self.children.as_mut_slice(),
            &outcomes,
            order,
        );
        tracing::debug!(
            supervisor = %self.name,
            children = self.children.len(),
            "shut down children"
        );
        self.children.clear();
    }

    /// Adds a newly started child and reports it.
    fn push_started(&mut self, child: Child) {
        emit(
            &self.events,
            SupervisorEvent::ChildStarted {
                id: Arc::clone(child.key()),
            },
        );
        self.children.push(child);
    }

    /// Reports a child this supervisor stopped. See [`Self::record_stop_into`].
    fn record_stop(&mut self, child: &Child, outcome: StopOutcome) {
        Self::record_stop_into(&mut self.silenced, &self.events, child, outcome);
    }

    /// Reports the children stopped together by [`stop_all`], in the order
    /// they were stopped.
    fn record_stops(
        silenced: &mut HashSet<u64>,
        events: &EventSender,
        children: &[Child],
        outcomes: &[StopOutcome],
        order: ShutdownOrder,
    ) {
        let stops = children.iter().zip(outcomes.iter().copied());
        match order {
            ShutdownOrder::Reverse => {
                for (child, outcome) in stops.rev() {
                    Self::record_stop_into(silenced, events, child, outcome);
                }
            }
            ShutdownOrder::Concurrent => {
                for (child, outcome) in stops {
                    Self::record_stop_into(silenced, events, child, outcome);
                }
            }
        }
    }

    /// Reports the termination of a child this supervisor stopped, exactly
    /// once: a child that had already exited reported itself; one that
    /// stopped cooperatively is reported now and its own later report is
    /// silenced; one that was killed sends no report, so it is reported now.
    fn record_stop_into(
        silenced: &mut HashSet<u64>,
        events: &EventSender,
        child: &Child,
        outcome: StopOutcome,
    ) {
        let reason = match outcome {
            StopOutcome::AlreadyExited => return,
            StopOutcome::Stopped => {
                silenced.insert(child.instance());
                ChildExitReason::Shutdown
            }
            StopOutcome::Killed => ChildExitReason::Killed,
        };
        emit(
            events,
            SupervisorEvent::ChildTerminated {
                id: Arc::clone(child.key()),
                reason,
            },
        );
    }
}
