//! Stateful supervisor with shared in-memory key-value store
//!
//! This module provides a parallel implementation of the supervisor tree with built-in
//! shared state via `WorkerContext`. Workers receive the context as a parameter in their
//! factory functions, allowing them to share data through a concurrent in-memory store.
//!
//! # Choosing Between Stateful and Regular Supervisors
//!
//! Use [`StatefulSupervisorSpec`] when:
//! - Workers need to share state (counters, caches, configuration)
//! - You need coordination between workers (flags, semaphores)
//! - State should survive worker restarts
//! - You want built-in concurrency-safe storage without external dependencies
//!
//! Use [`SupervisorSpec`](crate::SupervisorSpec) when:
//! - Workers are stateless or manage their own state independently
//! - No data sharing is required between workers
//! - You want minimal overhead (no shared context)
//! - Workers communicate through channels or external systems
//!
//! # Key Differences
//!
//! | Feature | `StatefulSupervisorSpec` | `SupervisorSpec` |
//! |---------|--------------------------|------------------|
//! | Worker Factory | `Fn(Arc<WorkerContext>) -> W` | `Fn() -> W` |
//! | Shared State | ✅ Built-in `WorkerContext` | ❌ None |
//! | Use Case | Coordinated workers | Independent workers |
//! | Overhead | Slightly higher (context management) | Minimal |
//!
//! # Example: When to Use Stateful
//!
//! ```rust,no_run
//! use ash_flare::{StatefulSupervisorSpec, StatefulSupervisorHandle, Worker, WorkerContext};
//! use async_trait::async_trait;
//! use std::sync::Arc;
//!
//! #[derive(Debug)]
//! struct CounterWorker {
//!     id: String,
//!     context: Arc<WorkerContext>,
//! }
//!
//! #[async_trait]
//! impl Worker for CounterWorker {
//!     type Error = std::io::Error;
//!     
//!     async fn run(&mut self) -> Result<(), Self::Error> {
//!         // Workers can share and update counters
//!         self.context.update("global_count", |v| {
//!             let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
//!             Some(serde_json::json!(count + 1))
//!         });
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let spec = StatefulSupervisorSpec::new("counter-supervisor")
//!     .with_worker("counter-1", |ctx| CounterWorker {
//!         id: "counter-1".to_string(),
//!         context: ctx
//!     }, ash_flare::RestartPolicy::Permanent);
//!
//! let handle = StatefulSupervisorHandle::start(spec);
//! // Workers share state through the context
//! # handle.shutdown().await.ok();
//! # }
//! ```

use crate::restart::{RestartIntensity, RestartPolicy, RestartStrategy, RestartTracker};
use crate::supervisor_common::{WorkerTermination, run_worker};
use crate::types::{ChildExitReason, ChildId, ChildInfo, ChildType, WorkerContext};
use crate::worker::Worker;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

// ============================================================================
// Worker Specification (Stateful)
// ============================================================================

/// Specification for creating and restarting a stateful worker
pub(crate) struct StatefulWorkerSpec<W: Worker> {
    pub id: ChildId,
    pub worker_factory: Arc<dyn Fn(Arc<WorkerContext>) -> W + Send + Sync>,
    pub restart_policy: RestartPolicy,
    pub context: Arc<WorkerContext>,
}

impl<W: Worker> Clone for StatefulWorkerSpec<W> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            worker_factory: Arc::clone(&self.worker_factory),
            restart_policy: self.restart_policy,
            context: Arc::clone(&self.context),
        }
    }
}

impl<W: Worker> StatefulWorkerSpec<W> {
    pub(crate) fn new(
        id: impl Into<String>,
        factory: impl Fn(Arc<WorkerContext>) -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
        context: Arc<WorkerContext>,
    ) -> Self {
        Self {
            id: id.into(),
            worker_factory: Arc::new(factory),
            restart_policy,
            context,
        }
    }

    pub(crate) fn create_worker(&self) -> W {
        (self.worker_factory)(Arc::clone(&self.context))
    }
}

impl<W: Worker> fmt::Debug for StatefulWorkerSpec<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatefulWorkerSpec")
            .field("id", &self.id)
            .field("restart_policy", &self.restart_policy)
            .finish()
    }
}

// ============================================================================
// Worker Process (Stateful)
// ============================================================================

/// Running stateful worker process with its specification and task handle
pub(crate) struct StatefulWorkerProcess<W: Worker> {
    pub spec: StatefulWorkerSpec<W>,
    pub handle: Option<JoinHandle<()>>,
}

impl<W: Worker> StatefulWorkerProcess<W> {
    pub(crate) fn spawn<Cmd>(
        spec: StatefulWorkerSpec<W>,
        supervisor_name: String,
        control_tx: mpsc::UnboundedSender<Cmd>,
    ) -> Self
    where
        Cmd: From<WorkerTermination> + Send + 'static,
    {
        let worker = spec.create_worker();
        let worker_id = spec.id.clone();
        let handle = tokio::spawn(async move {
            run_worker(supervisor_name, worker_id, worker, control_tx, None).await;
        });

        Self {
            spec,
            handle: Some(handle),
        }
    }

    /// Spawns a worker with linked initialization handshake
    pub(crate) fn spawn_with_link<Cmd>(
        spec: StatefulWorkerSpec<W>,
        supervisor_name: String,
        control_tx: mpsc::UnboundedSender<Cmd>,
        init_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    ) -> Self
    where
        Cmd: From<WorkerTermination> + Send + 'static,
    {
        let worker = spec.create_worker();
        let worker_id = spec.id.clone();
        let handle = tokio::spawn(async move {
            run_worker(
                supervisor_name,
                worker_id,
                worker,
                control_tx,
                Some(init_tx),
            )
            .await;
        });

        Self {
            spec,
            handle: Some(handle),
        }
    }

    pub(crate) async fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl<W: Worker> Drop for StatefulWorkerProcess<W> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// ============================================================================
// Supervisor Specification (Stateful)
// ============================================================================

/// Specification for a child (either worker or supervisor)
pub(crate) enum StatefulChildSpec<W: Worker> {
    Worker(StatefulWorkerSpec<W>),
    Supervisor(Arc<StatefulSupervisorSpec<W>>),
}

impl<W: Worker> Clone for StatefulChildSpec<W> {
    fn clone(&self) -> Self {
        match self {
            StatefulChildSpec::Worker(w) => StatefulChildSpec::Worker(w.clone()),
            StatefulChildSpec::Supervisor(s) => StatefulChildSpec::Supervisor(Arc::clone(s)),
        }
    }
}

/// Describes a stateful supervisor and its children in a tree structure.
pub struct StatefulSupervisorSpec<W: Worker> {
    pub(crate) name: String,
    pub(crate) children: Vec<StatefulChildSpec<W>>,
    pub(crate) restart_strategy: RestartStrategy,
    pub(crate) restart_intensity: RestartIntensity,
    pub(crate) context: Arc<WorkerContext>,
}

impl<W: Worker> Clone for StatefulSupervisorSpec<W> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            children: self.children.clone(),
            restart_strategy: self.restart_strategy,
            restart_intensity: self.restart_intensity,
            context: Arc::clone(&self.context),
        }
    }
}

impl<W: Worker> StatefulSupervisorSpec<W> {
    /// Creates a new stateful supervisor specification with the provided name.
    /// Automatically initializes an empty WorkerContext for sharing state between workers.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            children: Vec::new(),
            restart_strategy: RestartStrategy::default(),
            restart_intensity: RestartIntensity::default(),
            context: Arc::new(WorkerContext::new()),
        }
    }

    /// Sets the restart strategy for this supervisor.
    pub fn with_restart_strategy(mut self, strategy: RestartStrategy) -> Self {
        self.restart_strategy = strategy;
        self
    }

    /// Sets the restart intensity for this supervisor.
    pub fn with_restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = intensity;
        self
    }

    /// Adds a stateful worker child to this supervisor specification.
    /// The factory function receives a `WorkerContext` parameter for accessing shared state.
    pub fn with_worker(
        mut self,
        id: impl Into<String>,
        factory: impl Fn(Arc<WorkerContext>) -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
    ) -> Self {
        self.children
            .push(StatefulChildSpec::Worker(StatefulWorkerSpec::new(
                id,
                factory,
                restart_policy,
                Arc::clone(&self.context),
            )));
        self
    }

    /// Adds a nested stateful supervisor child to this supervisor specification.
    pub fn with_supervisor(mut self, supervisor: StatefulSupervisorSpec<W>) -> Self {
        self.children
            .push(StatefulChildSpec::Supervisor(Arc::new(supervisor)));
        self
    }

    /// Returns a reference to the WorkerContext for this supervisor tree.
    pub fn context(&self) -> &Arc<WorkerContext> {
        &self.context
    }
}

// ============================================================================
// Child Management (Stateful)
// ============================================================================

/// Represents either a worker or a nested supervisor in the supervision tree
pub(crate) enum StatefulChild<W: Worker> {
    Worker(StatefulWorkerProcess<W>),
    Supervisor {
        handle: StatefulSupervisorHandle<W>,
        spec: Arc<StatefulSupervisorSpec<W>>,
    },
}

impl<W: Worker> StatefulChild<W> {
    #[inline]
    pub fn id(&self) -> &str {
        match self {
            StatefulChild::Worker(w) => &w.spec.id,
            StatefulChild::Supervisor { spec, .. } => &spec.name,
        }
    }

    #[inline]
    pub fn child_type(&self) -> ChildType {
        match self {
            StatefulChild::Worker(_) => ChildType::Worker,
            StatefulChild::Supervisor { .. } => ChildType::Supervisor,
        }
    }

    #[inline]
    pub fn restart_policy(&self) -> Option<RestartPolicy> {
        match self {
            StatefulChild::Worker(w) => Some(w.spec.restart_policy),
            StatefulChild::Supervisor { .. } => Some(RestartPolicy::Permanent),
        }
    }

    pub async fn shutdown(&mut self) {
        match self {
            StatefulChild::Worker(w) => w.stop().await,
            StatefulChild::Supervisor { handle, .. } => {
                let _ = handle.shutdown().await;
            }
        }
    }
}

/// Holds information needed to restart a child after termination
pub(crate) enum StatefulRestartInfo<W: Worker> {
    Worker(StatefulWorkerSpec<W>),
    Supervisor(Arc<StatefulSupervisorSpec<W>>),
}

// ============================================================================
// Supervisor Error (Stateful)
// ============================================================================

/// Errors returned by stateful supervisor operations.
#[derive(Debug)]
pub enum StatefulSupervisorError {
    /// Supervisor has no children
    NoChildren(String),
    /// All children have failed
    AllChildrenFailed(String),
    /// Supervisor is shutting down
    ShuttingDown(String),
    /// Child with this ID already exists
    ChildAlreadyExists(String),
    /// Child with this ID not found
    ChildNotFound(String),
    /// Child initialization failed
    InitializationFailed {
        /// ID of the child that failed to initialize
        child_id: String,
        /// Reason for initialization failure
        reason: String,
    },
    /// Child initialization timed out
    InitializationTimeout {
        /// ID of the child that timed out
        child_id: String,
        /// Duration after which timeout occurred
        timeout: std::time::Duration,
    },
}

impl fmt::Display for StatefulSupervisorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatefulSupervisorError::NoChildren(name) => {
                write!(f, "stateful supervisor '{}' has no children", name)
            }
            StatefulSupervisorError::AllChildrenFailed(name) => {
                write!(
                    f,
                    "all children failed for stateful supervisor '{}' - restart intensity limit exceeded",
                    name
                )
            }
            StatefulSupervisorError::ShuttingDown(name) => {
                write!(
                    f,
                    "stateful supervisor '{}' is shutting down - operation not permitted",
                    name
                )
            }
            StatefulSupervisorError::ChildAlreadyExists(id) => {
                write!(
                    f,
                    "child with id '{}' already exists - use a unique identifier",
                    id
                )
            }
            StatefulSupervisorError::ChildNotFound(id) => {
                write!(
                    f,
                    "child with id '{}' not found - it may have already terminated",
                    id
                )
            }
            StatefulSupervisorError::InitializationFailed { child_id, reason } => {
                write!(f, "child '{}' initialization failed: {}", child_id, reason)
            }
            StatefulSupervisorError::InitializationTimeout { child_id, timeout } => {
                write!(
                    f,
                    "child '{}' initialization timed out after {:?}",
                    child_id, timeout
                )
            }
        }
    }
}

impl std::error::Error for StatefulSupervisorError {}

// ============================================================================
// Supervisor Runtime (Stateful)
// ============================================================================

/// Internal commands sent to stateful supervisor runtime
pub(crate) enum StatefulSupervisorCommand<W: Worker> {
    StartChild {
        spec: StatefulWorkerSpec<W>,
        respond_to: oneshot::Sender<Result<ChildId, StatefulSupervisorError>>,
    },
    StartChildLinked {
        spec: StatefulWorkerSpec<W>,
        timeout: std::time::Duration,
        respond_to: oneshot::Sender<Result<ChildId, StatefulSupervisorError>>,
    },
    TerminateChild {
        id: ChildId,
        respond_to: oneshot::Sender<Result<(), StatefulSupervisorError>>,
    },
    WhichChildren {
        respond_to: oneshot::Sender<Result<Vec<ChildInfo>, StatefulSupervisorError>>,
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

impl<W: Worker> From<WorkerTermination> for StatefulSupervisorCommand<W> {
    fn from(term: WorkerTermination) -> Self {
        StatefulSupervisorCommand::ChildTerminated {
            id: term.id,
            reason: term.reason,
        }
    }
}

/// Internal state machine that manages stateful supervisor lifecycle and child processes
pub(crate) struct StatefulSupervisorRuntime<W: Worker> {
    name: String,
    children: Vec<StatefulChild<W>>,
    control_rx: mpsc::UnboundedReceiver<StatefulSupervisorCommand<W>>,
    control_tx: mpsc::UnboundedSender<StatefulSupervisorCommand<W>>,
    restart_strategy: RestartStrategy,
    restart_tracker: RestartTracker,
    created_at: std::time::Instant,
}

impl<W: Worker> StatefulSupervisorRuntime<W> {
    pub(crate) fn new(
        spec: StatefulSupervisorSpec<W>,
        control_rx: mpsc::UnboundedReceiver<StatefulSupervisorCommand<W>>,
        control_tx: mpsc::UnboundedSender<StatefulSupervisorCommand<W>>,
    ) -> Self {
        let mut children = Vec::with_capacity(spec.children.len());

        for child_spec in spec.children {
            match child_spec {
                StatefulChildSpec::Worker(worker_spec) => {
                    let worker = StatefulWorkerProcess::spawn(
                        worker_spec,
                        spec.name.clone(),
                        control_tx.clone(),
                    );
                    children.push(StatefulChild::Worker(worker));
                }
                StatefulChildSpec::Supervisor(supervisor_spec) => {
                    let supervisor = StatefulSupervisorHandle::start((*supervisor_spec).clone());
                    children.push(StatefulChild::Supervisor {
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
                StatefulSupervisorCommand::StartChild { spec, respond_to } => {
                    let result = self.handle_start_child(spec).await;
                    let _ = respond_to.send(result);
                }
                StatefulSupervisorCommand::StartChildLinked {
                    spec,
                    timeout,
                    respond_to,
                } => {
                    let result = self.handle_start_child_linked(spec, timeout).await;
                    let _ = respond_to.send(result);
                }
                StatefulSupervisorCommand::TerminateChild { id, respond_to } => {
                    let result = self.handle_terminate_child(&id).await;
                    let _ = respond_to.send(result);
                }
                StatefulSupervisorCommand::WhichChildren { respond_to } => {
                    let result = self.handle_which_children();
                    let _ = respond_to.send(result);
                }
                StatefulSupervisorCommand::GetRestartStrategy { respond_to } => {
                    let _ = respond_to.send(self.restart_strategy);
                }
                StatefulSupervisorCommand::GetUptime { respond_to } => {
                    let uptime = self.created_at.elapsed().as_secs();
                    let _ = respond_to.send(uptime);
                }
                StatefulSupervisorCommand::ChildTerminated { id, reason } => {
                    self.handle_child_terminated(id, reason).await;
                }
                StatefulSupervisorCommand::Shutdown => {
                    self.shutdown_children().await;
                    return;
                }
            }
        }

        self.shutdown_children().await;
    }

    async fn handle_start_child(
        &mut self,
        spec: StatefulWorkerSpec<W>,
    ) -> Result<ChildId, StatefulSupervisorError> {
        // Check if child with same ID already exists
        if self.children.iter().any(|c| c.id() == spec.id) {
            return Err(StatefulSupervisorError::ChildAlreadyExists(spec.id.clone()));
        }

        let id = spec.id.clone();
        let worker = StatefulWorkerProcess::spawn(spec, self.name.clone(), self.control_tx.clone());

        self.children.push(StatefulChild::Worker(worker));
        slog::debug!(slog_scope::logger(), "dynamically started child";
            "supervisor" => &self.name,
            "child" => &id
        );

        Ok(id)
    }

    async fn handle_start_child_linked(
        &mut self,
        spec: StatefulWorkerSpec<W>,
        timeout: std::time::Duration,
    ) -> Result<ChildId, StatefulSupervisorError> {
        // Check if child with same ID already exists
        if self.children.iter().any(|c| c.id() == spec.id) {
            return Err(StatefulSupervisorError::ChildAlreadyExists(spec.id.clone()));
        }

        let id = spec.id.clone();
        let (init_tx, init_rx) = oneshot::channel();

        let worker = StatefulWorkerProcess::spawn_with_link(
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
                self.children.push(StatefulChild::Worker(worker));
                slog::debug!(slog_scope::logger(), "linked child started successfully";
                    "supervisor" => &self.name,
                    "child" => &id
                );
                Ok(id)
            }
            Ok(Ok(Err(reason))) => {
                // Initialization failed - worker sent error
                slog::error!(slog_scope::logger(), "linked child initialization failed";
                    "supervisor" => &self.name,
                    "child" => &id,
                    "reason" => &reason
                );
                // Note: init failures do NOT trigger restart policies
                Err(StatefulSupervisorError::InitializationFailed {
                    child_id: id,
                    reason,
                })
            }
            Ok(Err(_)) => {
                // Channel closed - worker panicked before sending result
                slog::error!(slog_scope::logger(), "linked child panicked during initialization";
                    "supervisor" => &self.name,
                    "child" => &id
                );
                Err(StatefulSupervisorError::InitializationFailed {
                    child_id: id,
                    reason: "worker panicked during initialization".to_string(),
                })
            }
            Err(_) => {
                // Timeout
                slog::error!(slog_scope::logger(), "linked child initialization timed out";
                    "supervisor" => &self.name,
                    "child" => &id,
                    "timeout" => ?timeout
                );
                Err(StatefulSupervisorError::InitializationTimeout {
                    child_id: id,
                    timeout,
                })
            }
        }
    }

    async fn handle_terminate_child(&mut self, id: &str) -> Result<(), StatefulSupervisorError> {
        let position = self
            .children
            .iter()
            .position(|c| c.id() == id)
            .ok_or_else(|| StatefulSupervisorError::ChildNotFound(id.to_string()))?;

        let mut child = self.children.remove(position);
        child.shutdown().await;

        slog::debug!(slog_scope::logger(), "terminated child";
            "supervisor" => &self.name,
            "child" => id
        );
        Ok(())
    }

    fn handle_which_children(&self) -> Result<Vec<ChildInfo>, StatefulSupervisorError> {
        let info = self
            .children
            .iter()
            .map(|child| ChildInfo {
                id: child.id().to_string(),
                child_type: child.child_type(),
                restart_policy: child.restart_policy(),
            })
            .collect();

        Ok(info)
    }

    async fn handle_child_terminated(&mut self, id: ChildId, reason: ChildExitReason) {
        slog::debug!(slog_scope::logger(), "child terminated";
            "supervisor" => &self.name,
            "child" => &id,
            "reason" => ?reason
        );

        let position = match self.children.iter().position(|c| c.id() == id) {
            Some(pos) => pos,
            None => {
                slog::warn!(slog_scope::logger(), "terminated child not found in list";
                    "supervisor" => &self.name,
                    "child" => &id
                );
                return;
            }
        };

        // Determine if we should restart based on policy and reason
        let should_restart = match &self.children[position] {
            StatefulChild::Worker(w) => match w.spec.restart_policy {
                RestartPolicy::Permanent => true,
                RestartPolicy::Temporary => false,
                RestartPolicy::Transient => reason == ChildExitReason::Abnormal,
            },
            StatefulChild::Supervisor { .. } => true, // Supervisors are always permanent
        };

        if !should_restart {
            slog::debug!(slog_scope::logger(), "not restarting child";
                "supervisor" => &self.name,
                "child" => &id,
                "policy" => ?self.children[position].restart_policy(),
                "reason" => ?reason
            );
            self.children.remove(position);
            return;
        }

        // Check restart intensity
        if self.restart_tracker.record_restart() {
            slog::error!(slog_scope::logger(), "restart intensity exceeded, shutting down";
                "supervisor" => &self.name
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
            StatefulChild::Worker(worker) => StatefulRestartInfo::Worker(worker.spec.clone()),
            StatefulChild::Supervisor { spec, .. } => {
                StatefulRestartInfo::Supervisor(Arc::clone(spec))
            }
        };

        // Shutdown old child
        self.children[position].shutdown().await;

        // Restart based on type
        match restart_info {
            StatefulRestartInfo::Worker(spec) => {
                slog::debug!(slog_scope::logger(), "restarting worker";
                    "supervisor" => &self.name,
                    "worker" => &spec.id
                );
                let new_worker = StatefulWorkerProcess::spawn(
                    spec.clone(),
                    self.name.clone(),
                    self.control_tx.clone(),
                );
                self.children[position] = StatefulChild::Worker(new_worker);
                slog::debug!(slog_scope::logger(), "worker restarted";
                    "supervisor" => &self.name,
                    "worker" => &spec.id
                );
            }
            StatefulRestartInfo::Supervisor(spec) => {
                let name = spec.name.clone();
                slog::debug!(slog_scope::logger(), "restarting supervisor";
                    "supervisor" => &self.name,
                    "child_supervisor" => &name
                );
                let new_handle = StatefulSupervisorHandle::start((*spec).clone());
                self.children[position] = StatefulChild::Supervisor {
                    handle: new_handle,
                    spec,
                };
                slog::debug!(slog_scope::logger(), "supervisor restarted";
                    "supervisor" => &self.name,
                    "child_supervisor" => &name
                );
            }
        }
    }

    async fn restart_all_children(&mut self) {
        slog::debug!(slog_scope::logger(), "restarting all children (one_for_all)";
            "supervisor" => &self.name
        );

        // Shutdown all children
        for child in &mut self.children {
            child.shutdown().await;
        }

        // Restart all worker children
        for child in &mut self.children {
            if let StatefulChild::Worker(worker) = child {
                let spec = worker.spec.clone();
                let new_worker = StatefulWorkerProcess::spawn(
                    spec.clone(),
                    self.name.clone(),
                    self.control_tx.clone(),
                );
                *child = StatefulChild::Worker(new_worker);
                slog::debug!(slog_scope::logger(), "child restarted";
                    "supervisor" => &self.name,
                    "child" => &spec.id
                );
            }
        }
    }

    async fn restart_from(&mut self, position: usize) {
        slog::debug!(slog_scope::logger(), "restarting from position (rest_for_one)";
            "supervisor" => &self.name,
            "position" => position
        );

        for i in position..self.children.len() {
            self.children[i].shutdown().await;

            if let StatefulChild::Worker(worker) = &self.children[i] {
                let spec = worker.spec.clone();
                let new_worker = StatefulWorkerProcess::spawn(
                    spec.clone(),
                    self.name.clone(),
                    self.control_tx.clone(),
                );
                self.children[i] = StatefulChild::Worker(new_worker);
                slog::debug!(slog_scope::logger(), "child restarted";
                    "supervisor" => &self.name,
                    "child" => &spec.id
                );
            }
        }
    }

    async fn shutdown_children(&mut self) {
        for child in self.children.drain(..) {
            let id = child.id().to_string();
            let mut child = child;
            child.shutdown().await;
            slog::debug!(slog_scope::logger(), "shut down child";
                "supervisor" => &self.name,
                "child" => &id
            );
        }
    }
}

// ============================================================================
// Supervisor Handle (Stateful)
// ============================================================================

/// Handle used to interact with a running stateful supervisor tree.
#[derive(Clone)]
pub struct StatefulSupervisorHandle<W: Worker> {
    pub(crate) name: Arc<String>,
    pub(crate) control_tx: mpsc::UnboundedSender<StatefulSupervisorCommand<W>>,
}

impl<W: Worker> StatefulSupervisorHandle<W> {
    /// Spawns a stateful supervisor tree based on the provided specification.
    pub fn start(spec: StatefulSupervisorSpec<W>) -> Self {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let name_arc = Arc::new(spec.name.clone());
        let runtime = StatefulSupervisorRuntime::new(spec, control_rx, control_tx.clone());

        let runtime_name = Arc::clone(&name_arc);
        tokio::spawn(async move {
            runtime.run().await;
            slog::debug!(slog_scope::logger(), "supervisor stopped";
                "name" => &*runtime_name
            );
        });

        Self {
            name: name_arc,
            control_tx,
        }
    }

    /// Dynamically starts a new child worker
    pub async fn start_child(
        &self,
        id: impl Into<String>,
        factory: impl Fn(Arc<WorkerContext>) -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
        context: Arc<WorkerContext>,
    ) -> Result<ChildId, StatefulSupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();
        let spec = StatefulWorkerSpec::new(id, factory, restart_policy, context);

        self.control_tx
            .send(StatefulSupervisorCommand::StartChild {
                spec,
                respond_to: result_tx,
            })
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Dynamically starts a new child worker with linked initialization.
    ///
    /// This method waits for the worker's initialization to complete before returning.
    /// If initialization fails or times out, an error is returned and the worker is not added.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for the child
    /// * `factory` - Factory function to create the worker
    /// * `restart_policy` - How to handle worker termination after it starts running
    /// * `context` - Shared context for stateful workers
    /// * `timeout` - Maximum time to wait for initialization
    ///
    /// # Errors
    ///
    /// * `StatefulSupervisorError::InitializationFailed` - Worker initialization returned an error
    /// * `StatefulSupervisorError::InitializationTimeout` - Worker didn't initialize within timeout
    /// * `StatefulSupervisorError::ChildAlreadyExists` - A child with this ID already exists
    /// * `StatefulSupervisorError::ShuttingDown` - Supervisor is shutting down
    ///
    /// # Note
    ///
    /// Initialization failures do NOT trigger restart policies. The worker must successfully
    /// initialize before restart policies take effect.
    pub async fn start_child_linked(
        &self,
        id: impl Into<String>,
        factory: impl Fn(Arc<WorkerContext>) -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
        context: Arc<WorkerContext>,
        timeout: std::time::Duration,
    ) -> Result<ChildId, StatefulSupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();
        let spec = StatefulWorkerSpec::new(id, factory, restart_policy, context);

        self.control_tx
            .send(StatefulSupervisorCommand::StartChildLinked {
                spec,
                timeout,
                respond_to: result_tx,
            })
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Dynamically terminates a child
    pub async fn terminate_child(&self, id: &str) -> Result<(), StatefulSupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(StatefulSupervisorCommand::TerminateChild {
                id: id.to_string(),
                respond_to: result_tx,
            })
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Returns information about all children
    pub async fn which_children(&self) -> Result<Vec<ChildInfo>, StatefulSupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(StatefulSupervisorCommand::WhichChildren {
                respond_to: result_tx,
            })
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Requests a graceful shutdown of the supervisor tree.
    pub async fn shutdown(&self) -> Result<(), StatefulSupervisorError> {
        self.control_tx
            .send(StatefulSupervisorCommand::Shutdown)
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;
        Ok(())
    }

    /// Returns the supervisor's name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the supervisor's restart strategy.
    pub async fn restart_strategy(&self) -> Result<RestartStrategy, StatefulSupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(StatefulSupervisorCommand::GetRestartStrategy {
                respond_to: result_tx,
            })
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))
    }

    /// Returns the supervisor's uptime in seconds.
    pub async fn uptime(&self) -> Result<u64, StatefulSupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(StatefulSupervisorCommand::GetUptime {
                respond_to: result_tx,
            })
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| StatefulSupervisorError::ShuttingDown(self.name().to_string()))
    }
}
