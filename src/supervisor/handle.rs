//! Supervisor handle - public API for interacting with supervisors

use super::error::SupervisorError;
use super::runtime::{StatusSnapshot, SupervisorCommand, SupervisorRuntime};
use super::spec::SupervisorSpec;
use crate::restart::{RestartPolicy, RestartStrategy};
use crate::types::{ChildId, ChildInfo};
use crate::worker::{WorkerFactory, WorkerSpec};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Handle used to interact with a running supervisor tree.
///
/// Cloning a handle is cheap; every clone talks to the same supervisor.
#[derive(Clone)]
pub struct SupervisorHandle {
    pub(crate) name: Arc<str>,
    pub(crate) control_tx: mpsc::UnboundedSender<SupervisorCommand>,
}

impl SupervisorHandle {
    /// Spawns a supervisor tree based on the provided specification.
    ///
    /// Must be called from within a tokio runtime.
    #[must_use]
    pub fn start(spec: SupervisorSpec) -> Self {
        Self::start_inner(spec, None)
    }

    /// Spawns a supervisor as the child of another one, reporting its own
    /// termination to `parent_tx`.
    ///
    /// Without this a nested supervisor that escalates simply stops: the parent
    /// never learns, and keeps a dead handle in its children list forever.
    pub(crate) fn start_supervised(
        spec: SupervisorSpec,
        parent_tx: mpsc::UnboundedSender<SupervisorCommand>,
    ) -> Self {
        Self::start_inner(spec, Some(parent_tx))
    }

    fn start_inner(
        spec: SupervisorSpec,
        parent_tx: Option<mpsc::UnboundedSender<SupervisorCommand>>,
    ) -> Self {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let name_arc: Arc<str> = Arc::from(spec.name.as_str());
        let runtime =
            SupervisorRuntime::new(spec, Arc::clone(&name_arc), control_rx, control_tx.clone());

        let runtime_name = Arc::clone(&name_arc);
        tokio::spawn(async move {
            let reason = runtime.run().await;
            tracing::debug!(name = %runtime_name, reason = ?reason, "supervisor stopped");

            // A `Shutdown` reason is the parent's own doing and is ignored there;
            // an abnormal exit is what makes the parent restart this subtree.
            if let Some(tx) = parent_tx {
                let _send = tx.send(SupervisorCommand::ChildTerminated {
                    id: runtime_name.to_string(),
                    reason,
                });
            }
        });

        Self {
            name: name_arc,
            control_tx,
        }
    }

    /// Sends a command built around a reply channel and waits for the reply.
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> SupervisorCommand,
    ) -> Result<T, SupervisorError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control_tx
            .send(command(reply_tx))
            .map_err(|_| self.shutting_down())?;
        reply_rx.await.map_err(|_| self.shutting_down())
    }

    fn shutting_down(&self) -> SupervisorError {
        SupervisorError::ShuttingDown(self.name().to_owned())
    }

    /// Dynamically starts a new child worker.
    ///
    /// The factory may take no arguments or the worker's
    /// [`ShutdownSignal`](crate::ShutdownSignal), as with
    /// [`SupervisorSpec::with_worker`].
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor is shutting down or a child with this ID already exists.
    pub async fn start_child<M>(
        &self,
        id: impl Into<String>,
        factory: impl WorkerFactory<M>,
        restart_policy: RestartPolicy,
    ) -> Result<ChildId, SupervisorError> {
        let spec = WorkerSpec::new(id, factory, restart_policy);
        self.request(|respond_to| SupervisorCommand::StartChild { spec, respond_to })
            .await?
    }

    /// Dynamically starts a new child worker with linked initialization.
    ///
    /// This method waits for the worker's [`initialize`](crate::Worker::initialize)
    /// to complete before returning. If initialization fails or times out, an
    /// error is returned and the worker is not added.
    ///
    /// # Errors
    ///
    /// * `SupervisorError::InitializationFailed` - Worker initialization returned an error
    /// * `SupervisorError::InitializationTimeout` - Worker didn't initialize within timeout
    /// * `SupervisorError::ChildAlreadyExists` - A child with this ID already exists
    /// * `SupervisorError::ShuttingDown` - Supervisor is shutting down
    ///
    /// # Note
    ///
    /// Initialization failures do NOT trigger restart policies. The worker must successfully
    /// initialize before restart policies take effect.
    pub async fn start_child_linked<M>(
        &self,
        id: impl Into<String>,
        factory: impl WorkerFactory<M>,
        restart_policy: RestartPolicy,
        timeout: Duration,
    ) -> Result<ChildId, SupervisorError> {
        let spec = WorkerSpec::new(id, factory, restart_policy);
        self.request(|respond_to| SupervisorCommand::StartChildLinked {
            spec,
            timeout,
            respond_to,
        })
        .await?
    }

    /// Dynamically terminates a child
    ///
    /// # Errors
    ///
    /// Returns an error if the child is not found or the supervisor is shutting down.
    pub async fn terminate_child(&self, id: &str) -> Result<(), SupervisorError> {
        self.request(|respond_to| SupervisorCommand::TerminateChild {
            id: id.to_owned(),
            respond_to,
        })
        .await?
    }

    /// Returns information about all children
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor is shutting down.
    pub async fn which_children(&self) -> Result<Vec<ChildInfo>, SupervisorError> {
        self.request(|respond_to| SupervisorCommand::WhichChildren { respond_to })
            .await
    }

    /// Shuts the supervisor tree down and waits until it has stopped.
    ///
    /// Every child is asked to stop at once, and the children share one
    /// shutdown timeout, so a slow cleanup in one child does not delay the
    /// others. Nested supervisors are shut down the same way. When this returns, all `shutdown` hooks that were going to
    /// run have run.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorError::ShuttingDown`] if the supervisor had already
    /// stopped.
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        let done = self.begin_shutdown().ok_or_else(|| self.shutting_down())?;

        // A dropped reply means the supervisor stopped on its own (it escalated)
        // while this request was queued; it shut its children down on the way.
        let _done = done.await;
        Ok(())
    }

    /// Asks the supervisor to shut down without waiting for it.
    ///
    /// The returned receiver resolves once the tree has stopped. `None` means
    /// the supervisor had already stopped.
    pub(crate) fn begin_shutdown(&self) -> Option<oneshot::Receiver<()>> {
        let (done_tx, done_rx) = oneshot::channel();
        self.control_tx
            .send(SupervisorCommand::Shutdown {
                respond_to: Some(done_tx),
            })
            .ok()?;
        Some(done_rx)
    }

    /// Returns the supervisor's name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` once the supervisor has stopped and no longer accepts
    /// commands.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.control_tx.is_closed()
    }

    /// Returns the supervisor's restart strategy.
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor is shutting down.
    pub async fn restart_strategy(&self) -> Result<RestartStrategy, SupervisorError> {
        self.request(|respond_to| SupervisorCommand::GetRestartStrategy { respond_to })
            .await
    }

    /// Child count, restart strategy and uptime, fetched in one round trip.
    pub(crate) async fn status_snapshot(&self) -> Result<StatusSnapshot, SupervisorError> {
        self.request(|respond_to| SupervisorCommand::GetStatus { respond_to })
            .await
    }

    /// Returns how long the supervisor has been running.
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor is shutting down.
    pub async fn uptime(&self) -> Result<Duration, SupervisorError> {
        self.request(|respond_to| SupervisorCommand::GetUptime { respond_to })
            .await
    }
}

impl std::fmt::Debug for SupervisorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorHandle")
            .field("name", &self.name())
            .finish_non_exhaustive()
    }
}
