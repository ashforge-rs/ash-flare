//! Supervisor handle - public API for interacting with supervisors

use super::error::SupervisorError;
use super::runtime::{SupervisorCommand, SupervisorRuntime};
use super::spec::SupervisorSpec;
use crate::restart::RestartPolicy;
use crate::types::{ChildId, ChildInfo};
use crate::worker::{Worker, WorkerSpec};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Handle used to interact with a running supervisor tree.
#[derive(Clone)]
pub struct SupervisorHandle<W: Worker> {
    pub(crate) name: Arc<String>,
    pub(crate) control_tx: mpsc::UnboundedSender<SupervisorCommand<W>>,
}

impl<W: Worker> SupervisorHandle<W> {
    /// Spawns a supervisor tree based on the provided specification.
    pub fn start(spec: SupervisorSpec<W>) -> Self {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let name_arc = Arc::new(spec.name.clone());
        let runtime = SupervisorRuntime::new(spec, control_rx, control_tx.clone());

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
        factory: impl Fn() -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
    ) -> Result<ChildId, SupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();
        let spec = WorkerSpec::new(id, factory, restart_policy);

        self.control_tx
            .send(SupervisorCommand::StartChild {
                spec,
                respond_to: result_tx,
            })
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?
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
    /// * `timeout` - Maximum time to wait for initialization
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
    pub async fn start_child_linked(
        &self,
        id: impl Into<String>,
        factory: impl Fn() -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
        timeout: std::time::Duration,
    ) -> Result<ChildId, SupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();
        let spec = WorkerSpec::new(id, factory, restart_policy);

        self.control_tx
            .send(SupervisorCommand::StartChildLinked {
                spec,
                timeout,
                respond_to: result_tx,
            })
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Dynamically terminates a child
    pub async fn terminate_child(&self, id: &str) -> Result<(), SupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(SupervisorCommand::TerminateChild {
                id: id.to_string(),
                respond_to: result_tx,
            })
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Returns information about all children
    pub async fn which_children(&self) -> Result<Vec<ChildInfo>, SupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(SupervisorCommand::WhichChildren {
                respond_to: result_tx,
            })
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?
    }

    /// Requests a graceful shutdown of the supervisor tree.
    pub async fn shutdown(&self) -> Result<(), SupervisorError> {
        self.control_tx
            .send(SupervisorCommand::Shutdown)
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;
        Ok(())
    }

    /// Returns the supervisor's name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the supervisor's restart strategy.
    pub async fn restart_strategy(
        &self,
    ) -> Result<crate::restart::RestartStrategy, SupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(SupervisorCommand::GetRestartStrategy {
                respond_to: result_tx,
            })
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))
    }

    /// Returns the supervisor's uptime in seconds.
    pub async fn uptime(&self) -> Result<u64, SupervisorError> {
        let (result_tx, result_rx) = oneshot::channel();

        self.control_tx
            .send(SupervisorCommand::GetUptime {
                respond_to: result_tx,
            })
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))?;

        result_rx
            .await
            .map_err(|_| SupervisorError::ShuttingDown(self.name().to_string()))
    }
}
