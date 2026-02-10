//! Worker trait and related types

use crate::restart::RestartPolicy;
use crate::supervisor_common::run_worker;
use crate::types::ChildId;
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A trait that all workers must implement to work with the supervisor tree.
/// This allows for generic workers that can handle any type of work.
#[async_trait]
pub trait Worker: Send + Sync + 'static {
    /// The type of error this worker can return
    type Error: std::error::Error + Send + Sync + 'static;

    /// Run the worker's main loop - this should run until completion or error
    async fn run(&mut self) -> Result<(), Self::Error>;

    /// Called when the worker is initialized
    async fn initialize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Called when the worker is being shut down
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Specification for creating and restarting a worker
pub(crate) struct WorkerSpec<W: Worker> {
    pub id: ChildId,
    pub worker_factory: Arc<dyn Fn() -> W + Send + Sync>,
    pub restart_policy: RestartPolicy,
}

impl<W: Worker> Clone for WorkerSpec<W> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            worker_factory: Arc::clone(&self.worker_factory),
            restart_policy: self.restart_policy,
        }
    }
}

impl<W: Worker> WorkerSpec<W> {
    pub(crate) fn new(
        id: impl Into<String>,
        factory: impl Fn() -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
    ) -> Self {
        Self {
            id: id.into(),
            worker_factory: Arc::new(factory),
            restart_policy,
        }
    }

    pub(crate) fn create_worker(&self) -> W {
        (self.worker_factory)()
    }
}

impl<W: Worker> fmt::Debug for WorkerSpec<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerSpec")
            .field("id", &self.id)
            .field("restart_policy", &self.restart_policy)
            .finish()
    }
}

/// Running worker process with its specification and task handle
pub(crate) struct WorkerProcess<W: Worker> {
    pub spec: WorkerSpec<W>,
    pub handle: Option<JoinHandle<()>>,
}

impl<W: Worker> WorkerProcess<W> {
    pub(crate) fn spawn<Cmd>(
        spec: WorkerSpec<W>,
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
        spec: WorkerSpec<W>,
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

impl<W: Worker> Drop for WorkerProcess<W> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// Re-export WorkerTermination from supervisor_common
pub(crate) use crate::supervisor_common::WorkerTermination;

/// Errors returned by worker operations.
#[derive(Debug)]
pub enum WorkerError {
    /// Command channel was closed unexpectedly
    CommandChannelClosed(String),
    /// Worker panicked during execution
    WorkerPanicked(String),
    /// Worker failed with an error
    WorkerFailed(String),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerError::CommandChannelClosed(name) => {
                write!(f, "command channel to {} is closed", name)
            }
            WorkerError::WorkerPanicked(name) => {
                write!(f, "worker {} panicked", name)
            }
            WorkerError::WorkerFailed(msg) => {
                write!(f, "worker failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for WorkerError {}
