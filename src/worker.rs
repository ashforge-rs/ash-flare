//! Worker trait and related types

use crate::restart::RestartPolicy;
use crate::supervisor_common::run_worker;
use crate::types::{ChildId, ShutdownSignal};
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
    ///
    /// When the supervisor terminates this worker, this future is dropped at its
    /// next await point. To wind down on your own terms instead, take a
    /// [`ShutdownSignal`](crate::ShutdownSignal) via
    /// [`SupervisorSpec::with_worker_signal`](crate::SupervisorSpec::with_worker_signal)
    /// and select on it.
    async fn run(&mut self) -> Result<(), Self::Error>;

    /// Called when the worker is initialized
    async fn initialize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Called when the worker is being shut down
    ///
    /// Runs after `run` returns, whether it finished on its own, failed, or was
    /// cancelled because the supervisor asked the worker to stop. Use it to
    /// flush buffers, commit work, or release resources.
    ///
    /// It does **not** run if the worker cannot be cancelled within the
    /// supervisor's shutdown timeout — for example if `run` blocks the thread
    /// without awaiting — because the task is then aborted outright. Keep
    /// cleanup well under that timeout; work that overruns it is abandoned.
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Specification for creating and restarting a worker
///
/// The factory receives the worker's [`ShutdownSignal`] so a worker can observe
/// cooperative termination. Factories registered through the plain
/// `Fn() -> W` API simply ignore it.
pub(crate) struct WorkerSpec<W: Worker> {
    pub id: ChildId,
    pub worker_factory: Arc<dyn Fn(ShutdownSignal) -> W + Send + Sync>,
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
        Self::with_signal(id, move |_signal| factory(), restart_policy)
    }

    /// Builds a spec whose factory receives the worker's [`ShutdownSignal`].
    pub(crate) fn with_signal(
        id: impl Into<String>,
        factory: impl Fn(ShutdownSignal) -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
    ) -> Self {
        Self {
            id: id.into(),
            worker_factory: Arc::new(factory),
            restart_policy,
        }
    }

    pub(crate) fn create_worker(&self, signal: ShutdownSignal) -> W {
        (self.worker_factory)(signal)
    }
}

impl<W: Worker> fmt::Debug for WorkerSpec<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerSpec")
            .field("id", &self.id)
            .field("restart_policy", &self.restart_policy)
            .finish_non_exhaustive()
    }
}

/// Running worker process with its specification and task handle
pub(crate) struct WorkerProcess<W: Worker> {
    pub spec: WorkerSpec<W>,
    pub handle: Option<JoinHandle<()>>,
    /// Triggers cooperative shutdown; dropping it also releases the worker.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
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
        Self::spawn_inner(spec, supervisor_name, control_tx, None)
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
        Self::spawn_inner(spec, supervisor_name, control_tx, Some(init_tx))
    }

    fn spawn_inner<Cmd>(
        spec: WorkerSpec<W>,
        supervisor_name: String,
        control_tx: mpsc::UnboundedSender<Cmd>,
        init_tx: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    ) -> Self
    where
        Cmd: From<WorkerTermination> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let worker = spec.create_worker(ShutdownSignal::new(shutdown_rx.clone()));
        let worker_id = spec.id.clone();
        let signal = ShutdownSignal::new(shutdown_rx);

        let handle = tokio::spawn(async move {
            run_worker(
                supervisor_name,
                worker_id,
                worker,
                control_tx,
                init_tx,
                signal,
            )
            .await;
        });

        Self {
            spec,
            handle: Some(handle),
            shutdown_tx,
        }
    }

    /// Stops the worker, giving it `timeout` to wind down cooperatively before
    /// aborting the task.
    ///
    /// Returns `true` if the worker stopped on its own (so its `shutdown` hook
    /// ran), `false` if it had to be aborted.
    pub(crate) async fn stop(&mut self, timeout: std::time::Duration) -> bool {
        let Some(mut handle) = self.handle.take() else {
            return true;
        };

        // Ask the worker to stop. A send error means the task is already gone.
        let _notified = self.shutdown_tx.send(true);

        if timeout.is_zero() {
            handle.abort();
            let _join_result = handle.await;
            return false;
        }

        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(_joined) => true,
            Err(_elapsed) => {
                // The worker ignored the signal; drop it the hard way. Its
                // `shutdown` hook does not run in this path.
                tracing::warn!(
                    worker = %self.spec.id,
                    timeout_ms = %timeout.as_millis(),
                    "worker did not stop within shutdown timeout, aborting"
                );
                handle.abort();
                let _join_result = handle.await;
                false
            }
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
                write!(f, "command channel to {name} is closed")
            }
            WorkerError::WorkerPanicked(name) => {
                write!(f, "worker {name} panicked")
            }
            WorkerError::WorkerFailed(msg) => {
                write!(f, "worker failed: {msg}")
            }
        }
    }
}

impl std::error::Error for WorkerError {}
