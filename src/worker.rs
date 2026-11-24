//! Worker trait and related types

use crate::restart::RestartPolicy;
use crate::supervisor::SupervisorCommand;
use crate::supervisor_common::run_worker;
use crate::types::ShutdownSignal;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A unit of work run and restarted by a supervisor.
///
/// Implement it with plain `async fn`s; no attribute macro is needed:
///
/// ```
/// use ash_flare::Worker;
///
/// struct Ticker;
///
/// impl Worker for Ticker {
///     type Error = std::io::Error;
///
///     async fn run(&mut self) -> Result<(), Self::Error> {
///         tokio::time::sleep(std::time::Duration::from_millis(1)).await;
///         Ok(())
///     }
/// }
/// ```
///
/// The futures returned by the methods must be `Send`, since workers run on the
/// multi-threaded tokio runtime.
pub trait Worker: Send + 'static {
    /// The error this worker can fail with.
    ///
    /// Only [`Display`](fmt::Display) is required, so `anyhow::Error`,
    /// `eyre::Report` or a plain `String` all work.
    type Error: fmt::Display + Send + 'static;

    /// Runs the worker's main loop until it completes or fails.
    ///
    /// When the supervisor terminates this worker, this future is dropped at its
    /// next await point. To wind down on your own terms instead, take a
    /// [`ShutdownSignal`] in the worker's factory (`|signal| MyWorker { signal }`)
    /// and select on it.
    fn run(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Called once before [`run`](Worker::run).
    fn initialize(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Called when the worker is being shut down.
    ///
    /// Runs after `run` returns, whether it finished on its own, failed, or was
    /// cancelled because the supervisor asked the worker to stop. Use it to
    /// flush buffers, commit work, or release resources.
    ///
    /// It does **not** run if the worker cannot be cancelled within the
    /// supervisor's shutdown timeout — for example if `run` blocks the thread
    /// without awaiting — because the task is then aborted outright. Keep
    /// cleanup well under that timeout; work that overruns it is abandoned.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}

// ============================================================================
// Type erasure
// ============================================================================

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object-safe form of [`Worker`], so one supervisor can hold workers of
/// different concrete types. Errors are rendered to strings at this boundary;
/// the runtime only ever logs or forwards them.
pub(crate) trait DynWorker: Send {
    fn initialize(&mut self) -> BoxFuture<'_, Result<(), String>>;
    fn run(&mut self) -> BoxFuture<'_, Result<(), String>>;
    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), String>>;
}

impl<W: Worker> DynWorker for W {
    fn initialize(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move { Worker::initialize(self).await.map_err(|e| e.to_string()) })
    }

    fn run(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move { Worker::run(self).await.map_err(|e| e.to_string()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move { Worker::shutdown(self).await.map_err(|e| e.to_string()) })
    }
}

/// Builds worker instances for a supervisor, on first start and on every
/// restart.
///
/// Implemented for closures of two shapes, so either can be passed wherever a
/// factory is expected:
///
/// - `Fn() -> W` for workers that do not care how they are stopped;
/// - `Fn(ShutdownSignal) -> W` for workers that want to observe cooperative
///   shutdown from inside `run`.
///
/// The `Marker` parameter only exists to keep those two implementations apart
/// and is always inferred.
pub trait WorkerFactory<Marker>: Send + Sync + 'static {
    /// The worker type this factory produces.
    type Worker: Worker;

    /// Creates a fresh worker.
    fn create(&self, signal: ShutdownSignal) -> Self::Worker;
}

impl<F, W> WorkerFactory<()> for F
where
    F: Fn() -> W + Send + Sync + 'static,
    W: Worker,
{
    type Worker = W;

    fn create(&self, _signal: ShutdownSignal) -> W {
        self()
    }
}

impl<F, W> WorkerFactory<ShutdownSignal> for F
where
    F: Fn(ShutdownSignal) -> W + Send + Sync + 'static,
    W: Worker,
{
    type Worker = W;

    fn create(&self, signal: ShutdownSignal) -> W {
        self(signal)
    }
}

type ErasedFactory = Arc<dyn Fn(ShutdownSignal) -> Box<dyn DynWorker> + Send + Sync>;

/// Specification for creating and restarting a worker
#[derive(Clone)]
pub(crate) struct WorkerSpec {
    /// Shared, so spawning a worker and indexing it by id never copy it.
    pub id: Arc<str>,
    factory: ErasedFactory,
    pub restart_policy: RestartPolicy,
    /// How the worker is stopped; `None` uses the supervisor's timeout.
    pub shutdown: Option<crate::supervisor::Shutdown>,
    /// Whether this child ending on its own can shut its supervisor down.
    pub significant: bool,
}

impl WorkerSpec {
    pub(crate) fn new<M>(
        id: impl Into<String>,
        factory: impl WorkerFactory<M>,
        restart_policy: RestartPolicy,
    ) -> Self {
        Self {
            id: Arc::from(id.into()),
            factory: Arc::new(move |signal| Box::new(factory.create(signal))),
            restart_policy,
            shutdown: None,
            significant: false,
        }
    }

    fn create_worker(&self, signal: ShutdownSignal) -> Box<dyn DynWorker> {
        (self.factory)(signal)
    }
}

impl fmt::Debug for WorkerSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerSpec")
            .field("id", &self.id)
            .field("restart_policy", &self.restart_policy)
            .field("shutdown", &self.shutdown)
            .field("significant", &self.significant)
            .finish_non_exhaustive()
    }
}

/// Stands in for a worker whose factory panicked: it fails to initialize with
/// the panic message, which the supervisor handles like any failed start.
struct FactoryPanicked(String);

impl DynWorker for FactoryPanicked {
    fn initialize(&mut self) -> BoxFuture<'_, Result<(), String>> {
        let message = format!("worker factory {}", self.0);
        Box::pin(async move { Err(message) })
    }

    fn run(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

// ============================================================================
// Running worker
// ============================================================================

/// Running worker process with its specification and task handle
pub(crate) struct WorkerProcess {
    pub spec: WorkerSpec,
    /// Identifies this run of the worker; a restart gets a new one.
    pub instance: u64,
    handle: Option<JoinHandle<()>>,
    /// Triggers cooperative shutdown; dropping it also releases the worker.
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl WorkerProcess {
    pub(crate) fn spawn(
        spec: WorkerSpec,
        supervisor_name: Arc<str>,
        control_tx: mpsc::UnboundedSender<SupervisorCommand>,
    ) -> Self {
        Self::spawn_inner(spec, supervisor_name, control_tx, None)
    }

    /// Spawns a worker with linked initialization handshake
    pub(crate) fn spawn_with_link(
        spec: WorkerSpec,
        supervisor_name: Arc<str>,
        control_tx: mpsc::UnboundedSender<SupervisorCommand>,
        init_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    ) -> Self {
        Self::spawn_inner(spec, supervisor_name, control_tx, Some(init_tx))
    }

    fn spawn_inner(
        spec: WorkerSpec,
        supervisor_name: Arc<str>,
        control_tx: mpsc::UnboundedSender<SupervisorCommand>,
        init_tx: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        // The factory is user code running on the supervisor's own task; a
        // panic there must fail this child's start, not the supervisor.
        let signal_for_factory = ShutdownSignal::new(shutdown_rx.clone());
        let worker = crate::panic::catch(|| spec.create_worker(signal_for_factory))
            .unwrap_or_else(|message| Box::new(FactoryPanicked(message)));
        let worker_id = Arc::clone(&spec.id);
        let signal = ShutdownSignal::new(shutdown_rx);
        let instance = crate::events::next_instance();

        let handle = tokio::spawn(run_worker(
            supervisor_name,
            worker_id,
            instance,
            worker,
            control_tx,
            init_tx,
            signal,
        ));

        Self {
            spec,
            instance,
            handle: Some(handle),
            shutdown_tx,
        }
    }

    /// Whether the worker's task is still running. A worker that has finished
    /// has already sent its exit report.
    pub(crate) fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Sends the cooperative shutdown signal without waiting for the worker.
    ///
    /// Signalling every child first and only then waiting lets them all wind
    /// down at the same time instead of one after another.
    pub(crate) fn request_stop(&self) {
        // A send error means the task is already gone.
        let _notified = self.shutdown_tx.send(true);
    }

    /// Aborts the worker's task outright; its `shutdown` hook does not run.
    ///
    /// Returns `true` if the worker had already stopped on its own.
    pub(crate) async fn abort(&mut self) -> bool {
        let Some(handle) = self.handle.take() else {
            return true;
        };
        handle.abort();
        let _join_result = handle.await;
        false
    }

    /// Kills the worker outright, as Erlang's `exit(Pid, kill)` does.
    ///
    /// Returns `true` if it was still running and has been killed, in which
    /// case it sends no exit report. Returns `false` if it had already
    /// finished; its own exit report is then on its way.
    pub(crate) async fn kill(&mut self) -> bool {
        let Some(handle) = self.handle.take() else {
            return false;
        };
        if handle.is_finished() {
            let _join_result = handle.await;
            return false;
        }
        handle.abort();
        // A worker that finished between the check and the abort completes
        // normally here and has reported itself.
        matches!(handle.await, Err(e) if e.is_cancelled())
    }

    /// Waits for a worker that was already sent [`request_stop`](Self::request_stop)
    /// to finish, aborting it if it is still running at `deadline`. `None`
    /// waits indefinitely.
    ///
    /// Returns `true` if the worker stopped on its own (so its `shutdown` hook
    /// ran), `false` if it had to be aborted.
    pub(crate) async fn stop_by(&mut self, deadline: Option<tokio::time::Instant>) -> bool {
        let Some(mut handle) = self.handle.take() else {
            return true;
        };

        let Some(at) = deadline else {
            let _joined = (&mut handle).await;
            return true;
        };

        match tokio::time::timeout_at(at, &mut handle).await {
            Ok(_joined) => true,
            Err(_elapsed) => {
                // The worker ignored the signal; drop it the hard way. Its
                // `shutdown` hook does not run in this path.
                tracing::warn!(
                    worker = %self.spec.id,
                    "worker did not stop within shutdown timeout, aborting"
                );
                handle.abort();
                let _join_result = handle.await;
                false
            }
        }
    }
}

/// The instant `timeout` from now, or `None` if that is too far away to
/// represent (an effectively unbounded timeout).
pub(crate) fn deadline_after(timeout: std::time::Duration) -> Option<tokio::time::Instant> {
    tokio::time::Instant::now().checked_add(timeout)
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
