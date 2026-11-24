//! Supervisor specification and builder

use crate::restart::{RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy};
use crate::worker::{WorkerFactory, WorkerSpec};
use std::sync::Arc;
use std::time::Duration;

/// Specification for a child (either worker or supervisor)
#[derive(Clone)]
pub(crate) enum ChildSpec {
    Worker(WorkerSpec),
    Supervisor(Arc<SupervisorSpec>),
}

impl ChildSpec {
    /// Identifier this child will be registered under.
    #[cfg(feature = "python")]
    pub(crate) fn id(&self) -> &str {
        match self {
            ChildSpec::Worker(w) => &w.id,
            ChildSpec::Supervisor(s) => &s.name,
        }
    }
}

/// Describes a supervisor and its children in a tree structure.
///
/// Workers of different types can be mixed freely in one tree:
///
/// ```
/// use ash_flare::{RestartPolicy, SupervisorSpec, Worker};
///
/// struct Http;
/// struct Db;
///
/// impl Worker for Http {
///     type Error = std::io::Error;
///     async fn run(&mut self) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// impl Worker for Db {
///     type Error = String;
///     async fn run(&mut self) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// let spec = SupervisorSpec::new("app")
///     .with_worker("http", || Http, RestartPolicy::Permanent)
///     .with_worker("db", || Db, RestartPolicy::Transient);
/// ```
#[derive(Clone)]
pub struct SupervisorSpec {
    pub(crate) name: String,
    pub(crate) children: Vec<ChildSpec>,
    pub(crate) restart_strategy: RestartStrategy,
    pub(crate) restart_intensity: RestartIntensity,
    pub(crate) restart_backoff: RestartBackoff,
    pub(crate) shutdown_timeout: Duration,
}

/// Default grace period a worker gets to stop cooperatively before being aborted.
pub(crate) const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

impl SupervisorSpec {
    /// Creates a new supervisor specification with the provided name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            children: Vec::new(),
            restart_strategy: RestartStrategy::default(),
            restart_intensity: RestartIntensity::default(),
            restart_backoff: RestartBackoff::default(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    /// Returns true if a child with `id` is already registered.
    #[cfg(feature = "python")]
    pub(crate) fn has_child(&self, id: &str) -> bool {
        self.children.iter().any(|c| c.id() == id)
    }

    /// Sets the restart strategy for this supervisor.
    #[must_use]
    pub fn with_restart_strategy(mut self, strategy: RestartStrategy) -> Self {
        self.restart_strategy = strategy;
        self
    }

    /// Sets the restart intensity for this supervisor.
    #[must_use]
    pub fn with_restart_intensity(mut self, intensity: RestartIntensity) -> Self {
        self.restart_intensity = intensity;
        self
    }

    /// Sets the backoff applied between a child terminating and being restarted.
    ///
    /// Defaults to an exponential backoff of 100ms doubling up to 30s. Use
    /// [`RestartBackoff::none`] to restart immediately, at the cost of allowing a
    /// crash-looping child to saturate a CPU core.
    #[must_use]
    pub fn with_restart_backoff(mut self, backoff: RestartBackoff) -> Self {
        self.restart_backoff = backoff;
        self
    }

    /// Sets a fixed delay between a child terminating and being restarted.
    ///
    /// Convenience wrapper over [`SupervisorSpec::with_restart_backoff`] for a
    /// constant delay; `delay` is used as both the initial and maximum delay.
    #[must_use]
    pub fn with_restart_delay(self, delay: Duration) -> Self {
        self.with_restart_backoff(RestartBackoff::exponential(delay, delay))
    }

    /// Sets how long a worker may take to stop cooperatively before its task is
    /// aborted.
    ///
    /// On termination each worker is sent a [`ShutdownSignal`](crate::ShutdownSignal);
    /// a worker that returns from `run` within this window also gets its
    /// [`shutdown`](crate::Worker::shutdown) hook run. A worker that ignores the
    /// signal is aborted and does **not** get the hook. When several children
    /// stop together (supervisor shutdown, or a `OneForAll` or `RestForOne`
    /// restart) they share
    /// one timeout instead of each waiting in turn. Defaults to 5 seconds;
    /// `Duration::ZERO` aborts immediately.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Adds a worker child to this supervisor specification.
    ///
    /// The factory is called to create a fresh worker on first start and on
    /// every restart. It may take no arguments, or the worker's
    /// [`ShutdownSignal`](crate::ShutdownSignal) if the worker needs to observe
    /// cooperative shutdown from inside its `run` loop:
    ///
    /// ```
    /// use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorSpec, Worker};
    ///
    /// struct Pump { shutdown: ShutdownSignal }
    ///
    /// impl Worker for Pump {
    ///     type Error = std::io::Error;
    ///     async fn run(&mut self) -> Result<(), Self::Error> {
    ///         self.shutdown.cancelled().await;
    ///         Ok(())
    ///     }
    /// }
    ///
    /// let spec = SupervisorSpec::new("app")
    ///     .with_worker("pump", |shutdown| Pump { shutdown }, RestartPolicy::Permanent);
    /// ```
    ///
    /// To share state between workers, capture a clone of it in the factory,
    /// for example a [`WorkerContext`](crate::WorkerContext).
    #[must_use]
    pub fn with_worker<M>(
        mut self,
        id: impl Into<String>,
        factory: impl WorkerFactory<M>,
        restart_policy: RestartPolicy,
    ) -> Self {
        self.children.push(ChildSpec::Worker(WorkerSpec::new(
            id,
            factory,
            restart_policy,
        )));
        self
    }

    /// Adds a nested supervisor child to this supervisor specification.
    #[must_use]
    pub fn with_supervisor(mut self, supervisor: SupervisorSpec) -> Self {
        self.children
            .push(ChildSpec::Supervisor(Arc::new(supervisor)));
        self
    }
}
