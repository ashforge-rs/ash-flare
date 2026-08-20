//! Supervisor specification and builder

use crate::restart::{RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy};
use crate::worker::{Worker, WorkerSpec};
use std::sync::Arc;
use std::time::Duration;

/// Specification for a child (either worker or supervisor)
pub(crate) enum ChildSpec<W: Worker> {
    Worker(WorkerSpec<W>),
    Supervisor(Arc<SupervisorSpec<W>>),
}

/// Describes a supervisor and its children in a tree structure.
pub struct SupervisorSpec<W: Worker> {
    pub(crate) name: String,
    pub(crate) children: Vec<ChildSpec<W>>,
    pub(crate) restart_strategy: RestartStrategy,
    pub(crate) restart_intensity: RestartIntensity,
    pub(crate) restart_backoff: RestartBackoff,
    pub(crate) shutdown_timeout: Duration,
}

/// Default grace period a worker gets to stop cooperatively before being aborted.
pub(crate) const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

impl<W: Worker> Clone for SupervisorSpec<W> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            children: self.children.clone(),
            restart_strategy: self.restart_strategy,
            restart_intensity: self.restart_intensity,
            restart_backoff: self.restart_backoff,
            shutdown_timeout: self.shutdown_timeout,
        }
    }
}

// Only the Python bindings validate ids at registration time; the Rust builder
// leaves duplicate detection to `start_child`.
#[cfg(feature = "python")]
impl<W: Worker> ChildSpec<W> {
    /// Identifier this child will be registered under.
    pub(crate) fn id(&self) -> &str {
        match self {
            ChildSpec::Worker(w) => &w.id,
            ChildSpec::Supervisor(s) => &s.name,
        }
    }
}

#[cfg(feature = "python")]
impl<W: Worker> SupervisorSpec<W> {
    /// Returns true if a child with `id` is already registered.
    pub(crate) fn has_child(&self, id: &str) -> bool {
        self.children.iter().any(|c| c.id() == id)
    }
}

impl<W: Worker> Clone for ChildSpec<W> {
    fn clone(&self) -> Self {
        match self {
            ChildSpec::Worker(w) => ChildSpec::Worker(w.clone()),
            ChildSpec::Supervisor(s) => ChildSpec::Supervisor(Arc::clone(s)),
        }
    }
}

impl<W: Worker> SupervisorSpec<W> {
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
    /// signal is aborted and does **not** get the hook. Defaults to 5 seconds;
    /// `Duration::ZERO` aborts immediately.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Adds a worker child to this supervisor specification, giving the factory
    /// access to the worker's [`ShutdownSignal`](crate::ShutdownSignal).
    ///
    /// Use this instead of [`SupervisorSpec::with_worker`] when the worker needs
    /// to observe cooperative shutdown from inside its `run` loop.
    #[must_use]
    pub fn with_worker_signal(
        mut self,
        id: impl Into<String>,
        factory: impl Fn(crate::ShutdownSignal) -> W + Send + Sync + 'static,
        restart_policy: RestartPolicy,
    ) -> Self {
        self.children
            .push(ChildSpec::Worker(WorkerSpec::with_signal(
                id,
                factory,
                restart_policy,
            )));
        self
    }

    /// Adds a worker child to this supervisor specification.
    /// The factory function is used to create new worker instances (e.g., for restarts).
    #[must_use]
    pub fn with_worker(
        mut self,
        id: impl Into<String>,
        factory: impl Fn() -> W + Send + Sync + 'static,
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
    pub fn with_supervisor(mut self, supervisor: SupervisorSpec<W>) -> Self {
        self.children
            .push(ChildSpec::Supervisor(Arc::new(supervisor)));
        self
    }
}
