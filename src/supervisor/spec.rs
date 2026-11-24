//! Supervisor specification and builder

use super::error::SupervisorError;
use crate::restart::{RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy};
use crate::worker::{WorkerFactory, WorkerSpec};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// A child as declared in a [`SupervisorSpec`]: a worker or a nested supervisor.
#[derive(Clone)]
pub(crate) enum ChildDef {
    Worker(WorkerSpec),
    Supervisor(Arc<SupervisorSpec>),
}

impl ChildDef {
    /// Identifier this child will be registered under.
    pub(crate) fn id(&self) -> &str {
        match self {
            ChildDef::Worker(w) => &w.id,
            ChildDef::Supervisor(s) => &s.name,
        }
    }
}

/// How a child is stopped when its supervisor terminates it, as in OTP's
/// `shutdown` child-spec field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shutdown {
    /// Abort the worker's task at once. Its `shutdown` hook does not run.
    BrutalKill,
    /// Send the [`ShutdownSignal`](crate::ShutdownSignal) and wait up to this
    /// long for the worker to stop, then abort it. `Duration::ZERO` behaves
    /// like [`Shutdown::BrutalKill`].
    Timeout(Duration),
    /// Send the signal and wait for the worker however long it takes.
    Infinity,
}

/// The order in which a supervisor stops several children at once: on its own
/// shutdown and on a `OneForAll` or `RestForOne` restart.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOrder {
    /// One at a time, last started first, as OTP does. A child is never
    /// stopped while a child started after it, which may depend on it, is
    /// still running.
    #[default]
    Reverse,
    /// All at once. Stopping is as fast as the slowest child, but children may
    /// still be running while the ones they depend on stop.
    Concurrent,
}

/// Full specification of a worker child, for when the defaults of
/// [`SupervisorSpec::with_worker`] are not enough.
///
/// ```
/// use ash_flare::{ChildSpec, RestartPolicy, Shutdown, SupervisorSpec, Worker};
/// use std::time::Duration;
///
/// struct Flusher;
/// impl Worker for Flusher {
///     type Error = std::io::Error;
///     async fn run(&mut self) -> Result<(), Self::Error> { Ok(()) }
/// }
///
/// let spec = SupervisorSpec::new("app").with_child(
///     ChildSpec::new("flusher", || Flusher)
///         .restart(RestartPolicy::Transient)
///         .shutdown(Shutdown::Timeout(Duration::from_secs(30))),
/// );
/// ```
#[derive(Clone, Debug)]
pub struct ChildSpec {
    pub(crate) worker: WorkerSpec,
}

impl ChildSpec {
    /// A permanent worker built by `factory`, stopped with the supervisor's
    /// shutdown timeout. The factory may take no arguments or the worker's
    /// [`ShutdownSignal`](crate::ShutdownSignal).
    pub fn new<M>(id: impl Into<String>, factory: impl WorkerFactory<M>) -> Self {
        Self {
            worker: WorkerSpec::new(id, factory, RestartPolicy::Permanent),
        }
    }

    /// Sets when the child is restarted. Defaults to
    /// [`RestartPolicy::Permanent`].
    #[must_use]
    pub fn restart(mut self, policy: RestartPolicy) -> Self {
        self.worker.restart_policy = policy;
        self
    }

    /// Sets how the child is stopped, overriding the supervisor's
    /// [shutdown timeout](SupervisorSpec::with_shutdown_timeout).
    #[must_use]
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.worker.shutdown = Some(shutdown);
        self
    }

    /// The child's id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.worker.id
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
    pub(crate) children: Vec<ChildDef>,
    pub(crate) restart_strategy: RestartStrategy,
    pub(crate) restart_intensity: RestartIntensity,
    pub(crate) restart_backoff: RestartBackoff,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) shutdown_order: ShutdownOrder,
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
            shutdown_order: ShutdownOrder::default(),
        }
    }

    /// Returns true if a child with `id` is already registered.
    #[cfg(feature = "python")]
    pub(crate) fn has_child(&self, id: &str) -> bool {
        self.children.iter().any(|c| c.id() == id)
    }

    /// Checks this spec and every nested one: child ids must be unique within
    /// each supervisor, since children are found, restarted and terminated by
    /// id.
    pub(crate) fn validate(&self) -> Result<(), SupervisorError> {
        let mut seen = HashSet::with_capacity(self.children.len());
        for child in &self.children {
            if !seen.insert(child.id()) {
                return Err(SupervisorError::DuplicateChildId {
                    supervisor: self.name.clone(),
                    child_id: child.id().to_owned(),
                });
            }
            if let ChildDef::Supervisor(nested) = child {
                nested.validate()?;
            }
        }
        Ok(())
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

    /// Sets the order in which children are stopped together. Defaults to
    /// [`ShutdownOrder::Reverse`].
    #[must_use]
    pub fn with_shutdown_order(mut self, order: ShutdownOrder) -> Self {
        self.shutdown_order = order;
        self
    }

    /// Adds a worker child described by a full [`ChildSpec`].
    #[must_use]
    pub fn with_child(mut self, child: ChildSpec) -> Self {
        self.children.push(ChildDef::Worker(child.worker));
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
        self,
        id: impl Into<String>,
        factory: impl WorkerFactory<M>,
        restart_policy: RestartPolicy,
    ) -> Self {
        self.with_child(ChildSpec::new(id, factory).restart(restart_policy))
    }

    /// Adds a nested supervisor child to this supervisor specification.
    #[must_use]
    pub fn with_supervisor(mut self, supervisor: SupervisorSpec) -> Self {
        self.children
            .push(ChildDef::Supervisor(Arc::new(supervisor)));
        self
    }
}
