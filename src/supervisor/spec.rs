//! Supervisor specification and builder

use super::error::SupervisorError;
use crate::registry::Registry;
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

/// When a supervisor stops itself because its significant children ended, as
/// in OTP's `auto_shutdown` flag.
///
/// Only a significant child that terminates on its own and is not restarted
/// counts: a `Temporary` child that exits for any reason, or a `Transient`
/// child that exits normally. Children stopped with `terminate_child` or by
/// the supervisor do not.
///
/// A supervisor that stops itself this way exits with
/// [`ChildExitReason::Normal`](crate::ChildExitReason::Normal): that is the
/// reason in its `Stopped` event and the one its parent sees, so a parent
/// running it as `Transient` leaves it stopped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AutoShutdown {
    /// Never; no child may be significant.
    #[default]
    Never,
    /// As soon as any significant child has ended.
    AnySignificant,
    /// Once every significant child has ended.
    AllSignificant,
}

/// What a top-level supervisor does when it gives up: when its children fail
/// more often than its restart intensity allows, or its runtime panics.
///
/// A nested supervisor that gives up escalates to its parent, which applies
/// its own restart policy. A top-level one has no parent; this says what
/// happens instead.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GiveUp {
    /// Stop. Whoever holds the handle finds out through
    /// [`SupervisorHandle::wait`](crate::SupervisorHandle::wait), `is_stopped`
    /// or the `Stopped` event, and decides what to do.
    #[default]
    Stop,
    /// Once the tree has shut down, exit the process with this status code,
    /// so the process supervisor (systemd, Kubernetes, a container runtime)
    /// restarts it, as an OTP node shuts down when a permanent application
    /// fails.
    ///
    /// The tree's own `shutdown` hooks have run by then, but the process
    /// exits from inside the supervisor, so destructors of other tasks and
    /// threads, and code after `main`'s `await`, do not run. When more has to
    /// happen first, use [`GiveUp::Stop`] and exit from `main` instead:
    ///
    /// ```no_run
    /// # use ash_flare::{ChildExitReason, SupervisorHandle, SupervisorSpec};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let spec = SupervisorSpec::new("app");
    /// let app = SupervisorHandle::start(spec).unwrap();
    /// if app.wait().await == ChildExitReason::Abnormal {
    ///     // Flush logs and metrics, release locks, then exit for a restart.
    ///     std::process::exit(1);
    /// }
    /// # }
    /// ```
    ExitProcess(i32),
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

    /// Marks the child as significant for the supervisor's
    /// [`AutoShutdown`]. A significant child must be `Transient` or
    /// `Temporary`.
    #[must_use]
    pub fn significant(mut self, significant: bool) -> Self {
        self.worker.significant = significant;
        self
    }

    /// Runs the worker on a thread of its own, with its own single-threaded
    /// tokio runtime.
    ///
    /// Async Rust cannot preempt a task the way the BEAM preempts a process:
    /// a worker that blocks its thread, or computes for a long time without
    /// awaiting, holds up every other task on that runtime thread. An isolated
    /// worker only ever holds up its own thread. Use it for workers that call
    /// blocking APIs or do heavy computation.
    ///
    /// Stopping, killing and restarting work as for any worker. Tasks the
    /// worker spawns run on its own runtime and end with it. A worker stuck in
    /// a loop that never awaits still cannot be interrupted: it is reported as
    /// killed and replaced, while its old thread runs on until it yields.
    #[must_use]
    pub fn isolated(mut self, isolated: bool) -> Self {
        self.worker.isolated = isolated;
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
    pub(crate) auto_shutdown: AutoShutdown,
    pub(crate) stall_threshold: Option<Duration>,
    /// How this supervisor is restarted when it is the child of another.
    pub(crate) restart_policy: RestartPolicy,
    /// Where each run of this supervisor registers its handle, and under
    /// what name.
    pub(crate) registration: Option<(Registry, String)>,
    pub(crate) give_up: GiveUp,
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
            auto_shutdown: AutoShutdown::default(),
            stall_threshold: None,
            restart_policy: RestartPolicy::Permanent,
            registration: None,
            give_up: GiveUp::Stop,
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
            match child {
                ChildDef::Worker(worker) if worker.significant => {
                    self.validate_significant(worker)?;
                }
                ChildDef::Worker(_) => {}
                ChildDef::Supervisor(nested) => {
                    if nested.give_up != GiveUp::Stop {
                        return Err(SupervisorError::InvalidChildSpec {
                            supervisor: self.name.clone(),
                            child_id: nested.name.clone(),
                            reason: "only a top-level supervisor can set GiveUp; a nested one \
                                     escalates to its parent"
                                .to_owned(),
                        });
                    }
                    nested.validate()?;
                }
            }
        }
        Ok(())
    }

    /// A significant child must be able to end without being restarted, and
    /// only makes sense when the supervisor shuts down automatically.
    fn validate_significant(&self, worker: &WorkerSpec) -> Result<(), SupervisorError> {
        let invalid = |reason: &str| SupervisorError::InvalidChildSpec {
            supervisor: self.name.clone(),
            child_id: worker.id.to_string(),
            reason: reason.to_owned(),
        };
        if worker.restart_policy == RestartPolicy::Permanent {
            return Err(invalid(
                "a significant child must be Transient or Temporary; a Permanent one never ends",
            ));
        }
        if self.auto_shutdown == AutoShutdown::Never {
            return Err(invalid(
                "a significant child needs an auto_shutdown other than Never",
            ));
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

    /// Reports workers that block their thread for longer than `threshold`,
    /// like Erlang's `system_monitor(long_schedule)`.
    ///
    /// Async Rust cannot preempt a task: a worker that blocks or computes for
    /// a long time without awaiting holds up every other task on its runtime
    /// thread. With this set, each poll of this supervisor's workers is timed,
    /// and one stuck in a poll past `threshold` is reported, while it is still
    /// stuck, as [`SupervisorEvent::ChildStalled`](crate::SupervisorEvent) and
    /// a warning log. Move such workers to their own thread with
    /// [`ChildSpec::isolated`]. Off by default; when on, it costs a clock read
    /// per poll.
    #[must_use]
    pub fn with_stall_detection(mut self, threshold: Duration) -> Self {
        self.stall_threshold = Some(threshold);
        self
    }

    /// Makes the supervisor stop itself when its significant children end.
    /// Defaults to [`AutoShutdown::Never`].
    #[must_use]
    pub fn with_auto_shutdown(mut self, auto_shutdown: AutoShutdown) -> Self {
        self.auto_shutdown = auto_shutdown;
        self
    }

    /// Sets how this supervisor is restarted when it is nested in another.
    /// Defaults to [`RestartPolicy::Permanent`].
    ///
    /// A supervisor that stopped on its own, through its [`AutoShutdown`],
    /// counts as a normal exit; make it `Transient` so the parent does not
    /// start it again.
    #[must_use]
    pub fn with_restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
    }

    /// Registers each run of this supervisor in `registry` under `name`, like
    /// `supervisor:start_link({local, Name}, ...)` in OTP.
    ///
    /// The handle is registered before any child starts, so children can
    /// find their own supervisor, and unregistered when the run stops. After a
    /// restart the name refers to the new run, so looking it up always gives
    /// the current one. This is how workers reach a nested supervisor, for
    /// example to start children on it:
    ///
    /// ```
    /// use ash_flare::{Registry, SupervisorHandle, SupervisorSpec};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let registry = Registry::new();
    /// let app = SupervisorHandle::start(
    ///     SupervisorSpec::new("app")
    ///         .with_supervisor(SupervisorSpec::new("rooms").with_registered_name(&registry, "rooms")),
    /// )
    /// .unwrap();
    ///
    /// let rooms = registry.whereis::<SupervisorHandle>("rooms").unwrap();
    /// assert_eq!(rooms.name(), "rooms");
    /// # app.shutdown().await.unwrap();
    /// # }
    /// ```
    ///
    /// A registration replaces any earlier one of the same name.
    #[must_use]
    pub fn with_registered_name(mut self, registry: &Registry, name: impl Into<String>) -> Self {
        self.registration = Some((registry.clone(), name.into()));
        self
    }

    /// Sets what this supervisor does when it gives up, if it is a top-level
    /// one. Defaults to [`GiveUp::Stop`]. Setting anything else on a nested
    /// supervisor makes `SupervisorHandle::start` fail.
    #[must_use]
    pub fn with_give_up(mut self, give_up: GiveUp) -> Self {
        self.give_up = give_up;
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

    /// Adds `size` identical workers with ids `{prefix}-0` to
    /// `{prefix}-{size - 1}`. The factory receives each worker's index.
    ///
    /// Combined with a shared [`Mailbox`](crate::Mailbox), this makes a pool
    /// in which each message goes to whichever worker is free.
    #[must_use]
    pub fn with_worker_pool<W, F>(
        mut self,
        prefix: &str,
        size: usize,
        factory: F,
        restart_policy: RestartPolicy,
    ) -> Self
    where
        W: crate::Worker,
        F: Fn(usize) -> W + Send + Sync + 'static,
    {
        let shared = Arc::new(factory);
        for index in 0..size {
            let member = Arc::clone(&shared);
            self = self.with_worker(
                format!("{prefix}-{index}"),
                move || member(index),
                restart_policy,
            );
        }
        self
    }

    /// Adds a nested supervisor child to this supervisor specification.
    #[must_use]
    pub fn with_supervisor(mut self, supervisor: SupervisorSpec) -> Self {
        self.children
            .push(ChildDef::Supervisor(Arc::new(supervisor)));
        self
    }
}
