//! Supervisor specification and builder

use crate::restart::{RestartIntensity, RestartPolicy, RestartStrategy};
use crate::worker::{Worker, WorkerSpec};
use std::sync::Arc;

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
}

impl<W: Worker> Clone for SupervisorSpec<W> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            children: self.children.clone(),
            restart_strategy: self.restart_strategy,
            restart_intensity: self.restart_intensity,
        }
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
