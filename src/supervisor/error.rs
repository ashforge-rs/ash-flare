//! Supervisor errors

use std::time::Duration;

/// Errors returned by supervisor operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SupervisorError {
    /// The supervisor has stopped, or is stopping, and no longer accepts commands.
    #[error("supervisor '{0}' is shutting down - operation not permitted")]
    ShuttingDown(String),
    /// A child with this ID already exists.
    #[error("child with id '{0}' already exists - use a unique identifier")]
    ChildAlreadyExists(String),
    /// No child with this ID is supervised.
    #[error("child with id '{0}' not found - it may have already terminated")]
    ChildNotFound(String),
    /// The child with this ID is a worker, not a nested supervisor.
    #[error("child with id '{0}' is a worker, not a supervisor")]
    NotASupervisor(String),
    /// Two children of one supervisor share an id.
    #[error("supervisor '{supervisor}' has more than one child with id '{child_id}'")]
    DuplicateChildId {
        /// Name of the supervisor whose children clash
        supervisor: String,
        /// The id used more than once
        child_id: String,
    },
    /// A child's settings contradict each other or the supervisor's.
    #[error("invalid child '{child_id}' in supervisor '{supervisor}': {reason}")]
    InvalidChildSpec {
        /// Name of the supervisor the child belongs to
        supervisor: String,
        /// The child's id
        child_id: String,
        /// What is wrong
        reason: String,
    },
    /// Child initialization failed.
    #[error("child '{child_id}' initialization failed: {reason}")]
    InitializationFailed {
        /// ID of the child that failed to initialize
        child_id: String,
        /// Reason for initialization failure
        reason: String,
    },
    /// Child initialization timed out.
    #[error("child '{child_id}' initialization timed out after {timeout:?}")]
    InitializationTimeout {
        /// ID of the child that timed out
        child_id: String,
        /// Duration after which timeout occurred
        timeout: Duration,
    },
}
