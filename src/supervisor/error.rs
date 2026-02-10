//! Supervisor errors

use std::fmt;

/// Errors returned by supervisor operations.
#[derive(Debug)]
pub enum SupervisorError {
    /// Supervisor has no children
    NoChildren(String),
    /// All children have failed
    AllChildrenFailed(String),
    /// Supervisor is shutting down
    ShuttingDown(String),
    /// Child with this ID already exists
    ChildAlreadyExists(String),
    /// Child with this ID not found
    ChildNotFound(String),
    /// Child initialization failed
    InitializationFailed {
        /// ID of the child that failed to initialize
        child_id: String,
        /// Reason for initialization failure
        reason: String,
    },
    /// Child initialization timed out
    InitializationTimeout {
        /// ID of the child that timed out
        child_id: String,
        /// Duration after which timeout occurred
        timeout: std::time::Duration,
    },
}

impl fmt::Display for SupervisorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SupervisorError::NoChildren(name) => {
                write!(f, "supervisor '{}' has no children", name)
            }
            SupervisorError::AllChildrenFailed(name) => {
                write!(
                    f,
                    "all children failed for supervisor '{}' - restart intensity limit exceeded",
                    name
                )
            }
            SupervisorError::ShuttingDown(name) => {
                write!(
                    f,
                    "supervisor '{}' is shutting down - operation not permitted",
                    name
                )
            }
            SupervisorError::ChildAlreadyExists(id) => {
                write!(
                    f,
                    "child with id '{}' already exists - use a unique identifier",
                    id
                )
            }
            SupervisorError::ChildNotFound(id) => {
                write!(
                    f,
                    "child with id '{}' not found - it may have already terminated",
                    id
                )
            }
            SupervisorError::InitializationFailed { child_id, reason } => {
                write!(f, "child '{}' initialization failed: {}", child_id, reason)
            }
            SupervisorError::InitializationTimeout { child_id, timeout } => {
                write!(
                    f,
                    "child '{}' initialization timed out after {:?}",
                    child_id, timeout
                )
            }
        }
    }
}

impl std::error::Error for SupervisorError {}
