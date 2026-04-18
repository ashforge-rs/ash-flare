//! Supervisor errors

use std::fmt;

/// Errors returned by supervisor operations.
#[derive(Debug, Clone)]
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
                write!(f, "supervisor '{name}' has no children")
            }
            SupervisorError::AllChildrenFailed(name) => {
                write!(
                    f,
                    "all children failed for supervisor '{name}' - restart intensity limit exceeded"
                )
            }
            SupervisorError::ShuttingDown(name) => {
                write!(
                    f,
                    "supervisor '{name}' is shutting down - operation not permitted"
                )
            }
            SupervisorError::ChildAlreadyExists(id) => {
                write!(
                    f,
                    "child with id '{id}' already exists - use a unique identifier"
                )
            }
            SupervisorError::ChildNotFound(id) => {
                write!(
                    f,
                    "child with id '{id}' not found - it may have already terminated"
                )
            }
            SupervisorError::InitializationFailed { child_id, reason } => {
                write!(f, "child '{child_id}' initialization failed: {reason}")
            }
            SupervisorError::InitializationTimeout { child_id, timeout } => {
                write!(
                    f,
                    "child '{child_id}' initialization timed out after {timeout:?}"
                )
            }
        }
    }
}

impl std::error::Error for SupervisorError {}
