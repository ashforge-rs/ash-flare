//! Python bindings for ash-flare using PyO3
//!
//! This module provides comprehensive Python bindings for the ash-flare supervision library.
//! The bindings are organized into several submodules:
//!
//! - `types` - Basic restart and child types
//! - `context` - WorkerContext for stateful workers
//! - `mailbox` - Message-passing system
//! - `worker` - Worker implementation
//! - `supervisor` - Regular supervisor
//! - `stateful` - Stateful supervisor with shared context
//! - `distributed` - Distributed supervision over TCP/Unix sockets

use pyo3::prelude::*;
use std::sync::OnceLock;

mod context;
mod distributed;
mod mailbox;
mod stateful;
mod supervisor;
mod types;
mod worker;

// Re-export public types for use by submodules
pub(crate) use context::*;
pub(crate) use distributed::*;
pub(crate) use mailbox::*;
pub(crate) use stateful::*;
pub(crate) use supervisor::*;
pub(crate) use types::*;

// Global tokio runtime for Python bindings
static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

#[allow(clippy::expect_used)]
pub(crate) fn get_runtime() -> &'static tokio::runtime::Runtime {
    TOKIO_RUNTIME
        .get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create tokio runtime"))
}

/// Python module definition
#[pymodule]
fn ash_flare(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Restart types
    m.add_class::<PyRestartPolicy>()?;
    m.add_class::<PyRestartStrategy>()?;
    m.add_class::<PyRestartIntensity>()?;

    // Child types
    m.add_class::<PyChildType>()?;
    m.add_class::<PyChildExitReason>()?;
    m.add_class::<PyChildInfo>()?;

    // WorkerContext for stateful workers
    m.add_class::<PyWorkerContext>()?;

    // Mailbox system
    m.add_class::<PyMailboxConfig>()?;
    m.add_class::<PyMailboxHandle>()?;
    m.add_class::<PyMailbox>()?;
    m.add_function(wrap_pyfunction!(mailbox::mailbox, m)?)?;
    m.add_function(wrap_pyfunction!(mailbox::mailbox_named, m)?)?;

    // Regular supervisor
    m.add_class::<PySupervisorSpec>()?;
    m.add_class::<PySupervisorHandle>()?;

    // Stateful supervisor
    m.add_class::<PyStatefulSupervisorSpec>()?;
    m.add_class::<PyStatefulSupervisorHandle>()?;

    // Distributed supervision
    m.add_class::<PySupervisorAddress>()?;
    m.add_class::<PyRemoteSupervisorHandle>()?;

    Ok(())
}
