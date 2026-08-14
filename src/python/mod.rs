//! Python bindings for ash-flare using `PyO3`
//!
//! This module provides comprehensive Python bindings for the ash-flare supervision library.
//! The bindings are organized into several submodules:
//!
//! - `types` - Basic restart and child types
//! - `context` - `WorkerContext` for stateful workers
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

/// Runs a future to completion with the GIL released.
///
/// Holding the GIL across a blocking call stops every other Python thread,
/// including the one that would produce the value being waited for — for
/// unbounded waits such as `Mailbox.recv` that is a hard deadlock, and for
/// short calls it needlessly serialises the interpreter.
pub(crate) fn block_on_without_gil<F>(py: Python<'_>, future: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    py.detach(|| get_runtime().block_on(future))
}

/// Rejects a worker argument that is not callable, instead of accepting it and
/// failing silently later when the supervisor tries to invoke it.
pub(crate) fn ensure_callable(py: Python<'_>, obj: &Py<PyAny>, id: &str) -> PyResult<()> {
    if obj.bind(py).is_callable() {
        return Ok(());
    }
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "worker '{id}' must be callable, got {}",
        obj.bind(py)
            .get_type()
            .name()
            .map_or_else(|_e| "?".to_owned(), |n| n.to_string())
    )))
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

    // Cooperative-shutdown handle passed to worker callables
    m.add_class::<worker::PyShouldStop>()?;

    // Package version. `__version__` is set on the extension module itself;
    // `version` is also exported because maturin's generated `__init__.py`
    // re-exports with `from .ash_flare import *`, which skips dunder names.
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("version", env!("CARGO_PKG_VERSION"))?;

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
