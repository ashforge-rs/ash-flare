//! Python bindings for distributed supervision
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! handle = await asyncio.to_thread(RemoteSupervisorHandle.connect_tcp, "127.0.0.1:8080")
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::distributed::{RemoteSupervisorHandle, SupervisorAddress};

use super::block_on_without_gil;
use super::types::PyChildInfo;

/// Python-facing supervisor address for distributed supervision
#[pyclass(name = "SupervisorAddress", skip_from_py_object)]
#[derive(Clone)]
pub struct PySupervisorAddress {
    #[allow(dead_code)]
    inner: SupervisorAddress,
}

#[pymethods]
impl PySupervisorAddress {
    #[staticmethod]
    fn tcp(addr: String) -> Self {
        PySupervisorAddress {
            inner: SupervisorAddress::Tcp(addr),
        }
    }

    #[staticmethod]
    #[cfg(unix)]
    fn unix(path: String) -> Self {
        PySupervisorAddress {
            inner: SupervisorAddress::Unix(path),
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            SupervisorAddress::Tcp(addr) => format!("SupervisorAddress.tcp('{addr}')"),
            SupervisorAddress::Unix(path) => format!("SupervisorAddress.unix('{path}')"),
        }
    }
}

/// Python-facing remote supervisor handle for distributed supervision
#[pyclass(name = "RemoteSupervisorHandle")]
pub struct PyRemoteSupervisorHandle {
    /// Shared rather than cloned per call: a clone opens its own connection,
    /// and reusing one connection is what keeps repeated calls cheap.
    inner: Arc<RemoteSupervisorHandle>,
}

#[pymethods]
impl PyRemoteSupervisorHandle {
    #[staticmethod]
    fn connect_tcp(addr: String) -> Self {
        // No I/O happens here: each command opens its own connection.
        PyRemoteSupervisorHandle {
            inner: Arc::new(RemoteSupervisorHandle::tcp(addr)),
        }
    }

    #[staticmethod]
    #[cfg(unix)]
    fn connect_unix(path: String) -> Self {
        // No I/O happens here: each command opens its own connection.
        PyRemoteSupervisorHandle {
            inner: Arc::new(RemoteSupervisorHandle::unix(path)),
        }
    }

    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let handle = Arc::clone(&self.inner);
        block_on_without_gil(py, async move { handle.shutdown().await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to shutdown: {e}")))
    }

    fn which_children(&self, py: Python<'_>) -> PyResult<Vec<PyChildInfo>> {
        let handle = Arc::clone(&self.inner);
        let result = block_on_without_gil(py, async move { handle.which_children().await });

        match result {
            Ok(children) => Ok(children.into_iter().map(PyChildInfo::from).collect()),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to get children: {e}"
            ))),
        }
    }

    fn terminate_child(&self, child_id: String, py: Python<'_>) -> PyResult<()> {
        let handle = Arc::clone(&self.inner);
        block_on_without_gil(py, async move { handle.terminate_child(&child_id).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to terminate child: {e}")))
    }

    #[allow(clippy::unused_self)]
    fn __repr__(&self) -> String {
        "RemoteSupervisorHandle()".to_owned()
    }
}
