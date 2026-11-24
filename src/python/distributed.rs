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
use pyo3::types::PyDict;

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

impl PyRemoteSupervisorHandle {
    fn with_cookie(handle: RemoteSupervisorHandle, cookie: Option<String>) -> Self {
        let configured = match cookie {
            Some(secret) => handle.with_cookie(secret),
            None => handle,
        };
        Self {
            inner: Arc::new(configured),
        }
    }
}

#[pymethods]
impl PyRemoteSupervisorHandle {
    /// A handle to a supervisor on TCP `addr`. No connection is made until
    /// the first command; `cookie` authenticates both ends when set.
    #[staticmethod]
    #[pyo3(signature = (addr, *, cookie=None))]
    fn connect_tcp(addr: String, cookie: Option<String>) -> Self {
        Self::with_cookie(RemoteSupervisorHandle::tcp(addr), cookie)
    }

    /// A handle to a supervisor on the Unix socket at `path`.
    #[staticmethod]
    #[cfg(unix)]
    #[pyo3(signature = (path, *, cookie=None))]
    fn connect_unix(path: String, cookie: Option<String>) -> Self {
        Self::with_cookie(RemoteSupervisorHandle::unix(path), cookie)
    }

    /// The remote supervisor's name, child count, restart strategy and
    /// uptime in seconds.
    fn status(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let handle = Arc::clone(&self.inner);
        let status = block_on_without_gil(py, async move { handle.status().await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get status: {e}")))?;
        let dict = PyDict::new(py);
        dict.set_item("name", status.name)?;
        dict.set_item("children_count", status.children_count)?;
        dict.set_item("restart_strategy", format!("{:?}", status.restart_strategy))?;
        dict.set_item("uptime_secs", status.uptime_secs)?;
        Ok(dict.into())
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
