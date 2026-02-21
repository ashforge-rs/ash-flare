//! Python bindings for distributed supervision
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! handle = await asyncio.to_thread(RemoteSupervisorHandle.connect_tcp, "127.0.0.1:8080")
//! ```

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;

use crate::distributed::{RemoteSupervisorHandle, SupervisorAddress};

use super::types::{PyChildInfo, PyChildType};
use super::get_runtime;

/// Python-facing supervisor address for distributed supervision
#[pyclass(name = "SupervisorAddress")]
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
            SupervisorAddress::Tcp(addr) => format!("SupervisorAddress.tcp('{}')", addr),
            SupervisorAddress::Unix(path) => format!("SupervisorAddress.unix('{}')", path),
        }
    }
}

/// Python-facing remote supervisor handle for distributed supervision
#[pyclass(name = "RemoteSupervisorHandle")]
pub struct PyRemoteSupervisorHandle {
    inner: RemoteSupervisorHandle,
}

#[pymethods]
impl PyRemoteSupervisorHandle {
    #[staticmethod]
    fn connect_tcp(addr: String, _py: Python<'_>) -> PyResult<Self> {
        let runtime = get_runtime();
        let result = runtime.block_on(async move {
            RemoteSupervisorHandle::connect_tcp(addr).await
        });
        
        result
            .map(|inner| PyRemoteSupervisorHandle { inner })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))
    }

    #[staticmethod]
    #[cfg(unix)]
    fn connect_unix(path: String, _py: Python<'_>) -> PyResult<Self> {
        let runtime = get_runtime();
        let result = runtime.block_on(async move {
            RemoteSupervisorHandle::connect_unix(path).await
        });
        
        result
            .map(|inner| PyRemoteSupervisorHandle { inner })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to connect: {}", e)))
    }

    fn shutdown(&self, _py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        runtime.block_on(async move {
            handle.shutdown().await
        })
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to shutdown: {}", e)))
    }

    fn which_children(&self, _py: Python<'_>) -> PyResult<Vec<PyChildInfo>> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        let result = runtime.block_on(async move {
            handle.which_children().await
        });
        
        match result {
            Ok(children) => {
                // Convert distributed::ChildInfo to types::ChildInfo then to PyChildInfo
                let py_children = children
                    .into_iter()
                    .map(|child| PyChildInfo {
                        id: child.id,
                        child_type: PyChildType { inner: child.child_type },
                        restart_policy: child.restart_policy,
                    })
                    .collect();
                Ok(py_children)
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Failed to get children: {}", e)))
        }
    }

    fn terminate_child(&self, child_id: String, _py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        runtime.block_on(async move {
            handle.terminate_child(&child_id).await
        })
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to terminate child: {}", e)))
    }

    fn __repr__(&self) -> String {
        "RemoteSupervisorHandle()".to_string()
    }
}
