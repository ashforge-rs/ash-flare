//! Python bindings for regular supervisor
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! children = await asyncio.to_thread(handle.which_children)
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;
use std::time::Duration;

use crate::supervisor::{SupervisorHandle, SupervisorSpec};
use crate::types::ChildType;

use super::get_runtime;
use super::types::{PyChildInfo, PyRestartIntensity, PyRestartPolicy, PyRestartStrategy};
use super::worker::PyWorker;

/// Python-facing supervisor specification
#[pyclass(name = "SupervisorSpec")]
pub struct PySupervisorSpec {
    pub(crate) inner: SupervisorSpec<PyWorker>,
}

#[pymethods]
impl PySupervisorSpec {
    #[new]
    fn new(name: String) -> Self {
        PySupervisorSpec {
            inner: SupervisorSpec::new(name),
        }
    }

    fn with_restart_strategy(&mut self, strategy: PyRestartStrategy) {
        self.inner.restart_strategy = strategy.inner;
    }

    fn with_restart_intensity(&mut self, intensity: &PyRestartIntensity) {
        self.inner.restart_intensity = intensity.inner;
    }

    #[pyo3(signature = (id, restart_policy, worker_fn))]
    #[allow(clippy::needless_pass_by_value)]
    fn add_worker(
        &mut self,
        py: Python<'_>,
        id: String,
        restart_policy: PyRestartPolicy,
        worker_fn: Py<PyAny>,
    ) {
        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        self.inner = std::mem::replace(&mut self.inner, SupervisorSpec::new("temp")).with_worker(
            id,
            move || PyWorker {
                name: id_clone.clone(),
                callable: worker_fn_arc.clone(),
            },
            policy,
        );
    }

    fn add_supervisor(&mut self, supervisor: &PySupervisorSpec) {
        self.inner = std::mem::replace(&mut self.inner, SupervisorSpec::new("temp"))
            .with_supervisor(supervisor.inner.clone());
    }
}

/// Python-facing supervisor handle
#[pyclass(name = "SupervisorHandle")]
pub struct PySupervisorHandle {
    inner: SupervisorHandle<PyWorker>,
}

#[pymethods]
impl PySupervisorHandle {
    #[staticmethod]
    fn start(spec: &PySupervisorSpec) -> Self {
        let runtime = get_runtime();
        let _guard = runtime.enter();
        PySupervisorHandle {
            inner: SupervisorHandle::start(spec.inner.clone()),
        }
    }

    fn name(&self) -> String {
        self.inner.name().to_owned()
    }

    fn which_children(&self, _py: Python<'_>) -> PyResult<Vec<PyChildInfo>> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        let result = runtime.block_on(async move { handle.which_children().await });

        match result {
            Ok(children) => Ok(children.into_iter().map(PyChildInfo::from).collect()),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to get children: {e}"
            ))),
        }
    }

    fn count_children(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        let result = runtime.block_on(async move { handle.which_children().await });

        match result {
            Ok(children) => {
                let supervisors = children
                    .iter()
                    .filter(|c| matches!(c.child_type, ChildType::Supervisor))
                    .count();
                let workers = children
                    .iter()
                    .filter(|c| matches!(c.child_type, ChildType::Worker))
                    .count();
                let dict = PyDict::new(py);
                dict.set_item("supervisors", supervisors)?;
                dict.set_item("workers", workers)?;
                Ok(dict.into())
            }
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to count children: {e}"
            ))),
        }
    }

    #[pyo3(signature = (id, restart_policy, worker_fn))]
    #[allow(clippy::needless_pass_by_value)]
    fn start_child(
        &self,
        py: Python<'_>,
        id: String,
        restart_policy: PyRestartPolicy,
        worker_fn: Py<PyAny>,
    ) -> PyResult<String> {
        let handle = self.inner.clone();
        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        let runtime = get_runtime();
        let result = runtime.block_on(async move {
            handle
                .start_child(
                    id,
                    move || PyWorker {
                        name: id_clone.clone(),
                        callable: worker_fn_arc.clone(),
                    },
                    policy,
                )
                .await
        });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to start child: {e}")))
    }

    #[pyo3(signature = (id, restart_policy, timeout_secs, worker_fn))]
    #[allow(clippy::needless_pass_by_value)]
    fn start_child_linked(
        &self,
        py: Python<'_>,
        id: String,
        restart_policy: PyRestartPolicy,
        timeout_secs: u64,
        worker_fn: Py<PyAny>,
    ) -> PyResult<String> {
        let handle = self.inner.clone();
        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let timeout = Duration::from_secs(timeout_secs);
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        let runtime = get_runtime();
        let result = runtime.block_on(async move {
            handle
                .start_child_linked(
                    id,
                    move || PyWorker {
                        name: id_clone.clone(),
                        callable: worker_fn_arc.clone(),
                    },
                    policy,
                    timeout,
                )
                .await
        });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to start child: {e}")))
    }

    fn terminate_child(&self, child_id: String, _py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        let result = runtime.block_on(async move { handle.terminate_child(&child_id).await });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to terminate child: {e}")))
    }

    fn shutdown(&self, _py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        let result = runtime.block_on(async move { handle.shutdown().await });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to shutdown: {e}")))
    }

    fn __repr__(&self) -> String {
        format!("SupervisorHandle(name='{}')", self.inner.name())
    }
}
