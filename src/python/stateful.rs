//! Python bindings for stateful supervisor
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! children = await asyncio.to_thread(handle.which_children)
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::supervisor_stateful::{StatefulSupervisorHandle, StatefulSupervisorSpec};
use crate::types::ChildType;

use super::context::PyWorkerContext;
use super::types::{PyChildInfo, PyRestartIntensity, PyRestartPolicy, PyRestartStrategy};
use super::worker::PyWorker;
use super::{block_on_without_gil, duration_from_secs, ensure_callable, get_runtime};

/// Python-facing stateful supervisor specification
#[pyclass(name = "StatefulSupervisorSpec")]
pub struct PyStatefulSupervisorSpec {
    pub(crate) inner: StatefulSupervisorSpec<PyWorker>,
}

#[pymethods]
impl PyStatefulSupervisorSpec {
    #[new]
    fn new(name: String) -> Self {
        PyStatefulSupervisorSpec {
            inner: StatefulSupervisorSpec::new(name),
        }
    }

    fn with_restart_strategy(&mut self, strategy: PyRestartStrategy) {
        self.inner.restart_strategy = strategy.inner;
    }

    fn with_restart_intensity(&mut self, intensity: &PyRestartIntensity) {
        self.inner.restart_intensity = intensity.inner;
    }

    fn context(&self) -> PyWorkerContext {
        PyWorkerContext {
            inner: (**self.inner.context()).clone(),
        }
    }

    /// Sets how long a worker may take to stop before it is abandoned.
    fn with_shutdown_timeout(&mut self, seconds: f64) -> PyResult<()> {
        let timeout = duration_from_secs(seconds, "shutdown timeout")?;
        self.inner = std::mem::replace(&mut self.inner, StatefulSupervisorSpec::new("temp"))
            .with_shutdown_timeout(timeout);
        Ok(())
    }

    /// Registers a worker.
    ///
    /// The callable is invoked with the supervisor's shared `WorkerContext`, so
    /// `def worker(ctx):` is the expected shape. A zero-argument callable also
    /// works, and `def worker(ctx, should_stop):` additionally receives a handle
    /// for cooperative shutdown.
    #[pyo3(signature = (id, restart_policy, worker_fn))]
    #[allow(clippy::needless_pass_by_value)]
    fn add_worker(
        &mut self,
        py: Python<'_>,
        id: String,
        restart_policy: PyRestartPolicy,
        worker_fn: Py<PyAny>,
    ) -> PyResult<()> {
        ensure_callable(py, &worker_fn, &id)?;
        if self.inner.has_child(&id) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "a child with id '{id}' is already registered on this spec"
            )));
        }

        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        self.inner = std::mem::replace(&mut self.inner, StatefulSupervisorSpec::new("temp"))
            .with_worker_signal(
                id,
                move |ctx, signal| {
                    // Hand the worker the shared context, which is the entire
                    // point of a stateful supervisor.
                    PyWorker::with_context(
                        id_clone.clone(),
                        Arc::clone(&worker_fn_arc),
                        PyWorkerContext {
                            inner: (*ctx).clone(),
                        },
                        signal,
                    )
                },
                policy,
            );
        Ok(())
    }

    fn add_supervisor(&mut self, supervisor: &PyStatefulSupervisorSpec) {
        self.inner = std::mem::replace(&mut self.inner, StatefulSupervisorSpec::new("temp"))
            .with_supervisor(supervisor.inner.clone());
    }
}

/// Python-facing stateful supervisor handle
#[pyclass(name = "StatefulSupervisorHandle")]
pub struct PyStatefulSupervisorHandle {
    inner: StatefulSupervisorHandle<PyWorker>,
}

#[pymethods]
impl PyStatefulSupervisorHandle {
    #[staticmethod]
    fn start(spec: &PyStatefulSupervisorSpec) -> Self {
        let runtime = get_runtime();
        let _guard = runtime.enter();
        PyStatefulSupervisorHandle {
            inner: StatefulSupervisorHandle::start(spec.inner.clone()),
        }
    }

    fn name(&self) -> String {
        self.inner.name().to_owned()
    }

    fn which_children(&self, py: Python<'_>) -> PyResult<Vec<PyChildInfo>> {
        let handle = self.inner.clone();
        let result = block_on_without_gil(py, async move { handle.which_children().await });

        match result {
            Ok(children) => Ok(children.into_iter().map(PyChildInfo::from).collect()),
            Err(e) => Err(PyRuntimeError::new_err(format!(
                "Failed to get children: {e}"
            ))),
        }
    }

    fn count_children(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let handle = self.inner.clone();
        let result = block_on_without_gil(py, async move { handle.which_children().await });

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

    fn terminate_child(&self, child_id: String, py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let result =
            block_on_without_gil(py, async move { handle.terminate_child(&child_id).await });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to terminate child: {e}")))
    }

    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let result = block_on_without_gil(py, async move { handle.shutdown().await });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to shutdown: {e}")))
    }

    fn __repr__(&self) -> String {
        format!("StatefulSupervisorHandle(name='{}')", self.inner.name())
    }
}
