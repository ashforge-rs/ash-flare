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

use super::types::{PyChildInfo, PyRestartIntensity, PyRestartPolicy, PyRestartStrategy};
use super::worker::PyWorker;
use super::{block_on_without_gil, ensure_callable, get_runtime};

/// Rejects a duplicate child id at registration time.
///
/// `start_child` already errors on duplicates; without this check a spec could
/// silently accumulate two children sharing an id, which then behave
/// unpredictably because lookups match whichever comes first.
fn ensure_unique_id(spec: &SupervisorSpec<PyWorker>, id: &str) -> PyResult<()> {
    if spec.has_child(id) {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "a child with id '{id}' is already registered on this spec"
        )));
    }
    Ok(())
}

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

    /// Sets a fixed delay, in seconds, between a child terminating and being
    /// restarted. Prevents a crash-looping child from saturating a CPU core.
    fn with_restart_delay(&mut self, seconds: f64) {
        let delay = std::time::Duration::try_from_secs_f64(seconds).unwrap_or_default();
        self.inner.restart_backoff = crate::RestartBackoff::exponential(delay, delay);
    }

    /// Sets an exponential restart backoff, in seconds, starting at
    /// `initial_seconds` and doubling up to `max_seconds`.
    fn with_restart_backoff(&mut self, initial_seconds: f64, max_seconds: f64) {
        let initial = std::time::Duration::try_from_secs_f64(initial_seconds).unwrap_or_default();
        let max = std::time::Duration::try_from_secs_f64(max_seconds).unwrap_or_default();
        self.inner.restart_backoff = crate::RestartBackoff::exponential(initial, max);
    }

    /// Sets how long a worker may take to stop before it is abandoned.
    fn with_shutdown_timeout(&mut self, seconds: f64) {
        let timeout = Duration::try_from_secs_f64(seconds).unwrap_or_default();
        self.inner = std::mem::replace(&mut self.inner, SupervisorSpec::new("temp"))
            .with_shutdown_timeout(timeout);
    }

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
        ensure_unique_id(&self.inner, &id)?;

        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        self.inner = std::mem::replace(&mut self.inner, SupervisorSpec::new("temp"))
            .with_worker_signal(
                id,
                move |signal| PyWorker::new(id_clone.clone(), Arc::clone(&worker_fn_arc), signal),
                policy,
            );
        Ok(())
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

    /// Supervisor uptime in seconds.
    fn uptime(&self, py: Python<'_>) -> PyResult<u64> {
        let handle = self.inner.clone();
        block_on_without_gil(py, async move { handle.uptime().await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get uptime: {e}")))
    }

    /// The supervisor's restart strategy.
    fn restart_strategy(&self, py: Python<'_>) -> PyResult<PyRestartStrategy> {
        let handle = self.inner.clone();
        block_on_without_gil(py, async move { handle.restart_strategy().await })
            .map(|inner| PyRestartStrategy { inner })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get restart strategy: {e}")))
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

    #[pyo3(signature = (id, restart_policy, worker_fn))]
    #[allow(clippy::needless_pass_by_value)]
    fn start_child(
        &self,
        py: Python<'_>,
        id: String,
        restart_policy: PyRestartPolicy,
        worker_fn: Py<PyAny>,
    ) -> PyResult<String> {
        ensure_callable(py, &worker_fn, &id)?;
        let handle = self.inner.clone();
        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        let result = block_on_without_gil(py, async move {
            handle
                .start_child_with_signal(
                    id,
                    move |signal| {
                        PyWorker::new(id_clone.clone(), Arc::clone(&worker_fn_arc), signal)
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
        ensure_callable(py, &worker_fn, &id)?;
        let handle = self.inner.clone();
        let policy = restart_policy.inner;
        let id_clone = id.clone();
        let timeout = Duration::from_secs(timeout_secs);
        let worker_fn_arc = Arc::new(worker_fn.clone_ref(py));

        let result = block_on_without_gil(py, async move {
            handle
                .start_child_linked(
                    id,
                    move || {
                        PyWorker::new(
                            id_clone.clone(),
                            Arc::clone(&worker_fn_arc),
                            crate::ShutdownSignal::never(),
                        )
                    },
                    policy,
                    timeout,
                )
                .await
        });

        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to start child: {e}")))
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
        format!("SupervisorHandle(name='{}')", self.inner.name())
    }
}
