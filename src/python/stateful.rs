//! Python bindings for stateful supervisor
//!
//! In Rust a "stateful" supervisor is just a regular one whose worker factories
//! capture a shared [`WorkerContext`](crate::WorkerContext). Python keeps the
//! dedicated class so callables are handed the context automatically.
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! children = await asyncio.to_thread(handle.which_children)
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use crate::supervisor::{ChildSpec, SupervisorHandle, SupervisorSpec};
use crate::types::{ChildType, ShutdownSignal, WorkerContext};

use super::context::PyWorkerContext;
use super::events::PyEventStream;
use super::options::{PyAutoShutdown, PyShutdown, PyShutdownOrder};
use super::supervisor::ensure_unique_id;
use super::types::{PyChildInfo, PyRestartIntensity, PyRestartPolicy, PyRestartStrategy};
use super::worker::{PyWorker, PyWorkerFn};
use super::{block_on_without_gil, duration_from_secs, ensure_callable, get_runtime};

/// Python-facing stateful supervisor specification
#[pyclass(name = "StatefulSupervisorSpec")]
pub struct PyStatefulSupervisorSpec {
    pub(crate) inner: SupervisorSpec,
    /// Shared by every worker registered on this spec.
    context: WorkerContext,
}

#[pymethods]
impl PyStatefulSupervisorSpec {
    #[new]
    fn new(name: String) -> Self {
        PyStatefulSupervisorSpec {
            inner: SupervisorSpec::new(name),
            context: WorkerContext::new(),
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
            inner: self.context.clone(),
        }
    }

    /// Sets how long a worker may take to stop before it is abandoned.
    fn with_shutdown_timeout(&mut self, seconds: f64) -> PyResult<()> {
        let timeout = duration_from_secs(seconds, "shutdown timeout")?;
        self.inner.shutdown_timeout = timeout;
        Ok(())
    }

    /// Registers a worker.
    ///
    /// The callable is invoked with the supervisor's shared `WorkerContext`, so
    /// `def worker(ctx):` is the expected shape. A zero-argument callable also
    /// works, and `def worker(ctx, should_stop):` additionally receives a handle
    /// for cooperative shutdown.
    #[pyo3(signature = (id, restart_policy, worker_fn, *, shutdown=None, significant=false))]
    #[allow(clippy::needless_pass_by_value)]
    fn add_worker(
        &mut self,
        py: Python<'_>,
        id: String,
        restart_policy: PyRestartPolicy,
        worker_fn: Py<PyAny>,
        shutdown: Option<PyShutdown>,
        significant: bool,
    ) -> PyResult<()> {
        ensure_callable(py, &worker_fn, &id)?;
        ensure_unique_id(&self.inner, &id)?;

        let id_clone = id.clone();
        let worker_fn_arc = Arc::new(PyWorkerFn::new(py, worker_fn.clone_ref(py)));
        let context = self.context.clone();

        let mut child = ChildSpec::new(id, move |signal: ShutdownSignal| {
            // Hand the worker the shared context, which is the entire point of
            // a stateful supervisor.
            PyWorker::with_context(
                id_clone.clone(),
                Arc::clone(&worker_fn_arc),
                PyWorkerContext {
                    inner: context.clone(),
                },
                signal,
            )
        })
        .restart(restart_policy.inner)
        .significant(significant);
        if let Some(mode) = shutdown {
            child = child.shutdown(mode.inner);
        }

        self.inner =
            std::mem::replace(&mut self.inner, SupervisorSpec::new("temp")).with_child(child);
        Ok(())
    }

    /// The order in which children stopped together are stopped.
    fn with_shutdown_order(&mut self, order: PyShutdownOrder) {
        self.inner.shutdown_order = order.inner;
    }

    /// Makes the supervisor stop itself when its significant children end.
    fn with_auto_shutdown(&mut self, auto_shutdown: PyAutoShutdown) {
        self.inner.auto_shutdown = auto_shutdown.inner;
    }

    /// How this supervisor is restarted when nested in another.
    fn with_restart_policy(&mut self, policy: PyRestartPolicy) {
        self.inner.restart_policy = policy.inner;
    }

    fn add_supervisor(&mut self, supervisor: &PyStatefulSupervisorSpec) {
        self.inner = std::mem::replace(&mut self.inner, SupervisorSpec::new("temp"))
            .with_supervisor(supervisor.inner.clone());
    }
}

/// Python-facing stateful supervisor handle
#[pyclass(name = "StatefulSupervisorHandle")]
pub struct PyStatefulSupervisorHandle {
    pub(crate) inner: SupervisorHandle,
}

#[pymethods]
impl PyStatefulSupervisorHandle {
    #[staticmethod]
    fn start(spec: &PyStatefulSupervisorSpec) -> PyResult<Self> {
        let runtime = get_runtime();
        let _guard = runtime.enter();
        SupervisorHandle::start(spec.inner.clone())
            .map(|inner| PyStatefulSupervisorHandle { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
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

    /// Kills a child as if it crashed, so its restart policy applies. Returns
    /// False if it had already exited.
    fn kill_child(&self, py: Python<'_>, child_id: String) -> PyResult<bool> {
        let handle = self.inner.clone();
        block_on_without_gil(py, async move { handle.kill_child(&child_id).await })
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to kill child: {e}")))
    }

    /// A stream of this supervisor's events.
    fn subscribe(&self) -> PyEventStream {
        PyEventStream::new(self.inner.subscribe())
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
