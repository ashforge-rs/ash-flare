//! Python worker implementation

use super::context::PyWorkerContext;
use crate::types::ShutdownSignal;
use crate::worker::{Worker, WorkerError};
use pyo3::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Extra arguments passed to a Python worker callable when it is invoked.
#[derive(Clone)]
pub(crate) enum PyWorkerArgs {
    /// Called with no arguments.
    None,
    /// Called with the supervisor's shared `WorkerContext`.
    Context(PyWorkerContext),
}

/// Python-compatible wrapper for Worker that accepts Python callables
#[derive(Clone)]
pub struct PyWorker {
    pub(crate) name: String,
    pub(crate) callable: Arc<Py<PyAny>>,
    /// What to pass to the callable. Stateful supervisors pass their context.
    pub(crate) args: PyWorkerArgs,
    /// Set when the supervisor asks this worker to stop. Python workers can
    /// poll it via the `should_stop` argument they are handed.
    pub(crate) cancelled: Arc<AtomicBool>,
    /// Cooperative shutdown signal from the supervisor.
    pub(crate) shutdown: ShutdownSignal,
}

impl PyWorker {
    /// Builds a worker that calls `callable` with no extra arguments.
    pub(crate) fn new(name: String, callable: Arc<Py<PyAny>>, shutdown: ShutdownSignal) -> Self {
        Self {
            name,
            callable,
            args: PyWorkerArgs::None,
            cancelled: Arc::new(AtomicBool::new(false)),
            shutdown,
        }
    }

    /// Builds a worker that calls `callable` with the shared `WorkerContext`.
    pub(crate) fn with_context(
        name: String,
        callable: Arc<Py<PyAny>>,
        context: PyWorkerContext,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self {
            name,
            callable,
            args: PyWorkerArgs::Context(context),
            cancelled: Arc::new(AtomicBool::new(false)),
            shutdown,
        }
    }
}

/// Handed to Python workers so they can cooperatively stop.
///
/// Call it (or `.is_set()`) inside a loop and return when it reports `True`.
#[pyclass(name = "ShouldStop", skip_from_py_object)]
#[derive(Clone)]
pub struct PyShouldStop {
    flag: Arc<AtomicBool>,
}

#[pymethods]
impl PyShouldStop {
    /// Returns `True` once the supervisor has asked this worker to stop.
    fn is_set(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Alias for `is_set()`, so the object can be used as `if should_stop():`.
    fn __call__(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn __bool__(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    fn __repr__(&self) -> String {
        format!("ShouldStop(set={})", self.flag.load(Ordering::SeqCst))
    }
}

#[async_trait::async_trait]
impl Worker for PyWorker {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let callable = Arc::clone(&self.callable);
        let name = self.name.clone();
        let args = self.args.clone();
        let cancelled = Arc::clone(&self.cancelled);

        // Watch for cooperative shutdown and raise the flag the Python worker
        // polls. The worker body runs on a blocking thread that cannot be
        // preempted, so a flag is the only way to ask it to stop.
        let signal = self.shutdown.clone();
        let flag = Arc::clone(&self.cancelled);
        let watcher = tokio::spawn(async move {
            signal.cancelled().await;
            flag.store(true, Ordering::SeqCst);
        });

        // Run the Python callable on a blocking-pool thread. `spawn_blocking`
        // is used on its own here: spawning a bare OS thread as well would
        // double the per-worker thread cost for no benefit.
        let result = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let should_stop = PyShouldStop { flag: cancelled };
                let outcome = match &args {
                    PyWorkerArgs::None => {
                        call_with_optional_args(py, &callable, None, &should_stop)
                    }
                    PyWorkerArgs::Context(ctx) => {
                        call_with_optional_args(py, &callable, Some(ctx), &should_stop)
                    }
                };

                outcome.map(|_ok| ()).map_err(|e| {
                    // Surface the Python traceback rather than discarding it.
                    let detail = format_py_error(py, &e);
                    // Report through Python's own logging so the failure is
                    // visible by default instead of vanishing silently.
                    log_worker_failure(py, &name, &detail);
                    WorkerError::WorkerFailed(format!("worker '{name}' raised: {detail}"))
                })
            })
        })
        .await;

        watcher.abort();

        match result {
            Ok(inner) => inner,
            Err(join_err) => Err(WorkerError::WorkerFailed(format!(
                "worker task failed to run: {join_err}"
            ))),
        }
    }

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Calls a Python worker, adapting to the signature the user actually wrote.
///
/// Workers may accept nothing, just the context (stateful only), or the context
/// plus a `should_stop` handle. Trying the richest signature first and falling
/// back on `TypeError` keeps simple `def worker():` callables working while
/// letting users opt into cancellation without a separate registration API.
fn call_with_optional_args(
    py: Python<'_>,
    callable: &Arc<Py<PyAny>>,
    context: Option<&PyWorkerContext>,
    should_stop: &PyShouldStop,
) -> PyResult<Py<PyAny>> {
    let stop_obj: Py<PyAny> = should_stop.clone().into_pyobject(py)?.into_any().unbind();

    let candidates: Vec<Vec<Py<PyAny>>> = match context {
        Some(ctx) => {
            let ctx_obj: Py<PyAny> = ctx.clone().into_pyobject(py)?.into_any().unbind();
            vec![
                vec![ctx_obj.clone_ref(py), stop_obj.clone_ref(py)],
                vec![ctx_obj],
                vec![],
            ]
        }
        None => vec![vec![stop_obj.clone_ref(py)], vec![]],
    };

    let mut last_err = None;
    for arity in candidates {
        let args = pyo3::types::PyTuple::new(py, arity)?;
        match callable.call1(py, args) {
            Ok(value) => return Ok(value),
            Err(err) if is_arity_error(py, &err) => {
                last_err = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        pyo3::exceptions::PyTypeError::new_err("worker callable rejected every supported signature")
    }))
}

/// Distinguishes "wrong number of arguments" from a genuine failure inside the
/// worker, so a `TypeError` raised by the worker body is never mistaken for a
/// signature mismatch and silently retried.
fn is_arity_error(py: Python<'_>, err: &PyErr) -> bool {
    if !err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) {
        return false;
    }
    let message = err.value(py).to_string();
    message.contains("positional argument")
        || message.contains("argument")
            && (message.contains("takes") || message.contains("missing"))
}

/// Reports a worker failure through Python's `logging` module.
///
/// A supervised worker's exception is otherwise swallowed: the supervisor
/// restarts or drops the child and the traceback is lost, which makes a
/// misbehaving worker very hard to debug. Routing it to `logging` means it is
/// visible under the user's existing configuration.
fn log_worker_failure(py: Python<'_>, worker: &str, detail: &str) {
    let logged = (|| -> PyResult<()> {
        let logging = py.import("logging")?;
        let logger = logging.call_method1("getLogger", ("ash_flare",))?;
        logger.call_method1("error", (format!("worker '{worker}' raised: {detail}"),))?;
        Ok(())
    })();

    if logged.is_err() {
        // Logging must never mask the original failure.
        tracing::error!(worker = %worker, detail = %detail, "python worker failed");
    }
}

/// Renders a Python exception with its traceback, if one is available.
fn format_py_error(py: Python<'_>, err: &PyErr) -> String {
    let value = err.value(py).to_string();
    let kind = err
        .get_type(py)
        .name()
        .map_or_else(|_e| "Exception".to_owned(), |n| n.to_string());

    err.traceback(py)
        .and_then(|tb| tb.format().ok())
        .map_or_else(
            || format!("{kind}: {value}"),
            |tb| format!("{kind}: {value}\n{tb}"),
        )
}
