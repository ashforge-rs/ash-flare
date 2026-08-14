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
    /// The in-flight Python call. A `spawn_blocking` task cannot be cancelled,
    /// so this is kept across a cancellation of `run` and joined in `shutdown`.
    body: Option<tokio::task::JoinHandle<Result<(), WorkerError>>>,
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
            body: None,
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
            body: None,
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
        self.body = Some(tokio::task::spawn_blocking(move || {
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
        }));

        // The handle stays in `self` while the call is in flight. If this future
        // is dropped for a cooperative shutdown, `shutdown` joins it there
        // rather than leaving the Python callable running unsupervised.
        let Some(handle) = self.body.as_mut() else {
            return Err(WorkerError::WorkerFailed(
                "worker body handle went missing".to_owned(),
            ));
        };
        let result = handle.await;
        self.body = None;

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

    /// Waits for a Python callable that is still running after `run` was
    /// cancelled.
    ///
    /// `spawn_blocking` work cannot be interrupted, so simply returning here
    /// would report a graceful stop while the callable kept running - and the
    /// supervisor would start its replacement on top of it. Blocking instead
    /// puts the wait inside the supervisor's shutdown timeout: a worker that
    /// polls `should_stop` returns promptly, and one that ignores it is aborted
    /// and correctly reported as not having stopped gracefully.
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        let Some(handle) = self.body.take() else {
            return Ok(());
        };

        // Raise the flag here too: the watcher task does it as well, but this
        // does not depend on that task being scheduled first.
        self.cancelled.store(true, Ordering::SeqCst);

        let _joined = handle.await;
        Ok(())
    }
}

/// Calls a Python worker, adapting to the signature the user actually wrote.
///
/// Workers may accept nothing, just the context (stateful only), or the context
/// plus a `should_stop` handle. The signature is inspected up front and the
/// richest one it accepts is used, so simple `def worker():` callables keep
/// working while users can opt into cancellation without a separate
/// registration API - and, crucially, the worker body is only ever entered once.
fn call_with_optional_args(
    py: Python<'_>,
    callable: &Arc<Py<PyAny>>,
    context: Option<&PyWorkerContext>,
    should_stop: &PyShouldStop,
) -> PyResult<Py<PyAny>> {
    let stop_obj: Py<PyAny> = should_stop.clone().into_pyobject(py)?.into_any().unbind();

    // Richest first: everything the worker could want, down to nothing.
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

    if let Some(accepted) = positional_arity(py, callable) {
        for arity in candidates {
            if accepted.accepts(arity.len()) {
                return callable.call1(py, pyo3::types::PyTuple::new(py, arity)?);
            }
        }

        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "worker callable takes {} positional arguments; ash-flare calls it \
             with the shared context and a should_stop handle, or fewer",
            accepted.describe()
        )));
    }

    // No introspectable signature (some builtins and C callables). Fall back to
    // trying each shape, which is only safe because `is_arity_error` rejects
    // anything raised from inside the worker body.
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

/// How many positional arguments a callable accepts.
struct Arity {
    required: usize,
    /// `None` when the callable takes `*args` and so accepts any count.
    limit: Option<usize>,
}

impl Arity {
    fn accepts(&self, count: usize) -> bool {
        count >= self.required && self.limit.is_none_or(|limit| count <= limit)
    }

    fn describe(&self) -> String {
        match self.limit {
            Some(limit) if limit == self.required => format!("exactly {limit}"),
            Some(limit) => format!("{} to {limit}", self.required),
            None => format!("at least {}", self.required),
        }
    }
}

/// Reads a callable's positional arity via `inspect.signature`.
///
/// Returns `None` when the signature cannot be determined, which is the case for
/// some builtins and C extension callables.
fn positional_arity(py: Python<'_>, callable: &Arc<Py<PyAny>>) -> Option<Arity> {
    let inspect = py.import("inspect").ok()?;
    let signature = inspect
        .call_method1("signature", (callable.bind(py),))
        .ok()?;
    let parameter = inspect.getattr("Parameter").ok()?;
    let empty = parameter.getattr("empty").ok()?;
    let var_positional = parameter.getattr("VAR_POSITIONAL").ok()?;
    let positional_only = parameter.getattr("POSITIONAL_ONLY").ok()?;
    let positional_or_keyword = parameter.getattr("POSITIONAL_OR_KEYWORD").ok()?;

    let params = signature
        .getattr("parameters")
        .ok()?
        .call_method0("values")
        .ok()?;

    let mut required = 0_usize;
    let mut limit = Some(0_usize);

    for param in params.try_iter().ok()? {
        let param = param.ok()?;
        let kind = param.getattr("kind").ok()?;

        if kind.eq(&var_positional).ok()? {
            limit = None;
            continue;
        }
        if !(kind.eq(&positional_only).ok()? || kind.eq(&positional_or_keyword).ok()?) {
            // Keyword-only and **kwargs parameters take no positional argument.
            continue;
        }

        limit = limit.map(|max| max.saturating_add(1));
        if param.getattr("default").ok()?.is(&empty) {
            required = required.saturating_add(1);
        }
    }

    Some(Arity { required, limit })
}

/// Distinguishes "wrong number of arguments" from a genuine failure inside the
/// worker, so a `TypeError` raised by the worker body is never mistaken for a
/// signature mismatch and silently retried.
fn is_arity_error(py: Python<'_>, err: &PyErr) -> bool {
    if !err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) {
        return false;
    }

    // A signature mismatch is raised by the call machinery before the worker
    // body is entered, so no Python frame ever ran and the traceback is empty.
    // A `TypeError` from inside the body always carries at least the worker's
    // own frame - retrying that would run its side effects a second time.
    if err.traceback(py).is_some() {
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
