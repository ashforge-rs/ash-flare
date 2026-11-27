//! Python bindings for supervisor events and links.

use pyo3::prelude::*;
use std::sync::Arc;

use crate::events::{Monitor, SupervisorEvent};
use crate::supervisor::SupervisorHandle;
use crate::types::ChildExitReason;

use super::types::PyChildExitReason;
use super::{block_on_without_gil, duration_from_secs, get_runtime};

/// Something that happened in a supervisor.
///
/// `kind` is one of `"child_started"`, `"child_restarted"`,
/// `"child_terminated"`, `"child_stalled"`, `"restart_intensity_exceeded"` or
/// `"stopped"`.
#[pyclass(name = "SupervisorEvent", skip_from_py_object, frozen)]
pub struct PySupervisorEvent {
    #[pyo3(get)]
    kind: &'static str,
    #[pyo3(get)]
    child_id: Option<String>,
    exit_reason: Option<ChildExitReason>,
}

#[pymethods]
impl PySupervisorEvent {
    /// Why the child or supervisor stopped, for `child_terminated` and
    /// `stopped` events.
    #[getter]
    fn reason(&self) -> Option<PyChildExitReason> {
        self.exit_reason.map(|inner| PyChildExitReason { inner })
    }

    fn __repr__(&self) -> String {
        match (&self.child_id, self.exit_reason) {
            (Some(id), Some(reason)) => {
                format!(
                    "SupervisorEvent({}, child_id='{id}', reason={reason:?})",
                    self.kind
                )
            }
            (Some(id), None) => format!("SupervisorEvent({}, child_id='{id}')", self.kind),
            (None, Some(reason)) => format!("SupervisorEvent({}, reason={reason:?})", self.kind),
            (None, None) => format!("SupervisorEvent({})", self.kind),
        }
    }
}

impl From<SupervisorEvent> for PySupervisorEvent {
    fn from(event: SupervisorEvent) -> Self {
        let (kind, child_id, exit_reason) = match event {
            SupervisorEvent::ChildStarted { id } => ("child_started", Some(id), None),
            SupervisorEvent::ChildRestarted { id } => ("child_restarted", Some(id), None),
            SupervisorEvent::ChildTerminated { id, reason } => {
                ("child_terminated", Some(id), Some(reason))
            }
            SupervisorEvent::ChildStalled { id, .. } => ("child_stalled", Some(id), None),
            SupervisorEvent::RestartIntensityExceeded => ("restart_intensity_exceeded", None, None),
            SupervisorEvent::Stopped { reason } => ("stopped", None, Some(reason)),
        };
        Self {
            kind,
            child_id: child_id.map(|id| id.to_string()),
            exit_reason,
        }
    }
}

/// A supervisor's events, from `SupervisorHandle.subscribe()`.
///
/// Backed by a `Monitor`, so no event is ever missed: events queue until
/// read. Iterating blocks for each event (with the GIL released) and ends
/// after the supervisor's `stopped` event.
#[pyclass(name = "EventStream")]
pub struct PyEventStream {
    events: Arc<tokio::sync::Mutex<Monitor>>,
    finished: std::sync::atomic::AtomicBool,
}

impl PyEventStream {
    pub(crate) fn new(events: Monitor) -> Self {
        Self {
            events: Arc::new(tokio::sync::Mutex::new(events)),
            finished: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl PyEventStream {
    /// The next event, waiting up to `timeout` seconds (forever if `None`).
    /// Returns `None` on timeout.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<PySupervisorEvent>> {
        let limit = timeout
            .map(|seconds| duration_from_secs(seconds, "timeout"))
            .transpose()?;
        let stream = Arc::clone(&self.events);
        let next = block_on_without_gil(py, async move {
            let receive = async { stream.lock().await.recv().await };
            match limit {
                Some(wait) => tokio::time::timeout(wait, receive).await.ok().flatten(),
                None => receive.await,
            }
        });
        Ok(next.map(PySupervisorEvent::from))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<PySupervisorEvent>> {
        use std::sync::atomic::Ordering;
        if self.finished.load(Ordering::SeqCst) {
            return Ok(None);
        }
        let event = self.recv(py, None)?;
        if event.as_ref().is_none_or(|e| e.kind == "stopped") {
            self.finished.store(true, Ordering::SeqCst);
        }
        Ok(event)
    }
}

/// Keeps two children linked; see `link()`. Dropping it, or calling
/// `unlink()`, removes the link.
#[pyclass(name = "Link")]
pub struct PyLink {
    inner: Option<crate::link::Link>,
}

#[pymethods]
impl PyLink {
    /// Removes the link.
    fn unlink(&mut self) {
        self.inner = None;
    }

    fn is_active(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(crate::link::Link::is_active)
    }
}

/// Links two children, possibly of different supervisors: when either ends
/// for any reason other than a normal exit, the other is killed and
/// restarted by its own supervisor.
#[pyfunction]
pub fn link(
    a: &Bound<'_, PyAny>,
    a_child: &str,
    b: &Bound<'_, PyAny>,
    b_child: &str,
) -> PyResult<PyLink> {
    let (first, second) = (handle_of(a)?, handle_of(b)?);
    let _guard = get_runtime().enter();
    Ok(PyLink {
        inner: Some(crate::link::link(&first, a_child, &second, b_child)),
    })
}

/// The Rust handle behind a `SupervisorHandle` or `StatefulSupervisorHandle`.
fn handle_of(obj: &Bound<'_, PyAny>) -> PyResult<SupervisorHandle> {
    if let Ok(handle) = obj.cast::<super::PySupervisorHandle>() {
        return Ok(handle.borrow().inner.clone());
    }
    if let Ok(handle) = obj.cast::<super::PyStatefulSupervisorHandle>() {
        return Ok(handle.borrow().inner.clone());
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected a SupervisorHandle or StatefulSupervisorHandle",
    ))
}
