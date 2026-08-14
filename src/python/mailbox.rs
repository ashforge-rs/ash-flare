//! Python bindings for mailbox system
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! await asyncio.to_thread(handle.send, "message")
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::mailbox::{Mailbox, MailboxConfig, MailboxHandle};
use std::sync::Arc;
use tokio::sync::Mutex;

use super::block_on_without_gil;

/// Python-facing mailbox configuration
#[pyclass(name = "MailboxConfig", skip_from_py_object)]
#[derive(Clone)]
pub struct PyMailboxConfig {
    pub(crate) inner: MailboxConfig,
}

#[pymethods]
impl PyMailboxConfig {
    #[staticmethod]
    fn unbounded() -> Self {
        PyMailboxConfig {
            inner: MailboxConfig::unbounded(),
        }
    }

    #[staticmethod]
    fn bounded(capacity: usize) -> Self {
        PyMailboxConfig {
            inner: MailboxConfig::bounded(capacity),
        }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            MailboxConfig::Unbounded => "MailboxConfig.unbounded()".to_owned(),
            MailboxConfig::Bounded { capacity } => format!("MailboxConfig.bounded({capacity})"),
        }
    }
}

/// Python-facing mailbox handle
#[pyclass(name = "MailboxHandle")]
pub struct PyMailboxHandle {
    inner: MailboxHandle,
}

#[pymethods]
impl PyMailboxHandle {
    /// Sends a message, waiting for capacity on a bounded mailbox.
    ///
    /// Releases the GIL while waiting so a full mailbox cannot stall the
    /// interpreter.
    fn send(&self, message: String, py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        block_on_without_gil(py, async move {
            handle
                .send(message)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to send: {e}")))
        })
    }

    fn try_send(&self, message: String) -> PyResult<()> {
        self.inner
            .try_send(message)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to send: {e}")))
    }

    fn worker_id(&self) -> &str {
        self.inner.worker_id()
    }

    fn is_open(&self) -> bool {
        self.inner.is_open()
    }

    fn __repr__(&self) -> String {
        format!("MailboxHandle(worker_id='{}')", self.inner.worker_id())
    }
}

/// Python-facing mailbox
///
/// The receiver is held behind a mutex rather than through `&mut self` so that
/// [`PyMailbox::recv`] can block without pyo3 holding a borrow guard on the
/// object for the whole wait. Combined with releasing the GIL, that is what
/// lets another Python thread send the message this call is waiting for.
#[pyclass(name = "Mailbox")]
pub struct PyMailbox {
    inner: Arc<Mutex<Mailbox>>,
}

#[pymethods]
impl PyMailbox {
    /// Blocks until a message arrives, returning `None` once every sender is
    /// dropped.
    ///
    /// Releases the GIL while waiting, so other Python threads keep running.
    fn recv(&self, py: Python<'_>) -> Option<String> {
        let inner = Arc::clone(&self.inner);
        block_on_without_gil(py, async move {
            let mut guard = inner.lock().await;
            guard.recv().await
        })
    }

    /// Returns a message if one is already queued, raising otherwise.
    fn try_recv(&self, py: Python<'_>) -> PyResult<String> {
        let inner = Arc::clone(&self.inner);
        let result = block_on_without_gil(py, async move {
            let mut guard = inner.lock().await;
            guard.try_recv()
        });
        result.map_err(|e| PyRuntimeError::new_err(format!("Failed to receive: {e}")))
    }

    #[allow(clippy::unused_self)]
    fn __repr__(&self) -> String {
        "Mailbox()".to_owned()
    }
}

#[pyfunction]
pub fn mailbox(config: &PyMailboxConfig) -> (PyMailboxHandle, PyMailbox) {
    let (handle, mailbox) = crate::mailbox::mailbox(config.inner);
    (
        PyMailboxHandle { inner: handle },
        PyMailbox {
            inner: Arc::new(Mutex::new(mailbox)),
        },
    )
}

#[pyfunction]
pub fn mailbox_named(worker_id: String, config: &PyMailboxConfig) -> (PyMailboxHandle, PyMailbox) {
    let (handle, mailbox) = crate::mailbox::mailbox_named(config.inner, worker_id);
    (
        PyMailboxHandle { inner: handle },
        PyMailbox {
            inner: Arc::new(Mutex::new(mailbox)),
        },
    )
}
