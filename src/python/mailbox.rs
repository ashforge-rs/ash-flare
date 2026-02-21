//! Python bindings for mailbox system
//!
//! For async Python code, wrap blocking methods with `asyncio.to_thread()`:
//! ```python
//! await asyncio.to_thread(handle.send, "message")
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::mailbox::{Mailbox, MailboxConfig, MailboxHandle};

use super::get_runtime;

/// Python-facing mailbox configuration
#[pyclass(name = "MailboxConfig")]
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
            MailboxConfig::Unbounded => "MailboxConfig.unbounded()".to_string(),
            MailboxConfig::Bounded { capacity } => format!("MailboxConfig.bounded({})", capacity),
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
    fn send(&self, message: String, _py: Python<'_>) -> PyResult<()> {
        let handle = self.inner.clone();
        let runtime = get_runtime();
        runtime.block_on(async move {
            handle
                .send(message)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to send: {}", e)))
        })
    }

    fn try_send(&self, message: String) -> PyResult<()> {
        self.inner
            .try_send(message)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to send: {}", e)))
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
#[pyclass(name = "Mailbox")]
pub struct PyMailbox {
    inner: Mailbox,
}

#[pymethods]
impl PyMailbox {
    fn recv(&mut self, _py: Python<'_>) -> PyResult<Option<String>> {
        let runtime = get_runtime();
        Ok(runtime.block_on(async move { self.inner.recv().await }))
    }

    fn try_recv(&mut self) -> PyResult<String> {
        self.inner
            .try_recv()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to receive: {}", e)))
    }

    #[allow(clippy::unused_self)]
    fn __repr__(&self) -> String {
        "Mailbox()".to_string()
    }
}

#[pyfunction]
pub fn mailbox(config: &PyMailboxConfig) -> (PyMailboxHandle, PyMailbox) {
    let (handle, mailbox) = crate::mailbox::mailbox(config.inner);
    (
        PyMailboxHandle { inner: handle },
        PyMailbox { inner: mailbox },
    )
}

#[pyfunction]
pub fn mailbox_named(worker_id: String, config: &PyMailboxConfig) -> (PyMailboxHandle, PyMailbox) {
    let (handle, mailbox) = crate::mailbox::mailbox_named(config.inner, worker_id);
    (
        PyMailboxHandle { inner: handle },
        PyMailbox { inner: mailbox },
    )
}
