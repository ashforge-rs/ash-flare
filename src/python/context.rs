//! Python bindings for WorkerContext

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::types::WorkerContext;

/// Python-facing WorkerContext for stateful workers
#[pyclass(name = "WorkerContext", skip_from_py_object)]
#[derive(Clone)]
pub struct PyWorkerContext {
    pub(crate) inner: WorkerContext,
}

#[pymethods]
impl PyWorkerContext {
    #[new]
    fn new() -> Self {
        PyWorkerContext {
            inner: WorkerContext::new(),
        }
    }

    fn get(&self, key: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.inner.get(key) {
            Some(value) => {
                // Convert serde_json::Value to Python object
                pythonize::pythonize(py, &value)
                    .map(pyo3::Bound::unbind)
                    .map_err(|e| PyValueError::new_err(format!("Failed to convert value: {}", e)))
            }
            None => Ok(py.None()),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn set(&self, key: &str, value: Py<PyAny>, py: Python<'_>) -> PyResult<()> {
        // Convert Python object to serde_json::Value
        let json_value: serde_json::Value = pythonize::depythonize(value.bind(py))
            .map_err(|e| PyValueError::new_err(format!("Failed to convert value: {}", e)))?;

        self.inner.set(key, json_value);
        Ok(())
    }

    fn delete(&self, key: &str, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.inner.delete(key) {
            Some(value) => pythonize::pythonize(py, &value)
                .map(pyo3::Bound::unbind)
                .map_err(|e| PyValueError::new_err(format!("Failed to convert value: {}", e))),
            None => Ok(py.None()),
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn __repr__(&self) -> String {
        format!("WorkerContext(len={})", self.inner.len())
    }
}
