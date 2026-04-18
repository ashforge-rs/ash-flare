//! Basic Python bindings for restart and child types

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::restart::{RestartIntensity, RestartPolicy, RestartStrategy};
use crate::types::{ChildExitReason, ChildInfo, ChildType};

/// Python-facing restart policy enum
#[pyclass(name = "RestartPolicy", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyRestartPolicy {
    pub(crate) inner: RestartPolicy,
}

#[pymethods]
impl PyRestartPolicy {
    #[new]
    fn new(policy: &str) -> PyResult<Self> {
        let inner = match policy.to_lowercase().as_str() {
            "permanent" => RestartPolicy::Permanent,
            "temporary" => RestartPolicy::Temporary,
            "transient" => RestartPolicy::Transient,
            _ => {
                return Err(PyValueError::new_err(
                    "Invalid restart policy. Use 'permanent', 'temporary', or 'transient'",
                ));
            }
        };
        Ok(PyRestartPolicy { inner })
    }

    #[staticmethod]
    fn permanent() -> Self {
        PyRestartPolicy {
            inner: RestartPolicy::Permanent,
        }
    }

    #[staticmethod]
    fn temporary() -> Self {
        PyRestartPolicy {
            inner: RestartPolicy::Temporary,
        }
    }

    #[staticmethod]
    fn transient() -> Self {
        PyRestartPolicy {
            inner: RestartPolicy::Transient,
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        match self.inner {
            RestartPolicy::Permanent => "RestartPolicy.permanent()".to_owned(),
            RestartPolicy::Temporary => "RestartPolicy.temporary()".to_owned(),
            RestartPolicy::Transient => "RestartPolicy.transient()".to_owned(),
        }
    }
}

/// Python-facing restart strategy enum
#[pyclass(name = "RestartStrategy", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyRestartStrategy {
    pub(crate) inner: RestartStrategy,
}

#[pymethods]
impl PyRestartStrategy {
    #[new]
    fn new(strategy: &str) -> PyResult<Self> {
        let inner = match strategy.to_lowercase().as_str() {
            "oneforone" | "one_for_one" => RestartStrategy::OneForOne,
            "oneforall" | "one_for_all" => RestartStrategy::OneForAll,
            "restforone" | "rest_for_one" => RestartStrategy::RestForOne,
            _ => {
                return Err(PyValueError::new_err(
                    "Invalid restart strategy. Use 'one_for_one', 'one_for_all', or 'rest_for_one'",
                ));
            }
        };
        Ok(PyRestartStrategy { inner })
    }

    #[staticmethod]
    fn one_for_one() -> Self {
        PyRestartStrategy {
            inner: RestartStrategy::OneForOne,
        }
    }

    #[staticmethod]
    fn one_for_all() -> Self {
        PyRestartStrategy {
            inner: RestartStrategy::OneForAll,
        }
    }

    #[staticmethod]
    fn rest_for_one() -> Self {
        PyRestartStrategy {
            inner: RestartStrategy::RestForOne,
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        match self.inner {
            RestartStrategy::OneForOne => "RestartStrategy.one_for_one()".to_owned(),
            RestartStrategy::OneForAll => "RestartStrategy.one_for_all()".to_owned(),
            RestartStrategy::RestForOne => "RestartStrategy.rest_for_one()".to_owned(),
        }
    }
}

/// Python-facing restart intensity
#[pyclass(name = "RestartIntensity", skip_from_py_object)]
#[derive(Clone)]
pub struct PyRestartIntensity {
    pub(crate) inner: RestartIntensity,
}

#[pymethods]
impl PyRestartIntensity {
    #[new]
    fn new(max_restarts: u32, time_window_secs: u64) -> Self {
        let inner = RestartIntensity {
            max_restarts: usize::try_from(max_restarts).unwrap_or(usize::MAX),
            within_seconds: time_window_secs,
        };
        PyRestartIntensity { inner }
    }

    #[getter]
    fn max_restarts(&self) -> u32 {
        u32::try_from(self.inner.max_restarts).unwrap_or(u32::MAX)
    }

    #[getter]
    fn time_window_secs(&self) -> u64 {
        self.inner.within_seconds
    }

    fn __repr__(&self) -> String {
        format!(
            "RestartIntensity(max_restarts={}, time_window_secs={})",
            self.inner.max_restarts, self.inner.within_seconds
        )
    }
}

/// Python-facing child type
#[pyclass(name = "ChildType", skip_from_py_object)]
#[derive(Clone)]
pub struct PyChildType {
    pub(crate) inner: ChildType,
}

#[pymethods]
impl PyChildType {
    fn __repr__(&self) -> String {
        match self.inner {
            ChildType::Worker => "ChildType.Worker".to_owned(),
            ChildType::Supervisor => "ChildType.Supervisor".to_owned(),
        }
    }

    fn is_worker(&self) -> bool {
        matches!(self.inner, ChildType::Worker)
    }

    fn is_supervisor(&self) -> bool {
        matches!(self.inner, ChildType::Supervisor)
    }
}

/// Python-facing child exit reason
#[pyclass(name = "ChildExitReason", skip_from_py_object)]
#[derive(Clone)]
pub struct PyChildExitReason {
    #[allow(dead_code)]
    pub(crate) inner: ChildExitReason,
}

#[pymethods]
impl PyChildExitReason {
    fn __repr__(&self) -> String {
        match self.inner {
            ChildExitReason::Normal => "ChildExitReason.Normal".to_owned(),
            ChildExitReason::Abnormal => "ChildExitReason.Abnormal".to_owned(),
            ChildExitReason::Shutdown => "ChildExitReason.Shutdown".to_owned(),
        }
    }

    fn is_normal(&self) -> bool {
        matches!(self.inner, ChildExitReason::Normal)
    }

    fn is_abnormal(&self) -> bool {
        matches!(self.inner, ChildExitReason::Abnormal)
    }

    fn is_shutdown(&self) -> bool {
        matches!(self.inner, ChildExitReason::Shutdown)
    }
}

/// Python-facing child info
#[pyclass(name = "ChildInfo", skip_from_py_object)]
#[derive(Clone)]
pub struct PyChildInfo {
    #[pyo3(get)]
    pub(crate) id: String,
    #[pyo3(get)]
    pub(crate) child_type: PyChildType,
    pub(crate) restart_policy: Option<RestartPolicy>,
}

#[pymethods]
impl PyChildInfo {
    #[getter]
    fn restart_policy(&self) -> Option<PyRestartPolicy> {
        self.restart_policy.map(|inner| PyRestartPolicy { inner })
    }

    fn __repr__(&self) -> String {
        format!(
            "ChildInfo(id='{}', child_type={:?}, restart_policy={:?})",
            self.id, self.child_type.inner, self.restart_policy
        )
    }
}

impl From<ChildInfo> for PyChildInfo {
    fn from(info: ChildInfo) -> Self {
        PyChildInfo {
            id: info.id,
            child_type: PyChildType {
                inner: info.child_type,
            },
            restart_policy: info.restart_policy,
        }
    }
}
