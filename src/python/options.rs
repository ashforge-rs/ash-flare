//! Python bindings for child and supervisor options.

use pyo3::prelude::*;

use crate::supervisor::{AutoShutdown, GiveUp, Shutdown, ShutdownOrder};

use super::duration_from_secs;

/// How a child is stopped when its supervisor terminates it.
#[pyclass(name = "Shutdown", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyShutdown {
    pub(crate) inner: Shutdown,
}

#[pymethods]
impl PyShutdown {
    /// Abandon the worker at once, without waiting for it.
    #[staticmethod]
    fn brutal_kill() -> Self {
        Self {
            inner: Shutdown::BrutalKill,
        }
    }

    /// Ask the worker to stop and wait up to `seconds` for it.
    #[staticmethod]
    fn timeout(seconds: f64) -> PyResult<Self> {
        Ok(Self {
            inner: Shutdown::Timeout(duration_from_secs(seconds, "shutdown timeout")?),
        })
    }

    /// Ask the worker to stop and wait for it however long it takes.
    #[staticmethod]
    fn infinity() -> Self {
        Self {
            inner: Shutdown::Infinity,
        }
    }

    fn __repr__(&self) -> String {
        match self.inner {
            Shutdown::BrutalKill => "Shutdown.brutal_kill()".to_owned(),
            Shutdown::Timeout(t) => format!("Shutdown.timeout({})", t.as_secs_f64()),
            Shutdown::Infinity => "Shutdown.infinity()".to_owned(),
        }
    }
}

/// The order in which children stopped together are stopped.
#[pyclass(name = "ShutdownOrder", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyShutdownOrder {
    pub(crate) inner: ShutdownOrder,
}

#[pymethods]
impl PyShutdownOrder {
    /// One at a time, last started first (the default).
    #[staticmethod]
    fn reverse() -> Self {
        Self {
            inner: ShutdownOrder::Reverse,
        }
    }

    /// All at once.
    #[staticmethod]
    fn concurrent() -> Self {
        Self {
            inner: ShutdownOrder::Concurrent,
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        match self.inner {
            ShutdownOrder::Reverse => "ShutdownOrder.reverse()".to_owned(),
            ShutdownOrder::Concurrent => "ShutdownOrder.concurrent()".to_owned(),
        }
    }
}

/// When a supervisor stops itself because its significant children ended.
#[pyclass(name = "AutoShutdown", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyAutoShutdown {
    pub(crate) inner: AutoShutdown,
}

#[pymethods]
impl PyAutoShutdown {
    /// Never (the default).
    #[staticmethod]
    fn never() -> Self {
        Self {
            inner: AutoShutdown::Never,
        }
    }

    /// As soon as any significant child has ended.
    #[staticmethod]
    fn any_significant() -> Self {
        Self {
            inner: AutoShutdown::AnySignificant,
        }
    }

    /// Once every significant child has ended.
    #[staticmethod]
    fn all_significant() -> Self {
        Self {
            inner: AutoShutdown::AllSignificant,
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        match self.inner {
            AutoShutdown::Never => "AutoShutdown.never()".to_owned(),
            AutoShutdown::AnySignificant => "AutoShutdown.any_significant()".to_owned(),
            AutoShutdown::AllSignificant => "AutoShutdown.all_significant()".to_owned(),
        }
    }
}

/// What a top-level supervisor does when it gives up.
#[pyclass(name = "GiveUp", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyGiveUp {
    pub(crate) inner: GiveUp,
}

#[pymethods]
impl PyGiveUp {
    /// Stop; `SupervisorHandle.wait()` reports it (the default).
    #[staticmethod]
    fn stop() -> Self {
        Self {
            inner: GiveUp::Stop,
        }
    }

    /// Exit the process with `code` once the tree has shut down, so a
    /// process supervisor restarts it. The interpreter does not clean up.
    #[staticmethod]
    fn exit_process(code: i32) -> Self {
        Self {
            inner: GiveUp::ExitProcess(code),
        }
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn __repr__(&self) -> String {
        match self.inner {
            GiveUp::ExitProcess(code) => format!("GiveUp.exit_process({code})"),
            _ => "GiveUp.stop()".to_owned(),
        }
    }
}
