//! Supervisor types and implementation

mod child;
mod error;
mod handle;
mod runtime;
mod spec;

pub use error::SupervisorError;
pub use handle::SupervisorHandle;
pub use spec::SupervisorSpec;

pub(crate) use runtime::RuntimeControl;
pub(crate) use spec::DEFAULT_SHUTDOWN_TIMEOUT;
