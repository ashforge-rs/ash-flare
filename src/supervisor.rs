//! Supervisor types and implementation

mod child;
mod error;
mod handle;
mod runtime;
mod spec;

pub use error::SupervisorError;
pub use handle::SupervisorHandle;
pub use spec::{AutoShutdown, ChildSpec, GiveUp, Shutdown, ShutdownOrder, SupervisorSpec};

pub(crate) use runtime::SupervisorCommand;
