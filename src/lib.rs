//! Fault-tolerant supervision trees for Rust, modelled on Erlang/OTP.
//!
//! A [`SupervisorSpec`] describes a tree of workers and nested supervisors;
//! [`SupervisorHandle::start`] runs it. Failed children are restarted according
//! to their [`RestartPolicy`], the supervisor's [`RestartStrategy`], and its
//! restart intensity and backoff.
//!
//! ```rust
//! use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker};
//!
//! struct Ticker;
//!
//! impl Worker for Ticker {
//!     type Error = std::io::Error;
//!
//!     async fn run(&mut self) -> Result<(), Self::Error> {
//!         tokio::time::sleep(std::time::Duration::from_millis(100)).await;
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let spec = SupervisorSpec::new("app")
//!     .with_worker("ticker", || Ticker, RestartPolicy::Permanent);
//!
//! let handle = SupervisorHandle::start(spec)?;
//! let children = handle.which_children().await?;
//! handle.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Overview
//!
//! - **Children**: [`Worker`], [`ChildSpec`] with per-child [`Shutdown`],
//!   significant children and [`AutoShutdown`], worker pools.
//! - **Failure handling**: panics are supervised like errors; [`ShutdownOrder`]
//!   controls how children are stopped together; [`GiveUp`] says what a
//!   top-level supervisor does when it gives up, and
//!   [`SupervisorHandle::wait`] reports why a supervisor stopped.
//! - **Nested supervisors**: [`SupervisorSpec::with_registered_name`] and
//!   [`SupervisorHandle::child_supervisor`].
//! - **Monitoring**: [`SupervisorHandle::monitor`] for a lossless stream of
//!   [`SupervisorEvent`]s, [`SupervisorHandle::subscribe`] for a lossy one,
//!   [`link`] and [`SupervisorHandle::kill_child`].
//! - **Messaging**: typed [mailboxes](mod@mailbox) with call/cast and a [`Registry`].
//! - **Blocking work**: [`ChildSpec::isolated`] and
//!   [`SupervisorSpec::with_stall_detection`].
//! - **Shared state**: [`WorkerContext`].
//! - **Remote control**: the [`distributed`] module.
//!
//! See the [examples](https://github.com/ashforge-rs/ash-flare/tree/master/examples).

#![deny(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]

/// Compiles the Rust examples in `README.md` as doctests so they cannot drift
/// out of sync with the real API.
#[doc = include_str!("../README.md")]
#[cfg(doctest)]
mod readme_doctests {}

#[macro_use]
mod macros;

mod events;
mod link;
mod panic;
mod restart;
mod stall;
mod supervisor;
mod supervisor_common;
mod types;
mod worker;

pub mod distributed;
pub mod mailbox;
pub mod registry;

// Python bindings module, compiled only for the `python` feature so pure-Rust
// consumers do not pull in pyo3 or link against Python.
#[cfg(feature = "python")]
pub mod python;

// Re-export public API
pub use events::{Monitor, SupervisorEvent};
pub use link::{Link, link};
pub use registry::{Registration, Registry, RegistryError};
pub use restart::{RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy};
pub use supervisor::{
    AutoShutdown, ChildSpec, GiveUp, Shutdown, ShutdownOrder, SupervisorError, SupervisorHandle,
    SupervisorSpec,
};
pub use types::{ChildExitReason, ChildId, ChildInfo, ChildType, ShutdownSignal, WorkerContext};
pub use worker::{Worker, WorkerFactory};

// Re-export mailbox types
pub use mailbox::{
    CallError, Mailbox, MailboxConfig, MailboxHandle, Reply, SendError, TryRecvError, TrySendError,
    mailbox, mailbox_named,
};
