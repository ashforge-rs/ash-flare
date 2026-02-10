//! Fault-tolerant supervision trees for Rust with distributed capabilities.
//!
//! Build resilient systems that automatically recover from failures using supervisor trees,
//! restart strategies, and distributed supervision inspired by Erlang/OTP.
//!
//! # Features
//!
//! - **Supervision Trees**: Hierarchical supervision with nested supervisors and workers
//! - **Restart Strategies**: `OneForOne`, `OneForAll`, and `RestForOne` strategies
//! - **Restart Policies**: `Permanent`, `Temporary`, and `Transient` restart behaviors
//! - **Restart Intensity**: Configurable restart limits with sliding time windows
//! - **Distributed**: Run supervisors across processes or machines via TCP/Unix sockets
//! - **Generic Workers**: Trait-based worker system for any async workload
//! - **Dynamic Management**: Add/remove children at runtime
//! - **Structured Logging**: Built-in support for `slog` structured logging
//! - **Mailbox System**: Optional message-passing for workers with string-based messages
//!
//! # Quick Start
//!
//! ```rust
//! use ash_flare::{SupervisorSpec, SupervisorHandle, RestartPolicy, Worker};
//! use async_trait::async_trait;
//!
//! struct Counter {
//!     id: u32,
//!     max: u32,
//! }
//!
//! #[async_trait]
//! impl Worker for Counter {
//!     type Error = std::io::Error;
//!
//!     async fn run(&mut self) -> Result<(), Self::Error> {
//!         for i in 0..self.max {
//!             tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Build supervisor tree
//! let spec = SupervisorSpec::new("root")
//!     .with_worker("counter-1", || Counter { id: 1, max: 5 }, RestartPolicy::Permanent)
//!     .with_worker("counter-2", || Counter { id: 2, max: 5 }, RestartPolicy::Permanent);
//!
//! // Start supervision tree
//! let handle = SupervisorHandle::start(spec);
//!
//! // Query children
//! let children = handle.which_children().await?;
//!
//! // Graceful shutdown
//! handle.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Restart Strategies
//!
//! - **OneForOne**: Restarts only the failed child (default)
//! - **OneForAll**: Restarts all children if any child fails
//! - **RestForOne**: Restarts the failed child and all children started after it
//!
//! # Examples
//!
//! See the [examples directory](https://github.com/gntem/ash-flare/tree/master/examples) for more:
//!
//! - `counter.rs` - Basic supervisor with multiple workers
//! - `distributed.rs` - Network-distributed supervisors
//! - `super_tree.rs` - Complex nested supervision trees

#![deny(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]

#[macro_use]
mod macros;

mod restart;
mod supervisor;
mod supervisor_common;
mod types;
mod worker;

pub mod distributed;
pub mod mailbox;
pub mod supervisor_stateful;

// Re-export public API
pub use restart::{RestartIntensity, RestartPolicy, RestartStrategy};
pub use supervisor::{SupervisorError, SupervisorHandle, SupervisorSpec};
pub use supervisor_stateful::{
    StatefulSupervisorError, StatefulSupervisorHandle, StatefulSupervisorSpec,
};
pub use types::{ChildExitReason, ChildId, ChildInfo, ChildType, WorkerContext};
pub use worker::{Worker, WorkerError};

// Re-export mailbox types
pub use mailbox::{Mailbox, MailboxConfig, MailboxHandle, SendError, TryRecvError, TrySendError};
