//! Links between children, like Erlang's `link/1`.
//!
//! Two linked children share their fate: when either one terminates for any
//! reason other than a normal exit (a crash, being killed, being shut down),
//! the other is killed too, and each one's supervisor then applies its own
//! restart policy. The children may belong to different supervisors, which is
//! what links add over a `OneForAll` strategy.
//!
//! ```
//! use ash_flare::{link, RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec, Worker};
//!
//! struct Service(ShutdownSignal);
//! impl Worker for Service {
//!     type Error = std::io::Error;
//!     async fn run(&mut self) -> Result<(), Self::Error> {
//!         self.0.cancelled().await;
//!         Ok(())
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let db = SupervisorHandle::start(
//!     SupervisorSpec::new("db").with_worker("pool", Service, RestartPolicy::Permanent),
//! )
//! .unwrap();
//! let web = SupervisorHandle::start(
//!     SupervisorSpec::new("web").with_worker("cache", Service, RestartPolicy::Permanent),
//! )
//! .unwrap();
//!
//! // If the database pool dies, the cache built on it is restarted too.
//! let _link = link(&db, "pool", &web, "cache");
//! # }
//! ```

use crate::events::{Monitor, SupervisorEvent};
use crate::supervisor::SupervisorHandle;
use crate::types::ChildExitReason;
use tokio::task::JoinHandle;

/// Keeps two children linked; dropping it removes the link.
///
/// The link is by child id, so it carries over to the instances that replace
/// them after restarts. When a link kills a child, the resulting termination
/// is not sent back across the link: the instance that died first is already
/// gone, and killing its replacement would only start a restart loop.
#[derive(Debug)]
pub struct Link {
    task: Option<JoinHandle<()>>,
}

impl Link {
    /// Whether the link is still in force.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.task.as_ref().is_some_and(|task| !task.is_finished())
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Links child `a_child` of supervisor `a` with child `b_child` of `b`.
///
/// Must be called from within a tokio runtime. Linking a child to itself does
/// nothing.
///
/// Each end is watched through a [`Monitor`], so no termination is missed,
/// however busy the supervisors are. The link ends by itself once both
/// supervisors have stopped.
#[must_use = "dropping the Link removes it immediately"]
pub fn link(a: &SupervisorHandle, a_child: &str, b: &SupervisorHandle, b_child: &str) -> Link {
    if a.control_tx.same_channel(&b.control_tx) && a_child == b_child {
        return Link { task: None };
    }

    // Monitored before returning, so nothing that happens after `link`
    // returns is missed.
    let a_end = End::new(a.clone(), a_child);
    let b_end = End::new(b.clone(), b_child);
    let task = tokio::spawn(propagate(a_end, b_end));
    Link { task: Some(task) }
}

/// One side of a link.
struct End {
    handle: SupervisorHandle,
    child: String,
    /// `None` once this side's supervisor has stopped.
    monitor: Option<Monitor>,
    /// Kills sent to this child by the link whose terminations are still to
    /// be seen; those are not propagated back.
    pending_kills: usize,
}

impl End {
    fn new(handle: SupervisorHandle, child: &str) -> Self {
        let monitor = Some(handle.monitor_child(child));
        Self {
            handle,
            child: child.to_owned(),
            monitor,
            pending_kills: 0,
        }
    }

    /// The next abnormal termination of this end's child, or `None` once its
    /// supervisor has stopped.
    async fn next_termination(&mut self) -> Option<ChildExitReason> {
        let monitor = self.monitor.as_mut()?;
        while let Some(event) = monitor.recv().await {
            // A normal exit does not travel along a link, as in Erlang.
            if let SupervisorEvent::ChildTerminated { reason, .. } = event
                && reason != ChildExitReason::Normal
            {
                return Some(reason);
            }
        }
        self.monitor = None;
        None
    }
}

/// Which end a termination is about.
#[derive(Clone, Copy)]
enum Side {
    A,
    B,
}

async fn propagate(mut a: End, mut b: End) {
    while a.monitor.is_some() || b.monitor.is_some() {
        let (side, reason) = tokio::select! {
            Some(reason) = a.next_termination(), if a.monitor.is_some() => (Side::A, reason),
            Some(reason) = b.next_termination(), if b.monitor.is_some() => (Side::B, reason),
            else => continue,
        };
        let (dead, other) = match side {
            Side::A => (&mut a, &mut b),
            Side::B => (&mut b, &mut a),
        };

        if dead.pending_kills > 0 {
            dead.pending_kills = dead.pending_kills.saturating_sub(1);
            continue;
        }
        if matches!(other.handle.kill_child(&other.child).await, Ok(true)) {
            other.pending_kills = other.pending_kills.saturating_add(1);
            tracing::debug!(
                dead = %dead.child,
                killed = %other.child,
                reason = ?reason,
                "linked child terminated, killed its partner"
            );
        }
    }
}
