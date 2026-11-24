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

use crate::events::SupervisorEvent;
use crate::supervisor::SupervisorHandle;
use crate::types::ChildExitReason;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
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
#[must_use = "dropping the Link removes it immediately"]
pub fn link(a: &SupervisorHandle, a_child: &str, b: &SupervisorHandle, b_child: &str) -> Link {
    let same_supervisor = a.control_tx.same_channel(&b.control_tx);
    if same_supervisor && a_child == b_child {
        return Link { task: None };
    }

    // Subscribed before returning, so nothing that happens after `link`
    // returns is missed.
    let a_end = End::new(a.clone(), a_child, a.subscribe());
    let b_end = End::new(
        b.clone(),
        b_child,
        (!same_supervisor).then(|| b.subscribe()),
    );
    let task = tokio::spawn(propagate(a_end, b_end));
    Link { task: Some(task) }
}

/// One side of a link.
struct End {
    handle: SupervisorHandle,
    child: String,
    /// `None` when both sides share one supervisor, and so one event stream.
    events: Option<Receiver<SupervisorEvent>>,
    /// Kills sent to this child by the link whose terminations are still to
    /// be seen; those are not propagated back.
    pending_kills: usize,
}

impl End {
    fn new(
        handle: SupervisorHandle,
        child: &str,
        events: impl Into<Option<Receiver<SupervisorEvent>>>,
    ) -> Self {
        Self {
            handle,
            child: child.to_owned(),
            events: events.into(),
            pending_kills: 0,
        }
    }
}

/// Which end an event is about.
#[derive(Clone, Copy)]
enum Side {
    A,
    B,
}

async fn propagate(mut a: End, mut b: End) {
    loop {
        let event = tokio::select! {
            event = recv(&mut a.events) => event,
            event = recv(&mut b.events) => event,
        };
        let Some(SupervisorEvent::ChildTerminated { id, reason }) = event else {
            continue;
        };
        // A normal exit does not travel along a link, as in Erlang.
        if reason == ChildExitReason::Normal {
            continue;
        }

        // With one supervisor, both ends' events arrive on `a`'s stream, so
        // the side is decided by the child id alone.
        let side = if *id == *a.child {
            Side::A
        } else if *id == *b.child {
            Side::B
        } else {
            continue;
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

/// Next event from `events`, or never when there is no stream.
async fn recv(events: &mut Option<Receiver<SupervisorEvent>>) -> Option<SupervisorEvent> {
    let Some(receiver) = events else {
        return std::future::pending().await;
    };
    loop {
        match receiver.recv().await {
            Ok(event) => return Some(event),
            // Fell behind; carry on with what is still buffered.
            Err(RecvError::Lagged(_)) => {}
            // Every handle is gone; this side will never report again.
            Err(RecvError::Closed) => return std::future::pending().await,
        }
    }
}
