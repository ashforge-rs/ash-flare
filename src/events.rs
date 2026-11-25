//! Supervisor events: what happened to which child, as it happens.
//!
//! [`SupervisorHandle::subscribe`](crate::SupervisorHandle::subscribe) returns
//! a stream of these, the counterpart of monitoring processes on the BEAM. Use
//! it to watch a child, react to crashes, or feed metrics and logs.

use crate::types::ChildExitReason;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Something that happened in a supervisor.
///
/// Every child that is started is reported terminated exactly once, with the
/// reason it ended for, whether it stopped on its own or was stopped by the
/// supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SupervisorEvent {
    /// A child was started, initially or with `start_child`.
    ChildStarted {
        /// The child's id.
        id: Arc<str>,
    },
    /// A child was started again after terminating.
    ChildRestarted {
        /// The child's id.
        id: Arc<str>,
    },
    /// A child terminated.
    ChildTerminated {
        /// The child's id.
        id: Arc<str>,
        /// Why it terminated.
        reason: ChildExitReason,
    },
    /// A child has been inside a single poll for longer than the
    /// supervisor's stall threshold: it is blocking its thread, holding up
    /// whatever else runs there. Reported once per stall, while it lasts.
    /// See `SupervisorSpec::with_stall_detection`.
    ChildStalled {
        /// The child's id.
        id: Arc<str>,
        /// How long it had been stuck when this was noticed.
        stalled_for: std::time::Duration,
    },
    /// Children failed more often than the restart intensity allows; the
    /// supervisor is shutting down and escalating to its parent.
    RestartIntensityExceeded,
    /// The supervisor itself stopped. No further events follow.
    Stopped {
        /// [`ChildExitReason::Shutdown`] when asked to stop,
        /// [`ChildExitReason::Normal`] when it stopped itself through its
        /// `AutoShutdown`, [`ChildExitReason::Abnormal`] when it gave up.
        reason: ChildExitReason,
    },
}

/// How many unread events a subscriber may fall behind by before it misses
/// some (and is told so with `RecvError::Lagged`).
pub(crate) const EVENT_CAPACITY: usize = 1024;

pub(crate) type EventSender = tokio::sync::broadcast::Sender<SupervisorEvent>;

/// Sends `event` to current subscribers. Having none is normal.
pub(crate) fn emit(sender: &EventSender, event: SupervisorEvent) {
    // Skip building nothing-to-nobody sends; `send` errors when no one listens.
    if sender.receiver_count() > 0 {
        let _delivered = sender.send(event);
    }
}

/// A unique number for each started child, the counterpart of an Erlang pid.
///
/// A child's id is reused when it restarts; its instance is not. Exit reports
/// carry the instance, so a report from an instance that has already been
/// replaced is recognised as stale instead of being blamed on its successor.
pub(crate) fn next_instance() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
