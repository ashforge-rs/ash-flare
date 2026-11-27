//! Supervisor events: what happened to which child, as it happens.
//!
//! There are two ways to receive them:
//!
//! - [`SupervisorHandle::monitor`](crate::SupervisorHandle::monitor) and
//!   [`monitor_child`](crate::SupervisorHandle::monitor_child) return a
//!   [`Monitor`], the counterpart of `erlang:monitor/2`: it never misses an
//!   event. Use it when acting on an event matters, as links do.
//! - [`SupervisorHandle::subscribe`](crate::SupervisorHandle::subscribe)
//!   returns a broadcast receiver that holds a bounded backlog; a subscriber
//!   that falls too far behind skips the oldest events. Use it for logs and
//!   metrics, where a slow reader must not cost memory.

use crate::types::ChildExitReason;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{broadcast, mpsc};

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

impl SupervisorEvent {
    /// The child the event is about, if it is about one.
    fn child(&self) -> Option<&str> {
        match self {
            Self::ChildStarted { id }
            | Self::ChildRestarted { id }
            | Self::ChildTerminated { id, .. }
            | Self::ChildStalled { id, .. } => Some(id),
            Self::RestartIntensityExceeded | Self::Stopped { .. } => None,
        }
    }
}

/// How many unread events a subscriber may fall behind by before it misses
/// some (and is told so with `RecvError::Lagged`).
pub(crate) const EVENT_CAPACITY: usize = 1024;

/// A lossless stream of a supervisor's events, the counterpart of
/// `erlang:monitor/2`.
///
/// Returned by [`SupervisorHandle::monitor`](crate::SupervisorHandle::monitor),
/// which receives every event of the supervisor, and
/// [`SupervisorHandle::monitor_child`](crate::SupervisorHandle::monitor_child),
/// which receives those about one child id, across its restarts, plus
/// [`SupervisorEvent::Stopped`].
///
/// Events are queued for the monitor until it reads them, so none is ever
/// missed, however far behind it falls; the cost is that a monitor nobody
/// reads keeps growing. Dropping it stops the queueing. After the supervisor's
/// `Stopped` event, [`recv`](Self::recv) returns `None`.
#[derive(Debug)]
pub struct Monitor {
    rx: mpsc::UnboundedReceiver<SupervisorEvent>,
}

impl Monitor {
    /// The next event, waiting until there is one. `None` once the supervisor
    /// has stopped and every event before that has been received.
    pub async fn recv(&mut self) -> Option<SupervisorEvent> {
        self.rx.recv().await
    }

    /// The next event if one is waiting.
    pub fn try_recv(&mut self) -> Option<SupervisorEvent> {
        self.rx.try_recv().ok()
    }
}

/// Where a supervisor's events go: broadcast subscribers, and monitors.
///
/// Shared by the supervisor's runtime, its handles and the stall watchdog,
/// which emits from a thread of its own.
#[derive(Clone)]
pub(crate) struct EventSender(Arc<EventHub>);

struct EventHub {
    broadcast: broadcast::Sender<SupervisorEvent>,
    monitors: Mutex<Monitors>,
}

#[derive(Default)]
struct Monitors {
    sinks: Vec<MonitorSink>,
    /// Set once the supervisor has stopped; no event follows.
    closed: bool,
}

struct MonitorSink {
    /// Only events about this child, if set.
    child: Option<Arc<str>>,
    tx: mpsc::UnboundedSender<SupervisorEvent>,
}

impl MonitorSink {
    fn wants(&self, event: &SupervisorEvent) -> bool {
        match (&self.child, event.child()) {
            (None, _) => true,
            (Some(wanted), Some(about)) => **wanted == *about,
            // A child monitor still learns that the supervisor stopped.
            (Some(_), None) => matches!(event, SupervisorEvent::Stopped { .. }),
        }
    }
}

impl EventSender {
    pub(crate) fn new() -> Self {
        let (broadcast, _no_subscriber_yet) = broadcast::channel(EVENT_CAPACITY);
        Self(Arc::new(EventHub {
            broadcast,
            monitors: Mutex::new(Monitors::default()),
        }))
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<SupervisorEvent> {
        self.0.broadcast.subscribe()
    }

    /// A monitor of every event, or of those about `child`.
    pub(crate) fn monitor(&self, child: Option<&str>) -> Monitor {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut monitors = self.monitors();
        // Once stopped, the monitor is born finished: `recv` returns `None`.
        if !monitors.closed {
            monitors.sinks.push(MonitorSink {
                child: child.map(Arc::from),
                tx,
            });
        }
        Monitor { rx }
    }

    /// Ends every monitor after the supervisor's last event.
    pub(crate) fn close(&self) {
        let mut monitors = self.monitors();
        monitors.closed = true;
        monitors.sinks.clear();
    }

    fn monitors(&self) -> std::sync::MutexGuard<'_, Monitors> {
        self.0
            .monitors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Sends `event` to current subscribers and monitors. Having none is normal.
pub(crate) fn emit(sender: &EventSender, event: SupervisorEvent) {
    {
        // Monitors are fed under the lock, so each sees events in the order
        // they were emitted, even when the stall watchdog emits concurrently.
        let mut monitors = sender.monitors();
        monitors.sinks.retain(|sink| {
            if sink.wants(&event) {
                sink.tx.send(event.clone()).is_ok()
            } else {
                !sink.tx.is_closed()
            }
        });
    }
    // Skip building nothing-to-nobody sends; `send` errors when no one listens.
    if sender.0.broadcast.receiver_count() > 0 {
        let _delivered = sender.0.broadcast.send(event);
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
