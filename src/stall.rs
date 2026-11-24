//! Detecting workers that hold their thread for too long, like Erlang's
//! `system_monitor(long_schedule)`.
//!
//! Async Rust cannot preempt a task: a worker that blocks or computes without
//! awaiting keeps its runtime thread until it yields, and other tasks on that
//! thread wait. With stall detection enabled, each poll of a worker is timed,
//! and a watchdog thread reports a worker that has been inside one poll for
//! longer than the threshold, while it is still stuck.

use crate::events::{EventSender, SupervisorEvent, emit};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// How often the watchdog looks at the workers it watches.
const WATCHDOG_TICK: Duration = Duration::from_millis(10);

/// The per-supervisor setting, handed to each worker it starts.
#[derive(Clone)]
pub(crate) struct StallWatch {
    pub threshold: Duration,
    pub events: EventSender,
}

/// Tracks the poll a worker is currently in, if any.
pub(crate) struct PollMonitor {
    id: Arc<str>,
    threshold: Duration,
    events: EventSender,
    /// When the current poll began, in nanoseconds since [`epoch`]; 0 when the
    /// worker is not being polled.
    polling_since: AtomicU64,
    /// Whether the current poll has already been reported.
    reported: AtomicBool,
}

impl PollMonitor {
    /// Starts watching a worker. The watchdog forgets the monitor once it is
    /// dropped along with the worker.
    pub(crate) fn watch(id: Arc<str>, watch: &StallWatch) -> Arc<Self> {
        let monitor = Arc::new(Self {
            id,
            threshold: watch.threshold,
            events: watch.events.clone(),
            polling_since: AtomicU64::new(0),
            reported: AtomicBool::new(false),
        });
        watchdog().register(&monitor);
        monitor
    }

    fn begin_poll(&self) {
        self.reported.store(false, Ordering::Relaxed);
        self.polling_since
            .store(now_nanos().max(1), Ordering::Release);
    }

    fn end_poll(&self) {
        self.polling_since.store(0, Ordering::Release);
    }

    /// Reports the worker if it has been in its current poll for too long.
    fn check(&self, now: u64) {
        let since = self.polling_since.load(Ordering::Acquire);
        if since == 0 {
            return;
        }
        let stalled_for = Duration::from_nanos(now.saturating_sub(since));
        if stalled_for >= self.threshold && !self.reported.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                worker = %self.id,
                stalled_ms = %stalled_for.as_millis(),
                "worker has not yielded; it is blocking its thread"
            );
            emit(
                &self.events,
                SupervisorEvent::ChildStalled {
                    id: Arc::clone(&self.id),
                    stalled_for,
                },
            );
        }
    }
}

/// Times every poll of `F` against an optional monitor.
pub(crate) struct Monitored<'m, F> {
    pub inner: F,
    pub monitor: Option<&'m PollMonitor>,
}

impl<F: Future + Unpin> Future for Monitored<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(monitor) = self.monitor else {
            return Pin::new(&mut self.inner).poll(cx);
        };
        monitor.begin_poll();
        // Ends the poll even if it unwinds, so a caught panic is not later
        // reported as a stall.
        let _end = EndPoll(monitor);
        Pin::new(&mut self.inner).poll(cx)
    }
}

struct EndPoll<'m>(&'m PollMonitor);

impl Drop for EndPoll<'_> {
    fn drop(&mut self) {
        self.0.end_poll();
    }
}

/// One thread watches every monitored worker in the process.
struct Watchdog {
    monitors: Mutex<Vec<Weak<PollMonitor>>>,
}

impl Watchdog {
    fn register(&self, monitor: &Arc<PollMonitor>) {
        if let Ok(mut monitors) = self.monitors.lock() {
            monitors.push(Arc::downgrade(monitor));
        }
    }

    fn run(&self) -> ! {
        loop {
            std::thread::sleep(WATCHDOG_TICK);
            let now = now_nanos();
            if let Ok(mut monitors) = self.monitors.lock() {
                monitors.retain(|weak| match weak.upgrade() {
                    Some(monitor) => {
                        monitor.check(now);
                        true
                    }
                    None => false,
                });
            }
        }
    }
}

fn watchdog() -> &'static Watchdog {
    static WATCHDOG: OnceLock<&'static Watchdog> = OnceLock::new();
    WATCHDOG.get_or_init(|| {
        let watchdog: &'static Watchdog = Box::leak(Box::new(Watchdog {
            monitors: Mutex::new(Vec::new()),
        }));
        let spawned = std::thread::Builder::new()
            .name("ash-flare:stall-watchdog".to_owned())
            .spawn(move || watchdog.run());
        if let Err(e) = spawned {
            tracing::error!(error = %e, "could not start the stall watchdog");
        }
        watchdog
    })
}

/// Nanoseconds since a fixed point early in the process's life.
fn now_nanos() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = *EPOCH.get_or_init(Instant::now);
    u64::try_from(epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
