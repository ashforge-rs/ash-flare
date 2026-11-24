//! Supervisor events: every start, restart and termination is reported, and
//! every termination exactly once, with the reason it ended for.

use ash_flare::{
    ChildExitReason, ChildSpec, RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy,
    Shutdown, ShutdownSignal, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;

/// Waits until it is told to stop.
struct Idle(ShutdownSignal);
impl Worker for Idle {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

/// Never looks at its shutdown signal and never finishes its cleanup.
struct Stubborn;
impl Worker for Stubborn {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        std::future::pending().await
    }
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        std::future::pending().await
    }
}

/// Fails after `delay` on its first start only, then stays up.
struct FailOnce {
    starts: Arc<AtomicUsize>,
    delay: Duration,
}
impl Worker for FailOnce {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        if self.starts.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(self.delay).await;
            return Err(std::io::Error::other("first run fails"));
        }
        std::future::pending().await
    }
}

fn fail_once(
    starts: &Arc<AtomicUsize>,
    delay: Duration,
) -> impl Fn() -> FailOnce + Send + Sync + 'static {
    let starts = Arc::clone(starts);
    move || FailOnce {
        starts: Arc::clone(&starts),
        delay,
    }
}

/// Collects the events that arrive within `wait`.
async fn collect(events: &mut Receiver<SupervisorEvent>, wait: Duration) -> Vec<SupervisorEvent> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + wait;
    while let Ok(Ok(event)) = tokio::time::timeout_at(deadline, events.recv()).await {
        seen.push(event);
    }
    seen
}

fn terminated(id: &str, reason: ChildExitReason) -> SupervisorEvent {
    SupervisorEvent::ChildTerminated {
        id: id.into(),
        reason,
    }
}

#[tokio::test]
async fn a_crash_is_reported_then_the_restart() {
    let starts = Arc::new(AtomicUsize::new(0));
    let spec = SupervisorSpec::new("crash")
        .with_restart_backoff(RestartBackoff::none())
        .with_worker(
            "w",
            fail_once(&starts, Duration::from_millis(30)),
            RestartPolicy::Permanent,
        );
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    let seen = collect(&mut events, Duration::from_millis(150)).await;
    assert_eq!(
        seen,
        vec![
            terminated("w", ChildExitReason::Abnormal),
            SupervisorEvent::ChildRestarted { id: "w".into() },
        ]
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminate_child_is_reported_once() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("terminate")).expect("valid spec");
    let mut events = handle.subscribe();

    handle
        .start_child("w", Idle, RestartPolicy::Permanent)
        .await
        .unwrap();
    handle.terminate_child("w").await.unwrap();

    let seen = collect(&mut events, Duration::from_millis(100)).await;
    assert_eq!(
        seen,
        vec![
            SupervisorEvent::ChildStarted { id: "w".into() },
            terminated("w", ChildExitReason::Shutdown),
        ],
        "the worker's own exit report must not be reported a second time"
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_killed_child_is_reported_as_killed() {
    let spec = SupervisorSpec::new("kill")
        .with_child(ChildSpec::new("brutal", || Stubborn).shutdown(Shutdown::BrutalKill))
        .with_child(
            ChildSpec::new("overrun", || Stubborn)
                .shutdown(Shutdown::Timeout(Duration::from_millis(20))),
        );
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();
    tokio::time::sleep(Duration::from_millis(20)).await;

    handle.shutdown().await.unwrap();
    let seen = collect(&mut events, Duration::from_millis(100)).await;

    // Reverse start order: "overrun" first, then "brutal".
    assert_eq!(
        seen,
        vec![
            terminated("overrun", ChildExitReason::Killed),
            terminated("brutal", ChildExitReason::Killed),
            SupervisorEvent::Stopped {
                reason: ChildExitReason::Shutdown
            },
        ]
    );
}

#[tokio::test]
async fn shutdown_reports_every_child_then_the_supervisor() {
    let spec = SupervisorSpec::new("shutdown")
        .with_worker("a", Idle, RestartPolicy::Permanent)
        .with_worker("b", Idle, RestartPolicy::Permanent);
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();
    tokio::time::sleep(Duration::from_millis(20)).await;

    handle.shutdown().await.unwrap();
    let seen = collect(&mut events, Duration::from_millis(100)).await;
    assert_eq!(
        seen,
        vec![
            terminated("b", ChildExitReason::Shutdown),
            terminated("a", ChildExitReason::Shutdown),
            SupervisorEvent::Stopped {
                reason: ChildExitReason::Shutdown
            },
        ]
    );
}

#[tokio::test]
async fn escalation_is_reported() {
    struct AlwaysFails;
    impl Worker for AlwaysFails {
        type Error = std::io::Error;
        async fn run(&mut self) -> Result<(), Self::Error> {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Err(std::io::Error::other("always"))
        }
    }

    let spec = SupervisorSpec::new("escalate")
        .with_restart_backoff(RestartBackoff::none())
        .with_restart_intensity(RestartIntensity::new(1, 60))
        .with_worker("w", || AlwaysFails, RestartPolicy::Permanent);
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    let seen = collect(&mut events, Duration::from_millis(200)).await;
    let tail: Vec<_> = seen.iter().rev().take(2).rev().cloned().collect();
    assert_eq!(
        tail,
        vec![
            SupervisorEvent::RestartIntensityExceeded,
            SupervisorEvent::Stopped {
                reason: ChildExitReason::Abnormal
            },
        ]
    );
}

/// Two children of a `OneForAll` supervisor crash at the same moment. The first
/// report triggers a restart round that replaces both; the second report then
/// refers to an instance that no longer exists and must not trigger another
/// round against its replacement.
#[tokio::test]
async fn a_report_from_a_replaced_instance_does_not_restart_its_successor() {
    let a = Arc::new(AtomicUsize::new(0));
    let b = Arc::new(AtomicUsize::new(0));
    let spec = SupervisorSpec::new("simultaneous")
        .with_restart_strategy(RestartStrategy::OneForAll)
        .with_restart_backoff(RestartBackoff::none())
        .with_restart_intensity(RestartIntensity::new(10, 60))
        .with_worker(
            "a",
            fail_once(&a, Duration::from_millis(30)),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "b",
            fail_once(&b, Duration::from_millis(30)),
            RestartPolicy::Permanent,
        );
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    let seen = collect(&mut events, Duration::from_millis(250)).await;
    assert_eq!(
        (a.load(Ordering::SeqCst), b.load(Ordering::SeqCst)),
        (2, 2),
        "one restart round for one simultaneous failure; events: {seen:?}"
    );
    let abnormal = seen
        .iter()
        .filter(|e| {
            matches!(
                e,
                SupervisorEvent::ChildTerminated {
                    reason: ChildExitReason::Abnormal,
                    ..
                }
            )
        })
        .count();
    assert_eq!(abnormal, 2, "both crashes are still reported: {seen:?}");
    handle.shutdown().await.unwrap();
}
