//! Monitors: lossless event streams. Unlike `subscribe`, a monitor that falls
//! behind never misses an event, and links, which are built on monitors, never
//! miss a termination.

use ash_flare::{
    ChildExitReason, ChildSpec, RestartBackoff, RestartPolicy, ShutdownSignal, SupervisorEvent,
    SupervisorHandle, SupervisorSpec, Worker, link,
};
use std::time::Duration;
use tokio::sync::broadcast::error::TryRecvError;

/// Waits until it is told to stop.
struct Idle(ShutdownSignal);
impl Worker for Idle {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
}

/// Finishes at once.
struct Done;
impl Worker for Done {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Far more than a subscriber's backlog of about a thousand events.
const BURST: usize = 3_000;

/// Starts `BURST` children that finish at once: `2 * BURST` events.
async fn burst(handle: &SupervisorHandle) {
    for _ in 0..BURST {
        handle
            .start_unique_child(ChildSpec::new("job", || Done).restart(RestartPolicy::Temporary))
            .await
            .unwrap();
    }
    // Let every job finish and be reported.
    while handle
        .which_children()
        .await
        .unwrap()
        .iter()
        .any(|child| child.id.starts_with("job-"))
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn a_monitor_that_falls_behind_misses_nothing() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("busy")).unwrap();
    let mut monitor = handle.monitor();
    let mut subscriber = handle.subscribe();

    burst(&handle).await;

    // The subscriber fell behind and lost the oldest events.
    assert!(matches!(
        subscriber.try_recv(),
        Err(TryRecvError::Lagged(_))
    ));

    let (mut started, mut terminated) = (0, 0);
    while let Some(event) = monitor.try_recv() {
        match event {
            SupervisorEvent::ChildStarted { .. } => started += 1,
            SupervisorEvent::ChildTerminated { .. } => terminated += 1,
            _ => {}
        }
    }
    assert_eq!((started, terminated), (BURST, BURST));
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_child_monitor_sees_only_its_child_and_the_end() {
    let handle = SupervisorHandle::start(
        SupervisorSpec::new("app")
            .with_restart_backoff(RestartBackoff::none())
            .with_worker("watched", Idle, RestartPolicy::Permanent)
            .with_worker("other", Idle, RestartPolicy::Permanent),
    )
    .unwrap();
    let mut monitor = handle.monitor_child("watched");

    handle.kill_child("other").await.unwrap();
    handle.kill_child("watched").await.unwrap();
    handle.shutdown().await.unwrap();

    let mut events = Vec::new();
    while let Some(event) = monitor.recv().await {
        events.push(event);
    }
    assert!(
        events.iter().all(|event| match event {
            SupervisorEvent::ChildTerminated { id, .. }
            | SupervisorEvent::ChildRestarted { id } => &**id == "watched",
            SupervisorEvent::Stopped { .. } => true,
            _ => false,
        }),
        "only events about the watched child: {events:?}"
    );
    assert!(events.contains(&SupervisorEvent::ChildTerminated {
        id: "watched".into(),
        reason: ChildExitReason::Killed,
    }));
    assert!(matches!(
        events.last(),
        Some(SupervisorEvent::Stopped {
            reason: ChildExitReason::Shutdown
        })
    ));
}

#[tokio::test]
async fn a_monitor_ends_after_the_supervisor_stops() {
    let handle = SupervisorHandle::start(SupervisorSpec::new("short")).unwrap();
    let mut before = handle.monitor();
    handle.shutdown().await.unwrap();

    assert!(matches!(
        before.recv().await,
        Some(SupervisorEvent::Stopped { .. })
    ));
    assert_eq!(before.recv().await, None);

    // Monitoring a stopped supervisor ends at once instead of waiting forever.
    let mut after = handle.monitor();
    assert_eq!(after.recv().await, None);
}

/// A link keeps working through a flood of unrelated events that would make
/// a broadcast subscriber skip some.
#[tokio::test(flavor = "current_thread")]
async fn a_link_fires_after_a_flood_of_events() {
    let busy = SupervisorHandle::start(SupervisorSpec::new("busy").with_worker(
        "pool",
        Idle,
        RestartPolicy::Permanent,
    ))
    .unwrap();
    let other = SupervisorHandle::start(
        SupervisorSpec::new("other")
            .with_restart_backoff(RestartBackoff::none())
            .with_worker("cache", Idle, RestartPolicy::Permanent),
    )
    .unwrap();
    let _link = link(&busy, "pool", &other, "cache");
    let mut cache = other.monitor_child("cache");

    burst(&busy).await;
    busy.kill_child("pool").await.unwrap();

    let killed = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = cache.recv().await {
            if let SupervisorEvent::ChildTerminated { reason, .. } = event {
                return reason;
            }
        }
        panic!("the cache's supervisor stopped");
    })
    .await
    .expect("the link killed the cache");
    assert_eq!(killed, ChildExitReason::Killed);

    busy.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}
