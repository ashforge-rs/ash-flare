//! Monitors: lossless event streams. Unlike `subscribe`, a monitor that falls
//! behind never misses an event, and links, which are built on monitors, never
//! miss a termination.

use ash_flare::{
    ChildExitReason, ChildSpec, RestartBackoff, RestartPolicy, Shutdown, ShutdownSignal,
    SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker, link,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// Stays up until stopped, then takes a while to clean up. Its supervisor
/// answers no command while it waits for that cleanup.
struct SlowToStop(ShutdownSignal);
impl Worker for SlowToStop {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.cancelled().await;
        Ok(())
    }
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    }
}

/// Counts its starts and stays up until stopped.
struct Counted(Arc<AtomicUsize>, ShutdownSignal);
impl Worker for Counted {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.cancelled().await;
        Ok(())
    }
}

/// A link that is busy killing a partner does not miss the next termination,
/// even when far more events than a broadcast holds arrive meanwhile. With
/// links on the lossy broadcast, the second termination below was dropped
/// and the cache was killed only once.
#[tokio::test]
async fn a_link_misses_no_termination_while_it_waits() {
    let busy = SupervisorHandle::start(
        SupervisorSpec::new("busy")
            .with_restart_backoff(RestartBackoff::none())
            .with_worker("pool", Idle, RestartPolicy::Permanent),
    )
    .unwrap();
    let cache_starts = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&cache_starts);
    let other = SupervisorHandle::start(
        SupervisorSpec::new("other")
            .with_restart_backoff(RestartBackoff::none())
            .with_child(ChildSpec::new("slow", SlowToStop).shutdown(Shutdown::Infinity))
            .with_worker(
                "cache",
                move |signal| Counted(Arc::clone(&counted), signal),
                RestartPolicy::Permanent,
            ),
    )
    .unwrap();
    let _link = link(&busy, "pool", &other, "cache");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(cache_starts.load(Ordering::SeqCst), 1);

    // `other` spends two seconds stopping `slow`, answering no command.
    let stopping = {
        let other = other.clone();
        tokio::spawn(async move { other.terminate_child("slow").await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The pool dies: the link asks `other` to kill the cache, and waits.
    assert!(busy.kill_child("pool").await.unwrap());
    // The pool dies again while the link is still waiting...
    while !busy.kill_child("pool").await.unwrap() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // ...and far more events than a broadcast holds follow.
    burst(&busy).await;
    stopping.await.unwrap().unwrap();

    // Both terminations reached the cache: killed twice, so started 3 times.
    let started = tokio::time::timeout(Duration::from_secs(5), async {
        while cache_starts.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        started.is_ok(),
        "the cache was started {} times; the link missed a termination",
        cache_starts.load(Ordering::SeqCst)
    );

    busy.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}
