//! Stall detection: a worker stuck inside one poll is reported while it is
//! still stuck.

use ash_flare::{RestartPolicy, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;

/// Blocks its thread once for `block`, then idles politely.
struct BlocksOnce {
    block: Duration,
    done: Arc<AtomicBool>,
}
impl Worker for BlocksOnce {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        std::thread::sleep(self.block);
        self.done.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
}

/// Yields often and never blocks.
struct Polite;
impl Worker for Polite {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn stalls(events: &mut Receiver<SupervisorEvent>) -> Vec<(String, Duration)> {
    let mut found = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let SupervisorEvent::ChildStalled { id, stalled_for } = event {
            found.push((id.to_string(), stalled_for));
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stuck_worker_is_reported_while_it_is_stuck() {
    let done = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let spec = SupervisorSpec::new("stalls")
        .with_stall_detection(Duration::from_millis(50))
        .with_worker(
            "stuck",
            move || BlocksOnce {
                block: Duration::from_millis(300),
                done: Arc::clone(&flag),
            },
            RestartPolicy::Permanent,
        )
        .with_worker("polite", || Polite, RestartPolicy::Permanent);
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    let event = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if let Ok(SupervisorEvent::ChildStalled { id, stalled_for }) = events.recv().await {
                return (id, stalled_for);
            }
        }
    })
    .await
    .expect("reported within 250ms");

    assert_eq!(&*event.0, "stuck");
    assert!(event.1 >= Duration::from_millis(50), "{:?}", event.1);
    assert!(
        !done.load(Ordering::SeqCst),
        "reported before the worker got unstuck"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        stalls(&mut events).is_empty(),
        "one report per stall, none for the polite worker"
    );
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_is_reported_when_detection_is_off() {
    let done = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let spec = SupervisorSpec::new("quiet").with_worker(
        "stuck",
        move || BlocksOnce {
            block: Duration::from_millis(120),
            done: Arc::clone(&flag),
        },
        RestartPolicy::Permanent,
    );
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(done.load(Ordering::SeqCst));
    assert!(stalls(&mut events).is_empty());
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_isolated_worker_is_watched_too() {
    let done = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&done);
    let spec = SupervisorSpec::new("isolated")
        .with_stall_detection(Duration::from_millis(30))
        .with_child(
            ash_flare::ChildSpec::new("stuck", move || BlocksOnce {
                block: Duration::from_millis(150),
                done: Arc::clone(&flag),
            })
            .isolated(true),
        );
    let handle = SupervisorHandle::start(spec).expect("valid spec");
    let mut events = handle.subscribe();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let found = stalls(&mut events);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].0, "stuck");
    handle.shutdown().await.unwrap();
}
