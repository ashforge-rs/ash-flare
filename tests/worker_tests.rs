use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time::{Duration, sleep};

struct TestWorker {
    counter: Arc<AtomicU32>,
    fail_after: Option<u32>,
}
impl Worker for TestWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            if let Some(limit) = self.fail_after {
                if count >= limit {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "intentional failure",
                    ));
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test]
async fn test_worker_basic_lifecycle() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let spec = SupervisorSpec::new("test").with_worker(
        "worker-1",
        move || TestWorker {
            counter: Arc::clone(&counter_clone),
            fail_after: Some(5),
        },
        RestartPolicy::Temporary,
    );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(100)).await;

    let count = counter.load(Ordering::SeqCst);
    assert!(count >= 5, "Worker should have run at least 5 times");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_worker_permanent_restart() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let spec = SupervisorSpec::new("test").with_worker(
        "worker-1",
        move || TestWorker {
            counter: Arc::clone(&counter_clone),
            fail_after: Some(3),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(200)).await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count > 3,
        "Worker should have restarted and continued counting"
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_worker_transient_normal_exit() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = Arc::clone(&counter);

    let spec = SupervisorSpec::new("test").with_worker(
        "worker-1",
        move || TestWorker {
            counter: Arc::clone(&counter_clone),
            fail_after: None,
        },
        RestartPolicy::Transient,
    );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(50)).await;

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_workers() {
    let counter1 = Arc::new(AtomicU32::new(0));
    let counter2 = Arc::new(AtomicU32::new(0));
    let c1 = Arc::clone(&counter1);
    let c2 = Arc::clone(&counter2);

    let spec = SupervisorSpec::new("test")
        .with_worker(
            "worker-1",
            move || TestWorker {
                counter: Arc::clone(&c1),
                fail_after: None,
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker-2",
            move || TestWorker {
                counter: Arc::clone(&c2),
                fail_after: None,
            },
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(50)).await;

    let count1 = counter1.load(Ordering::SeqCst);
    let count2 = counter2.load(Ordering::SeqCst);

    assert!(count1 > 0, "Worker 1 should have executed");
    assert!(count2 > 0, "Worker 2 should have executed");

    handle.shutdown().await.unwrap();
}
