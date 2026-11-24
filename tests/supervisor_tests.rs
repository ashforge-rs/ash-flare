use ash_flare::{
    ChildType, RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec,
    Worker,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::time::{Duration, sleep};

struct SimpleWorker {
    counter: Arc<AtomicU32>,
}
impl Worker for SimpleWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            self.counter.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tokio::test]
async fn test_supervisor_start_and_shutdown() {
    let spec: SupervisorSpec = SupervisorSpec::new("test-supervisor");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    assert_eq!(handle.name(), "test-supervisor");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_with_children() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let spec = SupervisorSpec::new("test").with_worker(
        "worker-1",
        move || SimpleWorker {
            counter: Arc::clone(&c),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(50)).await;

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, "worker-1");
    assert_eq!(children[0].child_type, ChildType::Worker);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_dynamic_start_child() {
    let spec = SupervisorSpec::new("test");
    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let child_id = handle
        .start_child(
            "dynamic-worker",
            move || SimpleWorker {
                counter: Arc::clone(&c),
            },
            RestartPolicy::Permanent,
        )
        .await
        .unwrap();

    assert_eq!(child_id, "dynamic-worker");

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);

    sleep(Duration::from_millis(50)).await;
    assert!(counter.load(Ordering::SeqCst) > 0);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_terminate_child() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let spec = SupervisorSpec::new("test").with_worker(
        "worker-1",
        move || SimpleWorker {
            counter: Arc::clone(&c),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(50)).await;

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);

    handle.terminate_child("worker-1").await.unwrap();
    sleep(Duration::from_millis(20)).await;

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 0);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_restart_strategies() {
    let spec_one_for_one: SupervisorSpec =
        SupervisorSpec::new("test-one-for-one").with_restart_strategy(RestartStrategy::OneForOne);

    let spec_one_for_all: SupervisorSpec =
        SupervisorSpec::new("test-one-for-all").with_restart_strategy(RestartStrategy::OneForAll);

    let spec_rest_for_one: SupervisorSpec =
        SupervisorSpec::new("test-rest-for-one").with_restart_strategy(RestartStrategy::RestForOne);

    let h1 = SupervisorHandle::start(spec_one_for_one).expect("valid supervisor spec");
    let h2 = SupervisorHandle::start(spec_one_for_all).expect("valid supervisor spec");
    let h3 = SupervisorHandle::start(spec_rest_for_one).expect("valid supervisor spec");

    h1.shutdown().await.unwrap();
    h2.shutdown().await.unwrap();
    h3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_restart_intensity() {
    let spec: SupervisorSpec =
        SupervisorSpec::new("test").with_restart_intensity(RestartIntensity {
            max_restarts: 10,
            within_seconds: 5,
        });

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_nested_supervisors() {
    let counter = Arc::new(AtomicU32::new(0));
    let c = Arc::clone(&counter);

    let child_spec = SupervisorSpec::new("child-supervisor").with_worker(
        "nested-worker",
        move || SimpleWorker {
            counter: Arc::clone(&c),
        },
        RestartPolicy::Permanent,
    );

    let parent_spec = SupervisorSpec::new("parent-supervisor").with_supervisor(child_spec);

    let handle = SupervisorHandle::start(parent_spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(50)).await;

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].child_type, ChildType::Supervisor);

    assert!(counter.load(Ordering::SeqCst) > 0);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_supervisor_multiple_children() {
    let c1 = Arc::new(AtomicU32::new(0));
    let c2 = Arc::new(AtomicU32::new(0));
    let c3 = Arc::new(AtomicU32::new(0));

    let spec = SupervisorSpec::new("test")
        .with_worker(
            "worker-1",
            {
                let c = Arc::clone(&c1);
                move || SimpleWorker {
                    counter: Arc::clone(&c),
                }
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker-2",
            {
                let c = Arc::clone(&c2);
                move || SimpleWorker {
                    counter: Arc::clone(&c),
                }
            },
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker-3",
            {
                let c = Arc::clone(&c3);
                move || SimpleWorker {
                    counter: Arc::clone(&c),
                }
            },
            RestartPolicy::Permanent,
        );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
    sleep(Duration::from_millis(50)).await;

    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 3);

    assert!(c1.load(Ordering::SeqCst) > 0);
    assert!(c2.load(Ordering::SeqCst) > 0);
    assert!(c3.load(Ordering::SeqCst) > 0);

    handle.shutdown().await.unwrap();
}
