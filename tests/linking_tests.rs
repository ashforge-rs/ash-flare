//! Tests for start_child_linked functionality

use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::sleep;

// ============================================================================
// Test Workers
// ============================================================================

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

// Worker that initializes successfully
#[derive(Clone)]
struct SuccessWorker {
    counter: Arc<AtomicU32>,
    init_flag: Arc<AtomicBool>,
}

#[async_trait]
impl Worker for SuccessWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_millis(50)).await;
        self.init_flag.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            self.counter.fetch_add(1, Ordering::SeqCst);
            sleep(Duration::from_millis(10)).await;
        }
    }
}

// Worker that fails during initialization
#[derive(Clone)]
struct FailingWorker {
    should_fail: Arc<AtomicBool>,
}

#[async_trait]
impl Worker for FailingWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_millis(50)).await;
        if self.should_fail.load(Ordering::SeqCst) {
            Err(WorkerError("Initialization failed".to_string()))
        } else {
            Ok(())
        }
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

// Worker that takes too long to initialize
#[derive(Clone)]
struct SlowWorker {
    init_duration: Duration,
}

#[async_trait]
impl Worker for SlowWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        sleep(self.init_duration).await;
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_start_child_linked_success() {
    let spec = SupervisorSpec::new("test-supervisor");
    let handle: SupervisorHandle<SuccessWorker> = SupervisorHandle::start(spec);

    let counter = Arc::new(AtomicU32::new(0));
    let init_flag = Arc::new(AtomicBool::new(false));
    let c = Arc::clone(&counter);
    let f = Arc::clone(&init_flag);

    // Start child with linked initialization
    let result = handle
        .start_child_linked(
            "success-worker",
            move || SuccessWorker {
                counter: Arc::clone(&c),
                init_flag: Arc::clone(&f),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await;

    assert!(result.is_ok(), "Worker should start successfully");
    assert_eq!(result.unwrap(), "success-worker");

    // Verify initialization ran
    assert!(
        init_flag.load(Ordering::SeqCst),
        "Initialization should have completed"
    );

    // Verify child is running
    sleep(Duration::from_millis(100)).await;
    assert!(
        counter.load(Ordering::SeqCst) > 0,
        "Worker should be running"
    );

    // Verify child appears in which_children
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, "success-worker");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_start_child_linked_initialization_failure() {
    let spec = SupervisorSpec::new("test-supervisor");
    let handle: SupervisorHandle<FailingWorker> = SupervisorHandle::start(spec);

    let should_fail = Arc::new(AtomicBool::new(true));
    let f = Arc::clone(&should_fail);

    // Start child with linked initialization
    let result = handle
        .start_child_linked(
            "failing-worker",
            move || FailingWorker {
                should_fail: Arc::clone(&f),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await;

    assert!(result.is_err(), "Worker should fail to start");

    // Verify error type
    match result {
        Err(ash_flare::SupervisorError::InitializationFailed { child_id, reason }) => {
            assert_eq!(child_id, "failing-worker");
            assert!(reason.contains("Initialization failed"));
        }
        _ => panic!("Expected InitializationFailed error"),
    }

    // Verify child is NOT in supervisor tree
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 0, "Failed child should not be in tree");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_start_child_linked_timeout() {
    let spec = SupervisorSpec::new("test-supervisor");
    let handle: SupervisorHandle<SlowWorker> = SupervisorHandle::start(spec);

    // Start child with linked initialization (timeout too short)
    let result = handle
        .start_child_linked(
            "slow-worker",
            || SlowWorker {
                init_duration: Duration::from_secs(5),
            },
            RestartPolicy::Permanent,
            Duration::from_millis(100), // Timeout before init completes
        )
        .await;

    assert!(result.is_err(), "Worker should timeout");

    // Verify error type
    match result {
        Err(ash_flare::SupervisorError::InitializationTimeout { child_id, timeout }) => {
            assert_eq!(child_id, "slow-worker");
            assert_eq!(timeout, Duration::from_millis(100));
        }
        _ => panic!("Expected InitializationTimeout error"),
    }

    // Verify child is NOT in supervisor tree
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 0, "Timed-out child should not be in tree");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_start_child_linked_duplicate_id() {
    let spec = SupervisorSpec::new("test-supervisor");
    let handle: SupervisorHandle<SuccessWorker> = SupervisorHandle::start(spec);

    let counter1 = Arc::new(AtomicU32::new(0));
    let init_flag1 = Arc::new(AtomicBool::new(false));
    let c1 = Arc::clone(&counter1);
    let f1 = Arc::clone(&init_flag1);

    // Start first child
    handle
        .start_child_linked(
            "worker-1",
            move || SuccessWorker {
                counter: Arc::clone(&c1),
                init_flag: Arc::clone(&f1),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await
        .unwrap();

    let counter2 = Arc::new(AtomicU32::new(0));
    let init_flag2 = Arc::new(AtomicBool::new(false));
    let c2 = Arc::clone(&counter2);
    let f2 = Arc::clone(&init_flag2);

    // Attempt to start second child with same ID
    let result = handle
        .start_child_linked(
            "worker-1", // Duplicate ID
            move || SuccessWorker {
                counter: Arc::clone(&c2),
                init_flag: Arc::clone(&f2),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await;

    assert!(result.is_err(), "Should fail with duplicate ID");

    match result {
        Err(ash_flare::SupervisorError::ChildAlreadyExists(id)) => {
            assert_eq!(id, "worker-1");
        }
        _ => panic!("Expected ChildAlreadyExists error"),
    }

    // Verify only one child exists
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 1);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_start_child_linked_multiple_children() {
    let spec = SupervisorSpec::new("test-supervisor");
    let handle: SupervisorHandle<SuccessWorker> = SupervisorHandle::start(spec);

    // Start multiple children with linked initialization
    for i in 0..5 {
        let counter = Arc::new(AtomicU32::new(0));
        let init_flag = Arc::new(AtomicBool::new(false));
        let c = Arc::clone(&counter);
        let f = Arc::clone(&init_flag);

        handle
            .start_child_linked(
                format!("worker-{}", i),
                move || SuccessWorker {
                    counter: Arc::clone(&c),
                    init_flag: Arc::clone(&f),
                },
                RestartPolicy::Permanent,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
    }

    // Verify all children are running
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 5);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_start_child_linked_mixed_with_regular_start() {
    let spec = SupervisorSpec::new("test-supervisor");
    let handle: SupervisorHandle<SuccessWorker> = SupervisorHandle::start(spec);

    // Start regular child (no linking)
    let counter1 = Arc::new(AtomicU32::new(0));
    let init_flag1 = Arc::new(AtomicBool::new(false));
    let c1 = Arc::clone(&counter1);
    let f1 = Arc::clone(&init_flag1);

    handle
        .start_child(
            "regular-worker",
            move || SuccessWorker {
                counter: Arc::clone(&c1),
                init_flag: Arc::clone(&f1),
            },
            RestartPolicy::Permanent,
        )
        .await
        .unwrap();

    // Start linked child
    let counter2 = Arc::new(AtomicU32::new(0));
    let init_flag2 = Arc::new(AtomicBool::new(false));
    let c2 = Arc::clone(&counter2);
    let f2 = Arc::clone(&init_flag2);

    handle
        .start_child_linked(
            "linked-worker",
            move || SuccessWorker {
                counter: Arc::clone(&c2),
                init_flag: Arc::clone(&f2),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await
        .unwrap();

    // Verify both children are running
    let children = handle.which_children().await.unwrap();
    assert_eq!(children.len(), 2);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_start_child_linked_no_restart_on_init_failure() {
    let spec = SupervisorSpec::new("test-supervisor")
        .with_restart_strategy(ash_flare::RestartStrategy::OneForOne)
        .with_restart_intensity(ash_flare::RestartIntensity::new(10, 5));

    let handle: SupervisorHandle<FailingWorker> = SupervisorHandle::start(spec);

    let should_fail = Arc::new(AtomicBool::new(true));
    let f = Arc::clone(&should_fail);

    // Start child with linked initialization
    let result = handle
        .start_child_linked(
            "failing-worker",
            move || FailingWorker {
                should_fail: Arc::clone(&f),
            },
            RestartPolicy::Permanent, // Even permanent workers don't restart on init failure
            Duration::from_secs(2),
        )
        .await;

    assert!(result.is_err(), "Worker should fail to start");

    // Wait a bit to ensure no restart attempts
    sleep(Duration::from_millis(500)).await;

    // Verify child is still NOT in supervisor tree (no restart triggered)
    let children = handle.which_children().await.unwrap();
    assert_eq!(
        children.len(),
        0,
        "Failed initialization should not trigger restart policy"
    );

    handle.shutdown().await.unwrap();
}
