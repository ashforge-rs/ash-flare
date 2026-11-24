//! Workers sharing state through a `WorkerContext` captured by their factories.

use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker, WorkerContext};
use tokio::time::{Duration, sleep};

struct StatefulCounter {
    id: u32,
    context: WorkerContext,
}
impl Worker for StatefulCounter {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Read counter from shared store
        let key = format!("counter-{}", self.id);
        let current = self.context.get(&key).and_then(|v| v.as_u64()).unwrap_or(0);

        // Increment and store back
        self.context
            .set(key.clone(), serde_json::json!(current + 1));

        // Also update a global counter
        self.context.update("global", |v| {
            let current = v.and_then(|v| v.as_u64()).unwrap_or(0);
            Some(serde_json::json!(current + 1))
        });

        // Sleep briefly to simulate work
        sleep(Duration::from_millis(50)).await;

        Ok(())
    }
}

#[tokio::test]
async fn test_stateful_workers_share_context() {
    let context = WorkerContext::new();
    let counter = |id| {
        let context = context.clone();
        move || StatefulCounter {
            id,
            context: context.clone(),
        }
    };

    let spec = SupervisorSpec::new("stateful-test")
        .with_worker("counter-1", counter(1), RestartPolicy::Temporary)
        .with_worker("counter-2", counter(2), RestartPolicy::Temporary);

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    // Wait for workers to complete
    sleep(Duration::from_millis(200)).await;

    // Verify shared state was updated
    let counter1 = context.get("counter-1").and_then(|v| v.as_u64());
    let counter2 = context.get("counter-2").and_then(|v| v.as_u64());
    let global = context.get("global").and_then(|v| v.as_u64());

    assert_eq!(counter1, Some(1), "Counter 1 should be incremented");
    assert_eq!(counter2, Some(1), "Counter 2 should be incremented");
    assert_eq!(global, Some(2), "Global counter should be 2");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_context_operations() {
    let context = WorkerContext::new();

    // Test set and get
    context.set("key1", serde_json::json!("value1"));
    assert_eq!(context.get("key1"), Some(serde_json::json!("value1")));

    // Test update
    context.update("counter", |v| {
        let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
        Some(serde_json::json!(count + 1))
    });
    assert_eq!(context.get("counter"), Some(serde_json::json!(1)));

    context.update("counter", |v| {
        let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
        Some(serde_json::json!(count + 1))
    });
    assert_eq!(context.get("counter"), Some(serde_json::json!(2)));

    // Test delete
    let deleted = context.delete("key1");
    assert_eq!(deleted, Some(serde_json::json!("value1")));
    assert_eq!(context.get("key1"), None);

    // Test update with None (delete)
    context.set("to-delete", serde_json::json!("value"));
    context.update("to-delete", |_| None);
    assert_eq!(context.get("to-delete"), None);
}

#[tokio::test]
async fn test_stateful_worker_restart_preserves_context() {
    struct FailingCounter {
        id: u32,
        context: WorkerContext,
    }
    impl Worker for FailingCounter {
        type Error = std::io::Error;

        async fn run(&mut self) -> Result<(), Self::Error> {
            // Increment counter
            let key = format!("restart-counter-{}", self.id);
            self.context.update(&key, |v| {
                let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
                Some(serde_json::json!(count + 1))
            });

            // Check if this is the first run
            let count = self.context.get(&key).and_then(|v| v.as_u64()).unwrap_or(0);

            if count == 1 {
                // Fail on first run
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "intentional failure",
                ));
            }

            // Succeed on restart
            sleep(Duration::from_millis(50)).await;
            Ok(())
        }
    }

    let context = WorkerContext::new();
    let factory_context = context.clone();
    let spec = SupervisorSpec::new("restart-test").with_worker(
        "failing-counter",
        move || FailingCounter {
            id: 1,
            context: factory_context.clone(),
        },
        RestartPolicy::Permanent,
    );

    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");

    // Wait for initial failure and restart
    sleep(Duration::from_millis(300)).await;

    // Verify counter was incremented twice (initial + restart)
    let count = context
        .get("restart-counter-1")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    assert!(
        count >= 2,
        "Counter should be at least 2 after restart, got {}",
        count
    );

    handle.shutdown().await.unwrap();
}
