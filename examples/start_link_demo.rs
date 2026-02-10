//! Demonstration of linked child startup with initialization handshake
//!
//! This example shows how to use `start_child_linked` to ensure workers
//! initialize successfully before being added to the supervision tree.

use ash_flare::{RestartPolicy, SupervisorHandle, Worker, supervision_tree};
use async_trait::async_trait;
use std::time::Duration;

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
struct GoodWorker {
    name: String,
}

#[async_trait]
impl Worker for GoodWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Initializing...", self.name);
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("[{}] ✓ Initialization successful!", self.name);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Running...", self.name);
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("[{}] Completed", self.name);
        Ok(())
    }
}

// Worker that fails during initialization
#[derive(Clone)]
struct BadWorker {
    name: String,
}

#[async_trait]
impl Worker for BadWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Initializing...", self.name);
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("[{}] ✗ Initialization failed!", self.name);
        Err(WorkerError(
            "Configuration error: missing database connection".to_string(),
        ))
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Running (this should never print)", self.name);
        Ok(())
    }
}

// Worker that takes too long to initialize
#[derive(Clone)]
struct SlowWorker {
    name: String,
}

#[async_trait]
impl Worker for SlowWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Initializing (this will take 5 seconds)...", self.name);
        tokio::time::sleep(Duration::from_secs(5)).await;
        println!("[{}] ✓ Initialization complete (but too late)", self.name);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        println!("[{}] Running (this should never print)", self.name);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== start_child_linked Demo ===\n");

    // Create supervisor with no initial children
    let spec = supervision_tree! {
        name: "demo-supervisor",
        strategy: OneForOne,
        intensity: (3, 5),
        workers: [],
        supervisors: []
    };

    let supervisor: SupervisorHandle<GoodWorker> = SupervisorHandle::start(spec);
    println!("✓ Supervisor started\n");

    // Test 1: Successfully initialize a worker
    println!("--- Test 1: Worker with successful initialization ---");
    match supervisor
        .start_child_linked(
            "good-worker",
            || GoodWorker {
                name: "GoodWorker".to_string(),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await
    {
        Ok(id) => println!("✓ Child '{}' started successfully\n", id),
        Err(e) => println!("✗ Failed to start child: {}\n", e),
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a new supervisor for BadWorker type
    let bad_spec = supervision_tree! {
        name: "bad-supervisor",
        strategy: OneForOne,
        intensity: (3, 5),
        workers: [],
        supervisors: []
    };
    let bad_supervisor: SupervisorHandle<BadWorker> = SupervisorHandle::start(bad_spec);

    // Test 2: Worker that fails during initialization
    println!("--- Test 2: Worker with initialization failure ---");
    match bad_supervisor
        .start_child_linked(
            "bad-worker",
            || BadWorker {
                name: "BadWorker".to_string(),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2),
        )
        .await
    {
        Ok(id) => println!("✓ Child '{}' started (unexpected!)\n", id),
        Err(e) => println!("✓ Correctly caught initialization error: {}\n", e),
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create a new supervisor for SlowWorker type
    let slow_spec = supervision_tree! {
        name: "slow-supervisor",
        strategy: OneForOne,
        intensity: (3, 5),
        workers: [],
        supervisors: []
    };
    let slow_supervisor: SupervisorHandle<SlowWorker> = SupervisorHandle::start(slow_spec);

    // Test 3: Worker that times out during initialization
    println!("--- Test 3: Worker with initialization timeout ---");
    match slow_supervisor
        .start_child_linked(
            "slow-worker",
            || SlowWorker {
                name: "SlowWorker".to_string(),
            },
            RestartPolicy::Permanent,
            Duration::from_secs(2), // Timeout after 2 seconds
        )
        .await
    {
        Ok(id) => println!("✓ Child '{}' started (unexpected!)\n", id),
        Err(e) => println!("✓ Correctly caught timeout: {}\n", e),
    }

    // Check which children are actually running
    println!("--- Final State ---");
    let children = supervisor.which_children().await?;
    println!("Supervisor has {} child(ren):", children.len());
    for child in children {
        println!(
            "  - {} ({})",
            child.id,
            if child.restart_policy == Some(RestartPolicy::Permanent) {
                "Permanent"
            } else {
                "Transient"
            }
        );
    }

    println!("\n=== Demo Complete ===");
    println!("Note: Only successfully initialized workers are added to the supervision tree.");
    println!("Init failures do NOT trigger restart policies.");

    Ok(())
}
