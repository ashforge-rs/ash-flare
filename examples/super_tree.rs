//! Minimalistic example showing nested supervisor trees
//! Demonstrates infinite depth supervision hierarchies

use ash_flare::{RestartPolicy, RestartStrategy, SupervisorSpec, Worker};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

struct SimpleWorker {
    name: String,
    fail_after: u32,
    counter: u32,
}

impl SimpleWorker {
    fn new(name: impl Into<String>, fail_after: u32) -> Self {
        Self {
            name: name.into(),
            fail_after,
            counter: 0,
        }
    }
}
impl Worker for SimpleWorker {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            self.counter += 1;
            println!("[{}] tick #{}", self.name, self.counter);

            if self.counter >= self.fail_after {
                return Err(WorkerError(format!(
                    "{} failed after {} ticks",
                    self.name, self.counter
                )));
            }

            sleep(Duration::from_millis(800)).await;
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== Nested Supervisor Tree Example ===\n");

    // Level 3: Leaf workers
    let leaf_supervisor_1 = SupervisorSpec::new("leaf-1")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-1a",
            || SimpleWorker::new("worker-1a", 5),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker-1b",
            || SimpleWorker::new("worker-1b", 7),
            RestartPolicy::Permanent,
        );

    let leaf_supervisor_2 = SupervisorSpec::new("leaf-2")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker-2a",
            || SimpleWorker::new("worker-2a", 6),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker-2b",
            || SimpleWorker::new("worker-2b", 8),
            RestartPolicy::Permanent,
        );

    // Level 2: Branch supervisors containing leaf supervisors
    let branch_supervisor = SupervisorSpec::new("branch")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(leaf_supervisor_1)
        .with_supervisor(leaf_supervisor_2)
        .with_worker(
            "branch-worker",
            || SimpleWorker::new("branch-worker", 10),
            RestartPolicy::Permanent,
        );

    // Level 1: Root supervisor
    let root = SupervisorSpec::new("root")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_supervisor(branch_supervisor)
        .with_worker(
            "root-worker",
            || SimpleWorker::new("root-worker", 12),
            RestartPolicy::Permanent,
        );

    println!("Tree structure:");
    println!("  root");
    println!("  ├─ branch (supervisor)");
    println!("  │  ├─ leaf-1 (supervisor)");
    println!("  │  │  ├─ worker-1a");
    println!("  │  │  └─ worker-1b");
    println!("  │  ├─ leaf-2 (supervisor)");
    println!("  │  │  ├─ worker-2a");
    println!("  │  │  └─ worker-2b");
    println!("  │  └─ branch-worker");
    println!("  └─ root-worker");
    println!();

    let handle = ash_flare::SupervisorHandle::start(root);

    // Let it run and watch restarts
    sleep(Duration::from_secs(15)).await;

    println!("\n=== Querying tree ===\n");
    if let Ok(children) = handle.which_children().await {
        for child in children {
            println!(
                "  {:?}: {} ({:?})",
                child.child_type, child.id, child.restart_policy
            );
        }
    }

    println!("\n=== Shutting down ===\n");
    let _ = handle.shutdown().await;
    sleep(Duration::from_millis(500)).await;
}
