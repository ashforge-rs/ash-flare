//! Elastic worker pool example - scaling workers dynamically based on workload
//!
//! A pool manager adds workers as work arrives with
//! `SupervisorHandle::start_unique_child`, which picks a free id
//! (`dynamic-worker-0`, `dynamic-worker-1`, ...) so the manager does not have
//! to keep a counter of its own.

use ash_flare::{
    ChildSpec, RestartPolicy, SupervisorHandle, impl_worker_stateful, supervision_tree,
};
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

#[derive(Clone)]
struct TaskWorker {
    task_type: String,
}

impl TaskWorker {
    fn new(task_type: impl Into<String>) -> Self {
        Self {
            task_type: task_type.into(),
        }
    }
}

impl_worker_stateful! {
    TaskWorker, WorkerError => |self| {
        println!("[{}] Started", self.task_type);

        for i in 0..8 {
            sleep(Duration::from_millis(800)).await;
            println!("[{}] Processing task #{}", self.task_type, i + 1);
        }
        println!("[{}] Work completed", self.task_type);
        Ok(())
    }
}

#[derive(Clone)]
struct WorkloadMonitorWrapper {
    supervisor: SupervisorHandle,
}

impl WorkloadMonitorWrapper {
    fn new(supervisor: SupervisorHandle) -> Self {
        Self { supervisor }
    }

    async fn add_worker(&self, task_type: &str) -> Result<(), WorkerError> {
        let task_type_owned = task_type.to_string();
        let worker = ChildSpec::new("dynamic-worker", move || {
            TaskWorker::new(task_type_owned.clone())
        })
        .restart(RestartPolicy::Transient);

        // The spec's id is a prefix; the supervisor returns the id it chose.
        let id = self
            .supervisor
            .start_unique_child(worker)
            .await
            .map_err(|e| WorkerError(format!("Failed to start worker: {}", e)))?;

        println!(
            "\n[Pool Manager] ➕ Scaling up: started {} for {}",
            id, task_type
        );
        Ok(())
    }

    async fn check_workload(&self) -> Result<usize, WorkerError> {
        let children = self
            .supervisor
            .which_children()
            .await
            .map_err(|e| WorkerError(format!("Failed to get children: {}", e)))?;

        println!(
            "\n[Pool Manager] Pool size: {} active workers (excluding manager)",
            children.len() - 1
        );
        Ok(children.len() - 1)
    }
}

impl_worker_stateful! {
    WorkloadMonitorWrapper, WorkerError => |self| {
        println!("[Pool Manager] Starting elastic pool management\n");

        sleep(Duration::from_secs(1)).await;

        self.add_worker("data-processing").await?;
        sleep(Duration::from_secs(2)).await;

        self.add_worker("image-processing").await?;
        sleep(Duration::from_secs(2)).await;

        self.check_workload().await?;
        sleep(Duration::from_secs(2)).await;

        self.add_worker("video-encoding").await?;
        sleep(Duration::from_secs(2)).await;

        self.add_worker("report-generation").await?;
        sleep(Duration::from_secs(2)).await;

        self.check_workload().await?;

        println!("\n[Pool Manager] All workers scaled up, monitoring pool...\n");
        sleep(Duration::from_secs(5)).await;

        println!("[Pool Manager] Pool management complete");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Elastic Worker Pool ===\n");

    let spec = supervision_tree! {
        name: "elastic-pool",
        strategy: OneForOne,
        intensity: (5, 10),
        workers: [],
        supervisors: []
    };

    let supervisor: SupervisorHandle =
        SupervisorHandle::start(spec).expect("valid supervisor spec");
    println!("Started elastic worker pool\n");

    let sup_clone = supervisor.clone();
    supervisor
        .start_child(
            "pool-manager",
            move || WorkloadMonitorWrapper::new(sup_clone.clone()),
            RestartPolicy::Permanent,
        )
        .await?;

    sleep(Duration::from_secs(18)).await;

    println!("\n=== Pool Statistics ===");
    let children = supervisor.which_children().await?;
    println!("Total workers in pool: {}", children.len());
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

    println!("\n=== Shutting down elastic pool ===");
    supervisor.shutdown().await?;

    println!("\n=== Example completed ===");
    Ok(())
}
