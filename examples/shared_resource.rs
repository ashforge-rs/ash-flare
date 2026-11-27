//! Shared resource access example - multiple workers sharing a resource pool

use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

#[derive(Debug)]
struct SharedResource {
    name: String,
    counter: usize,
    data: Vec<String>,
}

impl SharedResource {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            counter: 0,
            data: Vec::new(),
        }
    }

    fn access(&mut self, worker_id: usize, operation: &str) -> usize {
        self.counter += 1;
        self.data
            .push(format!("Worker {} - {}", worker_id, operation));
        println!(
            "  [Resource '{}'] Access #{} by Worker {} - {}",
            self.name, self.counter, worker_id, operation
        );
        self.counter
    }

    fn get_stats(&self) -> (usize, usize) {
        (self.counter, self.data.len())
    }
}

struct ResourceWorker {
    worker_id: usize,
    resource: Arc<Mutex<SharedResource>>,
    operations: Vec<String>,
}

impl ResourceWorker {
    fn new(worker_id: usize, resource: Arc<Mutex<SharedResource>>) -> Self {
        let operations = vec![
            "read data".to_string(),
            "write data".to_string(),
            "update record".to_string(),
            "process query".to_string(),
        ];

        Self {
            worker_id,
            resource,
            operations,
        }
    }

    async fn perform_operation(&self, op_num: usize) -> Result<(), WorkerError> {
        let operation = &self.operations[op_num % self.operations.len()];

        println!(
            "[Worker {}] Requesting access for: {}",
            self.worker_id, operation
        );

        let mut resource = self.resource.lock().await;
        let access_count = resource.access(self.worker_id, operation);
        drop(resource);

        sleep(Duration::from_millis(300)).await;

        if self.worker_id == 1 && op_num == 3 {
            return Err(WorkerError(format!(
                "Worker {} crashed during operation",
                self.worker_id
            )));
        }

        if self.worker_id == 2 && op_num == 5 {
            return Err(WorkerError(format!(
                "Worker {} encountered error",
                self.worker_id
            )));
        }

        println!(
            "[Worker {}] ✓ Completed: {} (total accesses: {})",
            self.worker_id, operation, access_count
        );
        Ok(())
    }
}
impl Worker for ResourceWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Initialized and ready", self.worker_id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        for i in 0..6 {
            self.perform_operation(i).await?;
            sleep(Duration::from_millis(400)).await;
        }

        println!("[Worker {}] All operations complete", self.worker_id);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Shutting down", self.worker_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Shared Resource Access Example ===\n");

    let resource = Arc::new(Mutex::new(SharedResource::new("Database")));

    let resource_1 = Arc::clone(&resource);
    let resource_2 = Arc::clone(&resource);

    let spec = SupervisorSpec::new("resource-supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        // The workers here fail on purpose, more often than the default of 3
        // restarts in 5 seconds allows; past that the supervisor would give up.
        .with_restart_intensity(RestartIntensity::new(10, 5))
        .with_worker(
            "worker-1",
            move || ResourceWorker::new(1, Arc::clone(&resource_1)),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker-2",
            move || ResourceWorker::new(2, Arc::clone(&resource_2)),
            RestartPolicy::Permanent,
        );

    let supervisor = SupervisorHandle::start(spec).expect("valid supervisor spec");

    println!("Started 2 workers sharing resource\n");

    sleep(Duration::from_secs(12)).await;

    println!("\n=== Final Resource Stats ===");
    let resource = resource.lock().await;
    let (total_accesses, total_ops) = resource.get_stats();
    println!("Total accesses: {}", total_accesses);
    println!("Total operations recorded: {}", total_ops);
    drop(resource);

    println!("\n=== Shutting down supervisor ===");
    supervisor.shutdown().await?;

    println!("\n=== Example completed ===");
    Ok(())
}
