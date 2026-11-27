//! Worker queue example - multiple workers processing items from a shared queue
//!
//! This example demonstrates:
//! - Multiple workers processing tasks from a shared queue
//! - Using channels for communication between supervisor and workers
//! - Worker restart on failure
//! - Graceful shutdown when queue is empty

use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(Debug)]
struct WorkerError(String);

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for WorkerError {}

/// A worker that processes tasks from a shared queue
struct QueueWorker {
    worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Task>>>,
}

#[derive(Debug, Clone)]
struct Task {
    id: usize,
    data: String,
    should_fail: bool,
}

impl QueueWorker {
    fn new(worker_id: usize, rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Task>>>) -> Self {
        Self { worker_id, rx }
    }

    async fn process_task(&self, task: Task) -> Result<(), WorkerError> {
        println!(
            "[Worker {}] Processing task #{}: {}",
            self.worker_id, task.id, task.data
        );

        // Simulate some work
        sleep(Duration::from_millis(500 + (task.id as u64 * 100))).await;

        if task.should_fail {
            println!("[Worker {}] Task #{} FAILED!", self.worker_id, task.id);
            return Err(WorkerError(format!(
                "Task {} failed intentionally",
                task.id
            )));
        }

        println!("[Worker {}] Task #{} completed ✓", self.worker_id, task.id);
        Ok(())
    }
}
impl Worker for QueueWorker {
    type Error = WorkerError;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Initialized and ready", self.worker_id);
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            // Try to get a task from the queue
            let task = {
                let mut rx = self.rx.lock().await;
                rx.recv().await
            };

            match task {
                Some(task) => {
                    self.process_task(task).await?;
                }
                None => {
                    // Channel closed, no more tasks
                    println!("[Worker {}] Queue closed, shutting down", self.worker_id);
                    return Ok(());
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Shutting down", self.worker_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Worker Queue Example ===\n");

    // Create a channel for the task queue
    let (tx, rx) = mpsc::channel::<Task>(32);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    // Prepare tasks
    let tasks = vec![
        Task {
            id: 1,
            data: "Process order #1001".to_string(),
            should_fail: false,
        },
        Task {
            id: 2,
            data: "Send email notification".to_string(),
            should_fail: false,
        },
        Task {
            id: 3,
            data: "Generate report".to_string(),
            should_fail: true, // This task will fail
        },
        Task {
            id: 4,
            data: "Update database".to_string(),
            should_fail: false,
        },
        Task {
            id: 5,
            data: "Archive old records".to_string(),
            should_fail: false,
        },
        Task {
            id: 6,
            data: "Process order #1002".to_string(),
            should_fail: false,
        },
        Task {
            id: 7,
            data: "Backup data".to_string(),
            should_fail: false,
        },
        Task {
            id: 8,
            data: "Clean up cache".to_string(),
            should_fail: false,
        },
    ];

    // Create supervisor with worker pool
    let num_workers = 3;
    let mut supervisor_spec = SupervisorSpec::new("queue-supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_restart_intensity(RestartIntensity {
            max_restarts: 5,
            within_seconds: 10,
        });

    // Add workers to the supervisor
    for worker_id in 0..num_workers {
        let rx_clone = Arc::clone(&rx);
        supervisor_spec = supervisor_spec.with_worker(
            format!("worker-{}", worker_id),
            move || QueueWorker::new(worker_id, Arc::clone(&rx_clone)),
            RestartPolicy::Transient, // Restart only on abnormal termination
        );
    }

    let supervisor = SupervisorHandle::start(supervisor_spec).expect("valid supervisor spec");
    println!("Started supervisor with {} workers\n", num_workers);

    // Enqueue all tasks
    println!("Enqueueing {} tasks...\n", tasks.len());
    for task in tasks {
        tx.send(task).await?;
    }

    // Close the sender to signal no more tasks
    drop(tx);

    // Wait a bit for processing to complete
    sleep(Duration::from_secs(8)).await;

    // Shutdown supervisor
    println!("\n=== Shutting down supervisor ===");
    supervisor.shutdown().await?;

    println!("\n=== Example completed ===");
    Ok(())
}
