//! Worker pool with shared mailbox example
//!
//! This example demonstrates:
//! - A pool of workers sharing one mailbox (clones of a `Mailbox` share a queue)
//! - Workers competing to process messages
//! - Simple string-based message passing
//! - Supervision of mailbox workers

use ash_flare::{
    Mailbox, RestartPolicy, SupervisorHandle, SupervisorSpec, Worker,
    mailbox::{MailboxConfig, mailbox},
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

/// Worker that processes messages from a shared mailbox
struct PoolWorker {
    worker_id: usize,
    mailbox: Mailbox,
}

impl PoolWorker {
    fn new(worker_id: usize, mailbox: Mailbox) -> Self {
        Self { worker_id, mailbox }
    }

    async fn process_message(&self, msg: String) -> Result<(), WorkerError> {
        println!("[Worker {}] Processing: {}", self.worker_id, msg);

        // Simulate work
        sleep(Duration::from_millis(100)).await;

        // Simulate occasional failure
        if msg.contains("fail") {
            println!("[Worker {}] Failed processing: {}", self.worker_id, msg);
            return Err(WorkerError(format!("Failed to process: {}", msg)));
        }

        println!("[Worker {}] Completed: {}", self.worker_id, msg);
        Ok(())
    }
}
impl Worker for PoolWorker {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Started", self.worker_id);

        loop {
            // Each message goes to whichever pool member asks first.
            match self.mailbox.recv().await {
                Some(msg) => {
                    self.process_message(msg).await?;
                }
                None => {
                    println!("[Worker {}] Mailbox closed, shutting down", self.worker_id);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[Worker {}] Shutting down", self.worker_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Worker Pool with Shared Mailbox Example ===\n");

    // Create a bounded mailbox for the worker pool
    let (handle, mailbox) = mailbox(MailboxConfig::bounded(20));

    // A pool of 4 workers sharing the mailbox; restarted members get a clone
    // of the same mailbox, so queued messages are not lost.
    let spec = SupervisorSpec::new("worker-pool").with_worker_pool(
        "worker",
        4,
        move |index| PoolWorker::new(index + 1, mailbox.clone()),
        RestartPolicy::Permanent,
    );

    let supervisor = SupervisorHandle::start(spec).expect("valid supervisor spec");

    // Wait for workers to start
    sleep(Duration::from_millis(100)).await;

    // Send messages to the shared mailbox
    println!("Sending messages to worker pool...\n");

    for i in 1..=10 {
        let msg = if i == 5 {
            format!("task-{} (should fail)", i)
        } else {
            format!("task-{}", i)
        };

        handle.send(msg).await?;
    }

    // Let workers process messages
    sleep(Duration::from_secs(2)).await;

    // Check supervisor status
    println!("\n=== Supervisor Status ===");
    let children = supervisor.which_children().await?;
    for child in children {
        println!("Child: {} (type: {:?})", child.id, child.child_type);
    }

    // Shutdown
    println!("\n=== Shutting Down ===");
    supervisor.shutdown().await?;

    println!("\nExample completed!");
    Ok(())
}
