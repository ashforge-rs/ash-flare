//! Worker-to-worker communication example
//!
//! This example demonstrates:
//! - Direct communication between workers using mailboxes
//! - Worker A sends tasks to Worker B
//! - Worker B processes and sends results back to Worker A
//! - Simple string-based peer-to-peer messaging
//!
//! Note: This example uses simple worker spawning without supervision
//! to keep the mailbox communication pattern clear.

use ash_flare::mailbox::{Mailbox, MailboxConfig, MailboxHandle, mailbox};
use std::time::Duration;
use tokio::time::sleep;

/// Worker A - Sends tasks to Worker B and receives replies
async fn worker_a(mut mailbox: Mailbox, worker_b_handle: MailboxHandle) {
    println!("[Worker A] Started");

    while let Some(msg) = mailbox.recv().await {
        println!("[Worker A] Received: {}", msg);

        // Check for replies from Worker B (prefixed with "result:")
        if msg.starts_with("result:") {
            println!("[Worker A] Got result from Worker B: {}", msg);
        } else {
            // Forward task to Worker B
            let task = format!("process:{}", msg);
            println!("[Worker A] Sending task to Worker B: {}", task);

            if let Err(e) = worker_b_handle.send(task).await {
                println!("[Worker A] Error sending to Worker B: {}", e);
                break;
            }
        }
    }

    println!("[Worker A] Mailbox closed, shutting down");
}

/// Worker B - Processes tasks from Worker A and sends results back
async fn worker_b(mut mailbox: Mailbox, worker_a_handle: MailboxHandle) {
    println!("[Worker B] Started");

    while let Some(msg) = mailbox.recv().await {
        println!("[Worker B] Received: {}", msg);

        // Process tasks from Worker A
        if msg.starts_with("process:") {
            let task = msg.strip_prefix("process:").unwrap_or(&msg);

            // Simulate processing
            sleep(Duration::from_millis(100)).await;

            let result = format!("result: completed '{}'", task);
            println!("[Worker B] Sending result to Worker A: {}", result);

            if let Err(e) = worker_a_handle.send(result).await {
                println!("[Worker B] Error sending to Worker A: {}", e);
                break;
            }
        }
    }

    println!("[Worker B] Mailbox closed, shutting down");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Worker-to-Worker Communication Example ===\n");

    // Create mailboxes for both workers
    let (handle_a, mailbox_a) = mailbox(MailboxConfig::bounded(10));
    let (handle_b, mailbox_b) = mailbox(MailboxConfig::bounded(10));

    // Clone handles for cross-communication
    let worker_a_handle = handle_a.clone();
    let worker_b_handle = handle_b.clone();

    // Spawn worker tasks
    let worker_a_task = tokio::spawn(worker_a(mailbox_a, handle_b.clone()));
    let worker_b_task = tokio::spawn(worker_b(mailbox_b, handle_a.clone()));

    // Wait for workers to start
    sleep(Duration::from_millis(100)).await;

    // Send initial tasks to Worker A
    println!("Sending tasks to Worker A...\n");

    for i in 1..=5 {
        let msg = format!("task-{}", i);
        worker_a_handle.send(msg).await?;
        sleep(Duration::from_millis(300)).await;
    }

    // Let workers finish processing
    sleep(Duration::from_secs(1)).await;

    // Close mailboxes to signal shutdown
    println!("\n=== Shutting Down ===");
    drop(handle_a);
    drop(handle_b);
    drop(worker_a_handle);
    drop(worker_b_handle);

    // Wait for workers to finish
    let _ = tokio::join!(worker_a_task, worker_b_task);

    println!("\nExample completed!");
    Ok(())
}
