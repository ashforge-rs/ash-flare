//! Example demonstrating the impl_worker_mailbox! macro
//!
//! This shows how to use the macro to create workers with mailboxes

use ash_flare::{
    Mailbox, Worker, impl_worker_mailbox,
    mailbox::{MailboxConfig, mailbox},
};

struct EchoWorker {
    id: usize,
    mailbox: Mailbox,
}

// Use the macro to implement Worker trait for a mailbox-based worker
impl_worker_mailbox! {
    EchoWorker, std::io::Error => |self, msg| {
        println!("[Echo Worker {}] Received: {}", self.id, msg);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Mailbox Macro Example ===\n");

    // Create mailboxes for workers
    let (handle1, mailbox1) = mailbox(MailboxConfig::bounded(10));
    let (handle2, mailbox2) = mailbox(MailboxConfig::bounded(10));

    // Create workers
    let mut worker1 = EchoWorker {
        id: 1,
        mailbox: mailbox1,
    };

    let mut worker2 = EchoWorker {
        id: 2,
        mailbox: mailbox2,
    };

    // Spawn worker tasks
    let task1 = tokio::spawn(async move { worker1.run().await });

    let task2 = tokio::spawn(async move { worker2.run().await });

    // Send messages to workers
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("Sending messages...\n");
    handle1.send("Hello from worker 1".to_string()).await?;
    handle2.send("Hello from worker 2".to_string()).await?;
    handle1
        .send("Another message for worker 1".to_string())
        .await?;
    handle2
        .send("Another message for worker 2".to_string())
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Close mailboxes to signal shutdown
    drop(handle1);
    drop(handle2);

    // Wait for workers
    let _ = tokio::join!(task1, task2);

    println!("\nExample completed!");
    Ok(())
}
