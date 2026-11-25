//! One worker per connection, started under ids the supervisor picks.
//!
//! This example demonstrates:
//! - `SupervisorHandle::start_unique_child`, like a child of OTP's
//!   `simple_one_for_one` supervisor: each child gets an id `{prefix}-{n}` that
//!   no current child uses, and per-child arguments are whatever the factory
//!   closure captures
//! - `RestartPolicy::Temporary` handlers that leave the tree when they finish,
//!   whether they succeed or fail
//!
//! An acceptor loop simulates incoming connections and hands each one to a new
//! handler. One client sends a malformed request, and its handler fails.
//!
//! Run with: `cargo run --example connection_handlers`

use ash_flare::{
    ChildSpec, RestartPolicy, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker,
};
use std::time::Duration;
use tokio::time::sleep;

/// Serves one connection, then exits.
struct ConnectionHandler {
    peer: String,
    requests: u32,
}

impl Worker for ConnectionHandler {
    type Error = String;

    async fn run(&mut self) -> Result<(), Self::Error> {
        for _ in 0..self.requests {
            sleep(Duration::from_millis(100)).await;
        }
        if self.peer.ends_with(":666") {
            return Err(format!("malformed request from {}", self.peer));
        }
        Ok(())
    }
}

async fn print_children(supervisor: &SupervisorHandle) -> Result<(), Box<dyn std::error::Error>> {
    let ids: Vec<_> = supervisor
        .which_children()
        .await?
        .into_iter()
        .map(|child| child.id)
        .collect();
    println!("  active handlers: {ids:?}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Connection handlers ===\n");

    // Starts empty; children are added as connections arrive.
    let supervisor = SupervisorHandle::start(SupervisorSpec::new("connections"))?;
    let mut events = supervisor.subscribe();
    let watcher = tokio::spawn(async move {
        while let Ok(event) = events.recv().await {
            match event {
                SupervisorEvent::ChildTerminated { id, reason } => {
                    println!("  {id} finished ({reason:?}), removed from the tree");
                }
                SupervisorEvent::Stopped { .. } => break,
                _ => {}
            }
        }
    });

    let incoming = [
        ("10.0.0.1:5001", 3),
        ("10.0.0.2:5002", 6),
        ("10.0.0.3:666", 2),
        ("10.0.0.4:5004", 4),
    ];
    for (peer, requests) in incoming {
        let handler = ChildSpec::new("conn", move || ConnectionHandler {
            peer: peer.to_owned(),
            requests,
        })
        .restart(RestartPolicy::Temporary);
        let id = supervisor.start_unique_child(handler).await?;
        println!("accepted {peer:<14} -> {id}");
    }
    print_children(&supervisor).await?;

    sleep(Duration::from_millis(450)).await;
    print_children(&supervisor).await?;

    // Handlers can be started at any time; each gets a fresh id.
    let id = supervisor
        .start_unique_child(
            ChildSpec::new("conn", || ConnectionHandler {
                peer: "10.0.0.5:5005".to_owned(),
                requests: 1,
            })
            .restart(RestartPolicy::Temporary),
        )
        .await?;
    println!("accepted 10.0.0.5:5005  -> {id}");

    sleep(Duration::from_millis(400)).await;
    print_children(&supervisor).await?;

    supervisor.shutdown().await?;
    watcher.await?;
    Ok(())
}
