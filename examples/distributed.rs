//! Minimal example: Distributed supervision with Unix sockets
//!
//! This example demonstrates running a supervisor in a detached mode
//! where it can be controlled via Unix socket commands.
//!
//! Note: This example requires Unix sockets and will not compile on Windows.

#[cfg(unix)]
use ash_flare::distributed::{RemoteSupervisorHandle, SupervisorAddress, SupervisorServer};
#[cfg(unix)]
use ash_flare::{RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker};

#[cfg(unix)]
#[cfg(unix)]
use std::time::Duration;
#[cfg(unix)]
use tokio::time::sleep;

#[cfg(unix)]
#[derive(Debug)]
struct WorkerError(String);

#[cfg(unix)]
impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(unix)]
impl std::error::Error for WorkerError {}

#[cfg(unix)]
struct Counter {
    name: String,
}

#[cfg(unix)]
impl Counter {
    fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[cfg(unix)]
impl Worker for Counter {
    type Error = WorkerError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let mut count = 0;
        loop {
            count += 1;
            println!("[{}] count: {}", self.name, count);
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        println!("[{}] shutdown", self.name);
        Ok(())
    }
}

/// Runs the supervisor server in this process
#[cfg(unix)]
async fn run_supervisor_server(socket_path: String) {
    let spec = SupervisorSpec::new("remote_supervisor")
        .with_restart_strategy(RestartStrategy::OneForOne)
        .with_worker(
            "worker1",
            || Counter::new("worker1"),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker2",
            || Counter::new("worker2"),
            RestartPolicy::Permanent,
        )
        .with_worker(
            "worker3",
            || Counter::new("worker3"),
            RestartPolicy::Permanent,
        );

    let supervisor = SupervisorHandle::start(spec);

    let server = SupervisorServer::new(supervisor);

    println!("\n🚀 Supervisor server starting...\n");
    if let Err(e) = server.listen_unix(&socket_path).await {
        eprintln!("Server error: {}", e);
    }
}

/// Runs the client that controls the remote supervisor
#[cfg(unix)]
async fn run_client(socket_path: String) {
    sleep(Duration::from_secs(1)).await; // Wait for server to start

    let remote = RemoteSupervisorHandle::new(SupervisorAddress::Unix(socket_path));

    println!("\n📡 Client connecting to remote supervisor...\n");

    // Get status
    println!("🔍 Getting supervisor status...");
    match remote.status().await {
        Ok(status) => {
            println!("   Name: {}", status.name);
            println!("   Children: {}", status.children_count);
            println!("   Strategy: {:?}", status.restart_strategy);
            println!("   Uptime: {}s", status.uptime_secs);
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    sleep(Duration::from_secs(2)).await;

    // List children
    println!("\n📋 Listing children...");
    match remote.which_children().await {
        Ok(children) => {
            for child in children {
                println!("   - {} ({:?})", child.id, child.child_type);
            }
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    sleep(Duration::from_secs(3)).await;

    // Terminate a child
    println!("\n🔧 Terminating worker2...");
    match remote.terminate_child("worker2").await {
        Ok(_) => println!("   ✓ Terminated"),
        Err(e) => eprintln!("   Error: {}", e),
    }

    sleep(Duration::from_secs(2)).await;

    // List children again
    println!("\n📋 Listing children after termination...");
    match remote.which_children().await {
        Ok(children) => {
            for child in children {
                println!("   - {} ({:?})", child.id, child.child_type);
            }
        }
        Err(e) => eprintln!("   Error: {}", e),
    }

    sleep(Duration::from_secs(2)).await;

    // Shutdown
    println!("\n🛑 Shutting down remote supervisor...");
    match remote.shutdown().await {
        Ok(_) => println!("   ✓ Shutdown complete"),
        Err(e) => eprintln!("   Error: {}", e),
    }
}

#[tokio::main]
#[cfg(unix)]
async fn main() {
    let socket_path = "/tmp/ash-flare-supervisor.sock";

    // Check if we should run as server or client
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 && args[1] == "server" {
        run_supervisor_server(socket_path.to_string()).await;
    } else {
        println!("=== Distributed Supervisor Demo ===");
        println!("\nℹ️  To run this demo:");
        println!("   Terminal 1: cargo run --example distributed server");
        println!("   Terminal 2: cargo run --example distributed\n");

        // For demo purposes, spawn both in same process
        tokio::spawn(run_supervisor_server(socket_path.to_string()));
        run_client(socket_path.to_string()).await;

        sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("This example requires Unix sockets and is not supported on Windows.");
    eprintln!("Please run this example on Linux or macOS.");
    std::process::exit(1);
}
