# ash-flare

[![Crates.io](https://img.shields.io/crates/v/ash-flare.svg)](https://crates.io/crates/ash-flare)
[![Documentation](https://docs.rs/ash-flare/badge.svg)](https://docs.rs/ash-flare)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust Version](https://img.shields.io/badge/rust-1.85%2B-blue.svg)](https://www.rust-lang.org)

Fault-tolerant supervision trees for Rust with distributed capabilities inspired by Erlang/OTP. Build resilient systems that automatically recover from failures with supervisor trees, restart strategies, and distributed supervision.

## Features

- **🌲 Supervision Trees**: Hierarchical supervision with nested supervisors and workers
- **🔄 Restart Strategies**: `OneForOne`, `OneForAll`, and `RestForOne` strategies
- **⚡ Restart Policies**: `Permanent`, `Temporary`, and `Transient` restart behaviors
- **📊 Restart Intensity**: Configurable restart limits with sliding time windows
- **🗂️ Stateful Workers**: Optional shared in-memory KV store for workers (`StatefulSupervisorSpec`)
- **🔗 Linked Startup**: Synchronous initialization with `start_child_linked` for reliable worker startup
- **📬 Mailbox System**: Simple message-passing between workers
- **🌐 Distributed**: Run supervisors across processes or machines via TCP/Unix sockets
- **🔌 Generic Workers**: Trait-based worker system for any async workload
- **🛠️ Dynamic Management**: Add/remove children at runtime
- **📝 Structured Logging**: Built-in support for `slog` structured logging

## Quick Start

Add to your `Cargo.toml`:

```bash
cargo add ash-flare
```

## Basic Example

```rust
use ash_flare::{SupervisorSpec, RestartPolicy, Worker};
use async_trait::async_trait;

// Define your worker
struct Counter {
    id: u32,
    max: u32,
}

#[async_trait]
impl Worker for Counter {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        for i in 0..self.max {
            println!("Counter {}: {}", self.id, i);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // Build supervisor tree
    let spec = SupervisorSpec::new("root")
        .with_worker("counter-1", || Counter { id: 1, max: 5 }, RestartPolicy::Permanent)
        .with_worker("counter-2", || Counter { id: 2, max: 5 }, RestartPolicy::Permanent);

    // Start supervision tree
    let handle = SupervisorHandle::start(spec);
    
    // Query children
    let children = handle.which_children().await.unwrap();
    println!("Running children: {}", children.len());
    
    // Graceful shutdown
    handle.shutdown().await.unwrap();
}
```

## Restart Strategies

### OneForOne

Restarts only the failed child (default):

```rust
use ash_flare::{SupervisorSpec, RestartStrategy};

let spec = SupervisorSpec::new("supervisor")
    .with_restart_strategy(RestartStrategy::OneForOne);
```

### OneForAll

Restarts all children if any child fails:

```rust
let spec = SupervisorSpec::new("supervisor")
    .with_restart_strategy(RestartStrategy::OneForAll);
```

### RestForOne

Restarts the failed child and all children started after it:

```rust
let spec = SupervisorSpec::new("supervisor")
    .with_restart_strategy(RestartStrategy::RestForOne);
```

## Restart Policies

Control when a child should be restarted:

```rust
use ash_flare::RestartPolicy;

// Always restart (default)
RestartPolicy::Permanent

// Never restart
RestartPolicy::Temporary

// Restart only on abnormal termination
RestartPolicy::Transient
```

## Nested Supervisors

Build hierarchical supervision trees:

```rust
let database_supervisor = SupervisorSpec::new("database")
    .with_worker("db-pool", || DbPool::new(), RestartPolicy::Permanent)
    .with_worker("db-cache", || DbCache::new(), RestartPolicy::Transient);

let app_supervisor = SupervisorSpec::new("app")
    .with_supervisor(database_supervisor)
    .with_worker("http-server", || HttpServer::new(), RestartPolicy::Permanent);

let handle = SupervisorHandle::start(app_supervisor);
```

## Restart Intensity

Configure maximum restart attempts within a time window:

```rust
use ash_flare::RestartIntensity;

let spec = SupervisorSpec::new("supervisor")
    .with_restart_intensity(RestartIntensity {
        max_restarts: 5,      // Maximum restarts
        within_seconds: 10,   // Within time window
    });
```

## Stateful Workers with Shared Store

Use `StatefulSupervisorSpec` for workers that need to share state via an in-memory KV store:

```rust
use ash_flare::{StatefulSupervisorSpec, StatefulSupervisorHandle, WorkerContext};
use std::sync::Arc;

struct AuctionWorker {
    id: u32,
    ctx: Arc<WorkerContext>,
}

#[async_trait]
impl Worker for AuctionWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Read from shared store
        let current_bid = self.ctx.get("highest_bid")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // Update shared store
        self.ctx.set("highest_bid", serde_json::json!(current_bid + 100));

        // Atomic update
        self.ctx.update("bid_count", |v| {
            let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
            Some(serde_json::json!(count + 1))
        });

        Ok(())
    }
}

// Create stateful supervisor (WorkerContext auto-initialized)
let spec = StatefulSupervisorSpec::new("auction-supervisor")
    .with_worker(
        "auction-worker",
        |ctx: Arc<WorkerContext>| AuctionWorker { id: 1, ctx },
        RestartPolicy::Permanent,
    );

let handle = StatefulSupervisorHandle::start(spec);
```

Or use the `stateful_supervision_tree!` macro for a more declarative approach:

```rust
use ash_flare::stateful_supervision_tree;

let spec = stateful_supervision_tree! {
    name: "auction-supervisor",
    strategy: OneForOne,
    intensity: (5, 10),
    workers: [
        ("bidder-1", |ctx| AuctionWorker::new(1, ctx), Permanent),
        ("bidder-2", |ctx| AuctionWorker::new(2, ctx), Permanent),
    ],
    supervisors: []
};
```

**WorkerContext API:**

- `get(key)` - Retrieve a value
- `set(key, value)` - Store a value
- `delete(key)` - Remove a key
- `update(key, fn)` - Atomic update with a function

The store is process-local, concurrent-safe (backed by `DashMap`), and persists across worker restarts.

## Dynamic Supervision

Add and remove children at runtime:

```rust
// Dynamically add a worker
let child_id = handle
    .start_child("dynamic-worker", || MyWorker::new(), RestartPolicy::Temporary)
    .await
    .unwrap();

// Terminate a specific child
handle.terminate_child(&child_id).await.unwrap();

// List all running children
let children = handle.which_children().await.unwrap();
```

## Linked Child Startup

Use `start_child_linked` to ensure workers initialize successfully before being added to the supervision tree:

```rust
use std::time::Duration;

#[async_trait]
impl Worker for DatabaseWorker {
    type Error = std::io::Error;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        // Connect to database
        self.connect_db().await?;
        // Verify connection
        self.ping().await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Main worker loop
        loop {
            // Process queries...
        }
    }
}

// Start worker with linked initialization
match handle
    .start_child_linked(
        "db-worker",
        || DatabaseWorker::new(),
        RestartPolicy::Permanent,
        Duration::from_secs(5),  // Initialization timeout
    )
    .await
{
    Ok(child_id) => println!("Worker {} started successfully", child_id),
    Err(e) => println!("Worker failed to initialize: {}", e),
}
```

**Key differences from `start_child`:**

- Waits for `initialize()` to complete before returning
- Returns error if initialization fails or times out
- Failed initialization does NOT trigger restart policies
- Worker is only added to the tree after successful initialization

**Error types:**

- `InitializationFailed` - Worker returned an error during initialization
- `InitializationTimeout` - Worker didn't initialize within the timeout
- `ChildAlreadyExists` - A child with this ID already exists

## Mailbox System

Workers can communicate using a simple mailbox system:

```rust
use ash_flare::{Worker, Mailbox, MailboxConfig, mailbox_named};

struct MessageWorker {
    id: usize,
    mailbox: Mailbox,
}

// Use the convenience macro for mailbox workers
ash_flare::impl_worker_mailbox! {
    MessageWorker, std::io::Error => |self, msg| {
        println!("[Worker {}] Received: {}", self.id, msg);
    }
}

// Create workers with mailboxes
let (handle1, mailbox1) = mailbox_named(MailboxConfig::default(), "worker-1");
let (handle2, mailbox2) = mailbox_named(MailboxConfig::default(), "worker-2");

// Workers can send messages to each other
handle2.send("Hello from worker 1").await?;
```

See `examples/mailbox_p2p.rs` and `examples/mailbox_pool.rs` for complete examples.

## Distributed Supervision

Run supervisors across processes or machines:

```rust
use ash_flare::distributed::{SupervisorServer, RemoteSupervisorHandle};

// Start supervisor server
let handle = SupervisorHandle::start(spec);
let server = SupervisorServer::new(handle);

tokio::spawn(async move {
    server.listen_tcp("127.0.0.1:8080").await.unwrap();
});

// Connect from another process/machine
let remote = RemoteSupervisorHandle::connect_tcp("127.0.0.1:8080").await.unwrap();
let children = remote.which_children().await.unwrap();
remote.shutdown().await.unwrap();
```

## Worker Lifecycle

Implement the `Worker` trait with optional lifecycle hooks:

```rust
use ash_flare::Worker;
use async_trait::async_trait;

struct MyWorker;

#[async_trait]
impl Worker for MyWorker {
    type Error = std::io::Error;

    async fn initialize(&mut self) -> Result<(), Self::Error> {
        // Called once before run()
        println!("Worker initializing...");
        Ok(())
    }

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Main worker loop
        loop {
            // Do work...
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        // Called during graceful shutdown
        println!("Worker shutting down...");
        Ok(())
    }
}
```

## Error Handling

Workers return errors that trigger restart policies:

```rust
#[async_trait]
impl Worker for MyWorker {
    type Error = MyError;

    async fn run(&mut self) -> Result<(), Self::Error> {
        match self.do_work().await {
            Ok(_) => Ok(()), // Normal termination
            Err(e) => Err(e), // Triggers restart based on policy
        }
    }
}
```

## Structured Logging

Ash Flare uses `slog` for structured logging. To see logs, set up a global logger:

```rust
use slog::{Drain, Logger, o};
use slog_async::Async;
use slog_term::{FullFormat, TermDecorator};

fn main() {
    // Set up logger
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = Async::new(drain).build().fuse();
    let logger = Logger::root(drain, o!());
    
    // Set as global logger
    let _guard = slog_scope::set_global_logger(logger);
    
    // Your supervision tree code here...
}
```

Logs include structured data for easy filtering:

```text
INFO server listening on tcp; address: "127.0.0.1:8080"
DEBUG child terminated; supervisor: "root", child: "worker-1", reason: Normal
ERROR restart intensity exceeded, shutting down; supervisor: "root"
```

## Examples

Check the `examples/` directory for more:

- `counter.rs` - Basic supervisor with multiple workers
- `start_link_demo.rs` - Linked child startup with initialization handshake
- `mailbox_p2p.rs` - Peer-to-peer worker communication with mailboxes
- `mailbox_pool.rs` - Worker pool sharing a mailbox
- `mailbox_macro.rs` - Using the mailbox worker macro
- `distributed.rs` - Network-distributed supervisors
- `super_tree.rs` - Complex nested supervision trees
- `interactive_demo.rs` - Interactive supervisor management

Run an example:

```bash
cargo run --example counter
cargo run --example start_link_demo
cargo run --example mailbox_p2p
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Inspired by Erlang/OTP's in some way.

Some code generated with the help of AI tools.
