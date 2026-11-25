# ash-flare

Fault-tolerant supervision trees for Rust, modelled on Erlang/OTP, with Python bindings.

## Features

- Supervision trees with `OneForOne`, `OneForAll` and `RestForOne` strategies
- `Permanent`, `Transient` and `Temporary` restart policies, restart intensity and exponential backoff
- Workers of different types in one tree; panics are supervised like any other failure
- OTP shutdown semantics: reverse start order, per-child `BrutalKill` / `Timeout` / `Infinity`
- Significant children and `auto_shutdown`
- Nested supervisors reachable by registered name, and a policy for when the whole tree gives up
- Lossless monitors, links between children, and killing children
- Typed mailboxes with call/cast that survive restarts, and a name registry
- Worker pools and children started under generated ids
- Isolated workers and stall detection for blocking or CPU-bound work
- Remote control over TCP or Unix sockets with a versioned, authenticated handshake
- Python bindings

## Installation

### Rust

```bash
cargo add ash-flare
```

### Python

Wheels are published on [GitHub Releases](https://github.com/ashforge-rs/ash-flare/releases):

```bash
uv add "ash-flare @ https://github.com/ashforge-rs/ash-flare/releases/download/vVERSION/ash_flare-VERSION-cp39-abi3-PLATFORM.whl"
```

## Quick start

```rust
use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker};
use std::time::Duration;

struct Ticker;

impl Worker for Ticker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let spec = SupervisorSpec::new("app")
    .with_worker("ticker", || Ticker, RestartPolicy::Permanent);

let handle = SupervisorHandle::start(spec)?;
handle.shutdown().await?;
# Ok(())
# }
```

`Worker` methods are plain `async fn`s; `Error` only needs `Display`, so `anyhow::Error` or `String` work.
The factory builds a fresh worker on every restart. A factory that takes a `ShutdownSignal` lets the worker
observe cooperative shutdown. `start` rejects an invalid tree, for example one with duplicate child ids.

## Supervision

```rust
use ash_flare::{
    AutoShutdown, ChildSpec, RestartBackoff, RestartIntensity, RestartPolicy, RestartStrategy,
    Shutdown, SupervisorSpec, Worker,
};
use std::time::Duration;

# struct Job;
# impl Worker for Job {
#     type Error = String;
#     async fn run(&mut self) -> Result<(), Self::Error> { Ok(()) }
# }
let spec = SupervisorSpec::new("app")
    .with_restart_strategy(RestartStrategy::RestForOne)
    .with_restart_intensity(RestartIntensity::new(5, 10))
    .with_restart_backoff(RestartBackoff::exponential(
        Duration::from_millis(100),
        Duration::from_secs(30),
    ))
    .with_auto_shutdown(AutoShutdown::AnySignificant)
    .with_child(
        ChildSpec::new("job", || Job)
            .restart(RestartPolicy::Transient)
            .shutdown(Shutdown::Timeout(Duration::from_secs(10)))
            .significant(true),
    );
```

Children are stopped in reverse start order, each within its own shutdown setting
(`ShutdownOrder::Concurrent` stops them together). A worker's `shutdown` hook runs unless it is
killed or overruns its timeout. Nested supervisors are added with `with_supervisor`.

## The top of the tree

```rust
use ash_flare::{ChildExitReason, GiveUp, Registry, SupervisorHandle, SupervisorSpec};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let registry = Registry::new();
let app = SupervisorHandle::start(
    SupervisorSpec::new("app")
        // When the whole tree fails, exit so systemd or Kubernetes restarts the process.
        .with_give_up(GiveUp::ExitProcess(1))
        .with_supervisor(SupervisorSpec::new("rooms").with_registered_name(&registry, "rooms")),
)?;

// A nested supervisor, by registered name (always its current run) or by child id.
let rooms = registry.whereis::<SupervisorHandle>("rooms").unwrap();
let same = app.child_supervisor("rooms").await?;
# assert_eq!(rooms.name(), same.name());

app.shutdown().await?;
assert_eq!(app.wait().await, ChildExitReason::Shutdown);
# Ok(())
# }
```

A nested supervisor that gives up escalates to its parent. A top-level one stops, and `wait()` tells
whoever holds its handle why; with `GiveUp::ExitProcess` it also exits the process once the tree is
down, as an OTP node does when a permanent application fails. `with_registered_name` registers each
run of a supervisor before its children start, so workers can find their own or a sibling supervisor
by name, as with `{local, Name}` in OTP.

## Messaging

```rust
use ash_flare::{MailboxConfig, MailboxHandle, Registry, Reply, mailbox};
use std::time::Duration;

enum Counter {
    Add(u64),
    Get(Reply<u64>),
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let (handle, inbox) = mailbox::<Counter>(MailboxConfig::default());
tokio::spawn(async move {
    let mut total = 0;
    while let Some(message) = inbox.recv().await {
        match message {
            Counter::Add(n) => total += n,
            Counter::Get(reply) => reply.send(total),
        }
    }
});

let registry = Registry::new();
let _name = registry.register("counter", handle);

let counter = registry.whereis::<MailboxHandle<Counter>>("counter").unwrap();
counter.send(Counter::Add(2)).await?;
assert_eq!(counter.call(Counter::Get, Duration::from_secs(1)).await?, 2);
# Ok(())
# }
```

`send` is a cast; `call` sends a request carrying a `Reply` and waits for the answer. Clones of a
`Mailbox` share one queue: give each worker instance a clone and its mailbox survives restarts, or
give clones to several workers to form a pool. `Registry` finds handles by name; a `Registration`
unregisters when dropped.

## Monitoring and links

```rust
use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorEvent, SupervisorHandle, SupervisorSpec, Worker, link};

# struct Service(ShutdownSignal);
# impl Worker for Service {
#     type Error = std::io::Error;
#     async fn run(&mut self) -> Result<(), Self::Error> { self.0.cancelled().await; Ok(()) }
# }
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let db = SupervisorHandle::start(SupervisorSpec::new("db").with_worker("pool", Service, RestartPolicy::Permanent))?;
let web = SupervisorHandle::start(SupervisorSpec::new("web").with_worker("cache", Service, RestartPolicy::Permanent))?;

let mut pool = db.monitor_child("pool");
let _link = link(&db, "pool", &web, "cache");

db.kill_child("pool").await?;
assert!(matches!(pool.recv().await, Some(SupervisorEvent::ChildTerminated { .. })));
# db.shutdown().await?;
# web.shutdown().await?;
# Ok(())
# }
```

Every started child is reported terminated exactly once. `monitor()` and `monitor_child(id)` never
miss an event: events queue until read, as with `erlang:monitor/2`. `subscribe()` is a bounded
broadcast for logs and metrics, where a reader that falls far behind skips the oldest events.
Linked children share their fate: when one ends other than normally, the other is killed and
restarted by its own supervisor. Links are built on monitors, so none misses a termination.

## Blocking work

Async tasks cannot be preempted. A worker that blocks or computes without awaiting holds up its runtime
thread. `ChildSpec::isolated(true)` runs a worker on a thread of its own, and
`SupervisorSpec::with_stall_detection(threshold)` reports workers stuck in a poll as
`SupervisorEvent::ChildStalled`.

## Remote control

```rust,no_run
use ash_flare::distributed::{RemoteSupervisorHandle, SupervisorServer};
# use ash_flare::{SupervisorHandle, SupervisorSpec};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let handle = SupervisorHandle::start(SupervisorSpec::new("app"))?;
let server = SupervisorServer::new(handle).with_cookie("secret");
tokio::spawn(server.listen_unix("/tmp/app.sock"));

let remote = RemoteSupervisorHandle::unix("/tmp/app.sock").with_cookie("secret");
println!("{:?}", remote.status().await?);
# Ok(())
# }
```

Both ends authenticate with the cookie; traffic is not encrypted. Bind to loopback or a Unix socket,
or put the listener behind an authenticated transport.

## Python

```python
import ash_flare as af

def worker(should_stop):
    while not should_stop():
        ...

spec = af.SupervisorSpec("app")
spec.add_worker("worker", af.RestartPolicy.permanent(), worker, shutdown=af.Shutdown.timeout(5.0))
handle = af.SupervisorHandle.start(spec)

for event in handle.subscribe():
    print(event)
```

Each Python worker runs on its own thread, and blocking calls release the GIL. CPU-bound Python code
is still serialised by the interpreter; use processes or Rust for parallel computation. Worker
exceptions are logged through the `ash_flare` logger with their traceback.

## Migrating from 3.x

- Remove `#[async_trait]` from `impl Worker`; `SupervisorSpec`, `SupervisorHandle` and `SupervisorServer` are no longer generic.
- `StatefulSupervisorSpec` is removed: capture a `WorkerContext` clone in each factory.
- `*_with_signal` methods are removed: pass a `|signal| ...` factory to the plain methods.
- `SupervisorHandle::start` returns `Result`; `shutdown` waits for the tree to stop.
- Children stop in reverse start order by default.
- `ChildExitReason`, `SupervisorError` and `DistributedError` are `#[non_exhaustive]`.
- Remote clients and servers must both be 4.x.

See [CHANGELOG.md](CHANGELOG.md) for the full list.

## Documentation

- [API documentation](https://docs.rs/ash-flare)
- [Architecture](architecture.md)
- [Examples](examples/) and [Python examples](python_examples/)

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Acknowledgements

- Inspired by Erlang/OTP supervision principles
- Some code generated with the help of AI tools
