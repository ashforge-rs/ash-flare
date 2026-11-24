# ash-flare

A Rust supervision framework inspired by Erlang/OTP, with Python bindings.

## Features

- Process supervision trees with automatic restart
- Workers of different types mixed freely in one tree
- Configurable restart policies (Permanent, Temporary, Transient)
- Supervision strategies (OneForOne, OneForAll, RestForOne)
- Exponential restart backoff, so a crash-looping child cannot peg a CPU core
- Graceful shutdown: workers are asked to stop and run their cleanup hook
- Worker messaging via mailboxes
- Distributed supervision across network nodes
- Python bindings with asyncio integration

## Installation

### Rust

```bash
cargo add ash-flare
```

### Python

Install from [GitHub Releases](https://github.com/ashforge-rs/ash-flare/releases):

```bash
# Find the wheel for your platform on the releases page, then:
uv add "ash-flare @ https://github.com/ashforge-rs/ash-flare/releases/download/vVERSION/ash_flare-VERSION-cp39-abi3-PLATFORM.whl"
```

For example, on Linux x86_64 with version 2.2.0:

```bash
uv add "ash-flare @ https://github.com/ashforge-rs/ash-flare/releases/download/v2.2.0/ash_flare-2.2.0-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
```

> Check the [releases page](https://github.com/ashforge-rs/ash-flare/releases) for the exact wheel filename matching your version and platform.

## Quick Start

### Rust

```rust
use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker};
use std::time::Duration;

struct MyWorker;

impl Worker for MyWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
// Describe the tree, then start it.
let spec = SupervisorSpec::new("my_supervisor")
    .with_restart_delay(Duration::from_secs(5))
    .with_worker("worker", || MyWorker, RestartPolicy::Permanent);

let handle = SupervisorHandle::start(spec);

// Add a child at runtime, waiting for its `initialize` to succeed.
handle.start_child_linked(
    "late-worker",
    || MyWorker,
    RestartPolicy::Permanent,
    Duration::from_secs(5),
).await?;

// Returns once every worker has stopped and run its cleanup hook.
handle.shutdown().await?;
# Ok(())
# }
```

Workers are created by a factory closure so the supervisor can build a fresh
instance on every restart. `Worker` is implemented with plain `async fn`s, and
`Error` only needs to implement `Display`, so `anyhow::Error` or `String` work.

Different worker types can share one tree, and state is shared by capturing a
clone of a `WorkerContext` (or anything else cheap to clone) in each factory:

```rust
use ash_flare::{RestartPolicy, SupervisorHandle, SupervisorSpec, Worker, WorkerContext};

struct Ingest { ctx: WorkerContext }
struct Report { ctx: WorkerContext }

impl Worker for Ingest {
    type Error = String;
    async fn run(&mut self) -> Result<(), Self::Error> {
        self.ctx.set("rows", serde_json::json!(42));
        Ok(())
    }
}

impl Worker for Report {
    type Error = std::io::Error;
    async fn run(&mut self) -> Result<(), Self::Error> {
        println!("rows so far: {:?}", self.ctx.get("rows"));
        Ok(())
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let ctx = WorkerContext::new();
let (a, b) = (ctx.clone(), ctx.clone());

let spec = SupervisorSpec::new("pipeline")
    .with_worker("ingest", move || Ingest { ctx: a.clone() }, RestartPolicy::Transient)
    .with_worker("report", move || Report { ctx: b.clone() }, RestartPolicy::Transient);

let handle = SupervisorHandle::start(spec);
handle.shutdown().await?;
# Ok(())
# }
```

### Python

Workers are callables; the supervisor calls one each time the child starts.

```python
import ash_flare as af

def worker_fn():
    print("working")

spec = af.SupervisorSpec("my_supervisor")
spec.with_restart_strategy(af.RestartStrategy.one_for_one())
spec.add_worker("worker", af.RestartPolicy.permanent(), worker_fn)

handle = af.SupervisorHandle.start(spec)

# Add a child at runtime.
handle.start_child("late-worker", af.RestartPolicy.permanent(), worker_fn)
```

Accept a `should_stop` argument to be told when to wind down. A worker that
ignores it is abandoned once the shutdown timeout elapses:

```python
def pump(should_stop):
    while not should_stop():
        do_one_unit_of_work()

spec = af.SupervisorSpec("my_supervisor")
spec.with_shutdown_timeout(5.0)
spec.add_worker("pump", af.RestartPolicy.permanent(), pump)
```

Stateful supervisors pass their shared context as the first argument, so
workers can coordinate through it:

```python
def counter(ctx):
    ctx.set("count", (ctx.get("count") or 0) + 1)

spec = af.StatefulSupervisorSpec("counters")
spec.add_worker("c1", af.RestartPolicy.permanent(), counter)
# `def counter(ctx, should_stop)` also works.
```

Worker exceptions are reported through the standard `logging` module under the
`ash_flare` logger, with the original traceback.

For async Python usage with `asyncio.to_thread()`:

```python
import asyncio

async def send_message(handle, msg):
    await asyncio.to_thread(handle.send, msg)
```

Blocking calls release the GIL, so other Python threads keep running while a
supervisor call or `Mailbox.recv()` is waiting.

### CPU-bound workers

CPython serialises pure-Python CPU work across threads, so CPU-bound workers do
not run in parallel — this is a property of CPython, not of ash-flare. IO-bound
workers (sleeping, network, subprocess) do run in parallel. For CPU-bound work,
use separate processes or move the hot loop into Rust.

## Graceful shutdown

When a worker is terminated the supervisor asks it to stop before forcing it
down. A worker whose factory takes a `ShutdownSignal` argument can wind down
on its own terms:

```rust
use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec, Worker};
use std::time::Duration;

struct Pump {
    shutdown: ShutdownSignal,
}

impl Worker for Pump {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(Duration::from_millis(50)) => {
                    // ... one unit of work ...
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        // Runs before the task goes away: flush, commit, release.
        Ok(())
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let spec = SupervisorSpec::new("root")
    .with_shutdown_timeout(Duration::from_secs(5))
    .with_worker("pump", |signal| Pump { shutdown: signal }, RestartPolicy::Permanent);

let handle = SupervisorHandle::start(spec);
handle.shutdown().await?;
# Ok(())
# }
```

`shutdown` runs after `run` returns — whether it completed, failed, or was
cancelled. A worker that cannot be cancelled within the shutdown timeout (for
example one that blocks the thread without awaiting) is aborted instead, and
does not get the hook.

## Migrating from 3.x

- Remove `#[async_trait]` from `impl Worker` blocks and drop the `async-trait`
  dependency; the trait methods are written as plain `async fn`.
- `SupervisorSpec`, `SupervisorHandle` and `distributed::SupervisorServer` are no
  longer generic. Delete the `<W>` and any enum that only existed to put several
  worker types in one tree.
- `StatefulSupervisorSpec` / `StatefulSupervisorHandle` and the
  `stateful_supervision_tree!` macro are gone. Use `SupervisorSpec` and capture a
  `WorkerContext` clone in each factory; the factory gets `WorkerContext` rather
  than `Arc<WorkerContext>`.
- `with_worker_signal`, `start_child_with_signal` and
  `start_child_linked_with_signal` are gone: pass a `|signal| ...` factory to
  `with_worker`, `start_child` or `start_child_linked`.
- `SupervisorHandle::shutdown` now waits until the tree has stopped. Children are stopped
  together and share one shutdown timeout, rather than one after another.
- `SupervisorHandle::uptime` returns a `Duration`.
- `ChildInfo::restart_policy` is `None` for nested supervisors, as documented.
- `SupervisorError` is `#[non_exhaustive]`, and its never-used `NoChildren` and
  `AllChildrenFailed` variants are removed. `WorkerError` is removed.
- `distributed::ChildInfo` is replaced by the crate-root `ChildInfo`, and
  `SupervisorStatus::restart_strategy` is a `RestartStrategy`.
- `RemoteSupervisorHandle::connect_tcp` / `connect_unix` (async, fallible, but
  never connecting) are replaced by the plain constructors `tcp` / `unix`.

The Python API is unchanged, except that `SupervisorHandle.shutdown()` (and the
stateful variant) now blocks until the tree has stopped. Workers stop together
and share one shutdown timeout, so a worker that never checks `should_stop`
keeps it waiting for that timeout.

## Documentation

- [Rust API Documentation](https://docs.rs/ash-flare)
- [Examples](examples/)
- [Python Examples](python_examples/)

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Acknowledgements

- Inspired by Erlang/OTP supervision principle
- Some code generated with the help of AI tools
