# ash-flare

A Rust supervision framework inspired by Erlang/OTP, with Python bindings.

## Features

- Process supervision trees with automatic restart
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
use async_trait::async_trait;
use std::time::Duration;

struct MyWorker;

#[async_trait]
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

handle.shutdown().await?;
# Ok(())
# }
```

Workers are created by a factory closure so the supervisor can build a fresh
instance on every restart.

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
down. A worker that takes a `ShutdownSignal` can wind down on its own terms:

```rust
use ash_flare::{RestartPolicy, ShutdownSignal, SupervisorHandle, SupervisorSpec, Worker};
use async_trait::async_trait;
use std::time::Duration;

struct Pump {
    shutdown: ShutdownSignal,
}

#[async_trait]
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
    .with_worker_signal("pump", |signal| Pump { shutdown: signal }, RestartPolicy::Permanent);

let handle = SupervisorHandle::start(spec);
handle.shutdown().await?;
# Ok(())
# }
```

`shutdown` runs after `run` returns — whether it completed, failed, or was
cancelled. A worker that cannot be cancelled within the shutdown timeout (for
example one that blocks the thread without awaiting) is aborted instead, and
does not get the hook.

## Documentation

- [Rust API Documentation](https://docs.rs/ash-flare)
- [Examples](examples/)
- [Python Examples](python_examples/)

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Acknowledgements

- Inspired by Erlang/OTP supervision principle
- Some code generated with the help of AI tools
