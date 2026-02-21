# ash-flare

A Rust supervision framework inspired by Erlang/OTP, with Python bindings.

## Features

- Process supervision trees with automatic restart
- Configurable restart policies (Permanent, Temporary, Transient)
- Supervision strategies (OneForOne, OneForAll, RestForOne)
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
uv add "ash-flare @ https://github.com/ashforge-rs/ash-flare/releases/latest/download/ash_flare-2.0.0-cp39-abi3-linux_x86_64.whl"
```

> Replace the wheel filename with the appropriate version and platform from the [releases page](https://github.com/ashforge-rs/ash-flare/releases)

## Quick Start

### Rust

```rust
use ash_flare::{Supervisor, RestartPolicy};
use std::time::Duration;

let handle = Supervisor::new("my_supervisor")
    .with_restart_policy(RestartPolicy::Permanent)
    .with_restart_delay(Duration::from_secs(5))
    .spawn()
    .await?;

handle.start_child_linked(
    "worker",
    || MyWorker::new(),
    RestartPolicy::Permanent,
    Duration::from_secs(5),
).await?;
```

### Python

```python
from ash_flare import Supervisor

supervisor = Supervisor("my_supervisor")
supervisor.start_child("worker", MyWorker)
```

For async Python usage with `asyncio.to_thread()`:

```python
import asyncio

async def send_message(handle, msg):
    await asyncio.to_thread(handle.send, msg)
```

## Documentation

- [Rust API Documentation](https://docs.rs/ash-flare)
- [Examples](examples/)
- [Python Examples](python_examples/)

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Acknowledgements

- Inspired by Erlang/OTP supervision principle
- Some code generated with the help of AI tools
