# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [4.0.0] - 2026-11-27

This is a major release; see "Migrating from 3.x" in the README.

### Breaking

- `Worker` uses native `async fn`; `#[async_trait]` is no longer needed. `Error` only requires `Display + Send`, and `Sync` is no longer required.
- `SupervisorSpec`, `SupervisorHandle` and `SupervisorServer` are no longer generic; workers of different types can share a tree.
- `StatefulSupervisorSpec`, `StatefulSupervisorHandle`, `StatefulSupervisorError` and `stateful_supervision_tree!` are removed. Share state by capturing a `WorkerContext` clone in each factory.
- `with_worker_signal`, `start_child_with_signal` and `start_child_linked_with_signal` are removed; factories may take a `ShutdownSignal` argument directly.
- `SupervisorHandle::start` returns `Result` and rejects duplicate child ids and invalid significant children.
- `SupervisorHandle::shutdown` waits until the tree has stopped; `uptime` returns a `Duration`.
- Children are stopped in reverse start order by default.
- `ChildExitReason` gains `Killed`; `ChildExitReason`, `SupervisorError` and `DistributedError` are `#[non_exhaustive]`.
- `SupervisorError::NoChildren`, `SupervisorError::AllChildrenFailed` and `WorkerError` are removed.
- `ChildInfo::restart_policy` reports a nested supervisor's own policy instead of `None`.
- `Mailbox::recv` and `try_recv` take `&self`.
- `distributed::ChildInfo` is replaced by the crate-level `ChildInfo`; `SupervisorStatus::restart_strategy` is a `RestartStrategy`.
- `RemoteSupervisorHandle::connect_tcp` / `connect_unix` are replaced by the `tcp` / `unix` constructors, which connect on the first command; `connect_supervisor!` evaluates to the handle instead of a future.
- Connections start with a versioned handshake; 3.x clients and servers cannot talk to 4.x.
- `distributed_system!` evaluates to a `Result`.

### Added

- `ChildSpec` with per-child `Shutdown::{BrutalKill, Timeout, Infinity}`; `with_child`, `start_child_spec`, `start_child_spec_linked`.
- `ShutdownOrder::{Reverse, Concurrent}`.
- `SupervisorHandle::monitor` and `monitor_child`, which return a `Monitor` that never misses an event, and `SupervisorHandle::subscribe`, a bounded broadcast for logs and metrics; `SupervisorEvent`.
- `SupervisorHandle::kill_child` and `link` for linking two children. Links are built on monitors, so a busy supervisor cannot make one miss a termination.
- Typed `Mailbox<M>` / `MailboxHandle<M>` that survive restarts and can be shared as a pool; `MailboxHandle::call` with `Reply` and `CallError`.
- `Registry` for finding workers by name.
- `AutoShutdown` with significant children; `SupervisorSpec::with_restart_policy` for nested supervisors.
- `SupervisorSpec::with_worker_pool` and `SupervisorHandle::start_unique_child`.
- `ChildSpec::isolated` and `SupervisorSpec::with_stall_detection`.
- Cookie authentication for remote supervisors (`SupervisorServer::with_cookie`, `RemoteSupervisorHandle::with_cookie`).
- `SupervisorHandle::is_stopped` and `wait`; `WorkerFactory`; `mailbox` and `mailbox_named` at the crate root.
- `SupervisorSpec::with_registered_name` and `SupervisorHandle::child_supervisor` for reaching nested supervisors.
- `GiveUp` and `SupervisorSpec::with_give_up`: a top-level supervisor that gives up can exit the process.

### Fixed

- A panicking worker, factory or supervisor is now reported as an abnormal exit; previously the child was never restarted and stayed listed.
- An exit report from a replaced child no longer triggers a second restart round.
- `distributed_system!` no longer requires `supervision_tree!` to be imported.
- `ShutdownSignal::is_cancelled` reports `true` once the supervisor is gone, matching `cancelled`.

### Performance

- Children are found by id through an index instead of a linear scan.
- Fewer allocations when spawning and restarting workers.
- Remote handles reuse one connection; `status` is answered in one supervisor round trip.
- `WorkerContext::update` no longer clones the stored value.
- Release builds use thin LTO and a single codegen unit.

### Python

- Each worker runs on its own thread; the 512-worker limit of the blocking pool is gone.
- `add_worker` accepts `shutdown=` and `significant=`; specs gain `with_shutdown_order`, `with_auto_shutdown` and `with_restart_policy`.
- Handles gain `kill_child`, `subscribe` (lossless) and `start_unique_child`; `link` links two children.
- `RemoteSupervisorHandle` accepts `cookie=` and gains `status`.
- Handles gain `wait` and `child_supervisor`; specs gain `with_give_up` and `with_registered_name`, and `SupervisorHandle.whereis` finds a registered supervisor.
- `SupervisorHandle.start` raises `ValueError` for an invalid spec; `shutdown` blocks until the tree has stopped.

### Dependencies

- `async-trait`, `rand`, `time` and `pyo3-async-runtimes` are no longer dependencies; tokio's `full` feature is no longer enabled.
- Added `hmac`, `sha2` and `getrandom` for authentication.

## [3.0.1]

See the [release notes on GitHub](https://github.com/ashforge-rs/ash-flare/releases) for earlier versions.
