use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, ShutdownSignal, SupervisorHandle,
    SupervisorSpec, Worker, WorkerContext,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;

// ============================================================================
// Test Workers
// ============================================================================

#[derive(Debug)]
#[allow(dead_code)]
struct SimpleWorker {
    id: u32,
}
impl Worker for SimpleWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_micros(10)).await;
        Ok(())
    }
}

#[derive(Debug)]
struct SharedContextWorker {
    id: u32,
    context: WorkerContext,
}
impl Worker for SharedContextWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        // Simulate some context operations
        self.context
            .set(format!("worker-{}", self.id), serde_json::json!(self.id));
        let _ = self.context.get(&format!("worker-{}", self.id));
        sleep(Duration::from_micros(10)).await;
        Ok(())
    }
}

/// Runs until asked to stop, then spends a few milliseconds cleaning up, like a
/// worker flushing a buffer or closing a connection.
struct CleanupWorker {
    shutdown: ShutdownSignal,
}
impl Worker for CleanupWorker {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        sleep(Duration::from_millis(5)).await;
        Ok(())
    }
}

/// Fails as soon as it starts. Signals `first_start` on its first run and
/// `done` once it has been restarted `restarts` times, so a benchmark can time
/// the restarts alone, without the tree's startup.
struct Crasher {
    starts: Arc<AtomicUsize>,
    restarts: usize,
    first_start: Arc<Notify>,
    done: Arc<Notify>,
}
impl Worker for Crasher {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let started = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
        if started == 1 {
            self.first_start.notify_one();
        } else if started == self.restarts + 1 {
            self.done.notify_one();
        }
        Err(std::io::Error::other("crash"))
    }
}

/// Idles until stopped; restarted alongside a crasher under `OneForAll`.
struct Idler {
    shutdown: ShutdownSignal,
}
impl Worker for Idler {
    type Error = std::io::Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        self.shutdown.cancelled().await;
        Ok(())
    }
}

// ============================================================================
// Benchmark Functions
// ============================================================================

fn bench_supervisor_startup(c: &mut Criterion) {
    let mut group = c.benchmark_group("supervisor_startup");

    for worker_count in [5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(worker_count),
            worker_count,
            |b, &count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async move {
                        let mut spec = SupervisorSpec::new("bench");
                        for i in 0..count {
                            spec = spec.with_worker(
                                format!("worker-{}", i),
                                move || SimpleWorker { id: i },
                                RestartPolicy::Temporary,
                            );
                        }

                        let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                        sleep(Duration::from_millis(10)).await;
                        let _ = handle.shutdown().await;
                    });
            },
        );
    }

    group.finish();
}

fn bench_supervisor_shutdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("supervisor_shutdown");

    for worker_count in [5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(worker_count),
            worker_count,
            |b, &count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async move {
                        // Setup: create and start supervisor
                        let mut spec = SupervisorSpec::new("bench");
                        for i in 0..count {
                            spec = spec.with_worker(
                                format!("worker-{}", i),
                                move || SimpleWorker { id: i },
                                RestartPolicy::Temporary,
                            );
                        }
                        let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                        sleep(Duration::from_millis(10)).await;

                        // Measure shutdown time
                        let _ = handle.shutdown().await;
                    });
            },
        );
    }

    group.finish();
}

/// Shutdown of a tree whose workers each need 5ms of cleanup. Measures only the
/// `shutdown()` call, which is where stopping children one at a time adds up.
fn bench_cooperative_shutdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("cooperative_shutdown");
    group.sample_size(10);

    for worker_count in [5, 20].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(worker_count),
            worker_count,
            |b, &count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter_custom(|iters| async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let mut spec = SupervisorSpec::new("bench");
                            for i in 0..count {
                                spec = spec.with_worker(
                                    format!("worker-{}", i),
                                    |shutdown| CleanupWorker { shutdown },
                                    RestartPolicy::Permanent,
                                );
                            }
                            let handle =
                                SupervisorHandle::start(spec).expect("valid supervisor spec");
                            sleep(Duration::from_millis(5)).await;

                            let started = std::time::Instant::now();
                            let _ = handle.shutdown().await;
                            total += started.elapsed();
                        }
                        total
                    });
            },
        );
    }

    group.finish();
}

/// Time for a supervisor to carry out `RESTARTS` restarts of a crashing child,
/// with no backoff and no fixed sleeps, so the supervisor's own restart path
/// (bookkeeping, child lookup, respawn, task spawn) is what gets measured. The
/// tree's startup is not timed. `one_for_one_1000` puts the crasher after 999
/// idle siblings, the worst case for finding it by id.
fn bench_restart_loop(c: &mut Criterion) {
    const RESTARTS: usize = 5_000;

    let mut group = c.benchmark_group("restart_loop");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(15));

    for (label, strategy, siblings) in [
        ("one_for_one", RestartStrategy::OneForOne, 0),
        ("one_for_all_10", RestartStrategy::OneForAll, 9),
        ("one_for_one_1000", RestartStrategy::OneForOne, 999),
    ] {
        group.bench_function(label, |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter_custom(|iters| async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let starts = Arc::new(AtomicUsize::new(0));
                        let first_start = Arc::new(Notify::new());
                        let done = Arc::new(Notify::new());
                        let (s, f, d) = (
                            Arc::clone(&starts),
                            Arc::clone(&first_start),
                            Arc::clone(&done),
                        );

                        let mut spec = SupervisorSpec::new("bench")
                            .with_restart_strategy(strategy)
                            .with_restart_backoff(ash_flare::RestartBackoff::none())
                            .with_restart_intensity(RestartIntensity::new(RESTARTS * 2, 60));
                        for i in 0..siblings {
                            spec = spec.with_worker(
                                format!("idler-{i}"),
                                |shutdown| Idler { shutdown },
                                RestartPolicy::Permanent,
                            );
                        }
                        spec = spec.with_worker(
                            "crasher",
                            move || Crasher {
                                starts: Arc::clone(&s),
                                restarts: RESTARTS,
                                first_start: Arc::clone(&f),
                                done: Arc::clone(&d),
                            },
                            RestartPolicy::Permanent,
                        );

                        let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                        first_start.notified().await;
                        let started = std::time::Instant::now();
                        done.notified().await;
                        total += started.elapsed();

                        let _ = handle.shutdown().await;
                    }
                    total
                });
        });
    }

    group.finish();
}

fn bench_nested_supervisors(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_supervisors");

    for depth in [2, 3, 5, 10].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(depth), depth, |b, &d| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async move {
                    // Build nested supervisor tree
                    let depth = d;
                    let mut spec = SupervisorSpec::new(format!("level-{}", depth)).with_worker(
                        "worker",
                        move || SimpleWorker { id: depth },
                        RestartPolicy::Temporary,
                    );

                    for i in (0..d).rev() {
                        let name = format!("level-{}", i);
                        let worker_id = i;
                        let prev_spec = spec;
                        spec = SupervisorSpec::new(name)
                            .with_worker(
                                "worker",
                                move || SimpleWorker { id: worker_id },
                                RestartPolicy::Temporary,
                            )
                            .with_supervisor(prev_spec);
                    }

                    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                    sleep(Duration::from_millis(10)).await;
                    let _ = handle.shutdown().await;
                });
        });
    }

    group.finish();
}

fn bench_worker_context_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("worker_context");

    let ctx = WorkerContext::new();

    // Pre-populate with some data
    for i in 0..100 {
        ctx.set(format!("key-{}", i), serde_json::json!(i));
    }

    group.bench_function("get", |b| {
        b.iter(|| {
            black_box(ctx.get("key-50"));
        });
    });

    group.bench_function("set", |b| {
        b.iter(|| {
            ctx.set("bench-key", serde_json::json!(42));
        });
    });

    group.bench_function("with_value", |b| {
        b.iter(|| {
            black_box(ctx.with_value("key-50", |v| v.is_some()));
        });
    });

    group.bench_function("update", |b| {
        b.iter(|| {
            ctx.update("counter", |v| {
                let count = v.and_then(|v| v.as_u64()).unwrap_or(0);
                Some(serde_json::json!(count + 1))
            });
        });
    });

    group.bench_function("contains_key", |b| {
        b.iter(|| {
            black_box(ctx.contains_key("key-50"));
        });
    });

    group.finish();
}

fn bench_shared_context_startup(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_context_startup");

    for worker_count in [5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(worker_count),
            worker_count,
            |b, &count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async move {
                        let ctx = WorkerContext::new();
                        let mut spec = SupervisorSpec::new("bench");
                        for i in 0..count {
                            let ctx = ctx.clone();
                            spec = spec.with_worker(
                                format!("worker-{}", i),
                                move || SharedContextWorker {
                                    id: i,
                                    context: ctx.clone(),
                                },
                                RestartPolicy::Temporary,
                            );
                        }

                        let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                        sleep(Duration::from_millis(10)).await;
                        let _ = handle.shutdown().await;
                    });
            },
        );
    }

    group.finish();
}

fn bench_restart_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("restart_strategies");

    let strategies = [
        ("OneForOne", RestartStrategy::OneForOne),
        ("OneForAll", RestartStrategy::OneForAll),
        ("RestForOne", RestartStrategy::RestForOne),
    ];

    for (name, strategy) in strategies.iter() {
        group.bench_function(*name, |b| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let spec = SupervisorSpec::new("bench")
                        .with_restart_strategy(*strategy)
                        .with_restart_intensity(RestartIntensity::new(10, 10))
                        .with_worker("w1", || SimpleWorker { id: 1 }, RestartPolicy::Temporary)
                        .with_worker("w2", || SimpleWorker { id: 2 }, RestartPolicy::Temporary)
                        .with_worker("w3", || SimpleWorker { id: 3 }, RestartPolicy::Temporary);

                    let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                    sleep(Duration::from_millis(10)).await;
                    let _ = handle.shutdown().await;
                });
        });
    }

    group.finish();
}

fn bench_dynamic_child_management(c: &mut Criterion) {
    let mut group = c.benchmark_group("dynamic_children");

    group.bench_function("add_child", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let spec = SupervisorSpec::new("bench");
                let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                sleep(Duration::from_micros(10)).await;

                let _ = handle
                    .start_child(
                        "dynamic-worker",
                        || SimpleWorker { id: 999 },
                        RestartPolicy::Temporary,
                    )
                    .await;
                let _ = handle.shutdown().await;
            });
    });

    group.bench_function("terminate_child", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let spec = SupervisorSpec::new("bench").with_worker(
                    "w1",
                    || SimpleWorker { id: 1 },
                    RestartPolicy::Temporary,
                );
                let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                sleep(Duration::from_micros(10)).await;

                let _ = handle.terminate_child("w1").await;
                let _ = handle.shutdown().await;
            });
    });

    group.finish();
}

fn bench_which_children(c: &mut Criterion) {
    let mut group = c.benchmark_group("which_children");

    for worker_count in [5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(worker_count),
            worker_count,
            |b, &count| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async move {
                        let mut spec = SupervisorSpec::new("bench");
                        for i in 0..count {
                            spec = spec.with_worker(
                                format!("worker-{}", i),
                                move || SimpleWorker { id: i },
                                RestartPolicy::Temporary,
                            );
                        }
                        let handle = SupervisorHandle::start(spec).expect("valid supervisor spec");
                        sleep(Duration::from_micros(10)).await;

                        let _ = black_box(handle.which_children().await);
                        let _ = handle.shutdown().await;
                    });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    benches,
    bench_supervisor_startup,
    bench_supervisor_shutdown,
    bench_cooperative_shutdown,
    bench_restart_loop,
    bench_nested_supervisors,
    bench_worker_context_operations,
    bench_shared_context_startup,
    bench_restart_strategies,
    bench_dynamic_child_management,
    bench_which_children,
);

criterion_main!(benches);
