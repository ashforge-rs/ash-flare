use ash_flare::{
    RestartIntensity, RestartPolicy, RestartStrategy, SupervisorHandle, SupervisorSpec, Worker,
    WorkerContext,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
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
struct StatefulWorker {
    id: u32,
    context: WorkerContext,
}
impl Worker for StatefulWorker {
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

                        let handle = SupervisorHandle::start(spec);
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
                        let handle = SupervisorHandle::start(spec);
                        sleep(Duration::from_millis(10)).await;

                        // Measure shutdown time
                        let _ = handle.shutdown().await;
                    });
            },
        );
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

                    let handle = SupervisorHandle::start(spec);
                    sleep(Duration::from_millis(10)).await;
                    let _ = handle.shutdown().await;
                });
        });
    }

    group.finish();
}

fn bench_stateful_context_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("stateful_context");

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

fn bench_stateful_supervisor_startup(c: &mut Criterion) {
    let mut group = c.benchmark_group("stateful_startup");

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
                                move || StatefulWorker {
                                    id: i,
                                    context: ctx.clone(),
                                },
                                RestartPolicy::Temporary,
                            );
                        }

                        let handle = SupervisorHandle::start(spec);
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

                    let handle = SupervisorHandle::start(spec);
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
                let handle = SupervisorHandle::start(spec);
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
                let handle = SupervisorHandle::start(spec);
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
                        let handle = SupervisorHandle::start(spec);
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
    bench_nested_supervisors,
    bench_stateful_context_operations,
    bench_stateful_supervisor_startup,
    bench_restart_strategies,
    bench_dynamic_child_management,
    bench_which_children,
);

criterion_main!(benches);
