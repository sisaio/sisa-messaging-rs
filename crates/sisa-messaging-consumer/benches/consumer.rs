//! No-I/O consumer runtime baseline for docs/benchmarks.md §7.
//! Each iteration includes decode, transaction, claim, handler, completion, commit, and
//! settlement against deterministic fakes. It does not measure PostgreSQL or broker latency.

#[path = "../tests/runtime/support.rs"]
mod support;

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sisa_messaging::FailureKind;
use sisa_messaging_consumer::SettlementMode;
use support::{HandlerStep, Harness};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

fn consumer(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("benchmark runtime: {error}"));

    for (name, count, duplicate, failure) in [
        ("full_pipeline", 1, false, None),
        ("completed_duplicate", 1, true, None),
        ("transient_failure", 1, false, Some(FailureKind::Transient)),
        ("permanent_failure", 1, false, Some(FailureKind::Permanent)),
        ("concurrency_1", 1, false, None),
        ("concurrency_8", 8, false, None),
        ("concurrency_32", 32, false, None),
        ("concurrency_128", 128, false, None),
        ("duplicate_contention", 8, true, None),
    ] {
        c.bench_function(name, |bench| {
            bench.iter_batched(
                || {
                    let mut harness = Harness::new(SettlementMode::Broker);

                    harness.settings.max_in_flight =
                        NonZeroUsize::new(count).unwrap_or(NonZeroUsize::MIN);

                    for index in 0..count {
                        let tag = if name == "duplicate_contention" {
                            1
                        } else {
                            (index + 1) as u8
                        };

                        if duplicate && name != "duplicate_contention" {
                            harness.probe.mark_completed(tag);
                        }

                        if let Some(kind) = failure {
                            harness.probe.script_handler(tag, HandlerStep::Fail(kind));
                        }

                        harness.deliver(tag, "bench");
                    }

                    harness.close();

                    harness
                },
                |harness| {
                    black_box(runtime.block_on(harness.run()).is_ok());
                },
                BatchSize::SmallInput,
            );
        });
    }

    // Virtual time keeps deadline waiting out of the measured no-I/O result. This scenario
    // measures coordinator scheduling and confirms heartbeat is actually invoked.
    let virtual_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap_or_else(|error| panic!("benchmark runtime: {error}"));

    c.bench_function("heartbeat_handler", |bench| {
        bench.iter_batched(
            || {
                let descriptor = sisa_messaging::IndividualSourceDescriptor::new(
                    Some(Duration::from_secs(10)),
                    None,
                    true,
                    true,
                    true,
                )
                .unwrap_or_else(|error| panic!("benchmark descriptor: {error}"));

                let mut harness = Harness::with_descriptor(SettlementMode::Broker, descriptor);
                harness.settings.heartbeat_interval = Some(Duration::from_secs(2));
                let gate = Arc::new(Semaphore::new(0));

                harness
                    .probe
                    .script_handler(1, HandlerStep::Block(Arc::clone(&gate)));

                harness.deliver(1, "bench");
                harness.close();

                (harness, gate)
            },
            |(harness, gate)| {
                virtual_runtime.block_on(async {
                    let task = harness.spawn(CancellationToken::new());
                    tokio::task::yield_now().await;
                    tokio::time::advance(Duration::from_secs(3)).await;
                    gate.add_permits(1);
                    let result = support::join(task).await;
                    black_box(result.0.is_ok());

                    let heartbeats = harness
                        .probe
                        .count(|event| matches!(event, support::Event::Heartbeat(1)));

                    assert!(heartbeats > 0, "heartbeat scenario did not heartbeat");
                    black_box(heartbeats);
                });
            },
            BatchSize::SmallInput,
        );
    });

    for completed in [0, 4, 8] {
        let name = format!("graceful_drain_{completed}_of_8");

        c.bench_function(&name, |bench| {
            bench.iter_batched(
                || {
                    let mut harness = Harness::new(SettlementMode::Broker);

                    harness.settings.max_in_flight =
                        NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN);

                    for tag in 1..=8 {
                        if tag > completed {
                            harness.probe.script_handler(tag, HandlerStep::Hang);
                        }

                        harness.deliver(tag, "bench");
                    }

                    harness
                },
                |harness| {
                    virtual_runtime.block_on(async {
                        let cancel = CancellationToken::new();
                        let task = harness.spawn(cancel.clone());

                        while harness.queued() > 0
                            || harness
                                .probe
                                .count(|event| matches!(event, support::Event::Commit(_)))
                                < usize::from(completed)
                        {
                            tokio::task::yield_now().await;
                        }

                        cancel.cancel();
                        let result = support::join(task).await;
                        black_box(result.0.is_ok());
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(benches, consumer);
criterion_main!(benches);
