use std::hint::black_box;
use std::num::NonZeroU32;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use sisa_messaging_outbox::{ExponentialBackoff, RetryPolicy};

fn retry_benchmarks(criterion: &mut Criterion) {
    let policy = ExponentialBackoff::default();
    let mut group = criterion.benchmark_group("outbox_retry_delay");

    for attempt in [1, 5, 9, 10] {
        let attempt = NonZeroU32::new(attempt).unwrap_or(NonZeroU32::MIN);

        group.bench_with_input(
            BenchmarkId::from_parameter(attempt),
            &attempt,
            |bencher, value| {
                bencher.iter(|| policy.retry_delay(black_box(*value)));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, retry_benchmarks);
criterion_main!(benches);
