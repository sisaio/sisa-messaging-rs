use std::hint::black_box;

use criterion::{BenchmarkId, Criterion};

pub(crate) fn benchmarks(criterion: &mut Criterion) {
    let mut capacity = criterion.benchmark_group("outbox_capacity_accounting");

    for size in [1_usize, 32, 256, 1_024] {
        let retained = size / 2;
        capacity.bench_with_input(
            BenchmarkId::new("available", size),
            &size,
            |bencher, value| {
                bencher.iter(|| black_box(*value).saturating_sub(black_box(retained)));
            },
        );
    }

    capacity.finish();
}
