use std::collections::HashSet;
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion};
use sisa_messaging_outbox::{Claim, ClaimToken, FencedClaims, OutboxId};
use uuid::Uuid;

fn claim(index: u128) -> Claim {
    Claim {
        id: OutboxId::from_uuid(Uuid::from_u128(index.saturating_add(1))),
        token: ClaimToken::from_uuid(Uuid::from_u128(index.saturating_add(10_000))),
    }
}

pub(crate) fn benchmarks(criterion: &mut Criterion) {
    let mut grouping = criterion.benchmark_group("outbox_outcome_grouping_fencing");

    for size in [1_usize, 32, 256, 1_024] {
        let requested = (0..size as u128).map(claim).collect::<Vec<_>>();
        let outcomes = requested
            .iter()
            .enumerate()
            .map(|(index, claim)| (*claim, index % 3))
            .collect::<Vec<_>>();
        let matches = FencedClaims {
            confirmed: requested.iter().copied().step_by(2).collect(),
        };

        grouping.bench_with_input(
            BenchmarkId::new("group_and_reduce", size),
            &size,
            |bencher, _| {
                bencher.iter(|| {
                    let confirmed = black_box(&matches)
                        .confirmed
                        .iter()
                        .copied()
                        .collect::<HashSet<_>>();
                    black_box(&outcomes)
                        .iter()
                        .filter(|(claim, _)| confirmed.contains(claim))
                        .fold([0_usize; 3], |mut groups, (_, group)| {
                            groups[*group] += 1;
                            groups
                        })
                });
            },
        );
    }

    grouping.finish();
}
