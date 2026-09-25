use std::collections::HashMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion};
use sisa_messaging_outbox::{Claim, ClaimToken, OutboxId};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum LeasePhase {
    Publishing,
    Resolved,
}

#[derive(Clone, Copy)]
struct LeaseEntry {
    phase: LeasePhase,

    safe_until: Instant,
}

fn claim(index: u128) -> Claim {
    Claim {
        id: OutboxId::from_uuid(Uuid::from_u128(index.saturating_add(1))),
        token: ClaimToken::from_uuid(Uuid::from_u128(index.saturating_add(10_000))),
    }
}

fn admission_blockers(
    claims: &HashMap<Claim, LeaseEntry>,
    now: Instant,
    timeout: Duration,
) -> Vec<Claim> {
    claims
        .iter()
        .filter_map(|(claim, entry)| {
            if !matches!(entry.phase, LeasePhase::Publishing) {
                return None;
            }

            let safe = has_store_headroom(now, timeout, entry.safe_until);

            (!safe).then_some(*claim)
        })
        .collect()
}

fn has_store_headroom(now: Instant, timeout: Duration, safe_until: Instant) -> bool {
    now.checked_add(timeout)
        .and_then(|deadline| deadline.checked_add(timeout))
        .is_some_and(|deadline| deadline < safe_until)
}

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

    let mut headroom = criterion.benchmark_group("outbox_lease_headroom");

    for size in [1_usize, 32, 256, 1_024] {
        let now = Instant::now();
        let timeout = Duration::from_millis(19);

        let boundary = now
            .checked_add(timeout)
            .and_then(|deadline| deadline.checked_add(timeout))
            .unwrap_or(now);

        let claims = (0..size as u128)
            .map(|index| {
                let phase = if index % 4 == 3 {
                    LeasePhase::Resolved
                } else {
                    LeasePhase::Publishing
                };

                let safe_until = if index % 2 == 0 {
                    boundary
                } else {
                    boundary
                        .checked_add(Duration::from_secs(1))
                        .unwrap_or(boundary)
                };

                (claim(index), LeaseEntry { phase, safe_until })
            })
            .collect::<HashMap<_, _>>();

        headroom.bench_with_input(
            BenchmarkId::new("admission_scan", size),
            &size,
            |bencher, _| {
                bencher.iter(|| {
                    black_box(admission_blockers(
                        black_box(&claims),
                        black_box(now),
                        black_box(timeout),
                    ))
                });
            },
        );
    }

    headroom.finish();
}
