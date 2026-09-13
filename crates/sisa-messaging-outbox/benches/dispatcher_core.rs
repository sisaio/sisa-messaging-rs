#[path = "dispatcher_core/capacity.rs"]
mod capacity;
#[path = "dispatcher_core/outcomes.rs"]
mod outcomes;

use criterion::{criterion_group, criterion_main};

criterion_group!(benches, capacity::benchmarks, outcomes::benchmarks);
criterion_main!(benches);
