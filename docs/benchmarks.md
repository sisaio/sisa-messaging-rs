# Benchmark program

## 1. Purpose

The benchmark suite answers four different questions and never combines them into one misleading
number:

1. Did a local algorithm or representation regress?
2. How do PostgreSQL queries scale with realistic table state?
3. What throughput and latency do NATS publication and consumption achieve independently?
4. What does the complete enqueue → dispatch → broker → consume → commit path cost?

Correctness tests remain separate. A fast result is invalid if rows are lost, acknowledged before
commit, processed twice, or left in an impossible database state.

## 2. Benchmark layout

```text
crates/
├── sisa-messaging/benches/
│   ├── envelope.rs
│   └── serialization.rs
├── sisa-messaging-outbox/benches/
│   ├── retry.rs
│   └── dispatcher_core.rs
├── sisa-messaging-inbox/benches/
│   └── outcome.rs
├── sisa-messaging-postgres/benches/
│   ├── outbox.rs
│   └── inbox.rs
├── sisa-messaging-nats/benches/
│   ├── mapping.rs
│   ├── publish.rs
│   └── consume.rs
└── sisa-messaging-consumer/benches/
    └── processing.rs
benchmarks/
└── system/
    ├── Cargo.toml
    ├── src/main.rs
    ├── scenarios/
    └── README.md
```

Crate benchmarks use Criterion for statistically sampled microbenchmarks. The system benchmark crate is a
`harness = false` load driver because Criterion is not a good controller for long-running Docker,
PostgreSQL, NATS, warm-up, steady-state, and drain phases.

`criterion`, histogram/reporting libraries, profilers, and container support are development-only
dependencies. They are not part of the library graph or runtime observability design.

## 3. Standard data profiles

Every relevant benchmark uses named profiles rather than arbitrary per-file fixtures:

| Profile | Payload | Custom headers | Ordering | Purpose |
|---|---:|---:|---|---|
| `small` | 256 B | 0 | none | High message-rate services |
| `typical` | 4 KiB | 8 | mixed | Default comparison profile |
| `large` | 64 KiB | 32 | none | Serialization, allocation and broker pressure |
| `ordered` | 4 KiB | 8 | 100 active keys | Per-key head-query behavior |
| `hot-key` | 4 KiB | 8 | one key | Intentional ordering contention |

Metadata values are deterministic and contain realistic correlation and trace fields. Benchmarks
never generate random data inside the timed section. Each run reports the exact profile, feature
set, worker count, batch size, concurrency, database size, and telemetry mode.

## 4. Microbenchmarks

### Shared messaging

- Construct an envelope with default and full metadata.
- Validate message types, headers, content type, scope, subject, and ordering key.
- Serialize and deserialize each data profile.
- Encode/decode metadata JSON with missing and unknown additive fields.
- Project framework and custom headers.
- Format safe errors and truncate multibyte error chains.

Use `black_box` for inputs and outputs. Setup, UUID generation, fixture construction, and random
payload generation stay outside the measured iteration unless they are the operation under test.

### Outbox and inbox logic

- Retry-delay calculation at first, middle, maximum and exhausted attempts.
- Dispatcher capacity accounting and outcome grouping for batches of 1, 32, 256 and 1,024.
- Claim-token match/shortfall reduction.
- Inbox claim/outcome-to-settlement decision.
- Typed consumer type/version gating and handler adapter overhead with a no-I/O transaction.

These benchmarks isolate CPU and allocation cost. They do not claim database or broker throughput.

### NATS mapping

- Encode and decode all standard profiles.
- Subject resolution for built-in and custom resolvers.
- Header projection with and without trace context.
- Wire-size calculation and payload-limit rejection.

The mapper benchmarks require no network. Broker publication/consumption belongs to the integration
suite below.

## 5. PostgreSQL benchmarks

Run against a real PostgreSQL 18 server with the release migration applied. Prepare table states
before timing and use database-generated timestamps.

### Outbox scenarios

- Single and batched enqueue inside one application transaction.
- Claim from empty, 1,000-row, 100,000-row, and optional 1,000,000-row active tables.
- Claim with terminal-history volume retained.
- Two, eight, and 32 concurrent workers claiming unordered rows.
- Ordered claims across many keys and one hot key.
- Complete, retry, dead, release, and renew batches with full and partial fencing matches.
- Expiry sweep, published/dead purge, statistics query, and dead-letter keyset pages.
- Poison-row isolation cost.

### Inbox scenarios

- First claim, complete and commit.
- Already-completed redelivery hot path.
- Handler rollback followed by failure recording.
- Permanent and attempts-exhausted transitions.
- Concurrent duplicate claims for the same key and distinct keys.
- Completed/dead retention and dead-letter keyset pages.

Capture messages or operations per second plus latency p50, p95, p99, and maximum. Also save
`EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)` for the standard query-plan cases outside the timed
throughput run. A plan benchmark fails review when the intended index disappears or examined rows
grow disproportionately to the requested batch.

Pool size, worker concurrency, statement settings, durability settings, hardware, PostgreSQL
configuration, cold/warm cache state, and seeded row distribution are recorded with every result.

## 6. NATS benchmarks

Run against a real NATS server with JetStream and a pre-created stream/consumer.

- Direct publish with awaited acknowledgement at concurrency 1, 8, 32, and 128.
- Ordered and unordered subject resolution.
- Consumer receive plus confirmed acknowledgement.
- Delayed nak, terminate, and heartbeat acknowledgement costs.
- Redelivery after withheld acknowledgement.
- Small, typical, and large data profiles.

Report application-observed latency and throughput separately from server statistics. Do not use
fire-and-forget publication to inflate throughput: the library contract awaits the JetStream
acknowledgement.

Loopback results measure software overhead, not network capacity. An optional remote profile may
measure representative latency, but it must never be compared directly to loopback baselines.

## 7. Consumer framework benchmarks

Measure both the generic runtime overhead and the real integration:

- decoded delivery → transaction → claim → no-op handler → complete → commit → ack;
- already-completed delivery → rollback → ack;
- transient handler failure → rollback → failure record → nak;
- permanent handler failure → rollback → dead record → terminate;
- slow handler with periodic heartbeat acknowledgements;
- independent messages at concurrency 1, 8, 32, and 128;
- duplicate contention for the same `(scope, message_id)`;
- graceful drain with 0%, 50%, and 100% of tasks complete at cancellation.

The no-I/O processor benchmark identifies framework overhead. The PostgreSQL/NATS benchmark is the
number users care about operationally. Keep the two results distinct.

## 8. End-to-end system scenarios

The standard system benchmark runs these phases:

```text
prepare clean schema and stream
  → seed or generate workload
  → warm up connections, statements and broker paths
  → measure steady-state enqueue/dispatch/consume
  → stop producers
  → drain outbox and consumer
  → verify database and broker invariants
  → write machine-readable result
```

Required scenarios:

| Scenario | Measurement |
|---|---|
| Enqueue only | Committed application transactions/s and enqueue latency |
| Dispatch only | Due row to broker acknowledgement |
| Consume only | Delivery to business commit and acknowledgement |
| Full pipeline | Enqueue commit to consumer commit |
| Ordered pipeline | Per-key throughput and cross-key parallelism |
| Retry pressure | Recovery throughput and retry amplification |
| Duplicate storm | Completed-inbox redelivery throughput |
| Backlog drain | Time and rate to drain a fixed 100k-message backlog |

The full pipeline oracle verifies:

- every enqueued ID is either published or intentionally dead;
- every broker delivery resolves to one committed business effect per scope;
- no acknowledgement precedes its database commit;
- outbox/inbox constraints remain valid;
- all tasks and database connections drain at the end.

A run that fails an oracle is a failed correctness test, not a performance sample.

## 9. Telemetry cost

Run the `typical` processor and full-pipeline scenarios in three modes:

1. OTel global no-op provider with normal tracing callsites;
2. SDK installed with sampling/export disabled or an in-memory reader;
3. representative production batching and local collector export.

Report the delta in throughput, latency, allocations when available, and CPU. This proves the cost
of direct global OTel instrumentation and prevents observability changes from silently taxing the
hot path. No benchmark asserts that telemetry is free.

## 10. Reproducibility and regression policy

Every result is written as JSON and includes:

- git revision and dirty flag;
- Rust version, target, optimization profile, and enabled features;
- OS, CPU model/count, memory, and container runtime;
- PostgreSQL/NATS versions and effective scenario configuration;
- warm-up duration, measured duration, repetitions, and seed;
- throughput, latency distribution, error count, retry count, and oracle result.

Use a pinned machine for comparisons. Pull requests run microbench compile/smoke checks; scheduled
or manually triggered jobs run the real service suite on dedicated hardware. Shared hosted CI is
too noisy for an automatic latency gate.

First establish and commit a baseline from the initial correct implementation. Do not invent
absolute throughput promises in advance. Flag a comparison for review when the same controlled
profile shows both a statistically credible change and at least a 10% regression in throughput or
p95/p99 latency. A reviewer may accept a regression only with an explicit correctness or
maintainability tradeoff recorded in the change.

## 11. Repository commands

The final workspace exposes stable commands:

```text
cargo bench --workspace --all-features --no-run  # compile every microbenchmark
cargo bench -p sisa-messaging --all-features     # shared hot paths, including optional JSON
cargo bench -p sisa-messaging-nats               # mapping and local broker benches
cargo run -p sisa-messaging-system-bench --release -- --profile smoke
cargo run -p sisa-messaging-system-bench --release -- --profile standard
cargo run -p sisa-messaging-system-bench --release -- --profile compare baseline.json
```

The system driver prints a concise human summary and writes the complete JSON artifact. The smoke
profile limits rows and duration for developer feedback; the standard profile is the only one used
for published comparisons.
