# Benchmark program

## 1. Purpose

The benchmark suite answers four different questions and never combines them into one misleading
number:

1. Did a local algorithm or representation regress?
2. How do PostgreSQL queries scale with realistic table state?
3. What throughput and latency do NATS, Kafka, Iggy, RabbitMQ, and Redis Streams publication and
   consumption achieve independently?
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
│   ├── provider.rs
│   └── consume.rs
├── sisa-messaging-kafka/benches/
│   └── mapping.rs
├── sisa-messaging-iggy/benches/
│   └── mapping.rs
├── sisa-messaging-rabbitmq/benches/
│   ├── mapping.rs
│   └── provider.rs
├── sisa-messaging-redis/benches/
│   ├── mapping.rs
│   └── provider.rs
└── sisa-messaging-consumer/benches/
    └── consumer.rs
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

### Kafka mapping

- Encode and decode a deterministic envelope with a representative payload, ordering key,
  framework metadata, and custom headers.
- Time the mapper independently of topic resolution, client construction, and broker I/O.
- Record the exact input profile, command, host, toolchain, and measured encode/decode results.

The named `sisa-messaging-kafka/benches/mapping.rs` benchmark is the provider's first hot-path
baseline. Broker publication and ordered offset settlement require separate real-broker profiles;
the local mapper result does not represent network throughput or durability.

Initial local smoke run (2026-09-24, macOS 26.5.2 arm64, rustc 1.98.0, default
Kafka features): `cargo bench -p sisa-messaging-kafka --bench mapping --
--warm-up-time 0.1 --measurement-time 0.5 --sample-size 10`. For the
`typical-4k-8-ordered` fixture, Criterion estimated 1.3107 µs for encode
(95% interval 1.3054–1.3173 µs) and 2.7586 µs for decode
(2.7425–2.7777 µs). This short run is a starting measurement, not a release
regression threshold.

### Iggy mapping

- Encode and decode a deterministic envelope with a representative payload, ordering key,
  framework metadata, and custom headers.
- Time the mapper independently of destination resolution, client construction, and broker I/O.
- Record the exact input profile, command, host, toolchain, and measured encode/decode results.

The named `sisa-messaging-iggy/benches/mapping.rs` benchmark is the provider's first hot-path
baseline. Broker publication requires a separate real-broker profile; the local mapper result does
not represent network throughput or durability.

Initial local run (2026-09-24, Apple M4 Pro, macOS arm64 Darwin 25.5.0, rustc 1.98.0, default
Iggy features): `cargo bench -p sisa-messaging-iggy --bench mapping -- --warm-up-time 1
--measurement-time 3`. For the 4 KiB payload, eight custom header, full framework metadata,
ordering-key fixture, Criterion estimated about 1.46 µs for encode (interval 1.4254–1.5134 µs)
and about 2.92 µs for decode (2.9025–2.9458 µs). This is a starting measurement, not a release
regression threshold.

### RabbitMQ mapping

- Encode and decode the `small` (256 B, no custom headers), `typical` (4 KiB, eight custom
  headers, ordering key), and `large` (64 KiB, 32 custom headers, ordering key) fixtures.
- Time the mapper, including `TypeRouteResolver` routing-key formatting and the payload copy into
  the wire value, independently of channel construction and broker I/O.

The named `sisa-messaging-rabbitmq/benches/mapping.rs` benchmark is the provider's broker-free
hot-path baseline. Initial local run (2026-09-25, macOS arm64 Darwin 25.5.0, rustc 1.98.0):
`cargo bench -p sisa-messaging-rabbitmq --bench mapping`. Encode measured about 614 ns, 1.86 µs,
and 6.02 µs; decode about 264 ns, 1.20 µs, and 4.88 µs for small, typical, and large. This is a
starting measurement, not a release regression threshold.

### Redis Streams mapping and provider

- Encode and decode a deterministic envelope independently of Redis I/O.
- Measure append/read/ack, pending reclaim/ack, and cancellation of a blocking read against a real
  Redis server. Keep the idle wait and block timeout explicit in the report.
- Record the image or server version, exact command, host, toolchain, feature set, and measured
  results. Provider measurements do not represent a cross-server durability guarantee.

Initial local smoke run (2026-09-25, MacBook Pro Mac16,8, Apple M4 Pro (12 CPU cores), macOS
26.5.2, 48 GB RAM, `aarch64-apple-darwin`, rustc 1.98.0 `88d9e12ae` (2026-08-18), local Docker
Redis 6.2.24 on port 16379, default provider features): `rtk cargo bench --offline -p
sisa-messaging-redis --bench mapping -- --quick` and `rtk env
SISA_REDIS_URL=redis://127.0.0.1:16379/ cargo bench --offline -p sisa-messaging-redis --bench
provider -- --quick`. With a 256-byte body and default metadata, Criterion `--quick` median point
estimates (reported low–high median intervals) were 245.54 ns (243.57–246.03 ns) for
`redis_mapper_encode_256b`, 355.31 ns (353.27–355.82 ns) for `redis_mapper_decode_256b`, 444.56
µs (425.61–449.30 µs) for `redis_append_read_ack_256b`, 4.3850 ms (4.2954–4.4074 ms) for
`redis_pending_reclaim_ack_256b` (including its 2 ms idle wait), and 2.1823 ms (2.1773–2.2023 ms)
for `redis_cancel_blocking_read` (with a 1 ms timeout). This local smoke run is a starting
measurement, not a release regression threshold or evidence of compatibility with the other server
families.

Additional local `--quick` provider runs on the same host and toolchain (2026-09-25, default
features) used `rtk env SISA_REDIS_URL=redis://127.0.0.1:<port>/ cargo bench --offline -p
sisa-messaging-redis --bench provider -- --quick`. Median point estimates follow; the reclaim
measurement includes a 2 ms idle wait, and cancellation uses a 1 ms timeout.

| Server image | Port | Append/read/ack | Pending reclaim/ack | Cancel blocking read |
| --- | ---: | ---: | ---: | ---: |
| `redis:7` | 16380 | 317.93 µs | 6.4787 ms | 2.4574 ms |
| `valkey/valkey:8` | 16381 | 297.67 µs | 4.4771 ms | 2.5118 ms |
| `docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.0` | 16382 | 297.53 µs | 4.6036 ms | 2.5798 ms |

The tested `ghcr.io/microsoft/garnet` image at digest
`sha256:880565c0c4186d0127846511174c732e60ba6dcb56f5bd8ac81fe78f1f34d753`
(Garnet 2.1.8) returns `ERR unknown command` for plain `XADD`. Its provider benchmark cannot run,
and no latency value is reported for that server image. Garnet is explicitly unsupported: the
latest official release is [v2.1.8](https://github.com/microsoft/garnet/releases/tag/v2.1.8), and
the [upstream Streams compatibility table](https://github.com/microsoft/garnet/blob/v2.1.8/website/docs/commands/api-compatibility.md#stream)
marks the required commands unsupported, including `XADD`, `XGROUP CREATE`, `XREADGROUP`, `XACK`,
`XPENDING`, `XCLAIM`, and `XINFO GROUPS`. The upstream implementation remains in a
[draft pull request](https://github.com/microsoft/garnet/pull/1461); no viable pinned build has
been identified. Issue [#75](https://github.com/sisaio/sisa-messaging-rs/issues/75) tracks
reevaluation when a build passes the real-server provider suite. No Garnet latency is reported.

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

## 6. Broker benchmarks

### NATS broker benchmarks

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

### RabbitMQ broker benchmarks

`sisa-messaging-rabbitmq/benches/provider.rs` runs against a real broker when `RABBITMQ_URL` is
set and registers nothing otherwise. It measures application-observed latency for:

- confirmed mandatory publication at concurrency 1, 8, 32, and 128;
- receive plus confirmed acknowledgement;
- zero-delay requeue and terminal reject.

Each settlement includes the following `basic.qos` round trip that confirms it. Initial local
run (2026-09-25, `rabbitmq:4.1.8-alpine` in Docker Desktop on macOS arm64, loopback):
confirmed publication took about 218 µs, 531 µs, 1.20 ms, and 3.07 ms per batch at concurrency
1, 8, 32, and 128 (about 4.6k, 15.1k, 26.6k, and 41.7k messages/s); receive plus ack about
217 µs, requeue about 272 µs, and terminate about 215 µs. These are loopback software-overhead
measurements, not network capacity or release thresholds.

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

`sisa-messaging-consumer/benches/consumer.rs` is the no-I/O processor benchmark.
`sisa-messaging-nats/benches/consume.rs` runs the generic consumer over a real JetStream server
with an in-memory inbox when `NATS_URL` is set, so it measures runtime plus NATS settlement but
not PostgreSQL. Timing starts at the source's first pull request, so source opening is excluded,
and every iteration verifies afterwards that no delivery was redelivered. Initial local run
(2026-09-26, `nats:2.11.8-alpine` in Docker on macOS arm64, loopback): 64 messages at
concurrency 1 took about 13–14 ms for ack, completed-duplicate ack, delayed nak, and terminate
alike (about 4.5k–4.9k messages/s). 256 independent no-op messages reached about 4.6k, 7.5k, 7.8k,
and 7.5k messages/s at concurrency 1, 8, 32, and 128; the plateau above 8 is consistent with the
source's single-message pull batches. A 600 ms handler with a 500 ms `ack_wait` and 200 ms
heartbeat completed without redelivery. These are loopback software-overhead
measurements, not network capacity or release thresholds; the PostgreSQL/NATS number remains owed
to the system benchmark.

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
cargo bench -p sisa-messaging-rabbitmq           # mapping; broker benches with RABBITMQ_URL
cargo run -p sisa-messaging-system-bench --release -- --profile smoke
cargo run -p sisa-messaging-system-bench --release -- --profile standard
cargo run -p sisa-messaging-system-bench --release -- --profile compare baseline.json
```

The system driver prints a concise human summary and writes the complete JSON artifact. The smoke
profile limits rows and duration for developer feedback; the standard profile is the only one used
for published comparisons.
