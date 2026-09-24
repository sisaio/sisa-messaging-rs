# Repository implementation plan

## 1. Purpose

This plan defines how `sisa-messaging-rs` is built and kept releasable. The repository begins with
the crate contracts and database schema in this documentation.

All crates start at `0.1.0`. They may evolve independently, but a workspace release records the
compatible set of crate and schema versions.

## 2. Repository layout

```text
sisa-messaging-rs/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── CHANGELOG.md
├── CONTRIBUTING.md
├── LICENSE
├── deny.toml
├── rust-toolchain.toml
├── atlas.hcl
├── migrations/
│   ├── 0001_messaging.sql
│   └── atlas.sum
├── crates/
│   ├── sisa-messaging/
│   ├── sisa-messaging-outbox/
│   ├── sisa-messaging-inbox/
│   ├── sisa-messaging-consumer/
│   ├── sisa-messaging-postgres/
│   ├── sisa-messaging-nats/
│   └── sisa-messaging-kafka/
├── examples/
│   ├── outbox-basic/
│   ├── axum-outbox/
│   ├── nats-publish/
│   ├── nats-postgres-consumer/
│   └── manual-inbox/
├── tests/
│   ├── architecture/
│   └── system/
├── benchmarks/
│   └── system/
└── docs/
    ├── README.md
    ├── architecture.md
    ├── database.md
    ├── migrations.md
    ├── runtime-flows.md
    ├── consumer-framework.md
    ├── api-conventions.md
    ├── observability.md
    ├── benchmarks.md
    └── implementation-plan.md
```

The root README is an installation and quick-start page. `docs/README.md` is the architecture
index. Public crate READMEs and rustdoc link to these documents rather than copying guarantees into
multiple sources.

## 3. Crate responsibilities

| Crate | Publishes | Depends on workspace crates |
|---|---|---|
| `sisa-messaging` | Envelope, metadata, serialization, publish, individual-delivery and partitioned-log inbound contracts | none |
| `sisa-messaging-outbox` | Enqueue/store capabilities, dispatcher, retry and dead-letter contracts | messaging |
| `sisa-messaging-inbox` | Inbox state, store, unit-of-work, maintenance and dead-letter contracts | messaging |
| `sisa-messaging-consumer` | Typed bounded consumer runtime and handler contract | messaging, inbox |
| `sisa-messaging-postgres` | PostgreSQL runtime implementations for outbox and inbox | messaging, outbox, inbox |
| `sisa-messaging-nats` | JetStream publisher, delivery source, wire mapper and settlement | messaging |
| `sisa-messaging-kafka` | Kafka publisher and wire mapper; partitioned-log delivery source and settlement in Phase 6a | messaging |

Provider crates never depend on each other. Examples, system tests, and the system benchmark are
the only workspace members that compose PostgreSQL, NATS, outbox, inbox, and consumer crates.

## 4. Implementation sequence

### Phase 1 — workspace foundation

- Create Rust 2024 manifests with `rust-version = "1.94"` inherited by every crate. Pin the
  development toolchain independently to the chosen current stable patch release.
- Create the license, security policy, dependency policy, lint policy, CI, release metadata, and
  feature-matrix jobs.
- Add a repository-root `.coderabbit.yaml` as the AI review layer. Keep it aligned with the
  normative docs and crate boundaries; CodeRabbit remains review input rather than the source of
  truth. GitHub CI separately rejects any commit above 20 changed paths and any PR at 100 or more
  changed paths; the non-overridable 99-path ceiling preserves CodeRabbit's strict less-than-100
  reviewability boundary because CodeRabbit path filters do not change the PR's actual file count.
- Declare each dependency once in `[workspace.dependencies]` and inherit exact workspace choices.
- Add architecture tests that reject provider-to-provider dependencies, library environment
  reads, exporter/SDK dependencies, `async-trait`, and unsafe code.
- Configure Atlas Community Edition for versioned migrations without committed database URLs or
  credentials. Keep migration execution outside every Rust crate.
- Pin the Atlas Community CLI release or container digest in CI and upgrade it through an explicit
  tooling change; do not build releases against a floating `latest-community` image.
- Establish the public API inventory before publishing a crate.
- Run a dedicated CI build on Rust 1.94; run formatting, Clippy, documentation, full tests, and
  benchmarks with the pinned development toolchain. Raising that toolchain alone does not raise
  the published MSRV.

### Phase 2 — shared messaging model

- Implement message and envelope identities, metadata, headers, serialization, failure
  classification, publisher, mapper, and closed individual-delivery and partitioned-log inbound
  contracts.
- Keep individual settlement capabilities truthful through immutable opened-source descriptors;
  reject unsupported required operations rather than emulating them. Require fenced, ordered
  partition advancement after durable resolution and surface ownership loss explicitly.
- Keep wire-independent values free of SQLx, async-nats, Tokio runtime, and telemetry SDK types.
- Freeze metadata JSON and framework-header mappings with contract fixtures and round-trip
  tests.
- Benchmark construction, validation, serialization, and safe error rendering.
- Defer concrete envelope-to-wire mapping benchmarks to the Phase 6 NATS provider, where the real
  subject, header, and wire projection exists.

### Phase 3 — outbox contracts and dispatcher

- Implement focused enqueue, worker-store, maintenance, and dead-letter capabilities.
- Implement retry policy and validate all dispatcher settings once at construction.
- Put the dispatcher façade and top-level coordination in `dispatcher.rs`; keep claim, publish,
  outcome, lease, state, and shutdown mechanics in `dispatcher/*.rs` without a `mod.rs`.
- Apply the same root-file-plus-directory pattern when publisher, maintenance, dead-letter,
  consumer, or provider implementations develop multiple distinct responsibilities.
- Guarantee readiness-only `select!` branch futures and bounded external operations.
- Add direct OTel API instruments and explicit unbranded tracing targets.

### Phase 4 — inbox contracts

- Implement transactional claim/complete and classified failure recording after rollback.
- Implement the unit-of-work capability for framework-managed transactions.
- Keep manual API outcomes explicit and omit any helper that performs only half of the
  transaction/settlement protocol.
- Implement terminal maintenance and dead-letter capabilities.

### Phase 5 — PostgreSQL provider and migration baseline

- Add the immutable PostgreSQL 18 baseline at `migrations/0001_messaging.sql`, generate
  `migrations/atlas.sum`, and apply it externally with Atlas Community Edition.
- Expose no Rust migration API and do not enable SQLx's migration feature. SQLx remains the runtime
  query client.
- Store `PgPool` directly and accept caller transactions/executors only where the capability needs
  them.
- Implement the shared additive metadata codec and structured SQLSTATE classifier.
- Implement claim-token fencing, short claims, poison isolation, per-key ordering, keyset
  pagination, bounded maintenance, and inbox advisory-lock behavior.
- Generate SQLx offline metadata and add real concurrency and query-plan tests.

### Phase 6 — NATS provider

- Implement subject resolution, envelope/header mapping, current negotiated payload checking, and
  JetStream publication with awaited acknowledgement.
- Implement delivery source, confirmed ack, delayed nak, terminate, and heartbeat acknowledgement.
- Keep stream/consumer creation, credential and TLS policy, connection initiation, and reconnect
  supervision application-owned. Offer typed provider settings and an explicit start operation
  that hides the NATS SDK client behind a provider handle.
- Implement OTel messaging semantic conventions without payload, raw dynamic subject, credential,
  or header-value leakage.

### Phase 6a — Kafka provider

- Use `rdkafka = 0.39.0` with an exact workspace pin. Its locked `rdkafka-sys
  4.10.0+2.12.1` builds and statically links bundled `librdkafka 2.12.1` with the provider's
  current feature set. The Rust bindings are MIT licensed; bundled librdkafka has
  a permissive BSD-style license with binary redistribution notice requirements. Review the
  native dependency and advisories during each version update, and run `cargo deny check` whenever
  dependency state changes. At selection, the upstream Rust repository was active (last push
  2026-07-15 and not archived); the exact pin makes upgrades an explicit security review decision.
- Let the application supply Kafka settings, explicitly start and own the provider handle, and
  supervise its lifecycle. The provider constructs its internal SDK client. Keep credentials/TLS,
  topic/group provisioning, and reconnect policy application-owned.
- Map envelopes and publish only after the configured delivery report. Implement the partitioned-log
  source and settlement profile with ordered post-processing offset commits, ownership fencing,
  cancellation safety, and explicit handling of ambiguous commits.
- Exercise deterministic contract tests and real Kafka publish, redelivery, rebalance, offset-gap,
  cancellation, source-failure, and commit-ambiguity cases. Record the Kafka mapping benchmark and
  its measured environment and result before provider review completes.

### Phase 7 — consumer runtime

- Implement typed `ConsumerHandler` and the bounded `Consumer` receive/process/settle loop.
- Open the source once under a timeout, validate individual-profile requirements against its
  immutable descriptor, and require cancel-safe receive readiness afterward.
- Centralize the full claim/handle/rollback/fail/complete/commit/settle decision table.
- Coordinate individual-profile heartbeat acknowledgement without placing workflow I/O in
  cancellable `select!` branch futures; do not represent automatic partition commit.
- Implement stop-receiving, bounded-drain, abort, and unacknowledged-redelivery shutdown.
- Test every settlement branch with deterministic protocol implementations, then prove the same
  behavior with real PostgreSQL and NATS.

### Phase 8 — examples and system proof

- Demonstrate a transactional application write plus outbox enqueue.
- Demonstrate dispatcher publication to JetStream.
- Demonstrate the typed NATS/PostgreSQL consumer, including an outbound event in the same handler
  transaction.
- Retain one manual inbox example for applications with an existing consumer engine.
- Demonstrate dead-letter, retry, retention, and statistics operations.

### Phase 9 — performance baseline

- Implement every crate and system scenario from [Benchmark program](benchmarks.md).
- Capture PostgreSQL plans and messaging/full-pipeline measurements on pinned hardware.
- Measure direct OTel overhead with a no-op provider, SDK-only provider, and representative local
  export pipeline.
- Commit machine-readable baseline metadata and results used for subsequent comparisons.

### Phase 10 — release readiness

- Complete every quality gate and review generated rustdoc.
- Verify feature combinations and dependency graphs from a downstream application fixture.
- Rehearse an empty PostgreSQL 18 install and application-owned schema selection.
- Package the complete checked migration directory as a versioned release asset compatible with
  `sisa-messaging-postgres`; verify the asset digest and a clean install from the packaged copy.
- Publish in dependency order: messaging; Kafka; outbox, inbox, and NATS; then consumer and PostgreSQL.
- Tag the compatible workspace and schema baseline together.

## 5. Dependency and feature policy

- A crate directly declares every external crate whose symbols or macros it uses. Transitive dependencies
  never satisfy source-level dependency ownership.
- Default features remain minimal. Serde support is feature-gated where it is not fundamental to
  the contract.
- Provider SDK features are limited to used capabilities. Applications choose TLS crypto,
  authentication, exporters, and runtime-wide integrations through Cargo feature unification.
- Library crates use `thiserror`, `tracing`, and the OTel API only where directly needed.
- OTel SDKs, exporters, `tracing-subscriber`, testcontainers, Criterion, profilers, and load-driver
  dependencies remain outside published runtime dependency paths.
- There is one Tokio version, UUID version, time library, Serde version, SQLx version, async-nats
  version, and OTel API version across the workspace.

Run `cargo tree -d` and `cargo deny check` as review inputs; duplicate crate versions require an explicit
reason rather than an automatic rejection when ecosystems cannot yet converge.

## 6. Test architecture

### Unit and property tests

- Value validation, parsing, retry math, state reduction, error mapping, redaction, metadata
  codecs, subject mapping, and settings validation.
- Property tests for metadata/header round trips, retry bounds, cursor monotonicity, and panic-free
  malformed input.

### Compile and architecture tests

- `Send`/`Sync` and native async trait bounds.
- Supported feature combinations and downstream construction examples.
- Crate dependency direction and forbidden dependencies/APIs.
- Public API snapshots for accidental surface growth.

### PostgreSQL integration tests

- Transaction atomicity, concurrent claiming, fencing, ordering, retries, expiry, poison rows,
  inbox advisory locking, dead letters, retention, and representative query plans.
- Tests use PostgreSQL 18 and the repository's immutable baseline schema.

### NATS integration tests

- Awaited publish acknowledgement, broker deduplication, negotiated payload limit, mapping,
  confirmed consumer ack, nak, terminate, heartbeat, redelivery, cancellation, and safe errors.
- Tests use a real JetStream server, with fault injection where an ambiguity window must be shown.

### System tests

- Transactional enqueue through dispatcher and JetStream.
- Typed consumer commit-before-ack and rollback-before-failure-recording.
- Crash/lease/drain duplicate windows and completed-inbox redelivery.
- Concurrent dispatchers and consumers.
- Trace/metadata continuity and observability redaction.

Test names describe behavior and risk. Numeric decision IDs and implementation-history labels do
not appear in test names.

## 7. Required behavior inventory

### Outbox

- Business and outbox rows commit or roll back together.
- Duplicate `message_id` aborts enqueue transaction.
- Concurrent workers claim disjoint rows and publish outside database transactions.
- Every outcome is claim-token fenced; lease loss cannot overwrite a newer claim.
- Crash, lease-expiry, acknowledgement-loss, and drain duplicate windows are demonstrated.
- Retry uses database time; permanent/exhausted/expired/undecodable rows become dead correctly.
- Durable ID ordering within a key, poison isolation, keyset pagination, and bounded maintenance
  remain correct under concurrency.

### Inbox and consumer

- Concurrent claims for one key run at most one handler.
- Handler effects and inbox completion commit atomically.
- A broker acknowledgement never precedes commit.
- Partition advancement never precedes commit or a durable terminal disposition, never crosses an
  unresolved earlier offset in its partition, and is fenced against ownership loss.
- Completed and in-progress duplicates never invoke the handler.
- Rollback precedes classified failure recording; permanent and exhausted failures become dead.
- Commit ambiguity never records handler failure or acknowledges.
- Permanent provider/settlement failures stop the runtime and leave the delivery unresolved.
- Individual-profile heartbeat acknowledgement protects slow work without becoming a correctness
  mechanism; sources without it do not claim it.
- Cancellation bounds new work, drains completions, and leaves unresolved deliveries for
  redelivery.
- Ownership loss stops its partition without advancement. An error, timeout, cancellation, or
  dropped partition advance is indeterminate: pause that partition and reconcile its authoritative
  committed cursor and ownership generation before later offsets; replay only if it did not
  advance, continue only if it did, and otherwise remain paused or fail. Other partitions may
  continue. A malformed partitioned record without a trustworthy identity and durable terminal
  disposition pauses its partition rather than skipping the offset.
- Retention deletes only completed/dead receipts; dead retry clears death fields and attempts.

### NATS and observability

- Framework headers round-trip without custom-header collision.
- Publication awaits JetStream acknowledgement and honors the negotiated payload limit.
- Errors use structured classification and render no payload, secret, or header values.
- Metrics contain no identity labels and empty database levels reset gauges to zero.
- Returned errors are not also logged by conversion helpers.
- Messaging spans/metrics use the pinned OTel semantic-convention version.

## 8. Completion gates

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --workspace --no-default-features
cargo doc --workspace --all-features --no-deps
cargo bench --workspace --all-features --no-run
cargo deny check
cargo semver-checks for every previously published crate
Atlas migration checksum is unchanged after regeneration
Atlas versioned migrations apply cleanly to empty PostgreSQL 18
Atlas reports no pending migration after the fresh apply
SQLx offline metadata check
real PostgreSQL 18 integration suite
real NATS JetStream integration suite
dependency-direction and forbidden-API checks
secret/payload observability redaction tests
system benchmark smoke profile with correctness oracle
```

## 9. Definition of done

- Public APIs, rustdoc, examples, versioned migrations, and these architecture documents agree.
- The crate graph has no provider-to-provider edge or dependency cycle.
- Atlas Community Edition reproduces the documented schema from the versioned migration directory
  on an empty PostgreSQL 18 database.
- Applications supply settings; libraries read no environment variables or configuration files.
- Dispatcher and consumer loops are decomposed, bounded, and cancellation-safe.
- PostgreSQL stores own application-supplied pools; operation signatures accept a pool,
  transaction, or executor according to ownership and atomicity.
- Normal consumer integrations contain no application copy of the transaction/settlement state
  machine.
- Errors are typed, sourced, structured for retry decisions, safely rendered, and logged once.
- OTel names are unbranded and metric labels are bounded.
- A reproducible machine-readable performance baseline covers every crate and the full pipeline.
