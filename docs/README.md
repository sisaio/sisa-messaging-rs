# Sisa Messaging documentation

**Status:** architecture baseline for `sisa-messaging-rs`, 2026-09-11.

These documents define the crates, contracts, database, runtime behavior, observability,
performance program, and engineering rules for the repository. They are normative: implementation
and public documentation must agree with them.

The project is a set of Rust libraries for durable message publication, transactional consumer
deduplication, a typed consumer runtime, PostgreSQL persistence, NATS JetStream transport, and
Kafka publication. It is not a service and does not own application startup, configuration loading,
database pools, broker connection lifecycles, telemetry exporters, or process shutdown.

## Read in this order

1. [Architecture](architecture.md) — boundaries, crate graph, public capabilities, construction,
   and source layout.
2. [Database](database.md) — the physical ERD, two-table model, invariants, state derivation,
   query/index contract, and metadata representation.
3. [Migration lifecycle](migrations.md) — Atlas's development, CI, release, and production roles,
   plus public migration distribution.
4. [Runtime flows](runtime-flows.md) — Mermaid sequences and decision flows for enqueue, dispatch,
   retry, shutdown, inbox processing, NATS publication, and maintenance.
5. [Consumer framework](consumer-framework.md) — typed handlers, delivery settlement,
   concurrency, heartbeat acknowledgement, and graceful shutdown.
6. [API and code conventions](api-conventions.md) — settings, constructors, validation, errors,
   IDs, async traits, and module rules.
7. [Observability](observability.md) — tracing, logging, direct OpenTelemetry metrics, names,
   attributes, and ownership.
8. [Benchmark program](benchmarks.md) — micro, PostgreSQL, NATS, consumer, and full-pipeline
   performance measurement.
9. [Implementation plan](implementation-plan.md) — repository layout, build order, test strategy,
   dependency policy, and completion gates.
10. [`0001_messaging.sql`](../migrations/0001_messaging.sql) — the executable PostgreSQL 18+
   baseline.
11. [Agent workflow](agent-workflow.md) — bounded task packets, review rounds, Git delivery, and
    review-size policy.
12. [Agent team](agent-team.md) — role boundaries, configuration, escalation, and measured
    orchestration rationale.

## Fixed decisions

- Eligible publication uses at-least-once broker delivery when the publisher confirms broker
  acceptance. Kafka `acks=0` allows a successful client delivery report without broker
  acknowledgement, so the outbox can complete a row the broker never received. Duplicate
  publication is still possible; expiry, permanent failure, or exhausted retry policy can instead
  make a row dead.
- The durable and direct paths remain visibly different: the store enqueues; the transport
  publishes.
- The application owns pools, broker connection initiation and lifecycle, broker resources,
  authentication policy, configuration sources, telemetry providers, and task supervision. A
  transport provider may construct its internal SDK client from application-supplied settings and
  return an application-owned handle. The application either owns inbox transactions through the
  low-level API or explicitly delegates each delivery transaction to the consumer framework.
- Libraries accept typed settings and never read environment variables.
- Every crate uses Rust edition 2024 with a single workspace MSRV of Rust 1.94. Provider crates
  do not introduce separate MSRV exceptions.
- Providers use static dispatch. No service locator, DI container, `async-trait`, or hot-path
  `Box<dyn Trait>` is introduced.
- PostgreSQL publication happens outside database transactions. Short statements claim and
  record outcomes using a per-claim fencing token.
- Inbox effects are effectively once only when those effects and the inbox completion share the
  caller's PostgreSQL transaction.
- PostgreSQL 18+ is the only supported database baseline; `uuidv7()` is used for row identities.
- `outbox_messages` and `inbox_receipts` are the fixed table names. A configured `search_path`
  selects their schema.
- Atlas Community Edition applies immutable versioned migrations outside the Rust crates. SQLx is
  used for runtime queries, not migration execution.
- Each database-contract release publishes the complete migration directory as a versioned
  artifact compatible with that release of `sisa-messaging-postgres`.
- OpenTelemetry data has no product-brand or crate-name prefix. Service identity comes from
  resource attributes and instrumentation scope.
- The standard consumer path is typed, bounded, transport-neutral, and owns the complete
  transaction-before-ack state machine.

## Excluded by design

- Library environment/file/argument configuration loading.
- Fluent setters that only assign a public field and duplicate constructor families.
- A partial inbox processing helper that stops before transaction and broker settlement; the
  consumer crate owns the complete protocol instead.
- Provider-owned database connection policy.
- An injected metrics facade, no-op metrics implementation, or dispatcher metrics generic.
- Statistics polling hidden inside every dispatcher.
- Automatic retention purge of incomplete inbox receipts.
- Public ID-generating macros.
- Logging inside error-conversion and classification helpers.

## Design rule

Every public type and operation must pay for itself with one of these:

- a correctness invariant;
- a capability boundary used by more than one implementation or caller;
- a material ergonomic improvement at a common call site; or
- a stable representation that crosses a crate, database, or wire boundary.

Symmetry, hypothetical future use, and one-line field assignment are not sufficient reasons to
add public API.
