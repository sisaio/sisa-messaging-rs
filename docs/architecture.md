# Architecture

## 1. Purpose and guarantees

Sisa Messaging provides two independent reliability mechanisms:

- a transactional outbox that commits a message with application data and publishes it later;
- a transactional inbox that deduplicates consumer effects in the application's transaction.

The outbox durably retries eligible messages. With broker-confirmed publisher settings, it provides
**at-least-once** publication semantics. Kafka `acks=0` can report success without broker receipt,
so the outbox may complete a row that Kafka never received. Once a broker may have accepted a
publish, duplicates are possible. The outbox does not promise that every row is eventually
published: expiry, a permanent failure, or exhausted retry policy moves a row to dead.
It does not guarantee exactly-once publication or global ordering. An ordering key serializes
publication for that key in durable row ID order; it does not claim application commit order or
domain sequence order.

The inbox provides **effectively-once database effects** only for writes committed in the same
transaction as the inbox completion. Calls to other databases, HTTP services, file systems, or
other brokers remain at least once and must be idempotent independently.

## 2. Crate names

The naming scheme is `sisa-<family>-<member>`: brand first, capability family second, member
third. Technology names already communicate the provider role, so `store-` and `transport-` are
not used.

| Crate | Responsibility |
|---|---|
| `sisa-messaging` | Envelope, metadata, message identity, serialization, publication and delivery contracts, failure classification |
| `sisa-messaging-outbox` | Outbox contracts, dispatcher, retry policy, records, maintenance and dead-letter operations |
| `sisa-messaging-inbox` | Inbox contracts, outcomes, records, unit of work, maintenance and dead-letter operations |
| `sisa-messaging-consumer` | Typed inbound receive/process/settle runtime |
| `sisa-messaging-postgres` | PostgreSQL runtime implementations for outbox and inbox |
| `sisa-messaging-nats` | NATS JetStream mapping, subject resolution, publication, and inbound delivery |
| `sisa-messaging-kafka` | Kafka envelope mapping, topic resolution, and outbound publication |
| `sisa-messaging-iggy` | Apache Iggy envelope mapping, stream/topic resolution, and outbound publication |
| `sisa-messaging-redis` | Redis Streams envelope mapping, publication, and individual inbound delivery |

Rust import names follow Cargo's hyphen-to-underscore conversion, for example
`sisa_messaging_outbox`.

Sibling families follow the same rule: `sisa-caching-memory`, `sisa-caching-redis`,
`sisa-caching-hybrid`, `sisa-configuration`, `sisa-secrets`, and `sisa-scheduler`. This repository
contains only the messaging family.

## 3. Dependency direction

```text
sisa-messaging-outbox  ───────▶ sisa-messaging
sisa-messaging-inbox   ───────▶ sisa-messaging
sisa-messaging-consumer ──────▶ sisa-messaging + sisa-messaging-inbox
sisa-messaging-postgres ──────▶ sisa-messaging + outbox + inbox
sisa-messaging-nats ──────────▶ sisa-messaging
sisa-messaging-kafka ─────────▶ sisa-messaging
sisa-messaging-iggy ──────────▶ sisa-messaging

application / system tests compose postgres + outbox + consumer + a transport provider
```

Rules:

- `sisa-messaging` has no SQLx, PostgreSQL, transport SDK, Tokio runtime, exporter, or application
  configuration dependency.
- Outbox and inbox depend only on the shared messaging model and the minimum runtime utilities
  their algorithms require.
- PostgreSQL depends on messaging, outbox, and inbox because it implements their traits.
- Consumer depends on messaging and inbox. It names neither SQLx nor a transport provider.
- NATS depends only on messaging. It implements both outbound publication and inbound delivery
  contracts, and does not know an outbox or inbox exists.
- Kafka depends only on messaging. It maps envelopes and implements outbound publication without
  depending on the outbox or inbox.
- Iggy depends only on messaging. It maps envelopes and implements outbound publication over the
  Iggy TCP protocol; its partitioned-log delivery source is deferred until Iggy can fence
  consumer-group offset stores by membership generation. Iggy limits each header name and value
  to 255 bytes: a custom header value over that bound is a permanent mapping error, and an
  oversized `tracestate` is omitted under the W3C Trace Context allowance while an oversized
  `traceparent` is rejected as a permanent mapping error.
- Redis depends only on messaging. It maps envelopes, publishes to a configured stream, and
  receives individual deliveries from a caller-provisioned consumer group. Its source descriptor
  advertises no delayed retry, terminal discard, or heartbeat; unsupported settlement operations
  return `Unsupported`. Cancellation leaves an unacknowledged entry pending for later bounded
  reclaim.
- Provider crates never depend on one another.
- Only applications and system tests name concrete provider combinations.

## 4. Ownership boundaries

| Concern | Owner |
|---|---|
| Outbox/manual-inbox transaction and commit/rollback | Application |
| Framework-managed per-delivery transaction | `sisa-messaging-consumer`, explicitly delegated by application |
| PostgreSQL pool and connection policy | Application |
| Transport authentication, TLS, connection initiation/lifecycle, and returned handle | Application |
| SDK client construction behind a provider handle, when supported | Transport provider |
| NATS stream and consumer provisioning | Application |
| Redis stream and consumer-group provisioning | Application |
| Configuration source and deserialization | Application |
| Envelope and transport-independent metadata | `sisa-messaging` |
| Retry decision and worker lifecycle | `sisa-messaging-outbox` |
| Consumer receive/process/settle sequencing | `sisa-messaging-consumer` |
| SQL state transitions | `sisa-messaging-postgres` |
| Versioned PostgreSQL migration authoring and release bundle | Repository, through Atlas Community Edition |
| Production schema deployment | Consumer's deployment pipeline/operator |
| NATS subject and wire projection | `sisa-messaging-nats` |
| OTel SDK, exporters, resource, filters and shutdown | Application |
| Metric instruments and tracing callsites | Owning library crate |
| Purge and statistics schedules | Application or one explicit supervised maintenance task |

Nothing starts in a constructor. Constructors perform no I/O. A host explicitly starts transport
clients, the dispatcher, observer, purge task, and consumer loop. Providers may offer typed settings
and an explicit start operation so the host does not import a broker SDK. Such an operation does not
imply broker readiness unless it performs and documents a bounded readiness check.

## 5. Public capabilities

The public API is organized around what a caller can do, not around internal layers.

### `sisa-messaging`

- `Message`: stable `TYPE`, `VERSION`, and optional ordering key.
- `Envelope<T>` and `SerializedEnvelope`.
- `Metadata` and validated transport-independent value types.
- `Serializer` and the optional JSON implementation.
- `Publisher`: one publish attempt that completes at the provider's configured confirmation level.
- `Delivery`: splits an owned wire value from a profile-bound settlement handle.
- `IndividualDeliverySource` and `IndividualSettlement`: individual-delivery receive and
  settlement, with an immutable source descriptor and truthful delayed-retry, terminal-discard,
  and heartbeat support.
- `PartitionedLogDeliverySource` and `PartitionedLogSettlement`: ordered partition progression
  with explicit ownership loss and consuming, fenced offset advancement.
- `ErrorClassifier` and `FailureKind`: retryability attached to errors that cross retry boundaries.
- `EnvelopeMapper`: transport wire conversion.

### `sisa-messaging-outbox`

- `OutboxEnqueue<Tx>`: writes an envelope into the caller's transaction.
- `OutboxStore`: claim, complete, fail, release, and extend a lease.
- `OutboxMaintenance`: purge and statistics; separate because the dispatcher does not require it.
- `OutboxDeadLetters`: list, retry, and delete dead rows.
- `OutboxDispatcher`: composes one store and one publisher.
- `RetryPolicy` and `ExponentialBackoff`.

`OutboxStore` excludes purge and statistics; those operations belong to `OutboxMaintenance`. This
keeps the hot worker capability focused and lets a provider or test implementation supply only
what the dispatcher consumes.

### `sisa-messaging-inbox`

- `InboxStore<Tx>`: claim and complete in the caller transaction; record a classified failure
  separately after rollback.
- `InboxUnitOfWork`: begin, commit, and roll back a provider transaction when transaction ownership
  is delegated to the consumer framework.
- `InboxMaintenance`: terminal-row purge and diagnostics.
- `InboxDeadLetters`: list, retry, and delete dead receipts.

The inbox crate does not expose a half-orchestrating processing helper. A helper that cannot
commit, roll back, record failure after rollback, and settle the broker still leaves the dangerous
half of the protocol to every caller. The consumer framework owns the complete protocol; a
lower-level caller uses `claim`/`complete`/`fail` explicitly and owns every operation itself.

### `sisa-messaging-consumer`

- `ConsumerHandler<M, Tx>`: application business logic over a typed envelope and transaction.
- `ConsumerSettings`: concurrency, timeout, heartbeat, negative-ack, and drain policy.
- `Consumer`: bounded typed receive/process/settle loop.
- `ConsumerExit` and `ConsumerError`: clean source/cancellation exits versus fatal supervision
  failures.

One consumer handles one message type/version by default. See
[Consumer framework](consumer-framework.md) for the complete state machine and integration API.

### Provider crates

- `PostgresOutboxStore<Ser>` implements enqueue, worker storage, maintenance, and dead letters.
- `PostgresInboxStore` implements inbox storage, maintenance, and dead letters.
- `NatsPublisher<R>` implements `Publisher` and awaits the JetStream acknowledgement.
- `NatsDeliverySource`, `NatsDelivery`, and `NatsSettlement` implement inbound receive and
  settlement contracts.
- The Redis Streams publisher and delivery source implement outbound publication, envelope
  mapping, and individual inbound delivery on servers that implement the required stream commands.
  The tested Garnet 2.1.8 image returns `ERR unknown command` for `XADD`, so it cannot run this
  delivery path.

## 6. Canonical construction

Each runtime class has one canonical constructor. There is no paired `new()` / `with_settings()`
surface that constructs the same value in two ways.

```rust,ignore
let outbox_store = PostgresOutboxStore::new(pool.clone(), serializer);
let inbox_store = PostgresInboxStore::new(pool.clone(), inbox_settings);

let publisher = NatsPublisher::new(jetstream, resolver, nats_publisher_settings)?;
let dispatcher = OutboxDispatcher::new(outbox_store, publisher, dispatcher_settings)?;

let source = NatsDeliverySource::new(pull_consumer);
let mapper = NatsMapper::new(inbound_subject_resolver);
let consumer = Consumer::new(
    source,
    mapper,
    JsonSerializer,
    inbox_store,
    scope,
    handler,
    consumer_settings,
)?;
```

The consumer lines are an intended integration sketch until the consumer runtime is implemented.
The application supplies `inbound_subject_resolver` because the mapper also supports encoding.

Defaults are applied by the application:

```rust,ignore
let dispatcher = OutboxDispatcher::new(store, publisher, DispatcherSettings::default())?;
```

This makes every dependency visible while avoiding a builder whose only job is field assignment.
Custom serializers and subject resolvers are ordinary constructor arguments rather than alternate
constructor families.

## 7. Static dispatch and async contracts

Store, publisher, serializer, handler, resolver, and inbound profile boundaries use generics.
Native async trait methods return `impl Future + Send`; library crates do not use `async-trait` or
allocate a boxed future on each operation. The two inbound profiles are explicit, closed trait
pairs rather than combinable capability markers or a per-record runtime capability query. This
keeps individual settlement and partition fencing statically distinct.

Dynamic dispatch remains acceptable at cold application-owned extension points, but no hot
claim/publish/complete/settle path requires it.

## 8. Repository source layout

```text
crates/
├── sisa-messaging/
│   └── src/
│       ├── lib.rs
│       ├── envelope.rs
│       ├── metadata.rs
│       ├── message.rs
│       ├── ids.rs
│       ├── headers.rs
│       ├── serializer.rs
│       ├── publisher.rs
│       ├── delivery.rs
│       ├── mapper.rs
│       ├── failure.rs
│       └── error.rs
├── sisa-messaging-outbox/
│   └── src/
│       ├── lib.rs
│       ├── dispatcher.rs      # public façade and top-level coordination
│       ├── dispatcher/
│       │   ├── state.rs        # in-memory leased/outcome state
│       │   ├── claim.rs        # claim scheduling and capacity
│       │   ├── publish.rs      # spawn/join publish attempts
│       │   ├── outcomes.rs     # complete/fail persistence
│       │   ├── leases.rs       # renewal and fencing shortfalls
│       │   └── shutdown.rs     # drain and release
│       ├── enqueue.rs
│       ├── store.rs
│       ├── maintenance.rs
│       ├── dead_letters.rs
│       ├── retry.rs
│       ├── settings.rs
│       ├── telemetry.rs
│       └── error.rs
├── sisa-messaging-inbox/
│   └── src/
│       ├── lib.rs
│       ├── store.rs
│       ├── claim.rs
│       ├── failure.rs
│       ├── record.rs
│       ├── maintenance.rs
│       ├── dead_letters.rs
│       ├── settings.rs
│       ├── unit_of_work.rs
│       └── error.rs
├── sisa-messaging-consumer/
│   └── src/
│       ├── lib.rs
│       ├── consumer.rs         # public façade and top-level receive loop
│       ├── consumer/
│       │   ├── receive.rs
│       │   ├── process.rs
│       │   ├── settlement.rs
│       │   ├── worker.rs
│       │   └── shutdown.rs
│       ├── handler.rs
│       ├── settings.rs
│       ├── telemetry.rs
│       └── error.rs
├── sisa-messaging-postgres/
│   └── src/
│       ├── lib.rs
│       ├── metadata.rs
│       ├── error.rs
│       ├── outbox.rs           # PostgresOutboxStore façade and trait implementations
│       ├── outbox/
│       │   ├── enqueue.rs
│       │   ├── claim.rs
│       │   ├── outcomes.rs
│       │   ├── maintenance.rs
│       │   └── dead_letters.rs
│       ├── inbox.rs            # PostgresInboxStore façade and trait implementations
│       └── inbox/
│           ├── claim.rs
│           ├── outcomes.rs
│           ├── maintenance.rs
│           └── dead_letters.rs
├── sisa-messaging-nats/
│   └── src/
│       ├── lib.rs
│       ├── publisher.rs        # NatsPublisher façade and Publisher implementation
│       ├── publisher/          # created only when publisher.rs has distinct concerns
│       │   ├── request.rs
│       │   └── acknowledgement.rs
│       ├── delivery_source.rs
│       ├── delivery.rs
│       ├── mapper.rs
│       ├── headers.rs
│       ├── subject.rs
│       ├── settings.rs
│       └── error.rs
└── sisa-messaging-redis/
    └── src/
        ├── lib.rs
        ├── publisher.rs        # Redis Streams publisher
        ├── source.rs           # IndividualDeliverySource and bounded pending recovery
        ├── mapper.rs
        └── error.rs
```

`lib.rs` contains only crate documentation, module declarations, and re-exports. It contains no
type, function, implementation, or runtime logic. The repository does not use `mod.rs`.

A complex capability uses a root file beside a same-named directory. For example,
`dispatcher.rs` contains `OutboxDispatcher`, its public entry points, and top-level coordination;
`dispatcher/claim.rs`, `dispatcher/publish.rs`, and the other children contain its focused
mechanics. The same pattern applies when publisher, consumer, maintenance, dead-letter, or provider
code develops multiple distinct responsibilities. A small cohesive capability remains one file;
the directory is not created merely for symmetry.

PostgreSQL stores own application-supplied `PgPool` values. Provider-owned operations use the
pool, query functions accept `PgExecutor` when they genuinely support both pool and transaction
contexts, and application-atomic operations accept the caller's transaction explicitly.

## 9. Non-goals

- Exactly-once broker delivery.
- A runtime provider registry.
- A framework that owns application startup.
- Broker stream/consumer provisioning or automatic source reconnection policy.
- An internal configuration loader.
- Cross-database transactions.
- Persisting inbox payloads as an event archive.
- Automatically provisioning NATS streams or consumers.
- Treating observability as a correctness source; the database remains authoritative.
