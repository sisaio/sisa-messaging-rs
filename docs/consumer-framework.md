# Consumer framework

## 1. Purpose

`sisa-messaging-consumer` turns an inbound delivery into one correctly settled transactional
handler execution. It exists because every application should not have to reproduce the same
claim, rollback, failure-recording, commit, acknowledgement, negative-acknowledgement, and
termination branches.

The framework provides convenience without weakening ownership boundaries:

- the application creates the database pool, broker client, stream, and durable consumer;
- the application supplies the handler, scope, codec, mapper, and typed settings;
- the framework owns the per-delivery transaction and settlement sequence;
- the application starts, cancels, awaits, and restarts the consumer task.

Using the framework is the recommended path. The lower-level inbox traits remain public for hosts
with an existing consumer runtime or an unusual transaction model.

## 2. Crate placement

The consumer runtime crate is `sisa-messaging-consumer`.

```text
sisa-messaging ───────────────▶ inbound delivery contracts and envelopes
       ▲
       ├── sisa-messaging-nats ─▶ NATS delivery source, mapper and settlement
       ├── sisa-messaging-rabbitmq ─▶ RabbitMQ source, mapper and settlement
       │
       └── sisa-messaging-consumer ─▶ generic receive/process/settle runtime
                    │
                    ▼
          sisa-messaging-inbox ─────▶ inbox and unit-of-work contracts
                    ▲
                    │
          sisa-messaging-postgres ──▶ PostgreSQL implementations
```

`sisa-messaging-consumer` does not depend on SQLx, async-nats, or lapin. No provider depends on
another. The application is still the only place that chooses the transport/PostgreSQL combination.

Transport-neutral inbound contracts live in `sisa-messaging`. `Delivery` always splits once into
an owned transport wire value and a profile-bound settlement handle; `EnvelopeMapper<Wire>` is the
same mapping contract used for publication. The profiles are closed and statically dispatched:

- `IndividualDeliverySource` and `IndividualSettlement` model per-delivery acknowledgement.
  Opening returns an immutable descriptor for acknowledgement wait, delivery bound, delayed retry,
  terminal discard, and heartbeat support; caller requirements are validated before receiving.
- `PartitionedLogDeliverySource` and `PartitionedLogSettlement` model ordered offset progression.
  Receive distinguishes a delivery, ownership loss, and clean close; consuming advancement reports
  either success or ownership loss.

Their semantic shape is:

```rust,ignore
pub trait IndividualSettlement: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static + ErrorClassifier;

    fn heartbeat(
        &mut self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;
    fn ack(self) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;
    fn nak(
        self,
        delay: Duration,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;
    fn terminate(
        self,
    ) -> impl Future<Output = Result<(), IndividualSettlementError<Self::Error>>> + Send;
}

pub trait Delivery: Send + 'static {
    type Wire: Send + 'static;
    type Settlement: Send + 'static;

    fn into_parts(self) -> (Self::Wire, Self::Settlement);
}

pub trait IndividualDeliverySource: Send {
    type Delivery: Delivery<Settlement: IndividualSettlement>;
    type Error: std::error::Error + Send + Sync + 'static + ErrorClassifier;

    fn open(
        &mut self,
        requirements: IndividualSourceRequirements,
    ) -> impl Future<
        Output = Result<IndividualSourceDescriptor, IndividualSourceOpenError<Self::Error>>,
    > + Send;

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send;

}

pub trait PartitionedLogSettlement: Send + 'static {
    type Partition: Clone + Eq + Hash + Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static + ErrorClassifier;

    fn advance(self) -> impl Future<Output = Result<PartitionAdvance, Self::Error>> + Send;
    fn partition(&self) -> &Self::Partition;
}

pub trait PartitionedLogDeliverySource: Send {
    type Partition: Clone + Eq + Hash + Send + Sync + 'static;
    type Delivery: Delivery<Settlement: PartitionedLogSettlement<Partition = Self::Partition>>;
    type Error: std::error::Error + Send + Sync + 'static + ErrorClassifier;

    fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<PartitionedLogReceive<_, _>, Self::Error>> + Send;
}
```

Individual terminal operations consume the handle, preventing a second terminal action through
safe Rust. Splitting lets the mapper consume the wire value without cloning or losing a settlement
capability. An individual `None` receive is a clean source close; a partitioned source returns its
distinct `Closed` outcome. Opening performs one-time initialization when `Consumer::run` starts;
constructors remain I/O-free. After opening, every readiness wait must be cancel-safe: dropping it
must neither lose nor settle/advance a delivery. Unsupported individual requirements fail at open
with a bounded classified error and are never emulated.

A partition settlement owns opaque partition, offset, and fencing generation, and exposes its
partition so a generic coordinator can associate an ownership-loss event. It advances only after
the consumer transaction commits or a durable terminal disposition exists. Initially the consumer
permits one unresolved record per partition: a later offset cannot advance until its earlier record
resolves. `OwnershipLost` proves fencing prevented advancement. A timeout, transient advance error,
or dropped advance future is indeterminate: the consumer leaves that partition unresolved and the
source pauses it. Before reading the authoritative committed cursor and ownership generation, the
source must establish that the previous advance cannot still change the cursor, either by proving it
is quiescent or by an authoritative fence against its old generation. It replays when the cursor did
not advance, continues only when it did, and stays paused or fails when the advance's effects,
cursor, or fencing cannot be established; other partitions may progress. A returned permanent
advance error ends the run as a typed settlement failure. No replay or later offset may be emitted
while the old advance could still take effect. A source must not emit a new generation while this
handling is underway. Automatic commit is not a profile option. Redis Streams reclaim is
individual delivery, not partitioned-log ownership; its unavailable delay, heartbeat, or terminal
operation must fail requirement validation.

`sisa-messaging-inbox` owns `InboxUnitOfWork`, the ability to begin, commit, and roll back the
transaction type used by an `InboxStore`. `PostgresInboxStore` implements both capabilities and
can therefore be passed once to the consumer. `InboxStore::max_attempts` exposes its configured
recorded-failure bound; the consumer compares it with a finite broker delivery bound after an
individual source opens and before its first receive.

```rust,ignore
pub trait InboxUnitOfWork: Send + Sync {
    type Transaction: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static + ErrorClassifier;

    fn begin(
        &self,
    ) -> impl Future<Output = Result<Self::Transaction, Self::Error>> + Send;

    fn commit(
        &self,
        tx: Self::Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn rollback(
        &self,
        tx: Self::Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

The consumer requires the same provider value to implement
`InboxStore<Inbox::Transaction>` and `InboxUnitOfWork`; no runtime provider registry is involved.

## 3. Recommended typed API

One `Consumer<M, ...>` handles one message type and version. This keeps routing static, avoids a
per-message service locator or boxed handler future, and maps naturally to a broker subject or
filter. Applications consuming several message types normally run several consumers under one
supervisor.

The public shape is intentionally small:

```rust,ignore
pub trait ConsumerHandler<M, Tx>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static + ErrorClassifier;

    fn handle(
        &self,
        tx: &mut Tx,
        envelope: &Envelope<M>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[non_exhaustive]
pub enum SettlementMode {
    Broker, // default
    BrokerImmediateRequeue,
    PendingRecovery,
}

#[non_exhaustive]
pub struct ConsumerSettings {
    pub max_in_flight: NonZeroUsize,
    pub source_timeout: Duration,
    pub database_timeout: Duration,
    pub settlement_timeout: Duration,
    pub heartbeat_interval: Option<Duration>,
    pub nak_delay: Duration,
    pub drain_timeout: Duration,
    pub mode: SettlementMode,
}

pub struct Consumer<M, Components> { /* private fields */ }

impl<M, S, Map, Codec, Inbox, H> Consumer<M, (S, Map, Codec, Inbox, H)> {
    pub fn new(
        source: S,
        mapper: Map,
        codec: Codec,
        inbox: Inbox,
        scope: InboxScope,
        handler: H,
        settings: ConsumerSettings,
    ) -> Result<Self, ConsumerConfigError>;

    pub async fn run(self, cancel: CancellationToken) -> Result<ConsumerExit, ConsumerError>;
}
```

The private component tuple keeps the user-facing annotation to `Consumer<Message, _>` rather
than exposing six generic placeholders. This is a contract sketch; public aliases or factory
return types may further shorten diagnostics, but they must not add dynamic dispatch to the normal
delivery path.

`ConsumerHandler` receives the complete typed envelope so it can propagate conversation,
causation, correlation, tenant, trace, and custom metadata to business writes or an outbound
message. The handler receives the transaction by mutable reference and must not commit, roll back,
or retain it.

Settings semantics are fixed:

- `max_in_flight` bounds received-but-unsettled deliveries and open transactions.
- `source_timeout` bounds source initialization; receive waits are cancel-safe rather than polled
  on a timeout loop.
- `database_timeout` bounds each framework-owned begin, inbox, commit, rollback, and failure-record
  operation separately. It does not time out application handler code.
- `settlement_timeout` bounds each ack, nak, terminate, and heartbeat operation.
- `nak_delay` is the broker redelivery delay for retryable/in-progress work in `Broker` mode and
  must be non-zero. `BrokerImmediateRequeue` requires exactly zero and a source that advertises
  immediate requeue. `PendingRecovery` does not use it and accepts zero. These modes are explicit;
  the runtime never substitutes one settlement operation for another.
- `drain_timeout` bounds the whole graceful drain after receiving stops.
- `mode` selects delayed broker settlement (the default), immediate broker requeue, or pending
  recovery (section 5). It is always an explicit application choice and is never inferred from a
  source descriptor.
- An optional `heartbeat_interval` is non-zero and requires an individual source whose reported
  acknowledgement wait is strictly greater than twice the interval. Partitioned logs use group
  ownership instead.

All constructor-known durations and concurrency values are validated once in `new`.
Descriptor-dependent acknowledgement-wait and delivery-bound checks run after individual-source
opening and before its first receive. The first release does not impose a handler timeout:
cancelling arbitrary application code can interrupt external effects without rolling them back.
Applications needing one implement it deliberately inside their handler and retain responsibility
for those effects.

There is no built-in heterogeneous handler registry in the first release. A host that truly needs
one durable consumer for multiple message types can implement an envelope-level router explicitly;
that opt-in path may use dynamic dispatch. The default typed path does not pay for it.

## 4. Application integration

The NATS/PostgreSQL call site, compiled in
[`examples/nats-postgres-consumer`](../examples/nats-postgres-consumer/src/main.rs), is:

```rust,ignore
let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default());
let source = NatsDeliverySource::new(pull_consumer);
let mapper = NatsMapper::new(subject_resolver);

let consumer = Consumer::<OrderCreated, _>::new(
    source,
    mapper,
    JsonSerializer,
    inbox,
    InboxScope::new("orders-projection")?,
    RecordOrder,
    ConsumerSettings::default(),
)?;

let task = tokio::spawn(consumer.run(cancel.child_token()));
```

`Consumer::run` implements this individual-delivery path, including optional heartbeat
acknowledgement. The separate partitioned-log profile uses `Consumer::new_partitioned` and
`Consumer::run_partitioned`. The application supplies `subject_resolver` because the mapper also
supports encoding. The consumer applies `source_timeout` to the whole source opening operation
and `settlement_timeout` to each settlement operation. Direct users of the NATS provider apply
their own time bounds.

Constructors do no I/O. The application creates or looks up the JetStream stream and durable
consumer before constructing `NatsDeliverySource`. It configures the same stable durable name and
`InboxScope` deliberately; neither is generated by the library.

No NATS-specific consumer façade exists: the generic `Consumer` composes the provider's source,
mapper, and settlement directly. The `sisa-messaging-nats` `jetstream` suite proves that
composition against a real JetStream server, including that a failed commit is never
acknowledged. `tests/system` proves with `PostgresInboxStore` that committed effects are durable
once acknowledged and that a failed attempt's effects roll back before the failure is recorded.

For long individual-delivery handlers, configure `heartbeat_interval` below half the broker's
acknowledgement wait. The source reports its descriptor during `run` opening, so requirements
are validated after I/O but before the consumer receives a delivery. `NatsDeliverySource`
currently starts its pull stream while opening, so a rejected start can consume one broker
delivery attempt; that delivery is never processed and is redelivered after the acknowledgement
wait. Inbox `max_attempts` must not exceed a finite
reported `max_deliver`; otherwise the broker can stop delivery before the inbox records its dead
transition.

## 5. Individual-delivery state machine

The application selects one of three individual-delivery settlement modes through
`ConsumerSettings::mode`; the runtime never infers a mode from a source descriptor. All modes
share one workflow and one centralized decision table, and keep the same guarantees:
delivery is at least once, the inbox deduplicates redelivery, and a delivery is never acknowledged
before its transaction commits. A partitioned log is a separate runtime profile, not an individual
settlement mode; it advances a partition cursor only after durable resolution.

Delayed broker settlement (`SettlementMode::Broker`, the default) follows the flow below, which
uses delayed retry and terminal discard. It requires both capabilities in
`IndividualSourceRequirements` when it opens the source; if either is absent, opening fails before
the first receive. When `heartbeat_interval` is configured, the source must also advertise
heartbeat support and an `ack_wait` strictly greater than twice the interval. A mode may omit an
optional requirement only if none of its reachable paths invokes that operation.
`BrokerImmediateRequeue` requires immediate-requeue and terminal-discard capabilities, uses
`nak(Duration::ZERO)` for retryable or in-progress work, and requires `nak_delay == Duration::ZERO`.
Pending recovery below requires neither retry nor terminal-discard capability. Unsupported
operations are never emulated with acknowledgement, another retry operation, or offset skip.

```text
receive delivery
      │
      ├─ map/decode wire failure ───────────────────────────────▶ terminate
      │
      ├─ wrong type/version or body decode failure
      │        └─ record permanent failure when identity is safe ─▶ terminate
      │
      └─ valid typed envelope
               │
               ├─ begin transaction
               ├─ inbox claim
               │      ├─ completed ─▶ rollback ─▶ ack
               │      ├─ in progress ▶ rollback ─▶ mode retry operation
               │      ├─ dead ───────▶ rollback ─▶ terminate
               │      └─ claimed
               │             ├─ handler
               │             │    ├─ success ▶ complete ▶ commit ▶ ack
               │             │    └─ error ─▶ rollback ▶ record failure
               │             │                              ├─ retry ▶ mode retry operation
               │             │                              ├─ dead ─▶ terminate
               │             │                              └─ completed elsewhere ▶ ack
               │             ├─ transient store/commit ambiguity ▶ mode retry operation
               │             └─ permanent provider failure ─────▶ leave unacked; stop runtime
               └─ settlement ambiguity ─────────────────────────▶ broker redelivery
```

Rules:

1. The delivery is never acknowledged before a successful commit.
2. A commit error or timeout is treated as ambiguous. Do not record a handler failure; leave or
   negatively acknowledge the delivery so redelivery resolves through the inbox.
3. Handler writes and inbox completion use the same transaction.
4. Failure recording uses the store's pool only after the handler transaction is rolled back or
   dropped.
5. A permanent classified handler/body error becomes dead immediately. A transient error becomes
   dead only when the recorded-attempt limit is reached.
6. An acknowledgement failure after commit does not undo anything. Redelivery observes
   `AlreadyCompleted` and acknowledges again.
7. A malformed individual delivery with no trustworthy message identity cannot safely create an
   inbox row; it is terminated only in a broker mode, which requires terminal discard. A malformed
   partitioned-log record without a trustworthy identity and durable terminal disposition leaves
   its partition unresolved and pauses it; the consumer must never silently advance the offset.
8. Broker settlement is idempotent from the framework's perspective. The settlement handle
   prevents two terminal settlement calls through safe Rust.
9. A transient provider error produces the selected mode's retry operation when settlement
   remains safe. A permanent unit-of-work/store error leaves the delivery unacknowledged and stops
   the runtime so an application supervisor and operator can address the system failure. This
   includes a permanent cleanup rollback after a duplicate, in-progress, or dead claim, and after
   a failed claim or completion. A transient failure of that rollback is logged and keeps the
   delivery's resolution.
10. A transient settlement error leaves that attempt unresolved and allows the runtime to
    continue. A permanent settlement error stops the runtime rather than producing an unbounded
    stream of messages that cannot be settled.

### Pending recovery

Pending-recovery settlement (`SettlementMode::PendingRecovery`) is an explicit opt-in for sources
that cannot perform delayed retry or terminal discard. It requests neither capability, so opening
still rejects any other configured requirement the source does not advertise. The consumer never
calls `nak` or `terminate` in this mode: it acknowledges only committed or already completed work,
and otherwise drops the settlement handle so the delivery stays pending for the source's bounded
recovery. It never acknowledges a durable dead result.

Select pending recovery only for a source that redelivers unsettled deliveries through bounded
recovery, such as Redis Streams idle reclaim. The runtime cannot detect a wrong choice: a source
that never redelivers an unsettled delivery would strand it. Enforcing this through a descriptor
capability is tracked by #80. RabbitMQ advertises immediate requeue and terminal discard, so it
supports `BrokerImmediateRequeue` with `nak_delay == Duration::ZERO`; it does not advertise delayed
retry or bounded pending recovery. Its `nak(0)` operation requests requeue but does not guarantee
eventual redelivery if broker queue policy intervenes.

Each redelivery runs the normal workflow, so the inbox `max_attempts` bound, not a broker delay,
limits retries: once the recorded-failure count reaches it, the failure record returns a durable
dead result. A finite source `max_deliver` below `max_attempts` is rejected before the first
receive, as in broker mode.

A durable dead result or a malformed delivery without a trustworthy identity cannot be resolved by
redelivery. The consumer leaves the entry pending, stops receiving, drains other work, and returns
`ConsumerErrorKind::OperatorActionRequired`. The operator removes the entry from the source or
moves it elsewhere, such as a dead-letter stream, before restarting the consumer. An opt-in policy
that acknowledges durable dead entries is tracked by #79.

The decision table maps each workflow resolution per mode:

| Resolution | Broker | Immediate requeue | Pending recovery |
| --- | --- | --- | --- |
| Committed success, completed duplicate, or completed elsewhere | ack | ack | ack |
| In progress, or retryable failure rolled back and recorded | delayed nak | `nak(0)` | leave pending |
| Transient begin, claim, completion, or commit failure (commit ambiguity) | delayed nak | `nak(0)` | leave pending |
| Transient rollback or failure-record error after a handler failure or body-decode failure | delayed nak | `nak(0)` | leave pending; stop |
| Durable dead claim or failure record | terminate | terminate | leave pending; stop for operator |
| Malformed wire value without a trustworthy identity | terminate | terminate | leave pending; stop for operator |
| Permanent or unknown provider failure, including a permanent cleanup rollback failure | leave pending; stop | leave pending; stop | leave pending; stop |

A transient settlement error or timeout is logged and leaves that attempt to redelivery; a
permanent or unsupported settlement error stops the consumer in every mode.

The partitioned profile consumes a `PartitionedLogDeliverySource` and does not call individual
`ack`, `nak`, or `terminate`. It keeps at most one unresolved record active per partition while
allowing other partitions to progress within `max_in_flight`. It calls `advance` only after the
record's transaction commits or a durable terminal disposition is recorded. A transient error,
timeout, or otherwise ambiguous outcome during `advance` leaves that partition unresolved and
paused while other partitions continue; a returned permanent provider error stops the run with
`Settlement`. The source must fence and reconcile its cursor and ownership generation before
replaying the record or continuing at a later offset. An observed ownership-loss event cancels that
partition's active workflow and leaves its record unresolved. Because ownership-loss events share
the source receive stream, the consumer cannot poll for one while all `max_in_flight` slots are
occupied; cancellation can therefore wait until a slot is available.

For NATS, successful processing uses a confirmed acknowledgement. `nak`, `terminate`, and
heartbeat operations remain bounded by `settlement_timeout`. A partitioned log instead advances
only after the transaction commits or a durable terminal disposition exists. A failed, timed-out,
or cancelled advance is indeterminate and requires the partition-scoped reconciliation described
above; failure to settle or advance is observable but does not change the database result.

For RabbitMQ, AMQP 0-9-1 does not confirm `basic.ack` or `basic.reject`, so each settlement is
followed by a no-op `basic.qos` on the same channel; its reply proves the broker processed the
settlement frame, at the cost of one extra round trip per settlement. `ack` maps to `basic.ack`,
`terminate` to `basic.reject` without requeue, and `nak(Duration::ZERO)` to `basic.reject` with
requeue. The descriptor reports immediate requeue and terminal discard, but no acknowledgement
wait, delivery bound, delayed retry, or heartbeat. The zero-delay operation is available only in
`BrokerImmediateRequeue`; a non-zero `nak` delay or heartbeat returns the bounded
unsupported-operation error without broker action. Requirements are validated before `basic.qos`
and `basic.consume`, so a rejected profile never starts a consumer. Delayed retry through TTL or
dead-letter queues is application-owned topology and is never emulated. Prefetch is the source's
`basic.qos` bound and should not exceed `max_in_flight`. A settlement whose channel has closed
fails locally, and the channel's connection must not enable lapin automatic recovery.

"No delivery bound" means the provider enforces none; AMQP 0-9-1 cannot report queue policy. A
broker delivery limit, such as the quorum-queue `delivery-limit` that defaults to 20 on RabbitMQ
4.x, is application-owned queue policy: past it the broker dead-letters the delivery, or drops it
when the queue has no dead-letter exchange, so a zero-delay `nak` does not guarantee redelivery.
Keep inbox `max_attempts` below that limit so the inbox records the dead transition first.

## 6. Concurrency, heartbeat and backpressure

`max_in_flight` bounds deliveries that have been received but not terminally settled. The source
is not polled for more work when all permits are occupied. This bounds:

- simultaneously open business transactions;
- handler tasks and decoded payload memory;
- outstanding broker acknowledgements;
- pressure on the database pool.

Each in-flight delivery has one coordinator. The database/handler workflow runs in an owned task.
For individual delivery, the coordinator selects over task readiness, cancellation, and the
configured heartbeat timer, and performs each heartbeat operation inside the selected arm, so
externally visible I/O is not embedded in a cancellable `select!` branch future. The partitioned
profile admits at most one unresolved record per partition while allowing separate partitions to
make bounded concurrent progress.

Heartbeat acknowledgement covers database work and handler work. It reduces needless redelivery
of slow messages but does not promise exclusivity; the inbox remains the correctness mechanism.
Heartbeat failures are warnings and do not cancel a handler whose transaction is still healthy.
Opening with a heartbeat interval rejects a source without heartbeat support or with
`ack_wait <= 2 * heartbeat_interval`.

A delivery takes its permit when it is received and holds it until both its coordinator and its
workflow task, the only owner of its transaction, have ended. Normally the coordinator ends only
after the workflow has finished and dropped its transaction, and then after a confirmed or failed
settlement operation or after the settlement handle is dropped to leave the delivery pending.
When the drain deadline aborts the coordinator, it can end before its aborted workflow is
dropped, so admission counts both live coordinators and live workflow tasks. The receive loop
reaps finished coordinators before admitting more work and waits on coordinator completion, never
on the source, while every permit is taken. Heartbeat acknowledgement follows the configured
individual-profile heartbeat interval.

For partitioned delivery, an uncertain `advance` pauses only the affected partition and retains
its unresolved record; it does not stop unrelated partitions. An ownership-loss event cancels the
active workflow for that partition when the receive loop observes the event. Since the event shares
the receive stream, a full `max_in_flight` bound can delay that observation.

The framework does not prefetch an unbounded batch. Provider buffering must also be bounded at or
close to `max_in_flight`.

## 7. Shutdown and failure supervision

Receiving stops on cancellation, a clean source close, a fatal source error, or a fatal delivery
outcome. For every cause the consumer:

1. stops requesting deliveries;
2. allows already received workflows to drain under one `drain_timeout` measured from when
   receiving stopped;
3. settles every workflow that finishes during the drain;
4. aborts remaining coordinators and workflows at the deadline, causing owned transactions to
   drop and roll back;
5. leaves unresolved deliveries unacknowledged for broker redelivery or pending recovery;
6. returns only after every aborted workflow, and so every transaction, has been dropped.

Cancellation returns `ConsumerExit::Cancelled` and a closed source returns
`ConsumerExit::SourceClosed`. A fatal source, permanent provider, or permanent settlement error,
and a pending-recovery operator-action stop, return `ConsumerError`; the application supervisor
decides whether and when to restart. Message-specific handler/decode failures and transient
database/settlement errors do not normally stop the whole consumer because they already have a
safe delivery disposition. When one in-flight workflow encounters a fatal error, the runtime stops
receiving, drains the other workflows under the normal bound, and then returns that error. The
first `ConsumerError`, including one observed during the drain, takes precedence over cancellation
or source close; otherwise the first of cancellation and close determines the exit. Any receive
error is fatal.

The framework never catches panics as business errors. A panic anywhere in a delivery's
processing drops its transaction, leaves the delivery unresolved, emits a bounded error event, and
stops the consumer so the application supervisor observes the programming fault. A panic while the
application handler runs returns `ConsumerErrorKind::HandlerPanicked`; any other processing-task
panic, including a mapper, codec, inbox provider, framework, or message-drop panic, returns
`ConsumerErrorKind::ProviderPanicked`. A panic in settlement or coordination returns
`ConsumerErrorKind::Runtime`. The panic payload is never rendered.

## 8. Error surface

Errors are separated by decision boundary:

- `ConsumerConfigError`: constructor-known invalid settings;
- `ConsumerError`: descriptor-dependent startup incompatibility after source opening, or a fatal
  source, provider, settlement, operator-action, panic, or runtime failure that ends `run`. It
  carries a `ConsumerErrorKind` (`SourceOpen`, `SourceOpenTimeout`, `Unsupported`,
  `AttemptBoundExceedsMaxDeliver`, `HeartbeatDeadlineTooShort`, `Source`, `Inbox`,
  `FailureNotRecorded`, `Settlement`, `PartitionOrder`, `PartitionUnresolved`,
  `OperatorActionRequired`, `HandlerPanicked`, `ProviderPanicked`, `Runtime`) and a `FailureKind`.
  Its `Display` and `Debug` output are fixed text, `Error::source` is `None`, and the typed
  provider error is available through `ConsumerError::provider_source`;
- handler error: application-owned and classified as transient or permanent;
- inbox/unit-of-work error: provider-owned and retained as a source;
- mapping/codec error: mapped to a stable poison reason without rendering payload bytes;
- delivery settlement error: logged once with operation and transport error type.

There is no enum variant containing every SQLx, NATS, codec, and handler error. Internal processing
reduces every path to a profile-neutral resolution, which a profile-specific private decision
table maps to a settlement plan: individual `Ack`, `Nak { delay }`, `Terminate`, or `Leave`
(drop the handle unsettled), with an optional stop cause, or partitioned `Advance` after durable
resolution / `LeaveUnresolved`. Typed sources remain
available for telemetry and debugging.

## 9. Observability

Consumer metrics use the direct global OTel Metrics API and follow [Observability](observability.md).
`consumer.processed.messages` increments after commit confirmation; duplicate and dead counters
use only the bounded claim state or dead reason; processing duration records the `process`
operation and a bounded `error.type` on failure. The in-flight gauge tracks admitted work while
its coordinator or workflow remains active. Database inbox state remains authoritative; consumer
counters describe activity observed by this process.

Each delivery also creates one `process {message type}` consumer span. The static message contract
supplies the suffix because source routing names are not stable. It captures the extracted remote
and valid ambient contexts before span creation, uses the remote context as parent with the ambient
context as a creation-time link when both are valid, and otherwise uses the valid ambient context
as parent or starts a new trace. It uses the direct OTel trace API; payload and header values are
never recorded.

## 10. Required framework tests

- Individual successful handler: claim, handler, complete, commit, confirmed ack—in that order.
- Handler error: rollback finishes before failure recording and broker settlement.
- In `Broker` mode, individual permanent failure terminates and transient failure uses delayed
  `nak`; in `BrokerImmediateRequeue`, permanent failure terminates and transient failure uses
  `nak(0)`; pending recovery leaves failures unresolved.
- Commit timeout/failure never calls `fail` and never acks.
- Ack failure after commit causes a harmless completed redelivery.
- Duplicate delivery does not invoke the handler.
- In `Broker` mode, a concurrent duplicate reports `InProgress` and receives a delayed nak; in
  `BrokerImmediateRequeue`, it receives `nak(0)`; in pending recovery, it remains pending.
- In `Broker` and `BrokerImmediateRequeue` modes, a dead receipt never invokes the handler and is
  terminated; in pending recovery, it remains pending and stops for operator action.
- Malformed individual wire input with no safe identity is terminated in `Broker` and
  `BrokerImmediateRequeue` only when terminal discard is supported; pending recovery leaves it
  pending. A partitioned record without durable terminal disposition pauses rather than skips its
  offset.
- Type/version/body mismatch cannot expose payload or header values in errors.
- `max_in_flight` bounds source polling and open transactions.
- In `Broker` mode, individual heartbeat acknowledgements occur during slow handlers and stop
  after terminal settlement; a source that lacks heartbeat cannot be configured to require it.
- In `Broker` mode, heartbeat failures do not cancel healthy workflows, and
  `ack_wait == 2 * heartbeat_interval` is rejected at startup.
- Cancellation stops pulls and drains resolved work; `Broker` and
  `BrokerImmediateRequeue` leave unresolved work for broker redelivery, while pending recovery
  leaves it pending for source recovery.
- A handler or provider panic cannot commit or acknowledge the delivery.
- Source closure and fatal source errors have distinct exit results.
- Permanent provider/settlement errors stop receiving and retain their typed source.
- The public API inventory checks `ErrorClassifier` implementations for
  `ConsumerConfigError` and `ConsumerError`, plus every `SettingsField::as_str` value.
- Opening rejects an unsupported individual requirement, a heartbeat/ack-wait mismatch in
  `Broker` mode, a non-zero delay in `BrokerImmediateRequeue`, and an inbox attempt bound above
  finite `max_deliver`; requirements are checked before receiving.
- Partitioned log tests prove commit-before-advance, no advancement past an unresolved earlier
  record, explicit ownership loss, reconciliation after indeterminate advancement, unresolved
  partition pause, independent progress in separate partitions, and delayed ownership-loss
  observation when all intake slots are occupied.
- Consumer benchmarks record bounded scheduling and state-machine paths, including successful and
  duplicate delivery processing.
