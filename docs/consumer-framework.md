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
       │
       └── sisa-messaging-consumer ─▶ generic receive/process/settle runtime
                    │
                    ▼
          sisa-messaging-inbox ─────▶ inbox and unit-of-work contracts
                    ▲
                    │
          sisa-messaging-postgres ──▶ PostgreSQL implementations
```

`sisa-messaging-consumer` does not depend on SQLx or async-nats. Neither provider depends on the
other. The application is still the only place that chooses the NATS/PostgreSQL combination.

Transport-neutral inbound contracts live in `sisa-messaging`:

- `DeliverySource`: asynchronously yields deliveries and stops cleanly when its source closes;
- `Delivery`: splits once into an owned transport wire value and settlement handle;
- `Settlement`: supports `ack`, delayed `nak`, `term`, and optional progress acknowledgement;
- `EnvelopeMapper<Wire>`: the same mapping contract used for publication decodes the wire value
  into `SerializedEnvelope`.

Their semantic shape is:

```rust,ignore
pub trait Settlement: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    fn progress(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn ack(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn nak(self, delay: Duration) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn term(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Delivery: Send + 'static {
    type Wire: Send + 'static;
    type Settlement: Settlement;

    fn into_parts(self) -> (Self::Wire, Self::Settlement);
}

pub trait DeliverySource: Send {
    type Delivery: Delivery;
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    fn open(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn receive(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Delivery>, Self::Error>> + Send;

    fn ack_wait(&self) -> Option<Duration>;
    fn max_deliver(&self) -> Option<NonZeroU64>;
}
```

Terminal settlement consumes the settlement handle, preventing a second terminal action through
safe Rust. Splitting lets the mapper consume the wire value without cloning or losing the broker
reply/acknowledgement capability. `None` from `receive` means a clean source close; an error is a
fatal source result. `open` performs one-time source initialization when `Consumer::run` starts;
constructors remain I/O-free. After opening, `receive` must be cancel-safe: dropping its readiness
wait must neither lose nor settle a delivery. `ack_wait` returns `None` when the transport has no
acknowledgement deadline or progress capability, in which case `progress_interval` must also be
`None`. `max_deliver` returns `None` for unlimited/unknown delivery count.

`sisa-messaging-inbox` owns `InboxUnitOfWork`, the ability to begin, commit, and roll back the
transaction type used by an `InboxStore`. `PostgresInboxStore` implements both capabilities and
can therefore be passed once to the consumer. `InboxStore::max_attempts` exposes its configured
recorded-failure bound so consumer construction can compare it with a finite broker delivery bound.

```rust,ignore
pub trait InboxUnitOfWork: Send + Sync {
    type Transaction: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static + Classify;

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
    type Error: std::error::Error + Send + Sync + 'static + Classify;

    fn handle(
        &self,
        tx: &mut Tx,
        envelope: &Envelope<M>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub struct ConsumerSettings {
    pub max_in_flight: NonZeroUsize,
    pub source_timeout: Duration,
    pub database_timeout: Duration,
    pub settlement_timeout: Duration,
    pub nak_delay: Duration,
    pub progress_interval: Option<Duration>,
    pub drain_timeout: Duration,
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
- `settlement_timeout` bounds each ack, nak, term, and progress operation.
- `nak_delay` is the broker redelivery delay for retryable/in-progress work.
- `progress_interval` is optional and must be non-zero and below half a source-reported ack wait.
- `drain_timeout` bounds the whole graceful drain after receiving stops.

All enabled durations and concurrency values are validated once in `new`. The first release does
not impose a handler timeout: cancelling arbitrary application code can interrupt external effects
without rolling them back. Applications needing one implement it deliberately inside their
handler and retain responsibility for those effects.

There is no built-in heterogeneous handler registry in the first release. A host that truly needs
one durable consumer for multiple message types can implement an envelope-level router explicitly;
that opt-in path may use dynamic dispatch. The default typed path does not pay for it.

## 4. Application integration

The intended NATS/PostgreSQL call site is:

```rust,ignore
let inbox = PostgresInboxStore::new(pool.clone(), InboxSettings::default())?;
let source = NatsDeliverySource::new(pull_consumer);

let consumer = Consumer::<OrderCreated, _>::new(
    source,
    NatsEnvelopeMapper::default(),
    JsonSerializer,
    inbox,
    InboxScope::new("orders-projection")?,
    RecordOrder,
    ConsumerSettings::default(),
)?;

let task = tokio::spawn(consumer.run(cancel.child_token()));
```

Constructors do no I/O. The application creates or looks up the JetStream stream and durable
consumer before constructing `NatsDeliverySource`. It configures the same stable durable name and
`InboxScope` deliberately; neither is generated by the library.

For long handlers, configure `progress_interval` below half the broker's acknowledgement wait.
`DeliverySource` reports the known acknowledgement deadline and finite delivery bound, allowing
`Consumer::new` to validate both relationships without performing I/O. Inbox `max_attempts` must
not exceed a finite broker `max_deliver`; otherwise the broker can stop delivery before the inbox
records its dead transition.

## 5. Per-delivery state machine

```text
receive delivery
      │
      ├─ map/decode wire failure ───────────────────────────────▶ term
      │
      ├─ wrong type/version or body decode failure
      │        └─ record permanent failure when identity is safe ─▶ term
      │
      └─ valid typed envelope
               │
               ├─ begin transaction
               ├─ inbox claim
               │      ├─ completed ─▶ rollback ─▶ ack
               │      ├─ in progress ▶ rollback ─▶ delayed nak
               │      ├─ dead ───────▶ rollback ─▶ term
               │      └─ claimed
               │             ├─ handler
               │             │    ├─ success ▶ complete ▶ commit ▶ ack
               │             │    └─ error ─▶ rollback ▶ record failure
               │             │                              ├─ retry ▶ delayed nak
               │             │                              ├─ dead ─▶ term
               │             │                              └─ completed elsewhere ▶ ack
               │             ├─ transient store/commit ambiguity ▶ delayed nak
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
7. A malformed delivery with no trustworthy message identity cannot safely create an inbox row;
   it is terminated and reported through telemetry.
8. Broker settlement is idempotent from the framework's perspective. The settlement handle
   prevents two terminal settlement calls through safe Rust.
9. A transient provider error produces a delayed nak when settlement remains safe. A permanent
   unit-of-work/store error leaves the delivery unacknowledged and stops the runtime so an
   application supervisor and operator can address the system failure.
10. A transient settlement error leaves that attempt unresolved and allows the runtime to
    continue. A permanent settlement error stops the runtime rather than producing an unbounded
    stream of messages that cannot be settled.

For NATS, successful processing uses a confirmed acknowledgement when the client supports it.
`nak`, `term`, and progress operations remain bounded by `settlement_timeout`. Failure to settle is
observable but does not change the database result.

## 6. Concurrency, progress and backpressure

`max_in_flight` bounds deliveries that have been received but not terminally settled. The source
is not polled for more work when all permits are occupied. This bounds:

- simultaneously open business transactions;
- handler tasks and decoded payload memory;
- outstanding broker acknowledgements;
- pressure on the database pool.

Each in-flight delivery has one coordinator. The database/handler workflow runs in an owned task;
the coordinator selects only over task readiness, cancellation, and a progress timer. A progress
acknowledgement is performed inside the selected arm, so externally visible I/O is not embedded in
a cancellable `select!` branch future.

Progress acknowledgement covers database work and handler work. It reduces needless redelivery
of slow messages but does not promise exclusivity; the inbox remains the correctness mechanism.
Progress failures are warnings and do not cancel a handler whose transaction is still healthy.

The framework does not prefetch an unbounded batch. Provider buffering must also be bounded at or
close to `max_in_flight`.

## 7. Shutdown and failure supervision

On cancellation the consumer:

1. stops requesting deliveries;
2. allows already received workflows to drain for `drain_timeout`;
3. settles every workflow that finishes during the drain;
4. aborts remaining tasks at the deadline, causing owned transactions to drop and roll back;
5. leaves unresolved deliveries unacknowledged for broker redelivery;
6. returns `ConsumerExit::Cancelled`.

A closed source returns `ConsumerExit::SourceClosed`. A fatal source, permanent provider, or
permanent settlement error returns `ConsumerError`; the application supervisor decides whether and
when to restart. Message-specific handler/decode failures and transient database/settlement errors
do not normally stop the whole consumer because they already have a safe delivery disposition.
When one in-flight workflow encounters a fatal error, the runtime stops receiving, drains the
other workflows under the normal bound, and then returns the fatal source chain.

The framework never catches panics as business errors. A panicked handler task drops its
transaction, leaves the delivery unacknowledged, emits a bounded error event, and lets the
framework continue or exit according to a fixed documented policy. The initial policy is to exit
the consumer so the application supervisor observes the programming fault.

## 8. Error surface

Errors are separated by decision boundary:

- `ConsumerConfigError`: invalid settings or incompatible acknowledgement timing;
- `ConsumerError`: fatal source, provider, settlement, panic, or runtime failure that ends `run`;
- handler error: application-owned and classified as transient or permanent;
- inbox/unit-of-work error: provider-owned and retained as a source;
- mapping/codec error: mapped to a stable poison reason without rendering payload bytes;
- delivery settlement error: logged once with operation and transport error type.

There is no enum variant containing every SQLx, NATS, codec, and handler error. Internal processing
produces a small private settlement plan—`Ack`, `Nak { delay }`, or `Term { reason }`—and retains
typed sources for telemetry and debugging.

## 9. Observability

The framework creates one OTel-conformant `process {destination template}` span per delivery. It
extracts remote trace context before opening the span and, for this single-message path, makes that
context the parent. Safe fields include message type, message ID, scope, attempt, outcome, and
bounded error category; payload and header values are never recorded.

Consumer metrics are defined in [Observability](observability.md). Measurements are emitted only
after the corresponding event: handler result, database commit, or broker settlement. Database
inbox state remains authoritative; consumer counters describe activity observed by this process.

## 10. Required framework tests

- Successful handler: claim, handler, complete, commit, confirmed ack—in that order.
- Handler error: rollback finishes before failure recording and broker settlement.
- Permanent failure terms immediately; transient failure naks until the recorded limit.
- Commit timeout/failure never calls `fail` and never acks.
- Ack failure after commit causes a harmless completed redelivery.
- Duplicate delivery does not invoke the handler.
- Concurrent duplicate reports `InProgress` and receives a delayed nak.
- Dead receipt never invokes the handler and is terminated.
- Malformed wire input with no safe identity is terminated without an inbox insert.
- Type/version/body mismatch cannot expose payload or header values in errors.
- `max_in_flight` bounds source polling and open transactions.
- Progress acknowledgements occur during slow handlers and stop after terminal settlement.
- Cancellation stops pulls, drains resolved work, and leaves unresolved work for redelivery.
- A handler panic cannot commit or acknowledge the delivery.
- Source closure and fatal source errors have distinct exit results.
- Permanent provider/settlement errors stop receiving and retain their typed source.
- Construction rejects a progress/ack-wait mismatch and an inbox attempt bound above finite broker
  `max_deliver`.
