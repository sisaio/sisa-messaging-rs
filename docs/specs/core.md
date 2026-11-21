# Crate: sisa-messaging-core

Status: **Implemented** in 0.1.0 · Published: yes · Label: `area:core` · Commit scope: `core`

## Purpose

The contracts every other crate builds on: messages and their CloudEvents envelope, codecs, and
the traits that transports and database stores implement. Contracts only, and the JSON codec. No
runtime, no SQL and no broker client live here, so adapter crates build fast.

## Public contract

Main types: `Message`, `Envelope`, `MessageId`, `MessageType`, `Codec`, `JsonCodec`,
`TransportPublisher`, `TransportSubscriber`, `Delivery`, `OutboxStore`, `InboxStore`,
`PartitionStore` and `Publisher`.

Sketches, to show where each responsibility sits. The code settles the exact signatures.

### Message and envelope

```rust,ignore
pub trait Message: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The CloudEvents `type`, such as `orders.order_placed.v1`.
    const TYPE: &'static str;
}

pub struct Envelope {
    pub id: MessageId,                  // ce-id: UUID version 7, so ids sort by creation time
    pub message_type: MessageType,      // ce-type: the `Message::TYPE`
    pub source: Source,                 // ce-source: the publishing service, such as `/orders`
    pub time: Timestamp,                // ce-time
    pub subject: Option<String>,        // ce-subject: what the message is about, such as an order id
    pub content_type: ContentType,      // ce-datacontenttype: from the codec
    pub data_schema: Option<String>,    // ce-dataschema: for Avro later
    pub partition_key: Option<String>,  // ce-partitionkey: picks the partition where the transport has them; order per key is not guaranteed
    pub correlation: Correlation,       // ce-correlationid, ce-causationid
    pub trace: Option<TraceContext>,    // ce-traceparent, ce-tracestate (W3C)
    pub extensions: Extensions,         // any other ce- attribute, kept as it came
    pub data: Bytes,
}

pub struct Correlation {
    pub correlation_id: Option<MessageId>,  // ties a conversation together
    pub causation_id: Option<MessageId>,    // the message being handled when this one was published
}

pub struct TraceContext {
    pub traceparent: String,
    pub tracestate: Option<String>,  // passed on unchanged, as W3C Trace Context requires
}
```

The fields are grouped in Rust and flat on the wire, where each CloudEvents binding wants one
header per attribute. Correlation and trace are typed fields, not extensions, because the runtime
sets and carries them: a handler's publishes get the handled message as their causation and its
correlation id, and a trace context as the
[runtime spec](runtime.md#trace-context) says. `extensions` holds what the runtime doesn't
interpret.

### Codec

```rust,ignore
pub trait Codec: Send + Sync + 'static {
    fn content_type(&self) -> &'static str;
    fn encode<Msg: Message>(&self, message: &Msg) -> Result<Bytes, CodecError>;
    fn decode<Msg: Message>(&self, data: &[u8]) -> Result<Msg, CodecError>;
    /// Whether it reads data of this content type; by default, when the media types match.
    fn can_decode(&self, content_type: &ContentType) -> bool { .. }
}
```

A bus has one codec, `JsonCodec` unless `BusBuilder::codec` sets another, such as MessagePack or
Avro over serde. `Codec` has generic methods, so the codec is a type parameter with `JsonCodec` as
its default: `Bus<Transport, Outbox, Codec>`, and handlers take `HandlerContext<Transaction,
Codec>`. Every publish, the outbox's and the handlers' included, is encoded with it, and every
handler decodes with it; a delivery whose content type it can't decode goes to the error
destination at once. Picking the codec by the envelope's content type, so a service can move to
another codec while old messages drain, would need a codec chosen at run time.

The library encrypts nothing. An application that must encrypts the data with a codec that wraps
another, or encrypts fields in the message itself. Either way only the data is encrypted: the
CloudEvents attributes go in the headers and the outbox's columns as they are.

### Transport

Two halves, so the dispatcher and a publish-only service need only the first. `NatsTransport`,
`IggyTransport` and `RedisTransport` implement both.

```rust,ignore
pub trait TransportPublisher: Send + Sync + 'static {
    /// Returns once the broker has the envelope: a JetStream publish acknowledgement, a Kafka
    /// produce acknowledgement, a RabbitMQ publisher confirm.
    async fn publish(&self, envelope: &Envelope) -> Result<(), TransportError>;
}

pub trait TransportSubscriber: Send + Sync + 'static {
    type Delivery: Delivery;
    /// Creates the endpoint's durable subscription if needed, and streams its deliveries.
    async fn subscribe(&self, endpoint: &EndpointSpec) -> Result<Deliveries<Self::Delivery>, TransportError>;
    /// Publishes an envelope the endpoint gave up on to its error destination.
    async fn publish_error(&self, endpoint: &str, envelope: &Envelope) -> Result<(), TransportError>;
}

pub trait Delivery: Send + 'static {
    fn envelope(&self) -> &Envelope;
    fn attempt(&self) -> u32;                 // 1 on the first delivery
    async fn ack(self) -> Result<(), TransportError>;
    async fn retry(self, delay: Duration) -> Result<(), TransportError>;
    async fn reject(self) -> Result<(), TransportError>; // no more attempts
}
```

### Stores

A database crate implements three narrow traits. Everything around them (when to claim, how long
the lease is, the backoff, when to give up, when to create and expire partitions) is in the
runtime, once for every database.

```rust,ignore
pub trait OutboxStore: Send + Sync + 'static {
    type Connection: ?Sized;  // sqlx::PgConnection; sqlx::MySqlConnection; a tiberius client

    /// Writes the rows on the caller's connection, inside the caller's transaction.
    async fn insert(&self, connection: &mut Self::Connection, envelopes: &[Envelope]) -> Result<()>;

    // The dispatcher's side, on the store's own pool. Each call commits before it returns, and
    // due times come from the database's clock. An `OutboxRecord` is the envelope, the row's
    // `created_at`, its failed attempts and when the claim's lease runs out, which each mark
    // matches, so a dispatcher whose lease ran out can't overwrite a newer claim.
    async fn claim(&self, limit: usize, lease: Duration) -> Result<Vec<OutboxRecord>>;
    async fn mark_dispatched(&self, records: &[OutboxRecord]) -> Result<()>;
    async fn mark_retry(&self, record: &OutboxRecord, error: &str, delay: Duration) -> Result<()>;
    async fn mark_failed(&self, record: &OutboxRecord, error: &str) -> Result<()>;
}

pub trait InboxStore: Send + Sync + 'static {
    type Transaction: DerefMut + Send;  // sqlx::PgTransaction<'static>; dropping it rolls it back

    async fn begin(&self) -> Result<Self::Transaction>;
    /// Writes the receipt; false when the endpoint recorded this message before.
    async fn record(&self, tx: &mut Self::Transaction, endpoint: &str, envelope: &Envelope) -> Result<bool>;
    async fn commit(&self, tx: Self::Transaction) -> Result<()>;
}

pub trait PartitionStore: Send + Sync + 'static {
    async fn try_lock(&self) -> Result<Option<PartitionLock>>;  // one instance at a time
    /// Fills the days that no partition holds yet, so a changed interval starts where the old partitions end.
    async fn ensure_partitions(&self, interval: PartitionInterval, from: Date, count: u32) -> Result<()>;
    async fn default_rows(&self) -> Result<u64>;  // the worker warns while it isn't 0
    async fn expirable(&self, older_than: Date) -> Result<Vec<PartitionInfo>>;  // with pending and failed counts
    async fn expire(&self, partition: &PartitionInfo, mode: Expiry) -> Result<()>;
}
```

### Publisher

One trait publishes, whatever the mode. The three types that implement it are in the
[runtime spec](runtime.md#public-contract).

```rust,ignore
pub trait Publisher {
    type Error: Error + Send + Sync + 'static;  // PublishError for all three below
    async fn publish<Msg: Message>(&mut self, message: &Msg) -> Result<(), Self::Error>;
}
```

## Behaviour

The crate runs nothing: no worker and no connection. What the implementations of its contracts
do is in the [runtime](runtime.md), transport ([NATS](nats.md), [Iggy](iggy.md),
[Redis](redis.md)) and [PostgreSQL](postgres.md) specs.

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits). This
crate adds none of its own. The contracts are open traits, so an application can bring its own
transport, persistence or codec ([ADR-0005](../adr/0005-open-traits-and-where-the-code-lives.md)).

## Errors

The error enums are `#[non_exhaustive]`, so a new variant isn't a breaking change.

| Type | Says |
| --- | --- |
| `EnvelopeError` | A message type, an envelope or one of its CloudEvents attributes isn't valid |
| `CodecError` | Why a message couldn't be encoded or decoded. A decode error says where the data stopped making sense, never what the data says, since a message can hold private data that must not reach logs |
| `TransportError` | Why a transport couldn't do what was asked |
| `StoreError` | Why a store couldn't do what was asked |

## Formats

The envelope's attributes and their names on each transport:
[Envelope on the wire](../formats.md#envelope-on-the-wire).
