//! Messaging for Rust services: publish and consume messages over a transport, with an optional
//! transactional outbox and inbox in the application's database.
//!
//! No transport or persistence is on by default; turn on the ones you use:
//!
//! | Feature | Adds |
//! | --- | --- |
//! | `nats` | `NatsTransport`, the NATS JetStream transport |
//! | `postgres` | `PostgresOutbox`, `PostgresInbox` and `PostgresPartitions`, on sqlx |
//!
//! A bus with an outbox, whose publishes commit with the application's own transaction:
//!
//! ```no_run
//! # #[cfg(all(feature = "nats", feature = "postgres"))]
//! # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! use sisa_messaging::{Bus, Message, NatsSettings, NatsTransport, PostgresOutbox, Publisher};
//!
//! #[derive(serde::Serialize, serde::Deserialize)]
//! struct OrderPlaced {
//!     id: i64,
//! }
//!
//! impl Message for OrderPlaced {
//!     const TYPE: &'static str = "orders.order_placed.v1";
//! }
//!
//! let transport = NatsTransport::connect(NatsSettings::default()).await?;
//! let bus = Bus::builder("/orders", transport)
//!     .outbox(PostgresOutbox::new(pool.clone()))
//!     .start()
//!     .await?;
//!
//! let mut tx = pool.begin().await?;
//! sqlx::query("INSERT INTO orders (id) VALUES ($1)").bind(42).execute(&mut *tx).await?;
//! bus.outbox(&mut tx).publish(&OrderPlaced { id: 42 }).await?;
//! tx.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Each part is also its own crate (`sisa-messaging-core`, `-runtime`, `-nats`, `-postgres`), whose
//! docs cover the details, for an application that wants only some of them.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

pub use sisa_messaging_core::{
    Codec, CodecError, ContentType, Correlation, Date, Deliveries, Delivery, EndpointSpec,
    Envelope, EnvelopeError, Expiry, Extensions, InboxStore, JsonCodec, Message, MessageId,
    MessageType, OutboxRecord, OutboxStore, PartitionInfo, PartitionInterval, PartitionLock,
    PartitionStore, Publisher, Source, StoreError, Timestamp, TraceContext, TransportError,
    TransportPublisher, TransportSubscriber,
};
pub use sisa_messaging_runtime::{
    Bus, BusBuilder, DispatchSettings, EndpointBuilder, Handler, HandlerContext, HandlerError,
    InMemoryDelivery, InMemoryTransport, NoInbox, NoOutbox, OutboxPublisher, PartitionMode,
    PartitionSettings, PublishError, RetryPolicy, StartError,
};

#[cfg(feature = "nats")]
pub use sisa_messaging_nats::{NatsDelivery, NatsSettings, NatsTransport};

#[cfg(feature = "postgres")]
pub use sisa_messaging_postgres::{PostgresInbox, PostgresOutbox, PostgresPartitions};
