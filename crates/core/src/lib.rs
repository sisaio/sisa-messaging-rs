//! The contracts of Sisa Messaging: messages and their CloudEvents envelope, codecs, and the traits
//! that transports and database stores implement. No runtime lives here; that is
//! `sisa-messaging-runtime`.
//!
//! Applications depend on `sisa-messaging`, which re-exports this crate's types, or on this crate
//! and the transport and persistence crates they use.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

mod clock;
mod codec;
mod envelope;
mod error;
mod message;
mod publisher;
mod store;
mod time;
mod transport;

pub use codec::{Codec, CodecError, JsonCodec};
pub use envelope::{ContentType, Correlation, Envelope, Extensions, Source, TraceContext};
pub use error::EnvelopeError;
pub use message::{Message, MessageId, MessageType};
pub use publisher::Publisher;
pub use store::{OutboxStore, StoreError};
pub use time::Timestamp;
pub use transport::{
    Deliveries, Delivery, EndpointSpec, TransportError, TransportPublisher, TransportSubscriber,
};
