//! Apache Iggy publisher provider for Sisa messaging contracts.
//!
//! Applications configure the server address, credentials, optional TLS, and timeouts through
//! [`IggyClientSettings`]. [`IggyClient::start`] connects over TCP and logs in within the
//! configured connect timeout, returning a cloneable application-owned handle. The underlying SDK
//! client is built with reconnection disabled, so reconnect supervision after the initial connect
//! stays application-owned: construct a new [`IggyClient`] to reconnect.
//!
//! The provider maps shared envelopes into Iggy messages and publishes each one individually
//! through the SDK's `send_messages` call, never the SDK's `IggyProducer`, which batches in the
//! background and reports success before a message is durable. Success means the server's reply
//! to the append request arrived: a VSR quorum commit for a clustered deployment, or a single-node
//! commit whose durability depends on the topic's durability policy and the server's storage
//! configuration.
//!
//! A timeout is reported as [`IggyPublishErrorKind::OutcomeUnknown`], a transient failure: the
//! SDK's transport task runs detached, so a timeout even while the request is still queued behind
//! the connection's stream lock does not stop the SDK from sending it. Within one `send_messages`
//! call the SDK replays the same VSR request id, not the shared message id, when it does not
//! observe a reply, and a request its own replay confirms already committed is reported here as
//! success with no fresh reply to inspect. An application-level retry after `OutcomeUnknown`
//! issues a new request with a new VSR request id, so the server deduplicates it against the
//! earlier attempt only if the server's message deduplication is enabled; otherwise a retry can
//! durably duplicate the message.
//!
//! Partitioning uses the shared ordering key as an Iggy messages-key when present, and balanced
//! (round-robin) partitioning otherwise. An ordering key longer than 255 bytes is a mapping error
//! and is never truncated.
//!
//! ## Wire limits
//!
//! Each Iggy header name and value is limited to 255 bytes, well under the shared envelope
//! contract's 8,192-byte header-value bound. A custom header value over 255 bytes is a permanent
//! mapping error ([`IggyMappingError::InvalidHeader`]). `tracestate` is the one exception: a value
//! over 255 bytes is omitted from the wire record instead of failing the envelope, following the
//! W3C Trace Context allowance for a participant to drop `tracestate` under vendor size
//! constraints; an oversized `traceparent` has no such allowance and is rejected as
//! [`IggyMappingError::InvalidFrameworkValue`].
//!
//! ## Logging
//!
//! This crate emits no tracing of its own. The underlying SDK logs under the `iggy` target,
//! including the configured username during sign-in and raw I/O error text on connection
//! failures; applications that filter or redact log output should suppress or scrub that target.
//!
//! This crate ships the publisher only. The inbound partitioned-log delivery source described by
//! the shared consumer contracts is out of scope: Iggy's consumer-group offset store carries no
//! membership generation, so a fencing-correct implementation of that profile is not possible with
//! the current server. See the crate's `tests/real_iggy_offset_fencing.rs` for the real-broker
//! feasibility proof and <https://github.com/sisaio/sisa-messaging-rs/issues/60> for tracking.

#![forbid(unsafe_code)]

mod client;
mod error;
mod mapper;
mod publisher;
mod settings;

pub use client::IggyClient;
pub use error::{
    IggyClientError, IggyClientErrorKind, IggyMappingError, IggyPublishError, IggyPublishErrorKind,
    InvalidSendTimeout, RoutingDestinationError,
};
pub use mapper::{IggyEnvelopeMapper, IggyHeader, IggyRecord};
pub use publisher::{
    Identifier, IggyDestinationResolver, IggyPublisher, RoutingDestinationResolver,
};
pub use settings::{IggyClientSettings, IggyCredentials, IggyPublisherSettings, IggyTlsSettings};
