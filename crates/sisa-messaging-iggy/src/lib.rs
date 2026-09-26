//! Apache Iggy publisher and consumer-group delivery source for Sisa messaging contracts.
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
//! and is never truncated. Because Iggy does not return the messages-key on reads, decoding
//! accepts a record with no wire key regardless of its ordering-key header, but rejects one whose
//! wire key disagrees with that header, or is present with no header at all, as
//! [`IggyMappingError::InvalidRecordKey`].
//!
//! ## Supervision
//!
//! Reconnect supervision is application-owned. A supervisor follows one cycle per session:
//!
//! 1. Start: [`IggyClient::start`] connects and logs in, then build an [`IggyPublisher`] around
//!    the client (clones of the client share its one session).
//! 2. Publish: each [`Publisher::publish`](sisa_messaging::Publisher::publish) call sends one
//!    message on that session.
//! 3. Observe: [`IggyClient::is_connected`] returning `false`, or a publish failing with
//!    [`IggyPublishErrorKind::ClientDisconnected`], means the session is closed and will not
//!    recover; the request was not sent. Retrying through the same client cannot succeed.
//! 4. Shut down: [`IggyClient::shutdown`] closes the session deterministically (it succeeds if the
//!    session is already closed), which also stops publishing through every clone.
//! 5. Rebuild: start a new [`IggyClient`] and a new [`IggyPublisher`], then resume publishing.
//!    Messages that failed with `ClientDisconnected` can succeed on the rebuilt client.
//!
//! A publish that fails with [`IggyPublishErrorKind::OutcomeUnknown`] may already have been
//! sent; that kind alone does not mean the session is closed, so check
//! [`IggyClient::is_connected`] before deciding to rebuild.
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
//! ## Delivery source
//!
//! [`IggyDeliverySource`] is a replay-only implementation of the shared partitioned-log delivery
//! profile for one pre-provisioned consumer group; the application composes it with the generic
//! consumer's `run_partitioned` and the [`IggyEnvelopeMapper`], as the `iggy-postgres-consumer`
//! example shows. It uses the SDK's low-level client, not `IggyConsumer`, which commits in the
//! background, buffers on its own, and hides revocation.
//!
//! Iggy's offset store carries no membership generation and checks ownership only when it admits
//! a store, so the source cannot fence a late store
//! (<https://github.com/sisaio/sisa-messaging-rs/issues/60>). It is correct because a late store
//! can only cause replay, never a skipped record, as long as the topic's offsets are never reused
//! (see the purge precondition below):
//!
//! 1. A settlement stores its own record's offset only after the consumer committed that record's
//!    inbox outcome or durable terminal disposition, and the source holds at most one unresolved
//!    record per partition and delivers each partition in offset order, so every earlier record
//!    is already resolved. A poll that would skip an offset fails the source with
//!    [`IggyDeliveryErrorKind::OffsetGap`].
//! 2. Nothing else writes the cursor: polls always disable auto-commit, and the source never
//!    commits on close and never leaves the group.
//! 3. A replayed record carries the same message id, so it keeps its envelope identity.
//! 4. The application must give every member of the group the same inbox.
//!
//! An indeterminate advance (an error, a timeout, or a dropped `advance` future) is not waited
//! out: the source withdraws the record with an ownership-loss event and, one poll interval
//! later, replays it from its own offset. A store that still applies afterwards, even a lower one
//! that moves the cursor back, causes only replay that the shared inbox absorbs.
//!
//! During a rebalance a member keeps the records it already polled: Iggy holds a partition's
//! revocation until the old owner has stored every offset it was served, then moves the partition
//! and fences the old owner's next poll, so the new owner resumes after the last stored record.
//! If the server's rebalancing timeout forces the move first, the revoked member may still
//! process the records it already polled for that partition, at most `batch_length`, until a
//! rejected offset store makes it withdraw them or its next poll is fenced, while the new owner
//! processes them too. The shared inbox keeps each effect once, but handler order across members
//! is not held during that window.
//!
//! Purging a topic is outside this guarantee. After a purge Iggy restarts the partition offsets
//! at 0, and the source cannot detect that an offset was reused: a new record whose offset is
//! below the source's next expected offset, or at or below the group's stored offset, is skipped
//! without being delivered. The application must stop every consumer of the group before
//! purging the topic and, if it reuses the group afterwards, reset or delete the group's stored
//! offsets first.
//!
//! ## Scope
//!
//! This crate ships the publisher and the consumer-group delivery source. Client and resource
//! provisioning, reconnect supervision, and automatic offset commits stay out of scope. The
//! crate's opt-in `tests/real_iggy_offset_fencing.rs` records the offset-store facts behind
//! <https://github.com/sisaio/sisa-messaging-rs/issues/60> against `apache/iggy:0.9.0`: a store
//! from a member that does not own the partition is refused with
//! `ConsumerGroupPartitionNotOwned` (5009), a store from an owner whose revocation is draining is
//! still admitted, stores are absolute so a lower one moves the cursor back, and the stored offset
//! is the last processed record. Because a draining owner is admitted and the SDK re-sends the same
//! request id when it does not observe a reply, a 5009 does not prove that an earlier transmission
//! of the store did nothing, so [`IggySettlement`]'s advance never reports an ownership loss.

#![forbid(unsafe_code)]

mod client;
mod error;
mod mapper;
mod publisher;
mod settings;
mod source;

pub use client::IggyClient;
pub use error::{
    IggyClientError, IggyClientErrorKind, IggyDeliveryError, IggyDeliveryErrorKind,
    IggyMappingError, IggyPublishError, IggyPublishErrorKind, InvalidSendTimeout,
    RoutingDestinationError,
};
pub use mapper::{IggyEnvelopeMapper, IggyHeader, IggyRecord};
pub use publisher::{
    Identifier, IggyDestinationResolver, IggyPublisher, RoutingDestinationResolver,
};
pub use settings::{IggyClientSettings, IggyCredentials, IggyPublisherSettings, IggyTlsSettings};
pub use source::{IggyDelivery, IggyDeliverySource, IggySettlement, IggySourceSettings};
