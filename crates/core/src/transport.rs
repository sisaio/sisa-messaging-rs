//! The transport contracts: publishing envelopes to a broker and receiving them on endpoints.

use std::error::Error;
use std::pin::Pin;
use std::time::Duration;

use futures_util::Stream;

use crate::{Envelope, MessageType};

/// Why a transport couldn't do what was asked.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// The transport was closed, or its connection is gone for good.
    #[error("the transport is closed")]
    Closed,
    /// The broker refused the request, didn't answer in time, or couldn't be reached. Trying
    /// again later may work.
    #[error("{context}: {source}")]
    Broker {
        /// What the transport was doing, such as `publishing to JetStream`.
        context: &'static str,
        /// The client's error.
        source: Box<dyn Error + Send + Sync>,
    },
}

/// What a transport needs to know about an endpoint to subscribe it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EndpointSpec {
    /// The endpoint's name, such as `billing`: the durable subscription every instance of the
    /// service shares. A transport rejects a name it can't use.
    pub name: String,
    /// The message types its handlers take. The endpoint gets a copy of every message of these
    /// types, and of no others.
    pub message_types: Vec<MessageType>,
}

impl EndpointSpec {
    /// An endpoint spec.
    #[must_use]
    pub fn new(name: impl Into<String>, message_types: Vec<MessageType>) -> Self {
        Self {
            name: name.into(),
            message_types,
        }
    }
}

/// The deliveries of one endpoint, as they arrive.
pub type Deliveries<Item> = Pin<Box<dyn Stream<Item = Result<Item, TransportError>> + Send>>;

/// The publishing half of a transport. The outbox dispatcher and publish-only services need only
/// this half.
///
/// Every transport keeps these promises:
///
/// - [`publish`](TransportPublisher::publish) returns `Ok` only once the broker has stored the
///   envelope: a JetStream publish acknowledgement, a Kafka produce acknowledgement, a RabbitMQ
///   publisher confirm. An error means the envelope may or may not be stored, so the caller
///   publishes it again with the same id.
/// - It writes the envelope as its CloudEvents binding says, in binary mode, so another
///   language's SDK reads it.
pub trait TransportPublisher: Send + Sync + 'static {
    /// Publishes an envelope to the destination its message type names.
    ///
    /// # Errors
    ///
    /// [`TransportError`] when the broker didn't confirm it has the envelope.
    fn publish(
        &self,
        envelope: &Envelope,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;
}

/// The receiving half of a transport.
///
/// Every transport keeps these promises:
///
/// - Each endpoint gets its own copy of every message of the types it subscribes to (fan-out),
///   and every subscription to one endpoint, from any instance, shares that endpoint's copies
///   (competing consumers): each copy goes to one subscriber at a time.
/// - A message stays with the endpoint until its delivery is acked or rejected. A delivery
///   dropped without either, or held past the transport's acknowledgement timeout, comes again.
/// - [`Delivery::attempt`] counts the deliveries of one message to one endpoint, from 1.
pub trait TransportSubscriber: Send + Sync + 'static {
    /// One received message.
    type Delivery: Delivery;

    /// Creates the endpoint's durable subscription if it doesn't exist, and streams its
    /// deliveries.
    ///
    /// # Errors
    ///
    /// [`TransportError`] when the subscription can't be created or joined.
    fn subscribe(
        &self,
        endpoint: &EndpointSpec,
    ) -> impl Future<Output = Result<Deliveries<Self::Delivery>, TransportError>> + Send;
}

/// One message received on an endpoint, until it is settled with
/// [`ack`](Delivery::ack), [`retry`](Delivery::retry) or [`reject`](Delivery::reject).
pub trait Delivery: Send + 'static {
    /// The received envelope.
    fn envelope(&self) -> &Envelope;

    /// Which delivery of this message to this endpoint this is: 1 the first time.
    fn attempt(&self) -> u32;

    /// Settles the delivery as done: the endpoint never gets this message again.
    ///
    /// # Errors
    ///
    /// [`TransportError`] when the broker didn't take the acknowledgement, so the message may
    /// come again.
    fn ack(self) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Settles the delivery as failed for now: the message comes again, no sooner than `delay`,
    /// with the next attempt number.
    ///
    /// # Errors
    ///
    /// [`TransportError`] when the broker didn't take the request; the message still comes again
    /// once its acknowledgement timeout passes.
    fn retry(self, delay: Duration) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Settles the delivery as failed for good: the endpoint never gets this message again. The
    /// runtime calls it once the envelope is in the endpoint's error destination.
    ///
    /// # Errors
    ///
    /// [`TransportError`] when the broker didn't take the request, so the message may come
    /// again.
    fn reject(self) -> impl Future<Output = Result<(), TransportError>> + Send;
}
