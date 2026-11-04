//! The receiving half: one durable pull consumer per endpoint.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use async_nats::jetstream::{self, AckKind, consumer};
use async_nats::{HeaderMap, HeaderValue};
use futures_util::StreamExt;
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, Envelope, TransportError, TransportSubscriber,
};

use crate::NatsTransport;
use crate::transport::{CE, MSG_ID, broker};

impl TransportSubscriber for NatsTransport {
    type Delivery = NatsDelivery;

    /// Creates the endpoint's durable pull consumer, named after the endpoint and filtered on
    /// the subjects of its message types, or updates it to them, and pulls its messages.
    ///
    /// A new consumer starts with the messages published after it is created. A message whose
    /// headers don't make an envelope goes to the error subject as it came, with the error in
    /// `ce-errorendpoint`, `ce-errortype` and `ce-errormessage`, and is then terminated.
    async fn subscribe(
        &self,
        endpoint: &EndpointSpec,
    ) -> Result<Deliveries<NatsDelivery>, TransportError> {
        if endpoint.message_types.is_empty() {
            // An empty filter would take every subject, the error subjects included.
            return Err(broker(
                "creating the JetStream consumer",
                std::io::Error::other("an endpoint needs at least one message type"),
            ));
        }
        let config = consumer::pull::Config {
            durable_name: Some(endpoint.name.clone()),
            filter_subjects: endpoint
                .message_types
                .iter()
                .map(|message_type| self.subject(message_type))
                .collect(),
            deliver_policy: consumer::DeliverPolicy::New,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: self.settings().ack_wait,
            ..Default::default()
        };
        let consumer = self
            .jetstream()
            .create_consumer_on_stream(config, self.settings().stream.as_str())
            .await
            .map_err(|error| broker("creating the JetStream consumer", error))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|error| broker("pulling from JetStream", error))?;

        let transport = self.clone();
        let endpoint = endpoint.name.clone();
        let deliveries = messages.filter_map(move |message| {
            let transport = transport.clone();
            let endpoint = endpoint.clone();
            async move {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => return Some(Err(broker("pulling from JetStream", error))),
                };
                match NatsDelivery::read(&message) {
                    Ok((envelope, attempt)) => Some(Ok(NatsDelivery {
                        envelope,
                        attempt,
                        message,
                    })),
                    Err(error) => {
                        transport.set_aside(&endpoint, message, &error).await;
                        None
                    }
                }
            }
        });
        Ok(Box::pin(deliveries))
    }

    /// Publishes the envelope to `{prefix}._error.{endpoint}`, without `Nats-Msg-Id`, so the
    /// stream keeps it next to the original of the same id. A line break in an attribute, which
    /// a NATS header can't hold, becomes a space.
    async fn publish_error(
        &self,
        endpoint: &str,
        envelope: &Envelope,
    ) -> Result<(), TransportError> {
        let mut headers = HeaderMap::new();
        for (name, value) in envelope.to_attributes() {
            headers.insert(format!("{CE}{name}"), one_line(&value));
        }
        self.publish_to(self.error_subject(endpoint), headers, envelope.data.clone())
            .await
    }
}

impl NatsTransport {
    fn error_subject(&self, endpoint: &str) -> String {
        format!("{}._error.{endpoint}", self.settings().prefix)
    }

    // Moves a message that isn't an envelope to the error subject, then terminates it. If the
    // copy fails, the message comes again after the ack wait, so it is never dropped without one.
    async fn set_aside(&self, endpoint: &str, message: jetstream::Message, error: &str) {
        let mut headers = HeaderMap::new();
        for (name, values) in message.headers.iter().flat_map(HeaderMap::iter) {
            if name.to_string() != MSG_ID {
                for value in values {
                    headers.append(name.clone(), value.clone());
                }
            }
        }
        headers.insert(format!("{CE}errorendpoint"), endpoint);
        headers.insert(format!("{CE}errortype"), "invalid_envelope");
        headers.insert(format!("{CE}errormessage"), one_line(error));
        let copied = self
            .publish_to(
                self.error_subject(endpoint),
                headers,
                message.payload.clone(),
            )
            .await;
        if copied.is_ok() {
            // A failed term only means the message comes again and is copied again.
            let _ = message.double_ack_with(AckKind::Term).await;
        }
    }
}

fn one_line(value: &str) -> HeaderValue {
    HeaderValue::from(value.replace(['\r', '\n'], " ").as_str())
}

/// A message received on an endpoint through its JetStream consumer.
pub struct NatsDelivery {
    envelope: Envelope,
    attempt: u32,
    message: jetstream::Message,
}

impl fmt::Debug for NatsDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsDelivery")
            .field("envelope", &self.envelope)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

impl NatsDelivery {
    // Reads the envelope from the `ce-` headers and the body, as the CloudEvents NATS binding
    // writes it in binary mode, and the attempt from the delivery count.
    fn read(message: &jetstream::Message) -> Result<(Envelope, u32), String> {
        let info = message.info().map_err(|error| error.to_string())?;
        let attempt = u32::try_from(info.delivered).unwrap_or(u32::MAX);
        let attributes: BTreeMap<String, String> = message
            .headers
            .iter()
            .flat_map(HeaderMap::iter)
            .filter_map(|(name, values)| {
                let name = name.to_string();
                let attribute = name.strip_prefix(CE)?.to_owned();
                Some((attribute, values.first()?.as_str().to_owned()))
            })
            .collect();
        let envelope = Envelope::from_attributes(attributes, message.payload.clone())
            .map_err(|error| error.to_string())?;
        Ok((envelope, attempt))
    }

    async fn settle(self, kind: AckKind) -> Result<(), TransportError> {
        self.message
            .double_ack_with(kind)
            .await
            .map_err(|error| broker("settling a JetStream message", error))
    }
}

impl Delivery for NatsDelivery {
    fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Acks the message, and waits until the server confirms it.
    async fn ack(self) -> Result<(), TransportError> {
        self.settle(AckKind::Ack).await
    }

    /// Naks the message with the delay, so JetStream delivers it again once the delay passes.
    async fn retry(self, delay: Duration) -> Result<(), TransportError> {
        self.settle(AckKind::Nak(Some(delay))).await
    }

    /// Terminates the message, so JetStream never delivers it to this endpoint again.
    async fn reject(self) -> Result<(), TransportError> {
        self.settle(AckKind::Term).await
    }
}
