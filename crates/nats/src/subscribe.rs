//! The receiving half: one durable pull consumer per endpoint.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::jetstream::context::ConsumerInfoErrorKind;
use async_nats::jetstream::{self, AckKind, consumer};
use futures_util::StreamExt;
use sisa_eventing_core::{
    Deliveries, Delivery, EndpointSpec, Envelope, TransportError, TransportSubscriber,
};

use crate::transport::{CE, MSG_ID, broker, insert};
use crate::{NatsTransport, binding};

impl TransportSubscriber for NatsTransport {
    type Delivery = NatsDelivery;

    /// Creates the endpoint's durable pull consumer, named after the endpoint and filtered on
    /// the subjects of its message types, and pulls its messages. When the consumer exists, its
    /// filter gains the subjects it lacks and keeps the ones it has.
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
        let stream = self
            .jetstream()
            .get_stream_no_info(self.settings().stream.as_str())
            .await
            .map_err(|error| broker("creating the JetStream consumer", error))?;
        let mut filter_subjects: Vec<String> = endpoint
            .message_types
            .iter()
            .map(|message_type| self.subject(message_type))
            .collect();
        // Instances of one endpoint may handle different types for a while, as in a rolling
        // deploy, so the filter only widens: a subject dropped from it would be skipped by every
        // instance.
        match stream.consumer_info(endpoint.name.as_str()).await {
            Ok(info) => {
                let existing =
                    info.config.filter_subjects.into_iter().chain(
                        Some(info.config.filter_subject).filter(|subject| !subject.is_empty()),
                    );
                for subject in existing {
                    if !filter_subjects.contains(&subject) {
                        filter_subjects.push(subject);
                    }
                }
            }
            Err(error) if matches!(error.kind(), ConsumerInfoErrorKind::NotFound) => {}
            Err(error) => return Err(broker("reading the JetStream consumer", error)),
        }
        let config = consumer::pull::Config {
            durable_name: Some(endpoint.name.clone()),
            filter_subjects,
            deliver_policy: consumer::DeliverPolicy::New,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: self.settings().ack_wait,
            ..Default::default()
        };
        let consumer = stream
            .create_consumer(config)
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
                    Err(error) => transport
                        .set_aside(&endpoint, message, &error)
                        .await
                        .err()
                        .map(Err),
                }
            }
        });
        Ok(Box::pin(deliveries))
    }

    /// Publishes the envelope to `{prefix}._error.{endpoint}`, without `Nats-Msg-Id`, so the
    /// stream keeps it next to the original of the same id.
    async fn publish_error(
        &self,
        endpoint: &str,
        envelope: &Envelope,
    ) -> Result<(), TransportError> {
        let mut headers = HeaderMap::new();
        for (name, value) in envelope.to_attributes() {
            insert(&mut headers, &name, &value);
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
    // copy fails, the message isn't terminated: it comes again after the ack wait, so it is never
    // dropped without a copy. A failed term means it comes again and is copied again.
    async fn set_aside(
        &self,
        endpoint: &str,
        message: jetstream::Message,
        error: &str,
    ) -> Result<(), TransportError> {
        let mut headers = HeaderMap::new();
        for (name, values) in message.headers.iter().flat_map(HeaderMap::iter) {
            if name.to_string() != MSG_ID {
                for value in values {
                    headers.append(name.clone(), value.clone());
                }
            }
        }
        insert(&mut headers, "errorendpoint", endpoint);
        insert(&mut headers, "errortype", "invalid_envelope");
        insert(&mut headers, "errormessage", error);
        self.publish_to(
            self.error_subject(endpoint),
            headers,
            message.payload.clone(),
        )
        .await?;
        message
            .double_ack_with(AckKind::Term)
            .await
            .map_err(|failure| broker("settling a JetStream message", failure))
    }
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
    // Reads the envelope from the `ce-` headers, percent-decoded once, and the body, as the
    // CloudEvents NATS binding writes it in binary mode, and the attempt from the delivery count.
    fn read(message: &jetstream::Message) -> Result<(Envelope, u32), String> {
        let info = message.info().map_err(|error| error.to_string())?;
        let attempt = u32::try_from(info.delivered).unwrap_or(u32::MAX);
        let attributes = message
            .headers
            .iter()
            .flat_map(HeaderMap::iter)
            .filter_map(|(name, values)| {
                // Header names are compared without case, so `Ce-Id` from another client counts.
                let name = name.to_string().to_ascii_lowercase();
                let attribute = name.strip_prefix(CE)?.to_owned();
                let value = binding::decode(values.first()?.as_str());
                Some(value.map(|value| (attribute, value)))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
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
