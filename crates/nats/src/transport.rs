//! The transport: a JetStream context and the settings it was made with.

use std::error::Error;
use std::sync::Arc;

use async_nats::{HeaderMap, HeaderValue, jetstream};
use bytes::Bytes;
use sisa_messaging_core::{Envelope, MessageType, TransportError, TransportPublisher};

use crate::NatsSettings;

/// The header that JetStream deduplicates publishes by.
pub(crate) const MSG_ID: &str = "Nats-Msg-Id";

/// The prefix of the CloudEvents attributes' headers in the NATS binding.
pub(crate) const CE: &str = "ce-";

/// The NATS JetStream transport. Clones share one connection.
///
/// It publishes each envelope to `{prefix}.{message type}` as the CloudEvents NATS binding says,
/// in binary mode: every attribute in a `ce-` header, such as `ce-id` and `ce-type`, and the data
/// as the body. It also sets `Nats-Msg-Id` to the message id, so the stream drops a second
/// publish of one message inside its duplicate window.
#[derive(Debug, Clone)]
pub struct NatsTransport {
    jetstream: jetstream::Context,
    settings: Arc<NatsSettings>,
}

impl NatsTransport {
    /// Connects to the server, and creates the stream that captures `{prefix}.>`, or updates it
    /// to the settings when it exists.
    ///
    /// # Errors
    ///
    /// [`TransportError::Broker`] when the server can't be reached, or refuses the stream, such
    /// as for a stream name or prefix it doesn't allow.
    pub async fn connect(settings: NatsSettings) -> Result<Self, TransportError> {
        let client = async_nats::connect(settings.url.as_str())
            .await
            .map_err(|error| broker("connecting to NATS", error))?;
        let jetstream = jetstream::new(client);

        let stream = jetstream::stream::Config {
            name: settings.stream.clone(),
            subjects: vec![format!("{}.>", settings.prefix)],
            duplicate_window: settings.duplicate_window,
            ..Default::default()
        };
        jetstream
            .create_or_update_stream(stream)
            .await
            .map_err(|error| broker("creating the JetStream stream", error))?;

        Ok(Self {
            jetstream,
            settings: Arc::new(settings),
        })
    }

    /// The settings the transport was made with.
    #[must_use]
    pub fn settings(&self) -> &NatsSettings {
        &self.settings
    }

    pub(crate) fn jetstream(&self) -> &jetstream::Context {
        &self.jetstream
    }

    pub(crate) fn subject(&self, message_type: &MessageType) -> String {
        format!("{}.{message_type}", self.settings.prefix)
    }

    // Publishes and waits for the JetStream acknowledgement. A duplicate is acked too: the stream
    // already has the message.
    pub(crate) async fn publish_to(
        &self,
        subject: String,
        headers: HeaderMap,
        data: Bytes,
    ) -> Result<(), TransportError> {
        let ack = self
            .jetstream
            .publish_with_headers(subject, headers, data)
            .await
            .map_err(|error| broker("publishing to JetStream", error))?;
        ack.await
            .map_err(|error| broker("waiting for the JetStream acknowledgement", error))?;
        Ok(())
    }
}

impl TransportPublisher for NatsTransport {
    async fn publish(&self, envelope: &Envelope) -> Result<(), TransportError> {
        self.publish_to(
            self.subject(&envelope.message_type),
            headers(envelope)?,
            envelope.data.clone(),
        )
        .await
    }
}

/// The NATS headers of an envelope: its CloudEvents attributes as `ce-` headers, and its id as
/// `Nats-Msg-Id`.
fn headers(envelope: &Envelope) -> Result<HeaderMap, TransportError> {
    let mut headers = HeaderMap::new();
    for (name, value) in envelope.to_attributes() {
        // A line break would end the header, so the server would read the rest as something else.
        let value: HeaderValue = value
            .parse()
            .map_err(|error| broker("writing the CloudEvents headers", error))?;
        headers.insert(format!("{CE}{name}"), value);
    }
    headers.insert(MSG_ID, envelope.id.to_string());
    Ok(headers)
}

pub(crate) fn broker(
    context: &'static str,
    error: impl Into<Box<dyn Error + Send + Sync>>,
) -> TransportError {
    TransportError::Broker {
        context,
        source: error.into(),
    }
}
