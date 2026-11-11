//! The transport: a connected Iggy client, the settings it was made with, and the topics it has
//! made sure of.

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use iggy::prelude::{
    Client, HeaderKey, HeaderValue, Identifier, IggyClient, IggyError, IggyMessage, MessageClient,
    Partitioning, StreamClient, StreamCreateOptions, TopicClient, TopicCreateOptions,
};
use sisa_messaging_core::{Envelope, TransportError, TransportPublisher};

use crate::IggySettings;

/// The prefix of the CloudEvents attributes' user headers. CloudEvents has no Iggy binding, so
/// this follows the Kafka binding, the closest one: Iggy is a partitioned log too.
pub(crate) const CE: &str = "ce_";

/// The longest user header value Iggy takes, in bytes.
const MAX_HEADER: usize = 255;

/// The Apache Iggy transport. Clones share one connection.
///
/// It publishes each envelope to the topic named after its message type, in the stream of
/// [`IggySettings::stream`], creating the topic with [`IggySettings::partitions`] partitions when
/// it is missing. The Iggy message id is the envelope's id, every CloudEvents attribute goes in a
/// `ce_` string user header, such as `ce_id` and `ce_type`, and the data is the payload. An
/// envelope with a `partitionkey` goes to the partition its key picks, so messages with one key
/// keep their order; the others are spread over the partitions.
///
/// Iggy takes at most 255 bytes in a header value and no empty data, so such an envelope isn't
/// published. Iggy doesn't drop a second publish of one id, so an outbox row published twice
/// reaches the endpoints twice, and an inbox drops the second.
#[derive(Clone)]
pub struct IggyTransport {
    inner: Arc<Inner>,
}

// Shows the settings, never the connection.
impl fmt::Debug for IggyTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IggyTransport")
            .field("settings", &self.inner.settings)
            .finish_non_exhaustive()
    }
}

struct Inner {
    client: IggyClient,
    stream: Identifier,
    settings: IggySettings,
    // The topics known to exist, so a publish checks each only once.
    topics: Mutex<HashSet<String>>,
}

impl IggyTransport {
    /// Connects to the server, logs in as the connection string's user, and creates the stream
    /// when it is missing.
    ///
    /// # Errors
    ///
    /// [`TransportError::Broker`] when the connection string is invalid, the server can't be
    /// reached within [`IggySettings::connect_timeout`], refuses the user, or refuses the stream.
    pub async fn connect(settings: IggySettings) -> Result<Self, TransportError> {
        let client = IggyClient::from_connection_string(settings.url.as_str())
            .map_err(|error| broker("reading the Iggy connection string", error))?;
        let stream = Identifier::named(settings.stream.as_str())
            .map_err(|error| broker("naming the Iggy stream", error))?;
        tokio::time::timeout(settings.connect_timeout, async {
            client
                .connect()
                .await
                .map_err(|error| broker("connecting to Iggy", error))?;
            let exists = client
                .get_stream(&stream)
                .await
                .map_err(|error| broker("reading the Iggy stream", error))?
                .is_some();
            if !exists {
                match client
                    .create_stream(settings.stream.as_str(), &StreamCreateOptions::default())
                    .await
                {
                    Ok(_) | Err(IggyError::StreamNameAlreadyExists(_)) => {}
                    Err(error) => return Err(broker("creating the Iggy stream", error)),
                }
            }
            Ok(())
        })
        .await
        .map_err(|elapsed| broker("connecting to Iggy", elapsed))??;

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                stream,
                settings,
                topics: Mutex::new(HashSet::new()),
            }),
        })
    }

    /// The settings the transport was made with.
    #[must_use]
    pub fn settings(&self) -> &IggySettings {
        &self.inner.settings
    }

    pub(crate) fn client(&self) -> &IggyClient {
        &self.inner.client
    }

    /// Creates the topic when it is missing.
    pub(crate) async fn ensure_topic(
        &self,
        topic: &str,
        partitions: u32,
    ) -> Result<(), TransportError> {
        if self.known(topic) {
            return Ok(());
        }
        let id = topic_id(topic)?;
        let exists = self
            .inner
            .client
            .get_topic(&self.inner.stream, &id)
            .await
            .map_err(|error| broker("reading the Iggy topic", error))?
            .is_some();
        if !exists {
            let options = TopicCreateOptions {
                partitions_count: Some(partitions),
                ..TopicCreateOptions::default()
            };
            match self
                .inner
                .client
                .create_topic(&self.inner.stream, topic, &options)
                .await
            {
                Ok(_) | Err(IggyError::TopicNameAlreadyExists(..)) => {}
                Err(error) => return Err(broker("creating the Iggy topic", error)),
            }
        }
        self.inner
            .topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(topic.to_owned());
        Ok(())
    }

    fn known(&self, topic: &str) -> bool {
        self.inner
            .topics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(topic)
    }

    /// Sends one message to the topic, creating it when missing, and waits until the server has
    /// stored it.
    pub(crate) async fn send(
        &self,
        topic: &str,
        partitions: u32,
        partitioning: &Partitioning,
        message: IggyMessage,
    ) -> Result<(), TransportError> {
        self.ensure_topic(topic, partitions).await?;
        self.inner
            .client
            .send_messages(
                &self.inner.stream,
                &topic_id(topic)?,
                partitioning,
                &mut [message],
            )
            .await
            .map_err(|error| broker("publishing to Iggy", error))?;
        Ok(())
    }

    /// The topic an endpoint's error copies go to.
    pub(crate) fn error_topic(endpoint: &str) -> String {
        format!("_error.{endpoint}")
    }

    /// Sends a copy to the endpoint's error topic, which has one partition.
    pub(crate) async fn send_error(
        &self,
        endpoint: &str,
        headers: BTreeMap<HeaderKey, HeaderValue>,
        data: Bytes,
    ) -> Result<(), TransportError> {
        let message = IggyMessage::builder()
            .payload(data)
            .user_headers(headers)
            .build()
            .map_err(|error| broker("writing the Iggy message", error))?;
        self.send(
            &Self::error_topic(endpoint),
            1,
            &Partitioning::balanced(),
            message,
        )
        .await
    }

    pub(crate) fn stream_name(&self) -> &str {
        &self.inner.settings.stream
    }
}

impl TransportPublisher for IggyTransport {
    async fn publish(&self, envelope: &Envelope) -> Result<(), TransportError> {
        let partitioning = match &envelope.partition_key {
            Some(key) => Partitioning::messages_key_str(key)
                .map_err(|error| broker("partitioning by the partition key", error))?,
            None => Partitioning::balanced(),
        };
        let message = IggyMessage::builder()
            .id(envelope.id.as_uuid().as_u128())
            .payload(envelope.data.clone())
            .user_headers(headers(envelope)?)
            .build()
            .map_err(|error| broker("writing the Iggy message", error))?;
        self.send(
            envelope.message_type.as_str(),
            self.inner.settings.partitions,
            &partitioning,
            message,
        )
        .await
    }
}

/// The user headers of an envelope: its CloudEvents attributes as `ce_` string headers.
fn headers(envelope: &Envelope) -> Result<BTreeMap<HeaderKey, HeaderValue>, TransportError> {
    envelope
        .to_attributes()
        .into_iter()
        .map(|(name, value)| {
            Ok((
                header_key(&format!("{CE}{name}"))?,
                HeaderValue::try_from(value.as_str())
                    .map_err(|error| broker("writing the CloudEvents headers", error))?,
            ))
        })
        .collect()
}

pub(crate) fn header_key(name: &str) -> Result<HeaderKey, TransportError> {
    HeaderKey::try_from(name).map_err(|error| broker("writing the CloudEvents headers", error))
}

/// A header value cut to what Iggy takes, at a character boundary; an empty value becomes a space.
/// For error copies, which must get through.
pub(crate) fn fitted(value: &str) -> HeaderValue {
    let mut end = value.len().min(MAX_HEADER);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    let value = if end == 0 { " " } else { &value[..end] };
    HeaderValue::try_from(value).expect("1 to 255 bytes")
}

pub(crate) fn topic_id(topic: &str) -> Result<Identifier, TransportError> {
    Identifier::named(topic).map_err(|error| broker("naming the Iggy topic", error))
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
