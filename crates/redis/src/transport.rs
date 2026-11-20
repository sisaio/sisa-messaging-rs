//! The transport: a connection that reconnects by itself, the client to open more with, and the
//! settings they were made with.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, Cmd};
use sisa_messaging_core::{Envelope, MessageType, TransportError, TransportPublisher};

use crate::RedisSettings;

/// The prefix of the CloudEvents attributes' fields. CloudEvents has no Redis binding, so this
/// follows the Kafka binding, the closest one: a stream entry is a record in a log too.
pub(crate) const CE: &str = "ce_";

/// The field that holds the encoded data.
pub(crate) const DATA: &str = "data";

/// The Redis Streams transport, for Redis, Valkey and Dragonfly. Clones share one connection.
///
/// It appends each envelope with `XADD` to the stream `{prefix}:{message type}`, such as
/// `sisa:orders.order_placed.v1`, creating the stream when it is missing. Every CloudEvents
/// attribute goes in a `ce_` field, such as `ce_id` and `ce_type`, and the data in the field
/// `data`. Streams have no partitions, so a `partitionkey` is carried but picks nothing.
///
/// A publish returns once the server has the entry in memory: how soon it reaches the disk and
/// the replicas is the server's `appendfsync` and replication, as for anything else stored in it.
/// Streams don't drop a second publish of one id, so an outbox row published twice reaches the
/// endpoints twice, and an inbox drops the second. The transport creates no limit on a stream;
/// trimming one with `MAXLEN` or `MINID` removes unhandled messages too.
#[derive(Clone)]
pub struct RedisTransport {
    inner: Arc<Inner>,
}

// Shows the settings, never the connection.
impl fmt::Debug for RedisTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisTransport")
            .field("settings", &self.inner.settings)
            .finish_non_exhaustive()
    }
}

struct Inner {
    client: Client,
    connection: ConnectionManager,
    settings: RedisSettings,
}

impl RedisTransport {
    /// Connects to the server and checks that it answers.
    ///
    /// # Errors
    ///
    /// [`TransportError::Broker`] when the URL is invalid, or the server can't be reached within
    /// [`RedisSettings::connect_timeout`] or refuses the user.
    pub async fn connect(settings: RedisSettings) -> Result<Self, TransportError> {
        let client = Client::open(settings.url.as_str())
            .map_err(|error| broker("reading the Redis URL", error))?;
        let mut connection = tokio::time::timeout(
            settings.connect_timeout,
            client.get_connection_manager_with_config(config(&settings, Duration::ZERO)),
        )
        .await
        .map_err(|elapsed| broker("connecting to Redis", elapsed))?
        .map_err(|error| broker("connecting to Redis", error))?;
        redis::cmd("PING")
            .query_async::<()>(&mut connection)
            .await
            .map_err(|error| broker("connecting to Redis", error))?;

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                connection,
                settings,
            }),
        })
    }

    /// The settings the transport was made with.
    #[must_use]
    pub fn settings(&self) -> &RedisSettings {
        &self.inner.settings
    }

    /// A connection of its own, for commands that block for up to `block`.
    pub(crate) async fn blocking_connection(
        &self,
        block: Duration,
    ) -> Result<ConnectionManager, TransportError> {
        // The timeout covers each attempt; this covers the retries between them too.
        tokio::time::timeout(
            self.inner.settings.connect_timeout,
            self.inner
                .client
                .get_connection_manager_with_config(config(&self.inner.settings, block)),
        )
        .await
        .map_err(|elapsed| broker("connecting to Redis", elapsed))?
        .map_err(|error| broker("connecting to Redis", error))
    }

    /// Runs a command on the shared connection, within the connect timeout, which also covers
    /// the reconnecting a lost connection does first.
    pub(crate) async fn query<Reply: redis::FromRedisValue>(
        &self,
        context: &'static str,
        command: &Cmd,
    ) -> Result<Reply, TransportError> {
        tokio::time::timeout(
            self.inner.settings.connect_timeout,
            command.query_async(&mut self.inner.connection.clone()),
        )
        .await
        .map_err(|elapsed| broker(context, elapsed))?
        .map_err(|error| broker(context, error))
    }

    pub(crate) fn stream(&self, message_type: &MessageType) -> String {
        format!("{}:{message_type}", self.inner.settings.prefix)
    }

    /// The stream an endpoint's error copies go to.
    pub(crate) fn error_stream(&self, endpoint: &str) -> String {
        format!("{}:_error:{endpoint}", self.inner.settings.prefix)
    }
}

/// How a connection reconnects and how long it waits, with `block` added to the command timeout
/// for the commands that block.
fn config(settings: &RedisSettings, block: Duration) -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        .set_connection_timeout(Some(settings.connect_timeout))
        .set_response_timeout(Some(settings.connect_timeout + block))
}

/// An `XADD` of the fields and the data, which creates the stream when it is missing.
pub(crate) fn append<'fields>(
    stream: &str,
    fields: impl IntoIterator<Item = (&'fields [u8], &'fields [u8])>,
    data: &[u8],
) -> Cmd {
    let mut command = redis::cmd("XADD");
    command.arg(stream).arg("*");
    for (name, value) in fields {
        command.arg(name).arg(value);
    }
    command.arg(DATA).arg(data);
    command
}

/// The `XADD` of an envelope to a stream: its attributes as `ce_` fields, and its data.
pub(crate) fn append_envelope(stream: &str, envelope: &Envelope) -> Cmd {
    let fields: Vec<(String, String)> = envelope
        .to_attributes()
        .into_iter()
        .map(|(name, value)| (format!("{CE}{name}"), value))
        .collect();
    append(
        stream,
        fields
            .iter()
            .map(|(name, value)| (name.as_bytes(), value.as_bytes())),
        &envelope.data,
    )
}

// Appends the envelope to its message type's stream.
impl TransportPublisher for RedisTransport {
    async fn publish(&self, envelope: &Envelope) -> Result<(), TransportError> {
        let command = append_envelope(&self.stream(&envelope.message_type), envelope);
        self.query::<String>("publishing to Redis", &command)
            .await
            .map(drop)
    }
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
