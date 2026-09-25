//! One-command stream append with an explicit finite deadline.

use crate::{RedisError, RedisMapper, error::map_stream_command};
use redis::aio::MultiplexedConnection;
use sisa_messaging::{EnvelopeMapper, Publisher, SerializedEnvelope};
use std::time::Duration;

/// Publisher over an application-supplied connection and pre-existing stream.
pub struct RedisPublisher {
    connection: MultiplexedConnection,

    stream: String,

    timeout: Duration,
}

impl RedisPublisher {
    /// Creates a publisher without I/O. Timeout must be non-zero.
    pub fn new(
        connection: MultiplexedConnection,
        stream: String,
        timeout: Duration,
    ) -> Result<Self, RedisError> {
        if stream.is_empty() || timeout.is_zero() {
            return Err(RedisError::Settings);
        }

        Ok(Self {
            connection,
            stream,
            timeout,
        })
    }

    /// Appends once and returns the server-issued stream ID.
    ///
    /// A deadline or connection error leaves the append outcome unknown. Repeating the call
    /// can produce a second stream entry with the same logical message ID.
    #[tracing::instrument(
        name = "publish",
        target = "messaging.redis",
        level = "debug",
        skip_all
    )]
    pub async fn append(&self, envelope: &SerializedEnvelope) -> Result<String, RedisError> {
        let wire = RedisMapper
            .encode(envelope)
            .map_err(|_| RedisError::Mapping)?;

        let mut connection = self.connection.clone();
        let mut command = redis::cmd("XADD");

        let query = command
            .arg(&self.stream)
            .arg("NOMKSTREAM")
            .arg("*")
            .arg("v")
            .arg(wire.version)
            .arg("h")
            .arg(wire.envelope)
            .arg("p")
            .arg(wire.payload)
            .query_async::<Option<String>>(&mut connection);

        let id = tokio::time::timeout(self.timeout, query)
            .await
            .map_err(|_| RedisError::Timeout)?
            .map_err(map_stream_command)?
            .ok_or(RedisError::SourceClosed)?;

        if id.is_empty() {
            return Err(RedisError::Protocol);
        }

        Ok(id)
    }
}

impl Publisher for RedisPublisher {
    type Error = RedisError;

    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        self.append(envelope).await.map(|_| ())
    }
}
