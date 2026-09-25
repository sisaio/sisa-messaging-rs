//! Acknowledged JetStream publication over a caller-owned context.

use crate::{
    error::{MappingError, NatsError},
    mapper::{NatsMapper, NatsWire, SubjectResolver},
    settings::NatsPublisherSettings,
    telemetry,
};
use async_nats::jetstream;
use sisa_messaging::{EnvelopeMapper, Publisher, SerializedEnvelope};
use std::time::Instant;

/// Publisher over a caller-owned JetStream context.
pub struct NatsPublisher<R> {
    context: jetstream::Context,
    mapper: NatsMapper<R>,
    settings: NatsPublisherSettings,
}

impl<R> NatsPublisher<R> {
    /// Creates a publisher without network I/O.
    ///
    /// A zero publish timeout is rejected as [`NatsError::Settings`].
    /// Publishing rejects invalid mapping and oversized frames before sending;
    /// SDK failures and timeouts can leave the broker outcome unknown.
    pub fn new(
        context: jetstream::Context,
        resolver: R,
        settings: NatsPublisherSettings,
    ) -> Result<Self, NatsError> {
        Ok(Self {
            context,
            mapper: NatsMapper::new(resolver),
            settings: settings.validate()?,
        })
    }
}

fn frame_len(wire: &NatsWire) -> Option<usize> {
    let mut size = wire.payload.len().checked_add(10)?;
    for (name, values) in wire.headers.iter() {
        for value in values {
            size = size
                .checked_add(name.to_string().len())?
                .checked_add(value.as_str().len())?
                .checked_add(4)?;
        }
    }
    size.checked_add(2)
}

impl<R: SubjectResolver> Publisher for NatsPublisher<R> {
    type Error = NatsError;

    #[tracing::instrument(name = "publish", target = "messaging.nats", level = "debug", skip_all)]
    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        let started = Instant::now();
        let result = async {
            let limit = self.context.client().max_payload();

            if envelope.payload.len() > limit {
                return Err(NatsError::PayloadTooLarge);
            }

            let wire = self
                .mapper
                .encode(envelope)
                .map_err(|_: MappingError| NatsError::Mapping)?;

            if frame_len(&wire).is_none_or(|length| length > limit) {
                return Err(NatsError::PayloadTooLarge);
            }

            let operation = async {
                telemetry::sent_attempted();

                let ack = self
                    .context
                    .publish_with_headers(wire.subject, wire.headers, wire.payload.into())
                    .await
                    .map_err(|_| NatsError::Publish)?;

                ack.await.map_err(|_| NatsError::Publish)?;

                Ok(())
            };

            tokio::time::timeout(self.settings.publish_timeout, operation)
                .await
                .map_err(|_| NatsError::Timeout)?
        };

        let result = result.await;
        telemetry::finished("publish", started.elapsed(), result);

        result
    }
}
