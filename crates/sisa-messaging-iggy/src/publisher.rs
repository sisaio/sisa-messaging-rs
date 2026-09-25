//! Iggy publication through an application-configured, connected client.

mod resolver;

use std::collections::BTreeMap;

use iggy::prelude::{
    HeaderKey, HeaderValue as SdkHeaderValue, IggyError, IggyMessage as SdkIggyMessage,
    MessageClient, Partitioning,
};
use sisa_messaging::{EnvelopeMapper, ErrorClassifier, FailureKind, Publisher, SerializedEnvelope};

use crate::{
    IggyClient, IggyMappingError, IggyPublishError, IggyPublishErrorKind, IggyPublisherSettings,
    IggyRecord,
};

pub use iggy::prelude::Identifier;
pub use resolver::{IggyDestinationResolver, RoutingDestinationResolver};

/// Publishes shared envelopes through an application-created, connected Iggy client.
///
/// Each publish sends exactly one message and awaits the server's reply directly; it never uses
/// the SDK's background `IggyProducer`. See the crate documentation for what success means and how
/// a timeout is classified.
pub struct IggyPublisher<R> {
    client: IggyClient,

    resolver: R,

    settings: IggyPublisherSettings,
}

impl<R> IggyPublisher<R> {
    /// Constructs a publisher without performing network I/O.
    ///
    /// The client was constructed from an application-owned, already-connected handle.
    pub fn new(client: IggyClient, resolver: R, settings: IggyPublisherSettings) -> Self {
        Self {
            client,
            resolver,
            settings,
        }
    }
}

impl<R> Publisher for IggyPublisher<R>
where
    R: IggyDestinationResolver,
{
    type Error = IggyPublishError;

    async fn publish(&self, envelope: &SerializedEnvelope) -> Result<(), Self::Error> {
        let (stream, topic) = self.resolver.resolve(envelope).map_err(map_routing_error)?;

        let record = crate::IggyEnvelopeMapper
            .encode(envelope)
            .map_err(map_mapping_error)?;

        // Compute partitioning from the record's key before the record is moved into the SDK
        // message, so the payload is moved rather than cloned a second time.
        let partitioning = to_partitioning(record.key.as_deref()).map_err(mapping_failed)?;
        let mut message = to_iggy_message(record).map_err(mapping_failed)?;

        match tokio::time::timeout(
            self.settings.send_timeout(),
            self.client.sdk_client().send_messages(
                &stream,
                &topic,
                &partitioning,
                std::slice::from_mut(&mut message),
            ),
        )
        .await
        {
            Ok(Ok(_response)) => Ok(()),
            // The server's request-deduplication layer confirms this request already committed.
            // There is no reply payload to inspect, but "already applied" means durably written.
            Ok(Err(error)) if is_confirmed_commit_without_reply(&error) => Ok(()),
            Ok(Err(error)) => Err(IggyPublishError::from(error)),
            Err(_elapsed) => Err(IggyPublishError::new(
                IggyPublishErrorKind::OutcomeUnknown,
                FailureKind::Transient,
            )),
        }
    }
}

fn map_routing_error<E: ErrorClassifier>(error: E) -> IggyPublishError {
    IggyPublishError::new(
        IggyPublishErrorKind::Routing,
        ErrorClassifier::classify(&error),
    )
}

fn map_mapping_error(_error: IggyMappingError) -> IggyPublishError {
    IggyPublishError::new(IggyPublishErrorKind::Mapping, FailureKind::Permanent)
}

fn mapping_failed(_error: ()) -> IggyPublishError {
    IggyPublishError::new(IggyPublishErrorKind::Mapping, FailureKind::Permanent)
}

fn to_iggy_message(record: IggyRecord) -> Result<SdkIggyMessage, ()> {
    let mut user_headers = BTreeMap::new();

    for header in &record.headers {
        let key = HeaderKey::try_from(header.name.as_str()).map_err(|_: IggyError| ())?;
        // Headers are projected as String-kind values: every byte string this mapper writes
        // originated from a validated UTF-8 string.
        let value_str = std::str::from_utf8(header.value.as_slice()).map_err(|_| ())?;
        let value = SdkHeaderValue::try_from(value_str).map_err(|_: IggyError| ())?;
        user_headers.insert(key, value);
    }

    // The payload is moved out of the owned record instead of cloned: this is the only copy
    // between the mapper's projection and the bytes handed to the SDK.
    SdkIggyMessage::builder()
        .id(record.id)
        .payload(record.payload.into())
        .user_headers(user_headers)
        .build()
        .map_err(|_: IggyError| ())
}

fn to_partitioning(key: Option<&[u8]>) -> Result<Partitioning, ()> {
    match key {
        Some(bytes) => Partitioning::messages_key(bytes).map_err(|_: IggyError| ()),
        None => Ok(Partitioning::balanced()),
    }
}

/// Reports whether the server's reply confirms the request already committed, with no fresh
/// reply payload to inspect. See the crate documentation for the same-session VSR replay and
/// message-deduplication behavior this reflects.
fn is_confirmed_commit_without_reply(error: &IggyError) -> bool {
    matches!(error, IggyError::RequestAlreadyApplied)
}
