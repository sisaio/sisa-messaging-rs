use std::time::Duration;

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use sisa_messaging::{ErrorClassifier, FailureKind};

use crate::{
    KafkaClient, KafkaMappingError, KafkaPublishError, KafkaPublishErrorKind, KafkaRecord,
};

pub(super) fn map_error<E: ErrorClassifier>(error: E) -> KafkaPublishError {
    KafkaPublishError::new(
        KafkaPublishErrorKind::TopicResolution,
        ErrorClassifier::classify(&error),
    )
}

pub(super) fn map_mapping_error(_error: KafkaMappingError) -> KafkaPublishError {
    KafkaPublishError::new(KafkaPublishErrorKind::Mapping, FailureKind::Permanent)
}

pub(super) async fn deliver(
    client: &KafkaClient,
    topic: &str,
    record: &KafkaRecord,
    enqueue_timeout: Duration,
) -> Result<(), KafkaPublishError> {
    match client
        .producer()
        .send(to_future_record(topic, record), enqueue_timeout)
        .await
    {
        Ok(_delivery) => Ok(()),
        Err((error, _message)) => Err(delivery_error(error)),
    }
}

fn to_future_record<'a>(topic: &'a str, record: &'a KafkaRecord) -> FutureRecord<'a, [u8], [u8]> {
    let mut headers = OwnedHeaders::new_with_capacity(record.headers.len());

    for header in &record.headers {
        headers = headers.insert(Header {
            key: &header.name,
            value: header.value.as_deref(),
        });
    }

    let mut message = FutureRecord::to(topic).payload(record.payload.as_slice());

    if let Some(key) = record.key.as_deref() {
        message = message.key(key);
    }

    if !record.headers.is_empty() {
        message = message.headers(headers);
    }

    message
}

fn delivery_error(error: KafkaError) -> KafkaPublishError {
    let kind = if matches!(
        error,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    ) {
        KafkaPublishErrorKind::Enqueue
    } else {
        KafkaPublishErrorKind::Delivery
    };

    let failure_kind = classify_delivery_error(&error);

    KafkaPublishError::new(kind, failure_kind)
}

fn classify_delivery_error(error: &KafkaError) -> FailureKind {
    match error {
        KafkaError::MessageProduction(RDKafkaErrorCode::InvalidArgument)
        | KafkaError::MessageProduction(RDKafkaErrorCode::InvalidTopic)
        | KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge)
        | KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed)
        | KafkaError::MessageProduction(RDKafkaErrorCode::ClusterAuthorizationFailed) => {
            FailureKind::Permanent
        }
        _ => FailureKind::Transient,
    }
}
