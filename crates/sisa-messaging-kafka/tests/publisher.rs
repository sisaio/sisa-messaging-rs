use std::{error::Error as StdError, sync::Arc, time::Duration};

use sisa_messaging::{
    ContentType, DeliveryMetadata, ErrorClassifier, FailureKind, MessageId, MessageType, Metadata,
    MetadataValue, Publisher, RoutingMetadata, SerializedEnvelope,
};
use sisa_messaging_kafka::{
    KafkaClient, KafkaClientErrorKind, KafkaClientSettings, KafkaPublishErrorKind, KafkaPublisher,
    KafkaPublisherSettings, RoutingDestinationResolver,
};

#[test]
fn delivery_report_only_error_cannot_disable_delivery_confirmation() {
    let error = KafkaClientSettings::new(["localhost:1"])
        .with_advanced_property(" Delivery.Report.Only.Error ", "true")
        .expect_err("delivery report suppression can leave publish futures pending");

    assert_eq!(error.kind(), KafkaClientErrorKind::TypedPropertyOverride);
}

fn local_client(properties: &[(&str, &str)]) -> KafkaClient {
    let config = properties.iter().fold(
        KafkaClientSettings::new(["localhost:1"]),
        |config, (name, value)| {
            config
                .with_advanced_property(*name, *value)
                .expect("test producer property is allowed")
        },
    );

    KafkaClient::start(config).expect("local producer configuration is valid")
}

fn envelope_without_destination() -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("publisher.test").expect("fixture type is valid"),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream")
            .expect("fixture content type is valid"),
        payload: Vec::new(),
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

fn envelope_for(topic: &str, payload: Vec<u8>) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("publisher.test").expect("fixture type is valid"),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream")
            .expect("fixture content type is valid"),
        payload,
        metadata: Metadata {
            delivery: DeliveryMetadata::default(),
            routing: RoutingMetadata {
                destination: Some(MetadataValue::new(topic).expect("fixture topic is metadata")),
                ..RoutingMetadata::default()
            },
            ..Metadata::default()
        },
        ordering_key: None,
    }
}

#[tokio::test]
async fn missing_destination_fails_permanently_before_producer_delivery() {
    let client = local_client(&[]);

    let publisher = KafkaPublisher::new(
        client,
        RoutingDestinationResolver,
        KafkaPublisherSettings::default(),
    );

    let error = publisher
        .publish(&envelope_without_destination())
        .await
        .expect_err("missing destination must fail before producer delivery");

    assert_eq!(error.kind(), KafkaPublishErrorKind::TopicResolution);
    assert_eq!(error.classify(), FailureKind::Permanent);
    assert_eq!(error.to_string(), "Kafka topic resolution failed");
}

#[tokio::test]
async fn oversized_message_is_rejected_permanently_without_leaking_payload() {
    let client = local_client(&[
        ("message.max.bytes", "1000"),
        ("message.timeout.ms", "1000"),
    ]);

    let publisher = KafkaPublisher::new(
        client,
        RoutingDestinationResolver,
        KafkaPublisherSettings::default(),
    );

    let payload = [b"PAYLOAD_REDACTION_SENTINEL".as_slice(), &vec![b'x'; 2048]].concat();
    let envelope = envelope_for("publisher-size-probe", payload);

    let error = tokio::time::timeout(Duration::from_secs(2), publisher.publish(&envelope))
        .await
        .expect("local size check should be bounded")
        .expect_err("oversized Kafka record must be rejected");

    assert_eq!(error.kind(), KafkaPublishErrorKind::Delivery);
    assert_eq!(error.classify(), FailureKind::Permanent);

    assert_eq!(
        error.to_string(),
        "Kafka producer reported an unsuccessful delivery"
    );

    assert!(!error.to_string().contains("PAYLOAD_REDACTION_SENTINEL"));
    assert!(StdError::source(&error).is_none());
}

#[tokio::test]
async fn full_local_queue_is_a_transient_enqueue_failure() {
    let client = local_client(&[
        ("queue.buffering.max.messages", "1"),
        ("message.timeout.ms", "5000"),
    ]);

    let queue_observer = client.clone();

    let publisher = Arc::new(KafkaPublisher::new(
        client,
        RoutingDestinationResolver,
        KafkaPublisherSettings {
            enqueue_timeout: Duration::ZERO,
        },
    ));

    let first_publisher = Arc::clone(&publisher);
    let first_envelope = envelope_for("publisher-queue-probe", b"first".to_vec());

    let first_delivery =
        tokio::spawn(async move { first_publisher.publish(&first_envelope).await });

    tokio::time::timeout(Duration::from_secs(1), async {
        while queue_observer.in_flight_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first record should become observable in the producer queue");

    let envelope = envelope_for("publisher-queue-probe", b"second".to_vec());

    let error = tokio::time::timeout(Duration::from_secs(2), publisher.publish(&envelope))
        .await
        .expect("queue-full response should be bounded")
        .expect_err("second record should encounter the full local queue");

    first_delivery.abort();

    assert_eq!(error.kind(), KafkaPublishErrorKind::Enqueue);
    assert_eq!(error.classify(), FailureKind::Transient);

    assert_eq!(
        error.to_string(),
        "Kafka producer queue rejected the record"
    );

    assert!(StdError::source(&error).is_none());
}
