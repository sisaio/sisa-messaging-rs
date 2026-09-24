//! Opt-in real-broker proof that publisher success follows a delivery report.

mod support;

use rdkafka::util::get_rdkafka_version;
use sisa_messaging::{
    ContentType, DeliveryMetadata, ErrorClassifier, FailureKind, MessageId, MessageType, Metadata,
    MetadataValue, Publisher, RoutingMetadata, SerializedEnvelope,
};
use sisa_messaging_kafka::{
    KafkaPublishErrorKind, KafkaPublisher, KafkaPublisherSettings, RoutingDestinationResolver,
};

use support::{BROKERS_ENV, TOPIC_ENV, new_kafka_client, required_env};

fn publisher_envelope(destination: MetadataValue) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("kafka.confirmation.probe").expect("fixture type is valid"),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream")
            .expect("fixture content type is valid"),
        payload: b"confirmation-probe".to_vec(),
        metadata: Metadata {
            delivery: DeliveryMetadata::default(),
            routing: RoutingMetadata {
                destination: Some(destination),
                ..RoutingMetadata::default()
            },
            ..Metadata::default()
        },
        ordering_key: None,
    }
}

#[tokio::test]
#[ignore = "requires a real Kafka broker and a pre-provisioned isolated test topic"]
async fn publisher_returns_after_successful_delivery_report() {
    let (_, client_version) = get_rdkafka_version();

    assert_eq!(client_version, "2.12.1");

    let brokers = required_env(BROKERS_ENV);
    let topic = required_env(TOPIC_ENV);
    let client = new_kafka_client(&brokers);
    let destination = MetadataValue::new(topic).expect("test topic value is valid metadata");
    let publisher = KafkaPublisher::new(
        client,
        RoutingDestinationResolver,
        KafkaPublisherSettings::default(),
    );
    let envelope = publisher_envelope(destination);

    let result = publisher.publish(&envelope).await;

    assert!(
        result.is_ok(),
        "Kafka publisher did not receive a delivery confirmation",
    );
}

#[tokio::test]
#[ignore = "requires a real Kafka broker to reject the intentionally invalid topic"]
async fn invalid_topic_delivery_is_classified_permanently() {
    let (_, client_version) = get_rdkafka_version();

    assert_eq!(client_version, "2.12.1");

    let brokers = required_env(BROKERS_ENV);
    let client = new_kafka_client(&brokers);
    let destination =
        MetadataValue::new("invalid topic").expect("fixture topic is valid shared metadata");
    let publisher = KafkaPublisher::new(
        client,
        RoutingDestinationResolver,
        KafkaPublisherSettings::default(),
    );

    let error = publisher
        .publish(&publisher_envelope(destination))
        .await
        .expect_err("Kafka must reject the invalid topic");

    assert_eq!(error.kind(), KafkaPublishErrorKind::Delivery);
    assert_eq!(error.classify(), FailureKind::Permanent);
}
