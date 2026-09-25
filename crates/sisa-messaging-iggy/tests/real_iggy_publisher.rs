//! Opt-in real-broker proof that publisher success follows a server reply, and that an unknown
//! topic is rejected permanently.

mod support;

use sisa_messaging::{
    ContentType, DeliveryMetadata, ErrorClassifier, FailureKind, MessageId, MessageType, Metadata,
    MetadataValue, Publisher, RoutingMetadata, SerializedEnvelope,
};
use sisa_messaging_iggy::{
    IggyPublishErrorKind, IggyPublisher, IggyPublisherSettings, RoutingDestinationResolver,
};

use support::{
    TEST_TIMEOUT, new_iggy_client, new_raw_client, poll_for_marker, test_stream, test_topic,
};

fn publisher_envelope(destination: MetadataValue, payload: Vec<u8>) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("iggy.confirmation.probe").expect("fixture type is valid"),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream")
            .expect("fixture content type is valid"),
        payload,
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
#[ignore = "requires a real Iggy broker; provisions its own stream and topic"]
async fn publisher_returns_after_successful_server_reply() {
    let stream = test_stream();
    let topic = test_topic();

    let provisioning_client = new_raw_client().await;
    support::provision_stream_and_topic(&provisioning_client, &stream, &topic).await;

    let client = new_iggy_client().await;
    let publisher = IggyPublisher::new(
        client,
        RoutingDestinationResolver,
        IggyPublisherSettings::default(),
    );

    let marker = MessageId::new().to_string();
    let destination = MetadataValue::new(format!("{stream}/{topic}"))
        .expect("test destination is valid metadata");
    let envelope = publisher_envelope(destination, marker.clone().into_bytes());

    let result = publisher.publish(&envelope).await;

    assert!(
        result.is_ok(),
        "Iggy publisher did not receive a server reply",
    );

    let seen = poll_for_marker(
        &provisioning_client,
        &stream,
        &topic,
        marker.as_bytes(),
        TEST_TIMEOUT,
    )
    .await;

    assert!(
        seen,
        "published message was not visible by polling the topic"
    );
}

#[tokio::test]
#[ignore = "requires a real Iggy broker with the test stream already provisioned; rejects the intentionally unknown topic"]
async fn unknown_topic_publish_is_classified_permanently() {
    let stream = test_stream();

    let client = new_iggy_client().await;
    let publisher = IggyPublisher::new(
        client,
        RoutingDestinationResolver,
        IggyPublisherSettings::default(),
    );

    let destination = MetadataValue::new(format!("{stream}/sisa-iggy-missing-topic"))
        .expect("test destination is valid metadata");
    let envelope = publisher_envelope(destination, b"unknown-topic-probe".to_vec());

    let error = publisher
        .publish(&envelope)
        .await
        .expect_err("Iggy must reject publication to an unknown topic");

    assert_eq!(error.kind(), IggyPublishErrorKind::Rejected);
    assert_eq!(error.classify(), FailureKind::Permanent);
}
