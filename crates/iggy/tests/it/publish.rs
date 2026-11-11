use std::time::Duration;

use iggy::prelude::{Identifier, StreamClient, TopicClient};
use sisa_messaging_core::{Correlation, MessageId, TraceContext, TransportPublisher};
use sisa_messaging_iggy::IggyTransport;
use sisa_messaging_testing::Iggy;

use crate::common::{envelope, header, settings, stored};

#[tokio::test]
async fn connect_creates_the_stream() {
    let iggy = Iggy::start().await;

    IggyTransport::connect(settings(&iggy)).await.unwrap();
    // A second instance finds it.
    IggyTransport::connect(settings(&iggy)).await.unwrap();

    let stream = iggy
        .client()
        .await
        .get_stream(&Identifier::named("sisa").unwrap())
        .await
        .unwrap();
    assert!(stream.is_some());
}

#[tokio::test]
async fn publish_creates_the_topic_with_the_partitions() {
    let iggy = Iggy::start().await;
    let mut settings = settings(&iggy);
    settings.partitions = 3;
    let transport = IggyTransport::connect(settings).await.unwrap();

    transport.publish(&envelope()).await.unwrap();

    let topic = iggy
        .client()
        .await
        .get_topic(
            &Identifier::named("sisa").unwrap(),
            &Identifier::named("orders.order_placed.v1").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(topic.partitions_count, 3);
}

#[tokio::test]
async fn publish_writes_the_cloudevents_headers_the_id_and_the_data() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("42".into());
    envelope.correlation = Correlation::new(Some(MessageId::new()), None);
    envelope.trace = Some(TraceContext::new(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("vendor=1".into()),
    ));
    envelope.extensions.insert("tenant", "acme").unwrap();

    transport.publish(&envelope).await.unwrap();

    let messages = stored(&iggy, "orders.order_placed.v1", 4).await;
    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert_eq!(message.payload, envelope.data);
    assert_eq!(message.header.id, envelope.id.as_uuid().as_u128());
    for (name, value) in envelope.to_attributes() {
        assert_eq!(
            header(message, &format!("ce_{name}")),
            Some(value),
            "ce_{name}"
        );
    }
    assert_eq!(header(message, "ce_specversion").as_deref(), Some("1.0"));
    assert_eq!(header(message, "ce_tenant").as_deref(), Some("acme"));
    let headers = message.user_headers_map().unwrap().unwrap();
    assert_eq!(headers.len(), envelope.to_attributes().len());
}

#[tokio::test]
async fn messages_with_one_partition_key_go_to_one_partition() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();

    for _ in 0..8 {
        let mut envelope = envelope();
        envelope.partition_key = Some("order-42".into());
        transport.publish(&envelope).await.unwrap();
    }

    let client = iggy.client().await;
    let topic = client
        .get_topic(
            &Identifier::named("sisa").unwrap(),
            &Identifier::named("orders.order_placed.v1").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let used = topic
        .partitions
        .iter()
        .filter(|partition| partition.messages_count > 0)
        .count();
    assert_eq!(used, 1);
}

#[tokio::test]
async fn a_second_publish_of_one_id_is_stored_too() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let envelope = envelope();

    transport.publish(&envelope).await.unwrap();
    transport.publish(&envelope).await.unwrap();

    assert_eq!(stored(&iggy, "orders.order_placed.v1", 4).await.len(), 2);
}

#[tokio::test]
async fn a_publish_while_the_server_is_down_errors() {
    let iggy = Iggy::start_restartable().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    transport.publish(&envelope()).await.unwrap();

    iggy.stop().await;

    let envelope = envelope();
    let publish = tokio::time::timeout(Duration::from_secs(60), transport.publish(&envelope));
    assert!(
        matches!(publish.await, Ok(Err(_))),
        "the publish should fail"
    );
}

#[tokio::test]
async fn connect_to_a_server_that_is_down_errors_within_the_timeout() {
    let iggy = Iggy::start_restartable().await;
    let mut settings = settings(&iggy);
    settings.connect_timeout = Duration::from_secs(2);
    iggy.stop().await;

    let connect = tokio::time::timeout(Duration::from_secs(10), IggyTransport::connect(settings));
    assert!(matches!(connect.await, Ok(Err(_))));
}

#[tokio::test]
async fn an_attribute_longer_than_a_header_value_isnt_published() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("x".repeat(256));

    assert!(transport.publish(&envelope).await.is_err());
}
