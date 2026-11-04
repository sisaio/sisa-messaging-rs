use std::time::Duration;

use bytes::Bytes;
use sisa_messaging_core::{
    ContentType, Correlation, Envelope, MessageId, MessageType, Source, TraceContext,
    TransportPublisher,
};
use sisa_messaging_nats::{NatsSettings, NatsTransport};
use sisa_messaging_testing::Nats;

fn settings(nats: &Nats) -> NatsSettings {
    let mut settings = NatsSettings::default();
    settings.url = nats.url().to_owned();
    settings
}

fn envelope() -> Envelope {
    Envelope::new(
        MessageType::new("orders.order_placed.v1").unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from_static(br#"{"id":42}"#),
    )
}

async fn stored_messages(nats: &Nats) -> u64 {
    let mut stream = nats.jetstream().await.get_stream("SISA").await.unwrap();
    stream.info().await.unwrap().state.messages
}

#[tokio::test]
async fn connect_creates_the_stream_on_the_prefix() {
    let nats = Nats::start().await;

    NatsTransport::connect(settings(&nats)).await.unwrap();

    let mut stream = nats.jetstream().await.get_stream("SISA").await.unwrap();
    let config = &stream.info().await.unwrap().config;
    assert_eq!(config.subjects, ["sisa.>"]);
    assert_eq!(config.duplicate_window, Duration::from_secs(120));
}

#[tokio::test]
async fn connect_updates_an_existing_stream() {
    let nats = Nats::start().await;
    NatsTransport::connect(settings(&nats)).await.unwrap();

    let mut changed = settings(&nats);
    changed.duplicate_window = Duration::from_secs(30);
    NatsTransport::connect(changed).await.unwrap();

    let mut stream = nats.jetstream().await.get_stream("SISA").await.unwrap();
    let config = &stream.info().await.unwrap().config;
    assert_eq!(config.duplicate_window, Duration::from_secs(30));
}

#[tokio::test]
async fn publish_writes_the_cloudevents_headers_and_the_data() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("42".into());
    envelope.correlation = Correlation::new(Some(MessageId::new()), None);
    envelope.trace = Some(TraceContext::new(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("vendor=1".into()),
    ));
    envelope.extensions.insert("tenant", "acme").unwrap();

    transport.publish(&envelope).await.unwrap();

    let stream = nats.jetstream().await.get_stream("SISA").await.unwrap();
    let message = stream.get_raw_message(1).await.unwrap();
    assert_eq!(message.subject.as_str(), "sisa.orders.order_placed.v1");
    assert_eq!(message.payload, envelope.data);
    let header = |name: &str| {
        message
            .headers
            .get(name)
            .map(|value| value.as_str().to_owned())
    };
    let id = envelope.id.to_string();
    assert_eq!(header("Nats-Msg-Id").as_deref(), Some(id.as_str()));
    for (name, value) in envelope.to_attributes() {
        assert_eq!(header(&format!("ce-{name}")), Some(value), "ce-{name}");
    }
    assert_eq!(header("ce-specversion").as_deref(), Some("1.0"));
    assert_eq!(header("ce-type").as_deref(), Some("orders.order_placed.v1"));
    assert_eq!(header("ce-tenant").as_deref(), Some("acme"));
    let attributes = message
        .headers
        .iter()
        .filter(|(name, _)| name.to_string().starts_with("ce-"))
        .count();
    assert_eq!(attributes, envelope.to_attributes().len());
}

#[tokio::test]
async fn a_second_publish_of_one_id_inside_the_window_is_dropped() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let envelope = envelope();

    transport.publish(&envelope).await.unwrap();
    transport.publish(&envelope).await.unwrap();
    transport.publish(&self::envelope()).await.unwrap();

    assert_eq!(stored_messages(&nats).await, 2);
}

#[tokio::test]
async fn a_publish_while_the_server_is_down_errors() {
    let nats = Nats::start_restartable().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();

    nats.stop().await;

    assert!(transport.publish(&envelope()).await.is_err());
}

#[tokio::test]
async fn connect_to_a_server_that_is_down_errors() {
    let nats = Nats::start_restartable().await;
    let settings = settings(&nats);
    nats.stop().await;

    assert!(NatsTransport::connect(settings).await.is_err());
}

#[tokio::test]
async fn an_attribute_with_a_line_break_isnt_published() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("42\r\nce-type: forged".into());

    assert!(transport.publish(&envelope).await.is_err());
    assert_eq!(stored_messages(&nats).await, 0);
}
