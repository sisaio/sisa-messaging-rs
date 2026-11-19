use std::time::{Duration, Instant};

use sisa_messaging_core::{TransportError, TransportPublisher};
use sisa_messaging_redis::{RedisSettings, RedisTransport};
use sisa_messaging_testing::Redis;

use crate::common::{envelope, settings, stored};

#[tokio::test]
async fn publish_writes_the_cloudevents_fields_and_the_data() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("Euro €".into());
    envelope.extensions.insert("tenant", "acme").unwrap();

    transport.publish(&envelope).await.unwrap();

    let entries = stored(&redis, "sisa:orders.order_placed.v1").await;
    assert_eq!(entries.len(), 1);
    let fields = &entries[0];
    assert_eq!(fields["ce_specversion"], "1.0");
    assert_eq!(fields["ce_id"], envelope.id.to_string());
    assert_eq!(fields["ce_type"], "orders.order_placed.v1");
    assert_eq!(fields["ce_source"], "/orders");
    assert_eq!(fields["ce_datacontenttype"], "application/json");
    assert_eq!(fields["ce_subject"], "Euro €");
    assert_eq!(fields["ce_tenant"], "acme");
    assert!(fields.contains_key("ce_time"));
    assert_eq!(fields["data"], r#"{"id":42}"#);
}

#[tokio::test]
async fn the_prefix_starts_the_stream_name() {
    let redis = Redis::start().await;
    let mut settings = settings(&redis);
    settings.prefix = "staging".into();
    let transport = RedisTransport::connect(settings).await.unwrap();

    transport.publish(&envelope()).await.unwrap();

    assert_eq!(
        stored(&redis, "staging:orders.order_placed.v1").await.len(),
        1
    );
    assert!(
        stored(&redis, "sisa:orders.order_placed.v1")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn a_second_publish_of_one_id_is_stored_too() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let envelope = envelope();

    transport.publish(&envelope).await.unwrap();
    transport.publish(&envelope).await.unwrap();

    assert_eq!(stored(&redis, "sisa:orders.order_placed.v1").await.len(), 2);
}

#[tokio::test]
async fn connect_to_a_server_that_is_down_errors_within_the_timeout() {
    let mut settings = RedisSettings::default();
    // Nothing listens on port 1.
    settings.url = "redis://127.0.0.1:1".into();
    settings.connect_timeout = Duration::from_secs(2);
    let started = Instant::now();

    let error = RedisTransport::connect(settings).await.unwrap_err();

    assert!(matches!(error, TransportError::Broker { .. }), "{error}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn a_publish_while_the_server_is_down_errors() {
    let redis = Redis::start().await;
    let mut settings = settings(&redis);
    settings.connect_timeout = Duration::from_secs(1);
    let transport = RedisTransport::connect(settings).await.unwrap();

    drop(redis);

    let error = transport.publish(&envelope()).await.unwrap_err();
    assert!(matches!(error, TransportError::Broker { .. }), "{error}");
}
