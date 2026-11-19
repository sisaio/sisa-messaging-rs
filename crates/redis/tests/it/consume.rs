use std::collections::HashSet;
use std::time::{Duration, Instant};

use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, MessageType, TransportPublisher, TransportSubscriber,
};
use sisa_messaging_redis::{RedisDelivery, RedisTransport};
use sisa_messaging_testing::{Redis, eventually};

use crate::common::{append_raw, envelope, envelope_of, next, next_within, settings, stored};

fn billing() -> EndpointSpec {
    EndpointSpec::new(
        "billing",
        vec![MessageType::new("orders.order_placed.v1").unwrap()],
    )
}

// Long enough that a message that was going to come again would, with a one-second ack wait.
const QUIET: Duration = Duration::from_secs(3);

/// A transport whose unsettled deliveries come again after a second.
async fn quick(redis: &Redis) -> RedisTransport {
    let mut settings = settings(redis);
    settings.ack_wait = Duration::from_secs(1);
    RedisTransport::connect(settings).await.unwrap()
}

// Asserts that nothing comes within the quiet time, and that the consumer still delivers: a new
// message is the next to come, so neither a stalled consumer nor a late redelivery passes.
async fn assert_quiet(transport: &RedisTransport, deliveries: &mut Deliveries<RedisDelivery>) {
    assert!(next_within(deliveries, QUIET).await.is_none());
    let sentinel = envelope();
    transport.publish(&sentinel).await.unwrap();
    let delivery = next(deliveries).await;
    assert_eq!(delivery.envelope().id, sentinel.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn an_endpoint_gets_the_published_envelope() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("Euro €".into());
    envelope.extensions.insert("tenant", "acme").unwrap();

    transport.publish(&envelope).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope(), &envelope);
    assert_eq!(delivery.attempt(), 1);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn a_new_endpoint_starts_with_the_messages_published_after_it() {
    let redis = Redis::start().await;
    let transport = quick(&redis).await;
    transport.publish(&envelope()).await.unwrap();

    let mut deliveries = transport.subscribe(&billing()).await.unwrap();

    assert_quiet(&transport, &mut deliveries).await;
}

#[tokio::test]
async fn two_instances_of_one_endpoint_share_its_messages() {
    let redis = Redis::start().await;
    let first = quick(&redis).await;
    let second = quick(&redis).await;
    let mut first_deliveries = first.subscribe(&billing()).await.unwrap();
    let mut second_deliveries = second.subscribe(&billing()).await.unwrap();

    let mut published = HashSet::new();
    for _ in 0..20 {
        let envelope = envelope();
        published.insert(envelope.id);
        first.publish(&envelope).await.unwrap();
    }

    let mut received = HashSet::new();
    let mut by_second = 0;
    while received.len() < published.len() {
        let (delivery, from_second) = tokio::select! {
            delivery = next(&mut first_deliveries) => (delivery, false),
            delivery = next(&mut second_deliveries) => (delivery, true),
        };
        by_second += usize::from(from_second);
        assert!(
            received.insert(delivery.envelope().id),
            "a message came twice"
        );
        delivery.ack().await.unwrap();
    }
    assert_eq!(received, published);
    assert!(
        by_second > 0 && by_second < 20,
        "{by_second} of 20 went to the second"
    );
    for deliveries in [&mut first_deliveries, &mut second_deliveries] {
        assert!(next_within(deliveries, QUIET).await.is_none());
    }
}

#[tokio::test]
async fn each_endpoint_gets_every_message_of_its_types() {
    let redis = Redis::start().await;
    let transport = quick(&redis).await;
    let shipping = EndpointSpec::new(
        "shipping",
        vec![
            MessageType::new("orders.order_placed.v1").unwrap(),
            MessageType::new("orders.order_paid.v1").unwrap(),
        ],
    );
    let mut billing_deliveries = transport.subscribe(&billing()).await.unwrap();
    let mut shipping_deliveries = transport.subscribe(&shipping).await.unwrap();

    let placed = envelope();
    let paid = envelope_of("orders.order_paid.v1");
    transport.publish(&placed).await.unwrap();
    transport.publish(&paid).await.unwrap();

    let billed = next(&mut billing_deliveries).await;
    assert_eq!(billed.envelope().id, placed.id);
    billed.ack().await.unwrap();

    let mut shipped = HashSet::new();
    for _ in 0..2 {
        let delivery = next(&mut shipping_deliveries).await;
        shipped.insert(delivery.envelope().id);
        delivery.ack().await.unwrap();
    }
    assert_eq!(shipped, HashSet::from([placed.id, paid.id]));
    assert_quiet(&transport, &mut billing_deliveries).await;
}

#[tokio::test]
async fn a_retried_message_comes_again_after_its_delay() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    let delay = Duration::from_secs(2);
    let retried_at = Instant::now();
    next(&mut deliveries).await.retry(delay).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert!(retried_at.elapsed() >= delay);
    assert_eq!(delivery.envelope().id, envelope.id);
    assert_eq!(delivery.attempt(), 2);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn a_retry_longer_than_the_ack_wait_comes_after_its_delay_on_any_instance() {
    let redis = Redis::start().await;
    let first = quick(&redis).await;
    let second = quick(&redis).await;
    let mut first_deliveries = first.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    first.publish(&envelope).await.unwrap();
    let delay = Duration::from_secs(3);
    let retried_at = Instant::now();
    next(&mut first_deliveries)
        .await
        .retry(delay)
        .await
        .unwrap();
    let mut second_deliveries = second.subscribe(&billing()).await.unwrap();

    let delivery = tokio::select! {
        delivery = next(&mut first_deliveries) => delivery,
        delivery = next(&mut second_deliveries) => delivery,
    };

    assert!(retried_at.elapsed() >= delay, "{:?}", retried_at.elapsed());
    assert_eq!(delivery.envelope().id, envelope.id);
    assert_eq!(delivery.attempt(), 2);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn a_rejected_or_acked_message_doesnt_come_again() {
    let redis = Redis::start().await;
    let transport = quick(&redis).await;
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();

    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.reject().await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();

    assert_quiet(&transport, &mut deliveries).await;
}

#[tokio::test]
async fn an_unsettled_message_comes_again_after_the_ack_wait() {
    let redis = Redis::start().await;
    let transport = quick(&redis).await;
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    drop(next(&mut deliveries).await);

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    assert_eq!(delivery.attempt(), 2);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn another_instance_takes_over_the_messages_of_one_that_stopped() {
    let redis = Redis::start().await;
    let stopped = quick(&redis).await;
    let mut stopped_deliveries = stopped.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    stopped.publish(&envelope).await.unwrap();
    let unsettled = next(&mut stopped_deliveries).await;
    drop(stopped_deliveries);
    let running = quick(&redis).await;

    let mut deliveries = running.subscribe(&billing()).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    assert_eq!(delivery.attempt(), 2);
    delivery.ack().await.unwrap();
    // The first instance's late ack finds nothing left to settle.
    unsettled.ack().await.unwrap();
}

#[tokio::test]
async fn consumption_resumes_after_the_server_restarts() {
    let redis = Redis::start_restartable().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();

    redis.stop().await;
    redis.restart().await;
    let envelope = envelope();
    // The connection comes back on its own; a publish fails until it has.
    let deadline = Instant::now() + Duration::from_secs(30);
    while transport.publish(&envelope).await.is_err() {
        assert!(Instant::now() < deadline, "no publish after the restart");
    }

    let delivery = next_within(&mut deliveries, Duration::from_secs(30))
        .await
        .expect("a delivery after the restart");
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn an_error_copy_goes_to_the_error_stream() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut envelope = envelope();
    envelope
        .extensions
        .insert("errormessage", "card declined\nretry later")
        .unwrap();

    transport.publish_error("billing", &envelope).await.unwrap();

    let copies = stored(&redis, "sisa:_error:billing").await;
    assert_eq!(copies.len(), 1);
    assert_eq!(copies[0]["ce_id"], envelope.id.to_string());
    assert_eq!(copies[0]["ce_errormessage"], "card declined\nretry later");
    assert_eq!(copies[0]["data"], r#"{"id":42}"#);
}

#[tokio::test]
async fn a_message_that_isnt_an_envelope_goes_to_the_error_stream() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    append_raw(
        &redis,
        "sisa:orders.order_placed.v1",
        &[("origin", b"legacy"), ("data", b"plain")],
    )
    .await;
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();

    let copies = stored(&redis, "sisa:_error:billing").await;
    assert_eq!(copies.len(), 1);
    assert_eq!(copies[0]["origin"], "legacy");
    assert_eq!(copies[0]["data"], "plain");
    assert_eq!(copies[0]["ce_errorendpoint"], "billing");
    assert_eq!(copies[0]["ce_errortype"], "invalid_envelope");
    assert_eq!(
        copies[0]["ce_errormessage"],
        "missing CloudEvents attribute `specversion`"
    );
}

#[tokio::test]
async fn an_entry_without_data_goes_to_the_error_stream() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    let id = envelope.id.to_string();
    append_raw(
        &redis,
        "sisa:orders.order_placed.v1",
        &[
            ("ce_specversion", b"1.0"),
            ("ce_id", id.as_bytes()),
            ("ce_type", b"orders.order_placed.v1"),
            ("ce_source", b"/orders"),
            ("ce_datacontenttype", b"application/json"),
        ],
    )
    .await;

    eventually("the entry is in the error stream", async || {
        stored(&redis, "sisa:_error:billing").await.len() == 1
    })
    .await;
    let copies = stored(&redis, "sisa:_error:billing").await;
    assert_eq!(copies[0]["ce_id"], id);
    assert_eq!(copies[0]["ce_errormessage"], "the entry has no data field");
    assert!(
        next_within(&mut deliveries, Duration::from_secs(1))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn ce_fields_are_read_without_regard_to_case() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    let id = envelope.id.to_string();
    let time = envelope.time.to_string();
    append_raw(
        &redis,
        "sisa:orders.order_placed.v1",
        &[
            ("Ce_Specversion", b"1.0"),
            ("CE_ID", id.as_bytes()),
            ("ce_Type", b"orders.order_placed.v1"),
            ("ce_source", b"/orders"),
            ("ce_time", time.as_bytes()),
            ("ce_datacontenttype", b"application/json"),
            ("data", br#"{"id":42}"#),
        ],
    )
    .await;

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope(), &envelope);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn the_kafka_content_type_field_names_the_content_type() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    let id = envelope.id.to_string();
    let time = envelope.time.to_string();
    append_raw(
        &redis,
        "sisa:orders.order_placed.v1",
        &[
            ("ce_specversion", b"1.0"),
            ("ce_id", id.as_bytes()),
            ("ce_type", b"orders.order_placed.v1"),
            ("ce_source", b"/orders"),
            ("ce_time", time.as_bytes()),
            ("content-type", b"application/json"),
            ("data", br#"{"id":42}"#),
        ],
    )
    .await;

    let delivery = next(&mut deliveries).await;
    assert_eq!(
        delivery.envelope().content_type.as_str(),
        "application/json"
    );
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn a_deleted_stream_gets_the_group_back() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    redis::cmd("DEL")
        .arg("sisa:orders.order_placed.v1")
        .query_async::<()>(&mut redis.connection().await)
        .await
        .unwrap();
    // The read that finds no group recreates it.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn an_endpoint_without_message_types_is_refused() {
    let redis = Redis::start().await;
    let transport = RedisTransport::connect(settings(&redis)).await.unwrap();

    let refused = transport
        .subscribe(&EndpointSpec::new("billing", Vec::new()))
        .await;

    assert!(refused.is_err());
}
