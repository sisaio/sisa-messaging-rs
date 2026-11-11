use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use iggy::prelude::{
    HeaderKey, HeaderValue, Identifier, IggyMessage, MessageClient, Partitioning, TopicClient,
};
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, MessageType, TransportPublisher, TransportSubscriber,
};
use sisa_messaging_iggy::{IggyDelivery, IggyTransport};
use sisa_messaging_testing::Iggy;

use crate::common::{envelope, envelope_of, header, next, next_within, settings, stored};

fn billing() -> EndpointSpec {
    EndpointSpec::new(
        "billing",
        vec![MessageType::new("orders.order_placed.v1").unwrap()],
    )
}

// Long enough that a message that was going to come again would.
const QUIET: Duration = Duration::from_secs(3);

// Asserts that nothing comes within the quiet time, and that the consumer still delivers: a new
// message is the next to come, so neither a stalled consumer nor a late redelivery passes.
async fn assert_quiet(transport: &IggyTransport, deliveries: &mut Deliveries<IggyDelivery>) {
    assert!(next_within(deliveries, QUIET).await.is_none());
    let sentinel = envelope();
    transport.publish(&sentinel).await.unwrap();
    let delivery = next(deliveries).await;
    assert_eq!(delivery.envelope().id, sentinel.id);
    delivery.ack().await.unwrap();
}

// Sends a message as another client would: any payload, any headers.
async fn send_raw(iggy: &Iggy, headers: BTreeMap<HeaderKey, HeaderValue>, payload: &'static [u8]) {
    let message = IggyMessage::builder()
        .payload(Bytes::from_static(payload))
        .user_headers(headers)
        .build()
        .unwrap();
    iggy.client()
        .await
        .send_messages(
            &Identifier::named("sisa").unwrap(),
            &Identifier::named("orders.order_placed.v1").unwrap(),
            &Partitioning::balanced(),
            &mut [message],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn an_endpoint_gets_the_published_envelope() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let mut envelope = envelope();
    envelope.subject = Some("42".into());
    envelope.extensions.insert("tenant", "acme").unwrap();

    transport.publish(&envelope).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope(), &envelope);
    assert_eq!(delivery.attempt(), 1);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn a_new_endpoint_gets_the_messages_already_in_its_topic() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    let mut deliveries = transport.subscribe(&billing()).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn two_instances_of_one_endpoint_share_its_messages() {
    let iggy = Iggy::start().await;
    let first = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let second = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut first_deliveries = first.subscribe(&billing()).await.unwrap();
    let mut second_deliveries = second.subscribe(&billing()).await.unwrap();

    let mut published = HashSet::new();
    for _ in 0..20 {
        let envelope = envelope();
        published.insert(envelope.id);
        first.publish(&envelope).await.unwrap();
    }

    let mut received = HashSet::new();
    let mut by_first = 0;
    while received.len() < published.len() {
        let (delivery, from_first) = tokio::select! {
            delivery = next(&mut first_deliveries) => (delivery, true),
            delivery = next(&mut second_deliveries) => (delivery, false),
        };
        by_first += usize::from(from_first);
        assert!(
            received.insert(delivery.envelope().id),
            "a message came twice"
        );
        delivery.ack().await.unwrap();
    }
    assert_eq!(received, published);
    assert!(
        0 < by_first && by_first < published.len(),
        "both instances got some"
    );
    for deliveries in [&mut first_deliveries, &mut second_deliveries] {
        assert!(next_within(deliveries, QUIET).await.is_none());
    }
}

#[tokio::test]
async fn each_endpoint_gets_every_message_of_its_types() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
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
    assert_quiet(&transport, &mut billing_deliveries).await;

    let mut shipped = HashSet::new();
    for _ in 0..2 {
        let delivery = next(&mut shipping_deliveries).await;
        shipped.insert(delivery.envelope().id);
        delivery.ack().await.unwrap();
    }
    assert!(shipped.contains(&placed.id) && shipped.contains(&paid.id));
}

#[tokio::test]
async fn a_retried_message_comes_again_after_its_delay_before_the_next() {
    let iggy = Iggy::start().await;
    let mut settings = settings(&iggy);
    settings.partitions = 1;
    let transport = IggyTransport::connect(settings).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let first = envelope();
    let second = envelope();
    transport.publish(&first).await.unwrap();
    transport.publish(&second).await.unwrap();

    let delay = Duration::from_secs(2);
    let first_try = next(&mut deliveries).await;
    assert_eq!(first_try.envelope().id, first.id);
    let retried_at = Instant::now();
    first_try.retry(delay).await.unwrap();

    let second_try = next(&mut deliveries).await;
    assert!(retried_at.elapsed() >= delay);
    assert_eq!(second_try.envelope().id, first.id);
    assert_eq!(second_try.attempt(), 2);
    second_try.retry(Duration::ZERO).await.unwrap();
    let third_try = next(&mut deliveries).await;
    assert_eq!(third_try.envelope().id, first.id);
    assert_eq!(third_try.attempt(), 3);
    third_try.ack().await.unwrap();

    let behind = next(&mut deliveries).await;
    assert_eq!(behind.envelope().id, second.id);
    assert_eq!(behind.attempt(), 1);
    behind.ack().await.unwrap();
}

#[tokio::test]
async fn a_rejected_or_acked_message_doesnt_come_again() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();

    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.reject().await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();

    assert_quiet(&transport, &mut deliveries).await;
}

#[tokio::test]
async fn an_unsettled_message_comes_again() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    drop(next(&mut deliveries).await);

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    assert_eq!(delivery.attempt(), 2);
}

#[tokio::test]
async fn a_new_subscription_resumes_after_the_committed_offset() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();
    drop(deliveries);

    // Another instance, after this one left: it starts where the endpoint's commits end.
    let other = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut other_deliveries = other.subscribe(&billing()).await.unwrap();
    let later = envelope();
    transport.publish(&later).await.unwrap();

    let delivery = next(&mut other_deliveries).await;
    assert_eq!(delivery.envelope().id, later.id);
    delivery.ack().await.unwrap();
    assert_quiet(&other, &mut other_deliveries).await;
}

#[tokio::test]
async fn an_ack_doesnt_commit_past_an_earlier_message_still_unsettled() {
    let iggy = Iggy::start().await;
    let mut one_partition = settings(&iggy);
    one_partition.partitions = 1;
    let transport = IggyTransport::connect(one_partition.clone()).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let first = envelope();
    transport.publish(&first).await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    let earlier = next(&mut deliveries).await;
    let later = next(&mut deliveries).await;
    assert_eq!(earlier.envelope().id, first.id);

    // The later one is done first; the earlier one then waits for a retry this instance never
    // gets to.
    later.ack().await.unwrap();
    earlier.retry(Duration::from_secs(3600)).await.unwrap();
    drop(deliveries);

    let other = IggyTransport::connect(one_partition).await.unwrap();
    let mut other_deliveries = other.subscribe(&billing()).await.unwrap();
    let delivery = next(&mut other_deliveries).await;
    assert_eq!(delivery.envelope().id, first.id);
}

#[tokio::test]
async fn consumption_resumes_after_the_server_restarts() {
    let iggy = Iggy::start_restartable().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();

    iggy.stop().await;
    iggy.restart().await;
    let envelope = envelope();
    // The client reconnects on its own; a publish fails until it has.
    let deadline = Instant::now() + Duration::from_secs(60);
    while transport.publish(&envelope).await.is_err() {
        assert!(Instant::now() < deadline, "no publish after the restart");
    }

    let delivery = next_within(&mut deliveries, Duration::from_secs(60))
        .await
        .expect("a delivery after the restart");
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn an_error_copy_goes_to_the_error_topic() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut envelope = envelope();
    transport.publish(&envelope).await.unwrap();
    envelope
        .extensions
        .insert("errormessage", "x".repeat(300))
        .unwrap();

    transport.publish_error("billing", &envelope).await.unwrap();

    let copies = stored(&iggy, "_error.billing", 1).await;
    assert_eq!(copies.len(), 1);
    let copy = &copies[0];
    assert_eq!(copy.payload, envelope.data);
    assert_eq!(header(copy, "ce_id"), Some(envelope.id.to_string()));
    assert_eq!(header(copy, "ce_errormessage"), Some("x".repeat(255)));
    assert_eq!(stored(&iggy, "orders.order_placed.v1", 4).await.len(), 1);
}

#[tokio::test]
async fn a_message_that_isnt_an_envelope_goes_to_the_error_topic() {
    let iggy = Iggy::start().await;
    let mut settings = settings(&iggy);
    settings.partitions = 1;
    let transport = IggyTransport::connect(settings).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    send_raw(&iggy, BTreeMap::new(), b"plain").await;
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();

    let copies = stored(&iggy, "_error.billing", 1).await;
    assert_eq!(copies.len(), 1);
    let copy = &copies[0];
    assert_eq!(copy.payload, Bytes::from_static(b"plain"));
    assert_eq!(header(copy, "ce_errorendpoint").as_deref(), Some("billing"));
    assert_eq!(
        header(copy, "ce_errortype").as_deref(),
        Some("invalid_envelope")
    );
    assert!(header(copy, "ce_errormessage").is_some());
}

#[tokio::test]
async fn setting_aside_a_message_doesnt_commit_past_an_earlier_one_still_unsettled() {
    let iggy = Iggy::start().await;
    let mut one_partition = settings(&iggy);
    one_partition.partitions = 1;
    let transport = IggyTransport::connect(one_partition.clone()).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let first = envelope();
    transport.publish(&first).await.unwrap();
    send_raw(&iggy, BTreeMap::new(), b"plain").await;
    let earlier = next(&mut deliveries).await;
    assert_eq!(earlier.envelope().id, first.id);

    // The message after it is set aside while it is still being handled.
    let client = iggy.client().await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let set_aside = client
            .get_topic(
                &Identifier::named("sisa").unwrap(),
                &Identifier::named("_error.billing").unwrap(),
            )
            .await
            .unwrap()
            .is_some_and(|topic| topic.messages_count > 0);
        if set_aside {
            break;
        }
        assert!(Instant::now() < deadline, "the message wasn't set aside");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    earlier.retry(Duration::from_secs(3600)).await.unwrap();
    drop(deliveries);

    let other = IggyTransport::connect(one_partition).await.unwrap();
    let mut other_deliveries = other.subscribe(&billing()).await.unwrap();
    let delivery = next(&mut other_deliveries).await;
    assert_eq!(delivery.envelope().id, first.id);
}

#[tokio::test]
async fn ce_headers_are_read_without_regard_to_case() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    let headers = envelope
        .to_attributes()
        .into_iter()
        .map(|(name, value)| {
            (
                HeaderKey::try_from(format!("Ce_{name}").as_str()).unwrap(),
                HeaderValue::try_from(value.as_str()).unwrap(),
            )
        })
        .collect();
    send_raw(&iggy, headers, br#"{"id":42}"#).await;

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope(), &envelope);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn an_endpoint_without_message_types_is_refused() {
    let iggy = Iggy::start().await;
    let transport = IggyTransport::connect(settings(&iggy)).await.unwrap();

    let endpoint = EndpointSpec::new("billing", Vec::new());
    assert!(transport.subscribe(&endpoint).await.is_err());
}
