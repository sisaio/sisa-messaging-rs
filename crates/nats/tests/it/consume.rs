use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use sisa_messaging_core::{
    Deliveries, Delivery, EndpointSpec, MessageType, TransportPublisher, TransportSubscriber,
};
use sisa_messaging_nats::{NatsDelivery, NatsTransport};
use sisa_messaging_testing::Nats;

use crate::common::{envelope, envelope_of, next, next_within, settings, stored_messages};

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
async fn assert_quiet(transport: &NatsTransport, deliveries: &mut Deliveries<NatsDelivery>) {
    assert!(next_within(deliveries, QUIET).await.is_none());
    let sentinel = envelope();
    transport.publish(&sentinel).await.unwrap();
    let delivery = next(deliveries).await;
    assert_eq!(delivery.envelope().id, sentinel.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn an_endpoint_gets_the_published_envelope() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
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
async fn two_instances_of_one_endpoint_share_its_messages() {
    let nats = Nats::start().await;
    let first = NatsTransport::connect(settings(&nats)).await.unwrap();
    let second = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut first_deliveries = first.subscribe(&billing()).await.unwrap();
    let mut second_deliveries = second.subscribe(&billing()).await.unwrap();

    let mut published = HashSet::new();
    for _ in 0..20 {
        let envelope = envelope();
        published.insert(envelope.id);
        first.publish(&envelope).await.unwrap();
    }

    let mut received = HashSet::new();
    while received.len() < published.len() {
        let delivery = tokio::select! {
            delivery = next(&mut first_deliveries) => delivery,
            delivery = next(&mut second_deliveries) => delivery,
        };
        assert!(
            received.insert(delivery.envelope().id),
            "a message came twice"
        );
        delivery.ack().await.unwrap();
    }
    assert_eq!(received, published);
    for deliveries in [&mut first_deliveries, &mut second_deliveries] {
        assert!(next_within(deliveries, QUIET).await.is_none());
    }
    let sentinel = envelope();
    first.publish(&sentinel).await.unwrap();
    let delivery = tokio::select! {
        delivery = next(&mut first_deliveries) => delivery,
        delivery = next(&mut second_deliveries) => delivery,
    };
    assert_eq!(delivery.envelope().id, sentinel.id);
}

#[tokio::test]
async fn each_endpoint_gets_every_message_of_its_types() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
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
    assert_eq!(shipped, HashSet::from([placed.id, paid.id]));
}

#[tokio::test]
async fn a_retried_message_comes_again_after_its_delay() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
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
async fn a_rejected_or_acked_message_doesnt_come_again() {
    let nats = Nats::start().await;
    let mut settings = settings(&nats);
    settings.ack_wait = Duration::from_secs(1);
    let transport = NatsTransport::connect(settings).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();

    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.reject().await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();

    assert_quiet(&transport, &mut deliveries).await;
}

#[tokio::test]
async fn an_unsettled_message_comes_again_after_the_ack_wait() {
    let nats = Nats::start().await;
    let mut settings = settings(&nats);
    settings.ack_wait = Duration::from_secs(1);
    let transport = NatsTransport::connect(settings).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    drop(next(&mut deliveries).await);

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    assert_eq!(delivery.attempt(), 2);
}

#[tokio::test]
async fn consumption_resumes_after_the_server_restarts() {
    let nats = Nats::start_restartable().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    transport.publish(&envelope()).await.unwrap();
    next(&mut deliveries).await.ack().await.unwrap();

    nats.stop().await;
    nats.restart().await;
    let envelope = envelope();
    // The client reconnects on its own; a publish fails until it has.
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
async fn an_error_copy_goes_to_the_error_subject_next_to_the_original() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut envelope = envelope();
    transport.publish(&envelope).await.unwrap();
    envelope
        .extensions
        .insert("errormessage", "card declined\nretry later")
        .unwrap();

    transport.publish_error("billing", &envelope).await.unwrap();

    assert_eq!(stored_messages(&nats).await, 2);
    let stream = nats.jetstream().await.get_stream("SISA").await.unwrap();
    let copy = stream.get_raw_message(2).await.unwrap();
    assert_eq!(copy.subject.as_str(), "sisa._error.billing");
    assert_eq!(copy.payload, envelope.data);
    let header = |name: &str| {
        copy.headers
            .get(name)
            .map(|value| value.as_str().to_owned())
    };
    assert_eq!(header("ce-id"), Some(envelope.id.to_string()));
    assert_eq!(
        header("ce-errormessage").as_deref(),
        Some("card declined retry later")
    );
    assert_eq!(header("Nats-Msg-Id"), None);
}

#[tokio::test]
async fn a_message_that_isnt_an_envelope_goes_to_the_error_subject() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let jetstream = nats.jetstream().await;
    jetstream
        .publish("sisa.orders.order_placed.v1", Bytes::from_static(b"plain"))
        .await
        .unwrap()
        .await
        .unwrap();
    let envelope = envelope();
    transport.publish(&envelope).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, envelope.id);
    delivery.ack().await.unwrap();

    let stream = jetstream.get_stream("SISA").await.unwrap();
    sisa_messaging_testing::eventually("the copy is in the error subject", async || {
        stored_messages(&nats).await == 3
    })
    .await;
    let copy = stream
        .get_last_raw_message_by_subject("sisa._error.billing")
        .await
        .unwrap();
    assert_eq!(copy.payload, Bytes::from_static(b"plain"));
    let header = |name: &str| {
        copy.headers
            .get(name)
            .map(|value| value.as_str().to_owned())
    };
    assert_eq!(header("ce-errorendpoint").as_deref(), Some("billing"));
    assert_eq!(header("ce-errortype").as_deref(), Some("invalid_envelope"));
    assert!(header("ce-errormessage").is_some());
}

#[tokio::test]
async fn a_subscribe_with_fewer_types_keeps_the_endpoint_s_other_types() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let newer = EndpointSpec::new(
        "billing",
        vec![
            MessageType::new("orders.order_placed.v1").unwrap(),
            MessageType::new("orders.order_paid.v1").unwrap(),
        ],
    );
    drop(transport.subscribe(&newer).await.unwrap());

    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let paid = envelope_of("orders.order_paid.v1");
    transport.publish(&paid).await.unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope().id, paid.id);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn ce_headers_are_read_without_regard_to_case() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let envelope = envelope();
    let mut headers = async_nats::HeaderMap::new();
    for (name, value) in envelope.to_attributes() {
        headers.insert(format!("Ce-{name}"), value);
    }
    nats.jetstream()
        .await
        .publish_with_headers(
            "sisa.orders.order_placed.v1",
            headers,
            envelope.data.clone(),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.envelope(), &envelope);
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn a_message_that_isnt_an_envelope_stays_until_its_copy_is_stored() {
    let nats = Nats::start().await;
    let mut settings = settings(&nats);
    settings.ack_wait = Duration::from_secs(1);
    let transport = NatsTransport::connect(settings).await.unwrap();
    let mut deliveries = transport.subscribe(&billing()).await.unwrap();
    let jetstream = nats.jetstream().await;
    let mut stream = jetstream.get_stream("SISA").await.unwrap();
    let open = stream.info().await.unwrap().config.clone();
    jetstream
        .publish("sisa.orders.order_placed.v1", Bytes::from_static(b"plain"))
        .await
        .unwrap()
        .await
        .unwrap();
    // A full stream that refuses new messages makes the error copy fail.
    jetstream
        .update_stream(async_nats::jetstream::stream::Config {
            max_messages: 1,
            discard: async_nats::jetstream::stream::DiscardPolicy::New,
            ..open.clone()
        })
        .await
        .unwrap();

    let failure = tokio::time::timeout(Duration::from_secs(10), deliveries.next())
        .await
        .expect("an error within 10 seconds");
    assert!(matches!(failure, Some(Err(_))));

    // Pulling again redelivers the message after the ack wait, and now its copy is stored.
    jetstream.update_stream(open).await.unwrap();
    assert_quiet(&transport, &mut deliveries).await;
    let copy = stream
        .get_last_raw_message_by_subject("sisa._error.billing")
        .await
        .unwrap();
    assert_eq!(copy.payload, Bytes::from_static(b"plain"));
}

#[tokio::test]
async fn an_endpoint_without_message_types_is_refused() {
    let nats = Nats::start().await;
    let transport = NatsTransport::connect(settings(&nats)).await.unwrap();

    let endpoint = EndpointSpec::new("billing", Vec::new());
    assert!(transport.subscribe(&endpoint).await.is_err());
}
