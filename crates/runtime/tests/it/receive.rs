//! Handling deliveries: acks, a handler's publishes, retries and the error destination.

use std::time::Duration;

use bytes::Bytes;
use sisa_messaging_core::{
    ContentType, Correlation, Envelope, Message, MessageId, MessageType, Publisher, Source,
    TraceContext, TransportPublisher,
};
use sisa_messaging_runtime::{
    Bus, Handler, HandlerContext, HandlerError, InMemoryTransport, RetryPolicy,
};
use sisa_messaging_testing::eventually;

use crate::support::{Billing, InvoiceSent, OrderPlaced, billing_bus, published_of};

const AN_HOUR: Duration = Duration::from_secs(3600);

fn order_envelope(content_type: &str, data: &'static [u8]) -> Envelope {
    Envelope::new(
        MessageType::new(OrderPlaced::TYPE).unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new(content_type).unwrap(),
        Bytes::from_static(data),
    )
}

async fn billing_bus_with(
    transport: &InMemoryTransport,
    billing: &Billing,
    attempts: u32,
) -> Bus<InMemoryTransport> {
    let mut policy = RetryPolicy::default();
    policy.attempts = attempts;
    Bus::builder("/billing", transport.clone())
        .endpoint("billing", |endpoint| {
            billing.endpoint(endpoint).retry(policy)
        })
        .start()
        .await
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn handles_a_message_once_and_acks_it() {
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let mut bus = billing_bus(&transport, &billing).await;

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    eventually("the first handling", async || billing.handled().len() == 1).await;
    tokio::time::sleep(AN_HOUR).await;

    assert_eq!(billing.attempts(), [1]);
}

#[tokio::test(start_paused = true)]
async fn sends_a_handlers_publishes_tied_to_the_handled_message() {
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let _bus = billing_bus(&transport, &billing).await;
    let correlation_id = MessageId::new();
    let mut order = order_envelope("application/json", br#"{"id":42}"#);
    order.correlation = Correlation::new(Some(correlation_id), None);
    order.trace = Some(TraceContext::new(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("congo=t61rcWkgMzE".into()),
    ));

    transport.publish(&order).await.unwrap();
    eventually("the invoice", async || {
        published_of::<InvoiceSent>(&transport).len() == 1
    })
    .await;

    let [invoice] = published_of::<InvoiceSent>(&transport).try_into().unwrap();
    assert_eq!(invoice.source.as_str(), "/billing");
    assert_eq!(
        invoice.correlation,
        Correlation::new(Some(correlation_id), Some(order.id))
    );
    assert_eq!(invoice.trace, order.trace);
    assert_eq!(&invoice.data[..], br#"{"order":42}"#);
}

#[tokio::test(start_paused = true)]
async fn drops_a_failed_handlers_publishes_and_retries_it() {
    let transport = InMemoryTransport::new();
    let billing = Billing::failing(1);
    let mut bus = billing_bus(&transport, &billing).await;

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    eventually("the first handling", async || billing.handled().len() == 1).await;
    let after_the_failure = published_of::<InvoiceSent>(&transport).len();
    eventually("the second handling", async || billing.handled().len() == 2).await;
    tokio::time::sleep(AN_HOUR).await;

    assert_eq!(after_the_failure, 0);
    assert_eq!(published_of::<InvoiceSent>(&transport).len(), 1);
    assert_eq!(billing.attempts(), [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn waits_longer_before_each_retry() {
    let transport = InMemoryTransport::new();
    let billing = Billing::failing(3);
    let mut bus = billing_bus(&transport, &billing).await;

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    for attempts in 2..=4 {
        eventually("the next retry", async || {
            billing.handled().len() == attempts
        })
        .await;
    }

    let handled = billing.handled();
    let waits: Vec<Duration> = handled
        .windows(2)
        .map(|pair| pair[1].at - pair[0].at)
        .collect();
    assert_eq!(billing.attempts(), [1, 2, 3, 4]);
    assert_eq!(waits, [1, 2, 4].map(Duration::from_secs));
}

#[tokio::test(start_paused = true)]
async fn sends_the_message_to_the_error_destination_after_the_last_attempt() {
    let transport = InMemoryTransport::new();
    let billing = Billing::failing(u32::MAX);
    let mut bus = billing_bus_with(&transport, &billing, 3).await;

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    eventually("the error copy", async || {
        transport.errors("billing").len() == 1
    })
    .await;
    tokio::time::sleep(AN_HOUR).await;

    let [failed] = transport.errors("billing").try_into().unwrap();
    let [order] = published_of::<OrderPlaced>(&transport).try_into().unwrap();
    assert_eq!(failed.id, order.id);
    assert_eq!(failed.data, order.data);
    let extensions: Vec<(&str, &str)> = failed.extensions.iter().collect();
    assert_eq!(
        extensions,
        [
            ("errorattempts", "3"),
            ("errorendpoint", "billing"),
            ("errormessage", "the card was declined"),
            ("errortype", "payment_declined"),
        ]
    );
    assert_eq!(transport.rejected("billing"), [order.id]);
    assert_eq!(billing.attempts(), [1, 2, 3]);
    assert!(published_of::<InvoiceSent>(&transport).is_empty());
}

#[tokio::test(start_paused = true)]
async fn retries_when_the_error_destination_fails() {
    let transport = InMemoryTransport::new();
    let billing = Billing::failing(u32::MAX);
    let mut bus = billing_bus_with(&transport, &billing, 1).await;

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    transport.fail_publishes(true);
    eventually("the second handling", async || billing.handled().len() == 2).await;
    transport.fail_publishes(false);
    eventually("the error copy", async || {
        transport.errors("billing").len() == 1
    })
    .await;
    tokio::time::sleep(AN_HOUR).await;

    assert_eq!(billing.attempts(), [1, 2, 3]);
    let [failed] = transport.errors("billing").try_into().unwrap();
    assert_eq!(failed.extensions.get("errorattempts"), Some("3"));
}

#[tokio::test(start_paused = true)]
async fn doesnt_ack_until_the_handlers_publishes_are_sent() {
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let mut bus = billing_bus(&transport, &billing).await;

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    transport.fail_publishes(true);
    eventually("the second handling", async || billing.handled().len() == 2).await;
    transport.fail_publishes(false);
    eventually("the invoice", async || {
        published_of::<InvoiceSent>(&transport).len() == 1
    })
    .await;
    tokio::time::sleep(AN_HOUR).await;

    assert_eq!(billing.attempts(), [1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn sends_data_that_doesnt_decode_to_the_error_destination_at_once() {
    let transport = InMemoryTransport::new();
    let billing = Billing::default();
    let _bus = billing_bus(&transport, &billing).await;

    transport
        .publish(&order_envelope("application/json", b"not json"))
        .await
        .unwrap();
    transport
        .publish(&order_envelope("application/xml", b"<order/>"))
        .await
        .unwrap();
    eventually("both error copies", async || {
        transport.errors("billing").len() == 2
    })
    .await;

    let errors = transport.errors("billing");
    let messages: Vec<&str> = errors
        .iter()
        .map(|failed| failed.extensions.get("errormessage").unwrap())
        .collect();
    assert_eq!(
        messages,
        [
            "could not decode orders.order_placed.v1 from application/json: \
             invalid JSON at line 1, column 2",
            "no codec reads application/xml",
        ]
    );
    for failed in &errors {
        assert_eq!(failed.extensions.get("errorattempts"), Some("1"));
        assert!(
            failed
                .extensions
                .get("errortype")
                .unwrap()
                .ends_with("CodecError")
        );
    }
    assert!(billing.handled().is_empty());
}

struct Panicking;

impl Handler<OrderPlaced> for Panicking {
    async fn handle(&self, _: OrderPlaced, _: &mut HandlerContext) -> Result<(), HandlerError> {
        panic!("the card reader broke");
    }
}

#[tokio::test(start_paused = true)]
async fn retries_a_handler_that_panics_like_one_that_fails() {
    let transport = InMemoryTransport::new();
    let mut policy = RetryPolicy::default();
    policy.attempts = 2;
    let mut bus = Bus::builder("/billing", transport.clone())
        .endpoint("billing", |endpoint| {
            endpoint.handle::<OrderPlaced>(Panicking).retry(policy)
        })
        .start()
        .await
        .unwrap();

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    eventually("the error copy", async || {
        transport.errors("billing").len() == 1
    })
    .await;

    let [failed] = transport.errors("billing").try_into().unwrap();
    assert_eq!(failed.extensions.get("errortype"), Some("panic"));
    assert_eq!(
        failed.extensions.get("errormessage"),
        Some("the card reader broke")
    );
    assert_eq!(failed.extensions.get("errorattempts"), Some("2"));
}
