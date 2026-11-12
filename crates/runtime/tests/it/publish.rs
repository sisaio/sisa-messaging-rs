//! Publishing from the bus, and starting it.

use sisa_eventing_core::{Message, Publisher, TransportError};
use sisa_eventing_runtime::{Bus, InMemoryTransport, PublishError, StartError};

use crate::support::{Billing, OrderPlaced};

#[tokio::test(start_paused = true)]
async fn publishes_straight_to_the_broker() {
    let transport = InMemoryTransport::new();
    let mut bus = Bus::builder("/orders", transport.clone())
        .start()
        .await
        .unwrap();

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();

    let [envelope] = transport.published().try_into().unwrap();
    assert_eq!(envelope.message_type.as_str(), OrderPlaced::TYPE);
    assert_eq!(envelope.source.as_str(), "/orders");
    assert_eq!(envelope.content_type.as_str(), "application/json");
    assert_eq!(&envelope.data[..], br#"{"id":42}"#);
    assert_eq!(envelope.correlation, Default::default());
}

#[tokio::test(start_paused = true)]
async fn reports_a_publish_the_broker_didnt_confirm() {
    let transport = InMemoryTransport::new();
    let mut bus = Bus::builder("/orders", transport.clone())
        .start()
        .await
        .unwrap();
    transport.fail_publishes(true);

    let result = bus.publish(&OrderPlaced { id: 42 }).await;

    assert!(matches!(
        result,
        Err(PublishError::Transport(TransportError::Broker { .. }))
    ));
}

#[tokio::test(start_paused = true)]
async fn refuses_an_invalid_source() {
    let result = Bus::builder("my orders", InMemoryTransport::new())
        .start()
        .await;

    assert!(matches!(result, Err(StartError::Envelope(_))));
}

#[tokio::test(start_paused = true)]
async fn refuses_an_endpoint_added_twice() {
    let billing = Billing::default();

    let result = Bus::builder("/billing", InMemoryTransport::new())
        .endpoint("billing", |endpoint| billing.endpoint(endpoint))
        .endpoint("billing", |endpoint| billing.endpoint(endpoint))
        .start()
        .await;

    assert!(matches!(result, Err(StartError::DuplicateEndpoint(name)) if name == "billing"));
}

#[tokio::test(start_paused = true)]
async fn refuses_two_handlers_for_one_message_type() {
    let billing = Billing::default();

    let result = Bus::builder("/billing", InMemoryTransport::new())
        .endpoint("billing", |endpoint| {
            billing.endpoint(billing.endpoint(endpoint))
        })
        .start()
        .await;

    assert!(matches!(
        result,
        Err(StartError::DuplicateHandler { endpoint, message_type })
            if endpoint == "billing" && message_type == OrderPlaced::TYPE
    ));
}
