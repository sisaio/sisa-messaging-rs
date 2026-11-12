//! A bus with a codec of its own: a wrapping one, as an application would write to encrypt.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use sisa_eventing_core::{
    Codec, CodecError, ContentType, Envelope, JsonCodec, Message, MessageType, Publisher, Source,
    TransportPublisher,
};
use sisa_eventing_runtime::{
    Bus, Handler, HandlerContext, HandlerError, InMemoryTransport, StartError,
};
use sisa_eventing_testing::eventually;

use crate::support::{Billing, InvoiceSent, OrderPlaced, published_of};

/// JSON with its bytes reversed, standing in for a codec that encrypts what another encodes.
#[derive(Debug, Default)]
struct Reversed(JsonCodec);

impl Codec for Reversed {
    fn content_type(&self) -> &'static str {
        "application/x-reversed+json"
    }

    fn encode<Msg: Message>(&self, message: &Msg) -> Result<Bytes, CodecError> {
        let mut data = self.0.encode(message)?.to_vec();
        data.reverse();
        Ok(data.into())
    }

    fn decode<Msg: Message>(&self, data: &[u8]) -> Result<Msg, CodecError> {
        let mut data = data.to_vec();
        data.reverse();
        self.0.decode(&data)
    }
}

/// Records each order and publishes its invoice.
#[derive(Clone, Default)]
struct Charge(Arc<Mutex<Vec<u64>>>);

impl Handler<OrderPlaced, (), Reversed> for Charge {
    async fn handle(
        &self,
        order: OrderPlaced,
        context: &mut HandlerContext<(), Reversed>,
    ) -> Result<(), HandlerError> {
        self.0.lock().unwrap().push(order.id);
        context.publish(&InvoiceSent { order: order.id }).await?;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn publishes_with_the_bus_s_codec() {
    let transport = InMemoryTransport::new();
    let mut bus = Bus::builder("/orders", transport.clone())
        .codec(Reversed::default())
        .start()
        .await
        .unwrap();

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();

    let [envelope] = transport.published().try_into().unwrap();
    assert_eq!(
        envelope.content_type.as_str(),
        "application/x-reversed+json"
    );
    assert_eq!(&envelope.data[..], br#"}24:"di"{"#);
}

#[tokio::test(start_paused = true)]
async fn handlers_decode_and_publish_with_the_bus_s_codec() {
    let transport = InMemoryTransport::new();
    let charge = Charge::default();
    let mut bus = Bus::builder("/billing", transport.clone())
        .codec(Reversed::default())
        .endpoint("billing", |endpoint| endpoint.handle(charge.clone()))
        .start()
        .await
        .unwrap();

    bus.publish(&OrderPlaced { id: 42 }).await.unwrap();
    eventually("the invoice", async || {
        published_of::<InvoiceSent>(&transport).len() == 1
    })
    .await;

    assert_eq!(*charge.0.lock().unwrap(), [42]);
    let [invoice] = published_of::<InvoiceSent>(&transport).try_into().unwrap();
    assert_eq!(invoice.content_type.as_str(), "application/x-reversed+json");
    assert_eq!(&invoice.data[..], br#"}24:"redro"{"#);
}

#[tokio::test(start_paused = true)]
async fn a_delivery_the_codec_cant_read_goes_to_the_error_destination() {
    let transport = InMemoryTransport::new();
    let _bus = Bus::builder("/billing", transport.clone())
        .codec(Reversed::default())
        .endpoint("billing", |endpoint| endpoint.handle(Charge::default()))
        .start()
        .await
        .unwrap();
    let json = Envelope::new(
        MessageType::new(OrderPlaced::TYPE).unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from_static(br#"{"id":42}"#),
    );

    transport.publish(&json).await.unwrap();
    eventually("the error copy", async || {
        transport.errors("billing").len() == 1
    })
    .await;

    let [failed] = transport.errors("billing").try_into().unwrap();
    assert_eq!(failed.id, json.id);
    assert_eq!(
        failed.extensions.get("errormessage"),
        Some("no codec reads application/json")
    );
    assert!(published_of::<InvoiceSent>(&transport).is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_endpoint_added_before_the_codec_is_refused() {
    let transport = InMemoryTransport::new();

    let result = Bus::builder("/billing", transport)
        .endpoint("billing", |endpoint| Billing::default().endpoint(endpoint))
        .codec(Reversed::default())
        .start()
        .await;

    assert!(matches!(
        result,
        Err(StartError::EndpointBeforeCodec(name)) if name == "billing"
    ));
}
