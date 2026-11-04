use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use sisa_messaging_core::{ContentType, Deliveries, Envelope, MessageType, Source, TransportError};
use sisa_messaging_nats::{NatsDelivery, NatsSettings};
use sisa_messaging_testing::Nats;

pub fn settings(nats: &Nats) -> NatsSettings {
    let mut settings = NatsSettings::default();
    settings.url = nats.url().to_owned();
    settings
}

pub fn envelope_of(message_type: &str) -> Envelope {
    Envelope::new(
        MessageType::new(message_type).unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from_static(br#"{"id":42}"#),
    )
}

pub fn envelope() -> Envelope {
    envelope_of("orders.order_placed.v1")
}

pub async fn stored_messages(nats: &Nats) -> u64 {
    let mut stream = nats.jetstream().await.get_stream("SISA").await.unwrap();
    stream.info().await.unwrap().state.messages
}

/// The next delivery within `wait`, past the errors a reconnect yields; `None` when none comes.
pub async fn next_within(
    deliveries: &mut Deliveries<NatsDelivery>,
    wait: Duration,
) -> Option<NatsDelivery> {
    tokio::time::timeout(wait, async {
        loop {
            match deliveries.next().await {
                Some(Ok(delivery)) => return delivery,
                Some(Err(TransportError::Broker { .. })) => {}
                Some(Err(error)) => panic!("{error}"),
                None => panic!("the deliveries ended"),
            }
        }
    })
    .await
    .ok()
}

pub async fn next(deliveries: &mut Deliveries<NatsDelivery>) -> NatsDelivery {
    next_within(deliveries, Duration::from_secs(10))
        .await
        .expect("a delivery within 10 seconds")
}
