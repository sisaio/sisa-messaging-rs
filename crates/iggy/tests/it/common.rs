use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use iggy::prelude::{
    Consumer, Identifier, IggyMessage, MessageClient, PollOptions, PollingStrategy,
};
use sisa_eventing_core::{ContentType, Deliveries, Envelope, MessageType, Source, TransportError};
use sisa_eventing_iggy::{IggyDelivery, IggySettings};
use sisa_eventing_testing::Iggy;

pub fn settings(iggy: &Iggy) -> IggySettings {
    let mut settings = IggySettings::default();
    settings.url = iggy.url().to_owned();
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

/// Every message in the topic's first `partitions` partitions, read without a consumer group.
pub async fn stored(iggy: &Iggy, topic: &str, partitions: u32) -> Vec<IggyMessage> {
    let client = iggy.client().await;
    let mut messages = Vec::new();
    for partition in 0..partitions {
        let polled = client
            .poll_messages(
                &Identifier::named("sisa").unwrap(),
                &Identifier::named(topic).unwrap(),
                partition,
                &Consumer::new(Identifier::named("inspect").unwrap()),
                &PollingStrategy::offset(0),
                PollOptions::default(),
            )
            .await
            .unwrap();
        messages.extend(polled.messages);
    }
    messages
}

/// A string user header of a stored message.
pub fn header(message: &IggyMessage, name: &str) -> Option<String> {
    message
        .user_headers_map()
        .unwrap()
        .unwrap_or_default()
        .iter()
        .find(|(key, _)| key.as_str().unwrap() == name)
        .map(|(_, value)| value.as_str().unwrap().to_owned())
}

/// The next delivery within `wait`, past the errors a reconnect yields; `None` when none comes.
pub async fn next_within(
    deliveries: &mut Deliveries<IggyDelivery>,
    wait: Duration,
) -> Option<IggyDelivery> {
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

pub async fn next(deliveries: &mut Deliveries<IggyDelivery>) -> IggyDelivery {
    next_within(deliveries, Duration::from_secs(10))
        .await
        .expect("a delivery within 10 seconds")
}
