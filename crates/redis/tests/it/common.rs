use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use redis::Value;
use redis::streams::StreamRangeReply;
use sisa_messaging_core::{ContentType, Deliveries, Envelope, MessageType, Source, TransportError};
use sisa_messaging_redis::{RedisDelivery, RedisSettings};
use sisa_messaging_testing::Redis;

pub fn settings(redis: &Redis) -> RedisSettings {
    let mut settings = RedisSettings::default();
    settings.url = redis.url().to_owned();
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

/// The fields of every entry in a stream, as text.
pub async fn stored(redis: &Redis, stream: &str) -> Vec<HashMap<String, String>> {
    let range: StreamRangeReply = redis::cmd("XRANGE")
        .arg(stream)
        .arg("-")
        .arg("+")
        .query_async(&mut redis.connection().await)
        .await
        .unwrap();
    range
        .ids
        .into_iter()
        .map(|entry| {
            entry
                .map
                .into_iter()
                .map(|(name, value)| match value {
                    Value::BulkString(bytes) => (name, String::from_utf8(bytes).unwrap()),
                    other => panic!("{name} is {other:?}"),
                })
                .collect()
        })
        .collect()
}

/// Appends an entry with these fields, as another producer would.
pub async fn append_raw(redis: &Redis, stream: &str, fields: &[(&str, &[u8])]) {
    let mut command = redis::cmd("XADD");
    command.arg(stream).arg("*");
    for (name, value) in fields {
        command.arg(*name).arg(*value);
    }
    command
        .query_async::<String>(&mut redis.connection().await)
        .await
        .unwrap();
}

/// The next delivery within `wait`, past the errors a reconnect yields; `None` when none comes.
pub async fn next_within(
    deliveries: &mut Deliveries<RedisDelivery>,
    wait: Duration,
) -> Option<RedisDelivery> {
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

pub async fn next(deliveries: &mut Deliveries<RedisDelivery>) -> RedisDelivery {
    next_within(deliveries, Duration::from_secs(10))
        .await
        .expect("a delivery within 10 seconds")
}
