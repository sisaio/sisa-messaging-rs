#![cfg(feature = "integration")]

use redis::{
    aio::MultiplexedConnection,
    streams::{StreamPendingCountReply, StreamPendingReply},
};
use sisa_messaging::{
    ContentType, Delivery, EnvelopeMapper, IndividualCapability, IndividualDeliverySource,
    IndividualSettlement, IndividualSettlementError, IndividualSourceOpenError,
    IndividualSourceRequirements, MessageId, MessageType, Metadata, SerializedEnvelope,
};
use sisa_messaging_redis::{
    RedisDeliverySource, RedisError, RedisMapper, RedisPublisher, SourceSettings,
};
use std::time::Duration;

fn settings() -> SourceSettings {
    SourceSettings {
        min_idle: Duration::from_millis(50),
        scan_cadence: Duration::from_millis(10),
        page_size: 2,
        pages_per_receive: 2,
        read_block: Duration::from_millis(25),
        command_timeout: Duration::from_secs(2),
    }
}

fn envelope() -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("redis_test").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload: vec![0, 1, 255, 42],
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

async fn connection(client: &redis::Client) -> MultiplexedConnection {
    client.get_multiplexed_async_connection().await.unwrap()
}

async fn pending(connection: &mut MultiplexedConnection, stream: &str, group: &str) -> usize {
    let reply: StreamPendingReply = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .query_async(connection)
        .await
        .unwrap();

    reply.count()
}

async fn pending_ids(
    connection: &mut MultiplexedConnection,
    stream: &str,
    group: &str,
) -> Vec<String> {
    let reply: StreamPendingCountReply = redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .arg("-")
        .arg("+")
        .arg(16)
        .query_async(connection)
        .await
        .unwrap();

    reply.ids.into_iter().map(|pending| pending.id).collect()
}

#[tokio::test]
#[ignore = "requires a real Redis Streams server at SISA_REDIS_URL"]
async fn append_read_ack_reclaim_and_cancel() {
    let url =
        std::env::var("SISA_REDIS_URL").expect("SISA_REDIS_URL must point to a real RESP server");

    let client = redis::Client::open(url).unwrap();
    let mut commands = connection(&client).await;
    let suffix = MessageId::new().to_string().replace('-', "");
    let stream = format!("sisa-test-{suffix}");
    let group = format!("group-{suffix}");

    let publisher = RedisPublisher::new(
        connection(&client).await,
        stream.clone(),
        Duration::from_secs(2),
    )
    .unwrap();

    assert!(matches!(
        publisher.append(&envelope()).await,
        Err(RedisError::SourceClosed)
    ));

    let exists: i64 = redis::cmd("EXISTS")
        .arg(&stream)
        .query_async(&mut commands)
        .await
        .unwrap();

    assert_eq!(
        exists, 0,
        "NOMKSTREAM must leave provisioning to the caller"
    );

    let _: String = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("setup")
        .arg("1")
        .query_async(&mut commands)
        .await
        .unwrap();

    let _: String = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(&group)
        .arg("$")
        .query_async(&mut commands)
        .await
        .unwrap();

    let mut first = RedisDeliverySource::new(
        connection(&client).await,
        connection(&client).await,
        stream.clone(),
        group.clone(),
        "first".to_owned(),
        settings(),
    )
    .unwrap();

    let descriptor = first
        .open(IndividualSourceRequirements::new())
        .await
        .unwrap();

    assert_eq!(descriptor.ack_wait(), None);
    assert_eq!(descriptor.max_deliver(), None);
    assert!(!descriptor.supports_delayed_retry());
    assert!(!descriptor.supports_immediate_requeue());
    assert!(!descriptor.supports_terminal_discard());
    assert!(!descriptor.supports_heartbeat());

    for unsupported in [
        IndividualSourceRequirements::new().requiring_ack_wait(),
        IndividualSourceRequirements::new().requiring_max_deliver(),
        IndividualSourceRequirements::new().requiring_delayed_retry(),
        IndividualSourceRequirements::new().requiring_immediate_requeue(),
        IndividualSourceRequirements::new().requiring_terminal_discard(),
        IndividualSourceRequirements::new().requiring_heartbeat(),
    ] {
        assert!(matches!(
            first.open(unsupported).await,
            Err(IndividualSourceOpenError::Unsupported(_))
        ));
    }

    // A finite blocking read can be cancelled before publication.
    assert!(
        tokio::time::timeout(Duration::from_millis(40), first.receive())
            .await
            .is_err()
    );

    let original = envelope();
    let first_id = publisher.append(&original).await.unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(2), first.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let (wire, settlement) = delivered.into_parts();
    assert_eq!(RedisMapper.decode(wire).unwrap(), original);
    assert_eq!(pending(&mut commands, &stream, &group).await, 1);
    settlement.ack().await.unwrap();
    assert_eq!(pending(&mut commands, &stream, &group).await, 0);

    // A retry of the same logical envelope appends another entry and remains independently pending.
    let duplicate_id = publisher.append(&original).await.unwrap();
    assert_ne!(first_id, duplicate_id);

    let abandoned = tokio::time::timeout(Duration::from_secs(2), first.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    drop(abandoned);
    assert_eq!(pending(&mut commands, &stream, &group).await, 1);

    let mut second = RedisDeliverySource::new(
        connection(&client).await,
        connection(&client).await,
        stream.clone(),
        group.clone(),
        "second".to_owned(),
        settings(),
    )
    .unwrap();

    second
        .open(IndividualSourceRequirements::new())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(75)).await;

    let recovered = tokio::time::timeout(Duration::from_secs(2), second.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let (wire, mut settlement) = recovered.into_parts();

    assert_eq!(
        RedisMapper.decode(wire).unwrap().message_id,
        original.message_id
    );

    assert!(matches!(
        settlement.heartbeat().await,
        Err(IndividualSettlementError::Unsupported(_))
    ));

    settlement.ack().await.unwrap();
    assert_eq!(pending(&mut commands, &stream, &group).await, 0);

    let third_envelope = envelope();
    let third = publisher.append(&third_envelope).await.unwrap();
    assert!(!third.is_empty());

    let delivered = tokio::time::timeout(Duration::from_secs(2), second.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let (wire, settlement) = delivered.into_parts();

    assert_eq!(
        RedisMapper.decode(wire).unwrap().message_id,
        third_envelope.message_id
    );

    assert!(matches!(
        settlement.nak(Duration::from_secs(1)).await,
        Err(IndividualSettlementError::Unsupported(
            IndividualCapability::DelayedRetry
        ))
    ));

    assert_eq!(
        pending_ids(&mut commands, &stream, &group).await.as_slice(),
        std::slice::from_ref(&third)
    );

    let removed: i64 = redis::cmd("XACK")
        .arg(&stream)
        .arg(&group)
        .arg(&third)
        .query_async(&mut commands)
        .await
        .unwrap();

    assert_eq!(removed, 1);

    let fourth_envelope = envelope();
    let fourth = publisher.append(&fourth_envelope).await.unwrap();
    assert!(!fourth.is_empty());

    let delivered = tokio::time::timeout(Duration::from_secs(2), second.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let (wire, settlement) = delivered.into_parts();

    assert_eq!(
        RedisMapper.decode(wire).unwrap().message_id,
        fourth_envelope.message_id
    );

    assert!(matches!(
        settlement.terminate().await,
        Err(IndividualSettlementError::Unsupported(_))
    ));

    assert_eq!(
        pending_ids(&mut commands, &stream, &group).await.as_slice(),
        std::slice::from_ref(&fourth)
    );

    let removed: i64 = redis::cmd("XACK")
        .arg(&stream)
        .arg(&group)
        .arg(&fourth)
        .query_async(&mut commands)
        .await
        .unwrap();

    assert_eq!(removed, 1);

    let fifth_envelope = envelope();
    let fifth = publisher.append(&fifth_envelope).await.unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(2), second.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let (wire, settlement) = delivered.into_parts();

    assert_eq!(
        RedisMapper.decode(wire).unwrap().message_id,
        fifth_envelope.message_id
    );

    assert_eq!(
        pending_ids(&mut commands, &stream, &group).await.as_slice(),
        std::slice::from_ref(&fifth)
    );

    let removed: i64 = redis::cmd("XACK")
        .arg(&stream)
        .arg(&group)
        .arg(&fifth)
        .query_async(&mut commands)
        .await
        .unwrap();

    assert_eq!(removed, 1);

    assert!(matches!(
        settlement.ack().await,
        Err(IndividualSettlementError::Operation(RedisError::Protocol))
    ));

    let sixth = publisher.append(&envelope()).await.unwrap();

    let delivered = tokio::time::timeout(Duration::from_secs(2), second.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let (_, settlement) = delivered.into_parts();

    assert!(matches!(
        settlement.nak(Duration::ZERO).await,
        Err(IndividualSettlementError::Unsupported(
            IndividualCapability::ImmediateRequeue
        ))
    ));

    let _: i64 = redis::cmd("XACK")
        .arg(&stream)
        .arg(&group)
        .arg(&sixth)
        .query_async(&mut commands)
        .await
        .unwrap();

    second.close();
    assert!(second.receive().await.unwrap().is_none());

    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut commands)
        .await
        .unwrap();

    let mut gone = RedisDeliverySource::new(
        connection(&client).await,
        connection(&client).await,
        stream,
        group,
        "gone".to_owned(),
        settings(),
    )
    .unwrap();

    assert!(matches!(
        gone.open(IndividualSourceRequirements::new()).await,
        Err(IndividualSourceOpenError::Source(RedisError::SourceClosed))
    ));
}
