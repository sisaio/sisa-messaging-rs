//! Real-server benchmark. Set `SISA_REDIS_URL` and use `cargo bench -p sisa-messaging-redis --bench provider`.

use criterion::{Criterion, criterion_group, criterion_main};
use sisa_messaging::{
    ContentType, Delivery, IndividualDeliverySource, IndividualSettlement,
    IndividualSourceRequirements, MessageId, MessageType, Metadata, SerializedEnvelope,
};
use sisa_messaging_redis::{RedisDeliverySource, RedisPublisher, SourceSettings};
use std::time::Duration;

fn envelope() -> SerializedEnvelope {
    SerializedEnvelope {
        message_id: MessageId::new(),
        message_type: MessageType::new("benchmark").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload: vec![42; 256],
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

fn benchmark(c: &mut Criterion) {
    let Some(url) = std::env::var("SISA_REDIS_URL").ok() else {
        return;
    };

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let (publisher, mut fresh, mut reclaim, stream, mut commands) = runtime.block_on(async {
        let client = redis::Client::open(url).unwrap();
        let suffix = MessageId::new().to_string().replace('-', "");
        let stream = format!("sisa-bench-{suffix}");
        let group = format!("group-{suffix}");
        let mut commands = client.get_multiplexed_async_connection().await.unwrap();

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

        let settings = SourceSettings {
            min_idle: Duration::from_millis(1),
            scan_cadence: Duration::from_millis(1),
            page_size: 16,
            pages_per_receive: 2,
            read_block: Duration::from_millis(10),
            command_timeout: Duration::from_secs(2),
        };

        let mut fresh = RedisDeliverySource::new(
            client.get_multiplexed_async_connection().await.unwrap(),
            client.get_multiplexed_async_connection().await.unwrap(),
            stream.clone(),
            group.clone(),
            "fresh".into(),
            settings,
        )
        .unwrap();

        let mut reclaim = RedisDeliverySource::new(
            client.get_multiplexed_async_connection().await.unwrap(),
            client.get_multiplexed_async_connection().await.unwrap(),
            stream.clone(),
            group,
            "reclaim".into(),
            settings,
        )
        .unwrap();

        fresh
            .open(IndividualSourceRequirements::new())
            .await
            .unwrap();

        reclaim
            .open(IndividualSourceRequirements::new())
            .await
            .unwrap();

        let publisher = RedisPublisher::new(
            client.get_multiplexed_async_connection().await.unwrap(),
            stream.clone(),
            Duration::from_secs(2),
        )
        .unwrap();

        (publisher, fresh, reclaim, stream, commands)
    });

    c.bench_function("redis_append_read_ack_256b", |b| {
        b.iter(|| {
            runtime.block_on(async {
                publisher.append(&envelope()).await.unwrap();
                let (_, settlement) = fresh.receive().await.unwrap().unwrap().into_parts();
                settlement.ack().await.unwrap();
            })
        })
    });

    c.bench_function("redis_pending_reclaim_ack_256b", |b| {
        b.iter(|| {
            runtime.block_on(async {
                publisher.append(&envelope()).await.unwrap();
                drop(fresh.receive().await.unwrap().unwrap());
                tokio::time::sleep(Duration::from_millis(2)).await;
                let (_, settlement) = reclaim.receive().await.unwrap().unwrap().into_parts();
                settlement.ack().await.unwrap();
            })
        })
    });

    c.bench_function("redis_cancel_blocking_read", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let result = tokio::time::timeout(Duration::from_millis(1), fresh.receive()).await;
                assert!(result.is_err());
            })
        })
    });

    runtime.block_on(async {
        let _: i64 = redis::cmd("DEL")
            .arg(&stream)
            .query_async(&mut commands)
            .await
            .unwrap();
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
