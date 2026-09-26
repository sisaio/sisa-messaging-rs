//! Run ignored tests against a real server with `NATS_URL` set.

#[path = "jetstream/consumer.rs"]
mod consumer;
#[path = "jetstream/consumer_lifecycle.rs"]
mod consumer_lifecycle;
#[path = "jetstream/inbox.rs"]
mod inbox;

use std::{
    future::Future,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use async_nats::jetstream::{self, consumer::pull, stream};
use sisa_messaging::{
    ContentType, Delivery, IndividualDeliverySource, IndividualSettlement,
    IndividualSourceOpenError, IndividualSourceRequirements, MessageId, MessageType, Metadata,
    Publisher, SerializedEnvelope,
};
use sisa_messaging_nats::{
    NatsDeliverySource, NatsError, NatsPublisher, NatsPublisherSettings, Subject,
    TypeSubjectResolver,
};

fn settings() -> NatsPublisherSettings {
    NatsPublisherSettings {
        publish_timeout: Duration::from_secs(3),
    }
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn capability_fatal_error_and_clean_close() {
    let url = std::env::var("NATS_URL").expect("NATS_URL is required for this ignored test");
    let context = jetstream::new(async_nats::connect(url).await.unwrap());
    let suffix = MessageId::new().to_string().replace('-', "");

    let stream = context
        .create_stream(stream::Config {
            name: format!("TEST{suffix}"),
            subjects: vec![format!("test_{suffix}.>")],
            ..Default::default()
        })
        .await
        .unwrap();

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("individual".into()),
            ack_wait: Duration::from_millis(500),
            ..Default::default()
        })
        .await
        .unwrap();

    let mut source = NatsDeliverySource::new(consumer);
    let requirement = IndividualSourceRequirements::new().requiring_max_deliver();

    assert!(matches!(
        source.open(requirement).await,
        Err(IndividualSourceOpenError::Unsupported(_))
    ));

    source
        .open(IndividualSourceRequirements::new())
        .await
        .unwrap();

    stream.delete_consumer("individual").await.unwrap();
    assert_eq!(source.receive().await.err(), Some(NatsError::Source));
    source.close();
    assert!(source.receive().await.unwrap().is_none());
}

fn envelope(message_id: MessageId, payload: Vec<u8>) -> SerializedEnvelope {
    SerializedEnvelope {
        message_id,
        message_type: MessageType::new("event").unwrap(),
        message_version: 1,
        content_type: ContentType::new("application/octet-stream").unwrap(),
        payload,
        metadata: Metadata::default(),
        ordering_key: None,
    }
}

fn withholding_proxy(server_address: String) -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let withhold = Arc::new(AtomicBool::new(false));
    let signal = Arc::clone(&withhold);

    std::thread::spawn(move || {
        let (downstream, _) = listener.accept().unwrap();
        let upstream = TcpStream::connect(server_address).unwrap();
        let mut client_reader = downstream.try_clone().unwrap();
        let mut server_writer = upstream.try_clone().unwrap();

        std::thread::spawn(move || {
            let _ = std::io::copy(&mut client_reader, &mut server_writer);
        });

        let mut server_reader = upstream;
        let mut client_writer = downstream;
        let mut buffer = [0u8; 8192];

        while let Ok(count) = server_reader.read(&mut buffer) {
            if count == 0 {
                break;
            }

            if !signal.load(Ordering::SeqCst) && client_writer.write_all(&buffer[..count]).is_err()
            {
                break;
            }
        }
    });

    (format!("nats://{address}"), withhold)
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn heartbeat_requires_broker_confirmation() {
    let server = std::env::var("NATS_URL").expect("NATS_URL is required for this ignored test");

    let address = server
        .strip_prefix("nats://")
        .unwrap()
        .rsplit('@')
        .next()
        .unwrap()
        .split('/')
        .next()
        .unwrap()
        .to_owned();

    let (url, withhold) = withholding_proxy(address);
    let context = jetstream::new(async_nats::connect(url).await.unwrap());
    let suffix = MessageId::new().to_string().replace('-', "");
    let prefix = format!("test_{suffix}");

    let stream = context
        .create_stream(stream::Config {
            name: format!("TEST{suffix}"),
            subjects: vec![format!("{prefix}.>")],
            ..Default::default()
        })
        .await
        .unwrap();

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("individual".into()),
            ack_wait: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .unwrap();

    let publisher = NatsPublisher::new(
        context,
        TypeSubjectResolver::new(Subject::new(prefix).unwrap()),
        settings(),
    )
    .unwrap();

    let mut source = NatsDeliverySource::new(consumer);

    source
        .open(IndividualSourceRequirements::new().requiring_heartbeat())
        .await
        .unwrap();

    publisher
        .publish(&envelope(MessageId::new(), b"heartbeat".to_vec()))
        .await
        .unwrap();

    let mut settlement = source.receive().await.unwrap().unwrap().into_parts().1;
    withhold.store(true, Ordering::SeqCst);
    let started = Instant::now();

    assert!(
        tokio::time::timeout(Duration::from_millis(150), settlement.heartbeat())
            .await
            .is_err()
    );

    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn idle_receive_outlives_source_initialization_timeout() {
    let url = std::env::var("NATS_URL").expect("NATS_URL is required for this ignored test");
    let context = jetstream::new(async_nats::connect(url).await.unwrap());
    let suffix = MessageId::new().to_string().replace('-', "");
    let prefix = format!("test_{suffix}");

    let stream = context
        .create_stream(stream::Config {
            name: format!("TEST{suffix}"),
            subjects: vec![format!("{prefix}.>")],
            ..Default::default()
        })
        .await
        .unwrap();

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("individual".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let publisher = NatsPublisher::new(
        context,
        TypeSubjectResolver::new(Subject::new(prefix).unwrap()),
        settings(),
    )
    .unwrap();

    let source_timeout = Duration::from_millis(500);
    let mut source = NatsDeliverySource::new(consumer);

    tokio::time::timeout(
        source_timeout,
        source.open(IndividualSourceRequirements::new()),
    )
    .await
    .unwrap()
    .unwrap();

    let pending = tokio::time::timeout(
        source_timeout + Duration::from_millis(250),
        source.receive(),
    )
    .await;

    assert!(pending.is_err(), "an idle receive must remain pending");

    publisher
        .publish(&envelope(MessageId::new(), b"after-idle".to_vec()))
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(3), source.receive())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(delivery.into_parts().0.payload, b"after-idle");
}

#[tokio::test]
#[ignore = "requires a real JetStream server at NATS_URL"]
async fn acknowledged_publish_dedup_limit_and_settlement() {
    let url = std::env::var("NATS_URL").expect("NATS_URL is required for this ignored test");
    let client = async_nats::connect(url).await.unwrap();
    let context = jetstream::new(client);
    let suffix = MessageId::new().to_string().replace('-', "");
    let stream_name = format!("TEST{suffix}");
    let prefix = format!("test_{suffix}");
    let subject = format!("{prefix}.event.v1");

    let mut stream = context
        .create_stream(stream::Config {
            name: stream_name,
            subjects: vec![format!("{prefix}.>")],
            ..Default::default()
        })
        .await
        .unwrap();

    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("individual".into()),
            ack_wait: Duration::from_millis(500),
            max_deliver: 5,
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let publisher = NatsPublisher::new(
        context.clone(),
        TypeSubjectResolver::new(Subject::new(prefix).unwrap()),
        settings(),
    )
    .unwrap();

    let mut source = NatsDeliverySource::new(consumer);

    source
        .open(
            IndividualSourceRequirements::new()
                .requiring_ack_wait()
                .requiring_max_deliver()
                .requiring_delayed_retry()
                .requiring_terminal_discard()
                .requiring_heartbeat(),
        )
        .await
        .unwrap();

    // A cancelled readiness wait cannot consume a later delivery.
    let _ = tokio::time::timeout(Duration::from_millis(10), source.receive()).await;
    let message = envelope(MessageId::new(), b"one".to_vec());
    publisher.publish(&message).await.unwrap();
    publisher.publish(&message).await.unwrap();
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    let delivery = source.receive().await.unwrap().unwrap();
    let (wire, mut settlement) = delivery.into_parts();
    assert_eq!(wire.subject.as_str(), subject);
    assert_eq!(wire.payload, b"one");

    let heartbeat_started = Instant::now();

    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(220)).await;
        settlement.heartbeat().await.unwrap();
    }

    assert!(heartbeat_started.elapsed() > Duration::from_millis(500));

    assert!(
        tokio::time::timeout(Duration::from_millis(50), source.receive())
            .await
            .is_err(),
        "confirmed progress must keep the original delivery active"
    );

    settlement.ack().await.unwrap();

    publisher
        .publish(&envelope(MessageId::new(), b"delayed-nak".to_vec()))
        .await
        .unwrap();

    let settlement = source.receive().await.unwrap().unwrap().into_parts().1;
    settlement.nak(Duration::from_millis(100)).await.unwrap();
    let redelivery = source.receive().await.unwrap().unwrap();
    redelivery.into_parts().1.ack().await.unwrap();

    let too_large = envelope(MessageId::new(), vec![0; context.client().max_payload()]);
    let messages_before = stream.info().await.unwrap().state.messages;

    assert_eq!(
        publisher.publish(&too_large).await,
        Err(NatsError::PayloadTooLarge)
    );

    assert_eq!(stream.info().await.unwrap().state.messages, messages_before);

    publisher
        .publish(&envelope(MessageId::new(), b"poison".to_vec()))
        .await
        .unwrap();

    source
        .receive()
        .await
        .unwrap()
        .unwrap()
        .into_parts()
        .1
        .terminate()
        .await
        .unwrap();

    publisher
        .publish(&envelope(MessageId::new(), b"cancelled-ack".to_vec()))
        .await
        .unwrap();

    let settlement = source.receive().await.unwrap().unwrap().into_parts().1;
    let mut in_flight_ack = Box::pin(settlement.ack());

    let first_poll =
        std::future::poll_fn(|context| Poll::Ready(in_flight_ack.as_mut().poll(context))).await;

    assert!(matches!(first_poll, Poll::Pending));
    drop(in_flight_ack);

    publisher
        .publish(&envelope(MessageId::new(), b"after-cancel".to_vec()))
        .await
        .unwrap();

    let next = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let delivery = source.receive().await.unwrap().unwrap();
            let (wire, settlement) = delivery.into_parts();
            settlement.ack().await.unwrap();

            if wire.payload == b"after-cancel" {
                break wire;
            }

            assert_eq!(wire.payload, b"cancelled-ack");
        }
    })
    .await
    .unwrap();

    assert_eq!(next.payload, b"after-cancel");

    let mut tiny = settings();
    tiny.publish_timeout = Duration::from_nanos(1);

    let timed_publisher = NatsPublisher::new(
        context,
        TypeSubjectResolver::new(Subject::new(format!("test_{suffix}")).unwrap()),
        tiny,
    )
    .unwrap();

    let mut observed_timeout = false;

    for _ in 0..16 {
        match timed_publisher
            .publish(&envelope(MessageId::new(), b"ambiguous-publish".to_vec()))
            .await
        {
            Err(NatsError::Timeout) => {
                observed_timeout = true;
                break;
            }
            Ok(()) => {}
            Err(other) => panic!("unexpected publish result: {other:?}"),
        }
    }

    assert!(
        observed_timeout,
        "tiny deadline should expose an ambiguous publish"
    );
}
