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

mod typed_consumer {
    use super::{SourceSettings, connection, pending, settings};
    use futures_util::FutureExt;
    use redis::aio::MultiplexedConnection;
    use redis::streams::StreamPendingCountReply;
    use serde::{Deserialize, Serialize};
    use sisa_messaging::{
        Envelope, ErrorClassifier, FailureKind, JsonSerializer, Message, MessageId, Serializer,
    };
    use sisa_messaging_consumer::{
        Consumer, ConsumerExit, ConsumerHandler, ConsumerSettings, SettlementMode,
    };
    use sisa_messaging_inbox::{
        InboxClaimOutcome, InboxFailure, InboxFailureOutcome, InboxId, InboxReceipt, InboxRecord,
        InboxScope, InboxStore, InboxUnitOfWork,
    };
    use sisa_messaging_redis::{RedisDeliverySource, RedisMapper, RedisPublisher};
    use std::{
        collections::HashSet,
        num::NonZeroU32,
        panic::AssertUnwindSafe,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::{Notify, Semaphore};
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Deserialize, Serialize)]
    struct TestMessage {
        value: u32,
    }

    impl Message for TestMessage {
        const TYPE: &'static str = "redis.typed.test";
        const VERSION: u32 = 1;
    }

    #[derive(Debug)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("test retry")
        }
    }

    impl std::error::Error for TestError {}

    impl ErrorClassifier for TestError {
        fn classify(&self) -> FailureKind {
            FailureKind::Transient
        }
    }

    struct State {
        completed: Mutex<HashSet<MessageId>>,

        commit_started: Notify,

        commit_gate: Semaphore,

        failure_recorded: Notify,
    }

    impl Default for State {
        fn default() -> Self {
            Self {
                completed: Mutex::default(),
                commit_started: Notify::new(),
                commit_gate: Semaphore::new(0),
                failure_recorded: Notify::new(),
            }
        }
    }

    #[derive(Clone, Default)]
    struct TestInbox(Arc<State>);

    #[derive(Default)]
    struct TestTransaction {
        claimed: Option<MessageId>,

        completed: Option<MessageId>,
    }

    struct TestReceipt(InboxId);

    impl InboxReceipt for TestReceipt {
        fn id(&self) -> InboxId {
            self.0
        }
        fn recorded_failures(&self) -> u32 {
            0
        }
    }

    impl InboxUnitOfWork for TestInbox {
        type Transaction = TestTransaction;
        type Error = TestError;

        async fn begin(&self) -> Result<TestTransaction, TestError> {
            Ok(TestTransaction::default())
        }

        async fn commit(&self, tx: TestTransaction) -> Result<(), TestError> {
            if let Some(id) = tx.completed {
                self.0.commit_started.notify_one();
                let permit = self.0.commit_gate.acquire().await.map_err(|_| TestError)?;
                permit.forget();
                self.0.completed.lock().unwrap().insert(id);
            }

            Ok(())
        }

        async fn rollback(&self, _tx: TestTransaction) -> Result<(), TestError> {
            Ok(())
        }
    }

    impl InboxStore<TestTransaction> for TestInbox {
        type Error = TestError;
        type Receipt = TestReceipt;

        fn max_attempts(&self) -> NonZeroU32 {
            NonZeroU32::new(3).unwrap()
        }

        async fn claim(
            &self,
            tx: &mut TestTransaction,
            record: &InboxRecord,
        ) -> Result<InboxClaimOutcome<TestReceipt>, TestError> {
            if self
                .0
                .completed
                .lock()
                .unwrap()
                .contains(&record.message_id)
            {
                return Ok(InboxClaimOutcome::CompletedDuplicate);
            }

            tx.claimed = Some(record.message_id);

            Ok(InboxClaimOutcome::Claimed(TestReceipt(InboxId::from_uuid(
                record.message_id.into_uuid(),
            ))))
        }

        async fn complete(
            &self,
            tx: &mut TestTransaction,
            receipt: TestReceipt,
        ) -> Result<(), TestError> {
            assert_eq!(
                tx.claimed.map(MessageId::into_uuid),
                Some(receipt.id().into_uuid())
            );

            tx.completed = tx.claimed;

            Ok(())
        }

        async fn fail(
            &self,
            _record: &InboxRecord,
            failure: InboxFailure,
        ) -> Result<InboxFailureOutcome, TestError> {
            assert_eq!(failure.kind, FailureKind::Transient);
            self.0.failure_recorded.notify_one();

            Ok(InboxFailureOutcome::Retry { attempts: 1 })
        }
    }

    struct FailOnce(Arc<AtomicBool>);

    impl ConsumerHandler<TestMessage, TestTransaction> for FailOnce {
        type Error = TestError;

        async fn handle(
            &self,
            _tx: &mut TestTransaction,
            _envelope: &Envelope<TestMessage>,
        ) -> Result<(), TestError> {
            if self.0.swap(false, Ordering::SeqCst) {
                Err(TestError)
            } else {
                Ok(())
            }
        }
    }

    async fn source(
        client: &redis::Client,
        stream: &str,
        group: &str,
        name: &str,
        settings: SourceSettings,
    ) -> RedisDeliverySource {
        RedisDeliverySource::new(
            connection(client).await,
            connection(client).await,
            stream.to_owned(),
            group.to_owned(),
            name.to_owned(),
            settings,
        )
        .unwrap()
    }

    async fn run(
        source: RedisDeliverySource,
        inbox: TestInbox,
        fail: Arc<AtomicBool>,
        cancel: CancellationToken,
    ) -> Result<ConsumerExit, sisa_messaging_consumer::ConsumerError> {
        let mut settings = ConsumerSettings::default();
        settings.mode = SettlementMode::PendingRecovery;
        settings.max_in_flight = std::num::NonZeroUsize::new(1).unwrap();
        settings.drain_timeout = Duration::from_millis(200);

        let consumer = Consumer::<TestMessage, _>::new(
            source,
            RedisMapper,
            JsonSerializer,
            inbox,
            InboxScope::new("redis-typed-integration").unwrap(),
            FailOnce(fail),
            settings,
        )
        .unwrap();

        consumer.run(cancel).await
    }

    async fn create_stream_and_group(
        commands: &mut MultiplexedConnection,
        stream: &str,
        group: &str,
    ) {
        let _: String = redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("setup")
            .arg("1")
            .query_async(commands)
            .await
            .unwrap();

        let _: String = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .query_async(commands)
            .await
            .unwrap();
    }

    async fn pending_owner(
        commands: &mut MultiplexedConnection,
        stream: &str,
        group: &str,
    ) -> String {
        let reply: StreamPendingCountReply = redis::cmd("XPENDING")
            .arg(stream)
            .arg(group)
            .arg("-")
            .arg("+")
            .arg(1)
            .query_async(commands)
            .await
            .unwrap();

        assert_eq!(reply.ids.len(), 1);

        reply.ids[0].consumer.clone()
    }

    #[tokio::test]
    #[ignore = "requires Redis, Valkey, or Dragonfly at SISA_REDIS_URL"]
    async fn typed_pending_recovery_commits_before_ack() {
        let url = std::env::var("SISA_REDIS_URL").expect("SISA_REDIS_URL is required");
        let client = redis::Client::open(url).unwrap();
        let mut commands = connection(&client).await;
        let suffix = MessageId::new().to_string().replace('-', "");
        let stream = format!("sisa-typed-{suffix}");
        let group = format!("group-{suffix}");
        let first_cancel = CancellationToken::new();
        let second_cancel = CancellationToken::new();
        let mut first_task = None;
        let mut second_task = None;

        let scenario = AssertUnwindSafe(async {
            create_stream_and_group(&mut commands, &stream, &group).await;

            let publisher = RedisPublisher::new(
                connection(&client).await,
                stream.clone(),
                Duration::from_secs(2),
            )
            .unwrap();

            let envelope = Envelope::new(
                MessageId::new(),
                TestMessage { value: 7 },
                Default::default(),
            )
            .unwrap();

            publisher
                .append(&JsonSerializer.serialize(&envelope).unwrap())
                .await
                .unwrap();

            let inbox = TestInbox::default();
            let fail = Arc::new(AtomicBool::new(true));
            let mut first_settings = settings();
            first_settings.min_idle = Duration::from_secs(3600);

            first_task = Some(tokio::spawn(run(
                source(&client, &stream, &group, "first", first_settings).await,
                inbox.clone(),
                fail.clone(),
                first_cancel.clone(),
            )));

            tokio::time::timeout(Duration::from_secs(3), inbox.0.failure_recorded.notified())
                .await
                .unwrap();

            assert_eq!(pending(&mut commands, &stream, &group).await, 1);
            assert_eq!(pending_owner(&mut commands, &stream, &group).await, "first");
            first_cancel.cancel();

            let first_exit =
                tokio::time::timeout(Duration::from_secs(3), first_task.as_mut().unwrap()).await;

            if first_exit.is_ok() {
                first_task = None;
            }

            assert!(matches!(first_exit, Ok(Ok(Ok(ConsumerExit::Cancelled)))));

            tokio::time::sleep(Duration::from_millis(75)).await;

            second_task = Some(tokio::spawn(run(
                source(&client, &stream, &group, "second", settings()).await,
                inbox.clone(),
                fail,
                second_cancel.clone(),
            )));

            tokio::time::timeout(Duration::from_secs(3), inbox.0.commit_started.notified())
                .await
                .unwrap();

            assert_eq!(pending(&mut commands, &stream, &group).await, 1);

            assert_eq!(
                pending_owner(&mut commands, &stream, &group).await,
                "second"
            );

            assert!(
                !inbox
                    .0
                    .completed
                    .lock()
                    .unwrap()
                    .contains(&envelope.message_id())
            );

            inbox.0.commit_gate.add_permits(1);

            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if pending(&mut commands, &stream, &group).await == 0 {
                        break;
                    }

                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();

            assert!(
                inbox
                    .0
                    .completed
                    .lock()
                    .unwrap()
                    .contains(&envelope.message_id())
            );

            second_cancel.cancel();

            let second_exit =
                tokio::time::timeout(Duration::from_secs(3), second_task.as_mut().unwrap()).await;

            if second_exit.is_ok() {
                second_task = None;
            }

            assert!(matches!(second_exit, Ok(Ok(Ok(ConsumerExit::Cancelled)))));
        })
        .catch_unwind()
        .await;

        first_cancel.cancel();
        second_cancel.cancel();

        for task in [first_task, second_task].into_iter().flatten() {
            let mut task = task;

            if tokio::time::timeout(Duration::from_secs(3), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = tokio::time::timeout(Duration::from_secs(3), task).await;
            }
        }

        let deleted = tokio::time::timeout(
            Duration::from_secs(3),
            redis::cmd("DEL")
                .arg(&stream)
                .query_async::<i64>(&mut commands),
        )
        .await;

        if let Err(panic) = scenario {
            std::panic::resume_unwind(panic);
        }

        deleted.unwrap().unwrap();
    }
}

#[tokio::test]
#[ignore = "requires Garnet 2.1.8 at SISA_GARNET_URL"]
async fn garnet_2_1_8_rejects_xadd() {
    // Provider CI runs all ignored tests; this Garnet probe runs only when configured.
    let Ok(url) = std::env::var("SISA_GARNET_URL") else {
        return;
    };

    let client = redis::Client::open(url).unwrap();
    let mut commands = connection(&client).await;
    let stream = format!("sisa-garnet-negative-{}", MessageId::new());

    let error = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("setup")
        .arg("1")
        .query_async::<String>(&mut commands)
        .await
        .unwrap_err();

    assert_eq!(error.kind(), redis::ErrorKind::ResponseError);

    assert!(
        error
            .detail()
            .is_some_and(|detail| detail.to_ascii_lowercase().starts_with("unknown command"))
    );
}
