//! Dispatching the outbox: claims, marks, and the bus's dispatcher.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sisa_messaging_core::{
    ContentType, Envelope, Message, MessageId, MessageType, OutboxStore, Publisher, Source,
    TransportPublisher,
};
use sisa_messaging_nats::{NatsSettings, NatsTransport};
use sisa_messaging_postgres::PostgresOutbox;
use sisa_messaging_runtime::{
    Bus, DispatchSettings, Handler, HandlerContext, HandlerError, InMemoryTransport,
};
use sisa_messaging_testing::{Nats, Postgres, eventually};
use sqlx::PgPool;
use sqlx::types::Uuid;

use crate::migrate::migrated;

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    id: u64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

/// Records the id of each order it handles.
#[derive(Clone, Default)]
struct Billing {
    received: Arc<Mutex<Vec<MessageId>>>,
}

impl Handler<OrderPlaced> for Billing {
    async fn handle(
        &self,
        _order: OrderPlaced,
        context: &mut HandlerContext,
    ) -> Result<(), HandlerError> {
        self.received.lock().unwrap().push(context.envelope().id);
        Ok(())
    }
}

/// Quick rounds, and retries a few milliseconds apart, so the tests don't wait.
fn settings() -> DispatchSettings {
    let mut settings = DispatchSettings::default();
    settings.poll_interval = Duration::from_millis(20);
    settings.retry.first_delay = Duration::from_millis(10);
    settings.retry.max_delay = Duration::from_millis(10);
    settings
}

async fn dispatching_bus(
    transport: &InMemoryTransport,
    pool: &PgPool,
    settings: DispatchSettings,
) -> Bus<InMemoryTransport, PostgresOutbox> {
    Bus::builder("/orders", transport.clone())
        .outbox(PostgresOutbox::new(pool.clone()))
        .dispatch(settings)
        .start()
        .await
        .unwrap()
}

/// Commits `count` outbox rows through a bus that has no dispatcher running yet, as another
/// instance would, and returns their ids in order.
async fn commit_rows(pool: &PgPool, count: u64) -> Vec<MessageId> {
    let writer = dispatching_bus(&InMemoryTransport::new(), pool, settings()).await;
    writer.shutdown().await;
    let mut tx = pool.begin().await.unwrap();
    for id in 0..count {
        writer
            .outbox(&mut tx)
            .publish(&OrderPlaced { id })
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    statuses(pool).await.into_iter().map(|row| row.0).collect()
}

/// Each row's id, status and failed attempts, in id order.
async fn statuses(pool: &PgPool) -> Vec<(MessageId, String, i32)> {
    let rows: Vec<(Uuid, String, i32)> =
        sqlx::query_as("SELECT id, status, attempts FROM outbox_messages ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
    rows.into_iter()
        .map(|(id, status, attempts)| (MessageId::from(id), status, attempts))
        .collect()
}

async fn all_have_status(pool: &PgPool, status: &str) -> bool {
    statuses(pool).await.iter().all(|row| row.1 == status)
}

fn published_ids(transport: &InMemoryTransport) -> Vec<MessageId> {
    transport
        .published()
        .iter()
        .map(|envelope| envelope.id)
        .collect()
}

#[tokio::test]
async fn a_claim_takes_due_rows_in_id_order_and_holds_them_for_the_lease() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let ids = commit_rows(&pool, 5).await;
    let store = PostgresOutbox::new(pool.clone());

    let first = store.claim(3, Duration::from_secs(60)).await.unwrap();
    let second = store.claim(3, Duration::from_secs(60)).await.unwrap();
    let third = store.claim(3, Duration::from_secs(60)).await.unwrap();

    let claimed: Vec<MessageId> = first
        .iter()
        .chain(&second)
        .map(|record| record.envelope.id)
        .collect();
    assert_eq!(claimed, ids);
    assert!(third.is_empty(), "every row is leased");
    assert!(all_have_status(&pool, "pending").await);
}

#[tokio::test]
async fn concurrent_claims_never_take_the_same_row() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let ids = commit_rows(&pool, 200).await;
    let store = PostgresOutbox::new(pool.clone());

    let claims = (0..8).map(|_| {
        let store = store.clone();
        tokio::spawn(async move { store.claim(30, Duration::from_secs(60)).await.unwrap() })
    });
    let mut claimed = Vec::new();
    for claim in claims {
        claimed.extend(
            claim
                .await
                .unwrap()
                .into_iter()
                .map(|record| record.envelope.id),
        );
    }

    let unique: HashSet<_> = claimed.iter().collect();
    assert_eq!(unique.len(), claimed.len(), "a row was claimed twice");
    assert_eq!(unique.len(), ids.len());
}

#[tokio::test]
async fn the_dispatcher_publishes_committed_rows_in_id_order_and_marks_them_dispatched() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let transport = InMemoryTransport::new();
    let bus = dispatching_bus(&transport, &pool, settings()).await;

    let mut tx = pool.begin().await.unwrap();
    for id in 0..3 {
        bus.outbox(&mut tx)
            .publish(&OrderPlaced { id })
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    eventually("the rows are dispatched", async || {
        all_have_status(&pool, "dispatched").await
    })
    .await;
    let rows = statuses(&pool).await;
    let ids: Vec<MessageId> = rows.iter().map(|row| row.0).collect();
    assert_eq!(published_ids(&transport), ids);
    let undated: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox_messages WHERE dispatched_at IS NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(undated, 0);
}

#[tokio::test]
async fn two_dispatchers_never_publish_the_same_row() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let ids = commit_rows(&pool, 300).await;
    let transport = InMemoryTransport::new();
    let mut small_batches = settings();
    small_batches.batch_size = 10;

    let first = dispatching_bus(&transport, &pool, small_batches).await;
    let second = dispatching_bus(&transport, &pool, small_batches).await;
    eventually("every row is dispatched", async || {
        all_have_status(&pool, "dispatched").await
    })
    .await;
    first.shutdown().await;
    second.shutdown().await;

    let mut published = published_ids(&transport);
    published.sort();
    assert_eq!(published, ids, "each row is published exactly once");
}

#[tokio::test]
async fn a_row_published_before_a_crash_is_published_again_with_its_id_after_the_lease() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let [id] = commit_rows(&pool, 1).await.try_into().unwrap();
    let transport = InMemoryTransport::new();

    // A dispatcher that published the row and stopped before marking it.
    let store = PostgresOutbox::new(pool.clone());
    let [record] = store
        .claim(10, Duration::from_secs(1))
        .await
        .unwrap()
        .try_into()
        .unwrap();
    transport.publish(&record.envelope).await.unwrap();

    let _bus = dispatching_bus(&transport, &pool, settings()).await;
    eventually("the row is dispatched", async || {
        all_have_status(&pool, "dispatched").await
    })
    .await;

    assert_eq!(published_ids(&transport), [id, id]);
}

#[tokio::test]
async fn a_broker_outage_leaves_rows_pending_until_the_broker_is_back() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let ids = commit_rows(&pool, 3).await;
    let transport = InMemoryTransport::new();
    transport.fail_publishes(true);
    let mut patient = settings();
    patient.retry.attempts = u32::MAX;
    let _bus = dispatching_bus(&transport, &pool, patient).await;

    eventually("a publish failed", async || statuses(&pool).await[0].2 >= 2).await;
    assert!(all_have_status(&pool, "pending").await);
    transport.fail_publishes(false);

    eventually("the rows are dispatched", async || {
        all_have_status(&pool, "dispatched").await
    })
    .await;
    let mut published = published_ids(&transport);
    published.sort();
    assert_eq!(published, ids);
}

#[tokio::test]
async fn a_row_fails_after_its_last_attempt_and_goes_out_once_set_back_to_pending() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let [id] = commit_rows(&pool, 1).await.try_into().unwrap();
    let transport = InMemoryTransport::new();
    transport.fail_publishes(true);
    let mut three_attempts = settings();
    three_attempts.retry.attempts = 3;
    let _bus = dispatching_bus(&transport, &pool, three_attempts).await;

    eventually("the row failed", async || {
        all_have_status(&pool, "failed").await
    })
    .await;
    let error: Option<String> = sqlx::query_scalar("SELECT last_error FROM outbox_messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(statuses(&pool).await[0].2, 3);
    assert!(error.is_some_and(|error| error.contains("publishes are set to fail")));

    transport.fail_publishes(false);
    sqlx::query("UPDATE outbox_messages SET status = 'pending'")
        .execute(&pool)
        .await
        .unwrap();
    eventually("the row is dispatched", async || {
        all_have_status(&pool, "dispatched").await
    })
    .await;
    assert_eq!(published_ids(&transport), [id]);
}

#[tokio::test]
async fn shutdown_stops_the_dispatcher() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let transport = InMemoryTransport::new();
    let bus = dispatching_bus(&transport, &pool, settings()).await;

    bus.shutdown().await;
    commit_rows(&pool, 1).await;
    tokio::time::sleep(settings().poll_interval * 5).await;

    assert!(transport.published().is_empty());
    assert!(all_have_status(&pool, "pending").await);
}

#[tokio::test]
async fn a_committed_message_reaches_a_nats_endpoint_with_its_outbox_id() {
    let postgres = Postgres::start().await;
    let nats = Nats::start().await;
    let pool = migrated(&postgres).await;
    let mut nats_settings = NatsSettings::default();
    nats_settings.url = nats.url().to_owned();
    let billing = Billing::default();

    let bus = Bus::builder(
        "/orders",
        NatsTransport::connect(nats_settings).await.unwrap(),
    )
    .outbox(PostgresOutbox::new(pool.clone()))
    .dispatch(settings())
    .endpoint("billing", |endpoint| {
        endpoint.handle::<OrderPlaced>(billing.clone())
    })
    .start()
    .await
    .unwrap();
    let mut tx = pool.begin().await.unwrap();
    bus.outbox(&mut tx)
        .publish(&OrderPlaced { id: 42 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    eventually("billing handles the order", async || {
        !billing.received.lock().unwrap().is_empty()
    })
    .await;
    eventually("the row is dispatched", async || {
        all_have_status(&pool, "dispatched").await
    })
    .await;
    let [(id, _, _)] = statuses(&pool).await.try_into().unwrap();
    assert_eq!(*billing.received.lock().unwrap(), [id]);
    bus.shutdown().await;
}

#[tokio::test]
async fn a_claimed_record_holds_the_envelope_as_it_was_written() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let store = PostgresOutbox::new(pool.clone());
    let mut envelope = Envelope::new(
        MessageType::new(OrderPlaced::TYPE).unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from_static(br#"{"id":42}"#),
    );
    envelope.subject = Some("42".to_owned());
    envelope.partition_key = Some("customer-7".to_owned());
    envelope.extensions.insert("tenant", "acme").unwrap();
    let mut connection = pool.acquire().await.unwrap();
    store
        .insert(&mut connection, std::slice::from_ref(&envelope))
        .await
        .unwrap();

    let [record] = store
        .claim(10, Duration::from_secs(60))
        .await
        .unwrap()
        .try_into()
        .unwrap();
    store
        .mark_dispatched(std::slice::from_ref(&record))
        .await
        .unwrap();

    assert_eq!(record.envelope, envelope);
    assert_eq!(record.attempts, 0);
    assert!(all_have_status(&pool, "dispatched").await);
}

#[tokio::test]
async fn a_row_that_isnt_a_valid_envelope_is_marked_failed_and_the_others_still_go_out() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let unreadable: Uuid = sqlx::query_scalar(
        "INSERT INTO outbox_messages (id, type, source, data_content_type, attributes, data)
         VALUES (gen_random_uuid(), 'orders.order_placed.v1', '/orders', 'application/json',
                 '{\"time\": \"yesterday\"}', '\\x7b7d')
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut ids = commit_rows(&pool, 1).await;
    ids.retain(|id| *id.as_uuid() != unreadable);
    let store = PostgresOutbox::new(pool.clone());

    let claimed = store.claim(10, Duration::from_secs(60)).await.unwrap();

    let claimed: Vec<MessageId> = claimed.iter().map(|record| record.envelope.id).collect();
    assert_eq!(claimed, ids);
    let error: String =
        sqlx::query_scalar("SELECT last_error FROM outbox_messages WHERE status = 'failed'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(error.starts_with("not a valid envelope"), "{error}");
}
