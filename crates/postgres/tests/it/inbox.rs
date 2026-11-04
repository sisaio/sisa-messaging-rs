//! Handling deliveries in a transaction with an inbox receipt.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sisa_messaging_core::{
    ContentType, Correlation, Envelope, InboxStore, Message, MessageId, MessageType, Publisher,
    Source, Timestamp, TransportPublisher,
};
use sisa_messaging_postgres::{PostgresInbox, PostgresOutbox};
use sisa_messaging_runtime::{
    Bus, Handler, HandlerContext, HandlerError, InMemoryTransport, RetryPolicy, StartError,
};
use sisa_messaging_testing::{Postgres, eventually};
use sqlx::types::Uuid;
use sqlx::{PgPool, PgTransaction};

use crate::migrate::migrated;

const A_DAY_IN_NANOS: i128 = 24 * 3600 * 1_000_000_000;

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    id: i64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

#[derive(Serialize, Deserialize)]
struct InvoiceSent {
    order: i64,
}

impl Message for InvoiceSent {
    const TYPE: &'static str = "billing.invoice_sent.v1";
}

/// A handler that writes a charge and publishes an `InvoiceSent` in the delivery's transaction,
/// then fails its first `failures` attempts.
#[derive(Clone, Default)]
struct Charges {
    attempts: Arc<Mutex<Vec<(i64, u32)>>>,
    failures: u32,
}

impl Charges {
    fn attempts(&self) -> Vec<(i64, u32)> {
        self.attempts.lock().unwrap().clone()
    }
}

impl Handler<OrderPlaced, PgTransaction<'static>> for Charges {
    async fn handle(
        &self,
        order: OrderPlaced,
        context: &mut HandlerContext<PgTransaction<'static>>,
    ) -> Result<(), HandlerError> {
        let attempt = context.attempt();
        self.attempts.lock().unwrap().push((order.id, attempt));
        sqlx::query("INSERT INTO charges (order_id) VALUES ($1)")
            .bind(order.id)
            .execute(context.transaction())
            .await?;
        context.publish(&InvoiceSent { order: order.id }).await?;
        if attempt <= self.failures {
            return Err(HandlerError::new(
                "payment_declined",
                "the card was declined",
            ));
        }
        Ok(())
    }
}

/// A migrated database with the application's own `charges` table.
async fn database(postgres: &Postgres) -> PgPool {
    let pool = migrated(postgres).await;
    sqlx::query(
        "CREATE TABLE charges (order_id bigint NOT NULL, CONSTRAINT pk_charges PRIMARY KEY (order_id))",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool
}

fn quick_retries() -> RetryPolicy {
    let mut policy = RetryPolicy::default();
    policy.first_delay = Duration::from_millis(10);
    policy
}

/// A bus with an outbox and a `billing` endpoint with an inbox, handled by `charges`.
async fn billing_bus(
    transport: &InMemoryTransport,
    pool: &PgPool,
    charges: &Charges,
) -> Bus<InMemoryTransport, PostgresOutbox> {
    Bus::builder("/billing", transport.clone())
        .outbox(PostgresOutbox::new(pool.clone()))
        .endpoint("billing", |endpoint| {
            endpoint
                .inbox(PostgresInbox::new(pool.clone()))
                .retry(quick_retries())
                .handle::<OrderPlaced>(charges.clone())
        })
        .start()
        .await
        .unwrap()
}

fn order(id: i64) -> Envelope {
    Envelope::new(
        MessageType::new(OrderPlaced::TYPE).unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from(format!(r#"{{"id":{id}}}"#)),
    )
}

async fn count(pool: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_redelivered_message_is_acked_without_running_the_handler_even_days_later() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;
    let transport = InMemoryTransport::new();
    let charges = Charges::default();
    let _bus = billing_bus(&transport, &pool, &charges).await;
    // Published five days ago: its receipt goes by that time, not by when it is written.
    let mut first = order(1);
    first.time = Timestamp::from_unix_nanos(Timestamp::now().unix_nanos() - 5 * A_DAY_IN_NANOS);

    transport.publish(&first).await.unwrap();
    transport.publish(&first).await.unwrap();
    // The endpoint handles one delivery at a time, so the redelivery is settled once this one is.
    transport.publish(&order(2)).await.unwrap();
    eventually("the next order's commit", async || {
        count(&pool, "inbox_receipts").await == 2
    })
    .await;

    assert_eq!(charges.attempts(), [(1, 1), (2, 1)]);
    assert_eq!(count(&pool, "charges").await, 2);
    assert!(transport.errors("billing").is_empty());
    assert!(transport.rejected("billing").is_empty());
    let (partition, message_time): (String, String) = sqlx::query_as(
        "SELECT tableoid::regclass::text, to_char(message_time AT TIME ZONE 'UTC', 'YYYY-MM-DD')
         FROM inbox_receipts WHERE message_id = $1",
    )
    .bind(first.id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(partition, "inbox_receipts_default");
    assert_eq!(message_time, first.time.to_string()[..10]);
}

#[tokio::test]
async fn a_handler_error_rolls_back_the_receipt_with_the_business_writes() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;
    let transport = InMemoryTransport::new();
    let charges = Charges {
        failures: 1,
        ..Charges::default()
    };
    let _bus = billing_bus(&transport, &pool, &charges).await;

    transport.publish(&order(1)).await.unwrap();
    eventually("the retry", async || charges.attempts().len() == 2).await;
    eventually("the commit", async || {
        count(&pool, "inbox_receipts").await == 1
    })
    .await;

    assert_eq!(charges.attempts(), [(1, 1), (1, 2)]);
    assert_eq!(count(&pool, "charges").await, 1);
    assert_eq!(count(&pool, "inbox_receipts").await, 1);
}

#[tokio::test]
async fn a_handlers_publishes_go_to_the_outbox_in_its_transaction() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;
    let transport = InMemoryTransport::new();
    let charges = Charges {
        failures: 1,
        ..Charges::default()
    };
    let _bus = billing_bus(&transport, &pool, &charges).await;

    transport.publish(&order(1)).await.unwrap();
    eventually("the commit", async || {
        count(&pool, "inbox_receipts").await == 1
    })
    .await;

    let types: Vec<String> = sqlx::query_scalar("SELECT type FROM outbox_messages")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        types,
        [InvoiceSent::TYPE],
        "the failed attempt's row rolled back"
    );
    assert!(
        transport
            .published()
            .iter()
            .all(|envelope| envelope.message_type.as_str() != InvoiceSent::TYPE),
        "nothing goes out before the dispatch",
    );
}

#[tokio::test]
async fn without_an_outbox_a_handlers_publishes_are_sent_after_the_commit() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;
    let transport = InMemoryTransport::new();
    let charges = Charges::default();
    let _bus = Bus::builder("/billing", transport.clone())
        .endpoint("billing", |endpoint| {
            endpoint
                .inbox(PostgresInbox::new(pool.clone()))
                .handle::<OrderPlaced>(charges.clone())
        })
        .start()
        .await
        .unwrap();

    transport.publish(&order(1)).await.unwrap();
    eventually("the invoice", async || {
        transport
            .published()
            .iter()
            .any(|envelope| envelope.message_type.as_str() == InvoiceSent::TYPE)
    })
    .await;

    assert_eq!(count(&pool, "inbox_receipts").await, 1);
    assert_eq!(count(&pool, "outbox_messages").await, 0);
}

#[tokio::test]
async fn an_inbox_added_before_the_outbox_fails_the_start() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;

    let started = Bus::builder("/billing", InMemoryTransport::new())
        .endpoint("billing", |endpoint| {
            endpoint
                .inbox(PostgresInbox::new(pool.clone()))
                .handle::<OrderPlaced>(Charges::default())
        })
        .outbox(PostgresOutbox::new(pool.clone()))
        .start()
        .await;

    assert!(matches!(
        started,
        Err(StartError::InboxBeforeOutbox(endpoint)) if endpoint == "billing"
    ));
}

type Receipt = (
    String,
    Uuid,
    String,
    String,
    Option<Uuid>,
    sqlx::types::Json<std::collections::BTreeMap<String, String>>,
    Option<Vec<u8>>,
);

async fn receipt(inbox: &PostgresInbox, pool: &PgPool, envelope: &Envelope) -> Receipt {
    let mut transaction = inbox.begin().await.unwrap();
    assert!(
        inbox
            .record(&mut transaction, "billing", envelope)
            .await
            .unwrap()
    );
    inbox.commit(transaction).await.unwrap();
    sqlx::query_as(
        "SELECT endpoint, message_id, type, source, correlation_id, attributes, data
         FROM inbox_receipts WHERE message_id = $1",
    )
    .bind(envelope.id.as_uuid())
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn a_receipt_keeps_the_attributes_but_not_the_data() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;
    let correlation_id = MessageId::new();
    let mut envelope = order(1);
    envelope.subject = Some("1".to_owned());
    envelope.correlation = Correlation::new(Some(correlation_id), None);

    let (endpoint, message_id, message_type, source, correlation, attributes, data) =
        receipt(&PostgresInbox::new(pool.clone()), &pool, &envelope).await;

    assert_eq!(endpoint, "billing");
    assert_eq!(message_id, *envelope.id.as_uuid());
    assert_eq!(message_type, OrderPlaced::TYPE);
    assert_eq!(source, "/orders");
    assert_eq!(correlation, Some(*correlation_id.as_uuid()));
    assert_eq!(attributes.0["subject"], "1");
    assert_eq!(attributes.0["time"], envelope.time.to_string());
    assert_eq!(data, None);
}

#[tokio::test]
async fn a_receipt_keeps_the_data_when_asked() {
    let postgres = Postgres::start().await;
    let pool = database(&postgres).await;
    let envelope = order(1);

    let inbox = PostgresInbox::new(pool.clone()).keep_data(true);
    let (.., data) = receipt(&inbox, &pool, &envelope).await;

    assert_eq!(data.as_deref(), Some(&envelope.data[..]));
}
