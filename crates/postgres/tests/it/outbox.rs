//! Writing the outbox in the application's transaction.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sisa_messaging_core::{
    ContentType, Correlation, Envelope, Message, MessageId, MessageType, OutboxStore, Publisher,
    Source, StoreError, TraceContext,
};
use sisa_messaging_postgres::PostgresOutbox;
use sisa_messaging_runtime::{Bus, InMemoryTransport, PublishError};
use sisa_messaging_testing::Postgres;
use sqlx::PgPool;
use sqlx::types::{Json, Uuid};

use crate::migrate::{messaging_pool, migrated};

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    id: u64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

type Row = (
    Uuid,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Json<BTreeMap<String, String>>,
    Vec<u8>,
    String,
);

/// The outbox rows, oldest first, with their status.
async fn rows(pool: &PgPool) -> Vec<(Envelope, String)> {
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, type, source, subject, partition_key, data_content_type, data_schema,
                attributes, data, status
         FROM outbox_messages ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap();

    rows.into_iter()
        .map(|row| {
            let (id, message_type, source, subject, partition_key, content_type, schema) =
                (row.0, row.1, row.2, row.3, row.4, row.5, row.6);
            let mut attributes = row.7.0;
            attributes.insert("specversion".to_owned(), "1.0".to_owned());
            attributes.insert("id".to_owned(), id.to_string());
            attributes.insert("type".to_owned(), message_type);
            attributes.insert("source".to_owned(), source);
            attributes.insert("datacontenttype".to_owned(), content_type);
            for (name, value) in [
                ("subject", subject),
                ("partitionkey", partition_key),
                ("dataschema", schema),
            ] {
                if let Some(value) = value {
                    attributes.insert(name.to_owned(), value);
                }
            }
            let envelope = Envelope::from_attributes(attributes, Bytes::from(row.8)).unwrap();
            (envelope, row.9)
        })
        .collect()
}

async fn bus(
    transport: &InMemoryTransport,
    pool: &PgPool,
) -> Bus<InMemoryTransport, PostgresOutbox> {
    Bus::builder("/orders", transport.clone())
        .outbox(PostgresOutbox::new(pool.clone()))
        .start()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_committed_transaction_leaves_a_pending_row() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let transport = InMemoryTransport::new();
    let bus = bus(&transport, &pool).await;

    let mut tx = pool.begin().await.unwrap();
    bus.outbox(&mut tx)
        .publish(&OrderPlaced { id: 42 })
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let [(envelope, status)] = rows(&pool).await.try_into().unwrap();
    assert_eq!(status, "pending");
    assert_eq!(envelope.message_type.as_str(), OrderPlaced::TYPE);
    assert_eq!(envelope.source.as_str(), "/orders");
    assert_eq!(envelope.content_type.as_str(), "application/json");
    assert_eq!(&envelope.data[..], br#"{"id":42}"#);
    assert!(
        transport.published().is_empty(),
        "nothing goes out before the dispatch"
    );
}

#[tokio::test]
async fn a_rolled_back_transaction_leaves_nothing_and_publishes_nothing() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let transport = InMemoryTransport::new();
    let bus = bus(&transport, &pool).await;

    let mut tx = pool.begin().await.unwrap();
    bus.outbox(&mut tx)
        .publish(&OrderPlaced { id: 42 })
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    assert!(rows(&pool).await.is_empty());
    assert!(transport.published().is_empty());
}

#[tokio::test]
async fn a_row_holds_every_attribute_of_the_envelope() {
    let postgres = Postgres::start().await;
    let pool = migrated(&postgres).await;
    let mut envelope = Envelope::new(
        MessageType::new(OrderPlaced::TYPE).unwrap(),
        Source::new("/orders").unwrap(),
        ContentType::new("application/json").unwrap(),
        Bytes::from_static(br#"{"id":42}"#),
    );
    envelope.subject = Some("42".to_owned());
    envelope.data_schema = Some("https://example.com/order_placed.json".to_owned());
    envelope.partition_key = Some("customer-7".to_owned());
    envelope.correlation = Correlation::new(Some(MessageId::new()), Some(MessageId::new()));
    envelope.trace = Some(TraceContext::new(
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        Some("vendor=value".to_owned()),
    ));
    envelope.extensions.insert("tenant", "acme").unwrap();

    let mut connection = pool.acquire().await.unwrap();
    PostgresOutbox::new(pool.clone())
        .insert(&mut connection, std::slice::from_ref(&envelope))
        .await
        .unwrap();

    let [(row, _)] = rows(&pool).await.try_into().unwrap();
    assert_eq!(row, envelope);
}

#[tokio::test]
async fn a_missing_table_fails_the_publish() {
    let postgres = Postgres::start().await;
    let pool = messaging_pool(&postgres).await;
    let bus = bus(&InMemoryTransport::new(), &pool).await;

    let mut tx = pool.begin().await.unwrap();
    let result = bus.outbox(&mut tx).publish(&OrderPlaced { id: 42 }).await;

    assert!(matches!(
        result,
        Err(PublishError::Store(StoreError::Database { .. }))
    ));
}
