//! An orders service: it places an order and publishes `OrderPlaced` through the outbox in the
//! same transaction, and its `billing` endpoint consumes the message with the inbox and writes an
//! invoice in the delivery's transaction. The bus runs the outbox dispatcher and keeps the
//! partitions of both tables.
//!
//! It reads `NATS_URL` and `DATABASE_URL`, with defaults for a local NATS and PostgreSQL; the
//! README says how to start them and apply the migrations.

use std::env;
use std::error::Error;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sisa_eventing::{
    Bus, HandlerContext, HandlerError, Message, NatsSettings, NatsTransport, PartitionSettings,
    PostgresInbox, PostgresOutbox, PostgresPartitions, Publisher,
};
use sqlx::{PgPool, PgTransaction};

// The eventing tables are in the `eventing` schema and the example's own in `public`, so the
// connection looks in both.
const DATABASE_URL: &str = "postgres://postgres:postgres@localhost:5432/orders?options=-c%20search_path%3Deventing%2Cpublic";

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    id: i64,
    customer: String,
    total_cents: i64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

/// Bills an order. The invoice commits with the inbox receipt, so a redelivered `OrderPlaced`
/// doesn't bill it twice.
async fn bill_order(
    order: OrderPlaced,
    context: &mut HandlerContext<PgTransaction<'static>>,
) -> Result<(), HandlerError> {
    sqlx::query("INSERT INTO invoices (order_id, total_cents) VALUES ($1, $2)")
        .bind(order.id)
        .bind(order.total_cents)
        .execute(context.transaction())
        .await?;
    println!(
        "billing: invoiced order {} for {} ({} cents), message {}",
        order.id,
        order.customer,
        order.total_cents,
        context.envelope().id
    );
    Ok(())
}

/// Writes the order and its `OrderPlaced` in one transaction: both commit, or neither does.
async fn place_order(
    bus: &Bus<NatsTransport, PostgresOutbox>,
    pool: &PgPool,
    customer: &str,
    total_cents: i64,
) -> Result<i64, Box<dyn Error>> {
    let mut tx = pool.begin().await?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO orders (customer, total_cents) VALUES ($1, $2) RETURNING id",
    )
    .bind(customer)
    .bind(total_cents)
    .fetch_one(&mut *tx)
    .await?;
    bus.outbox(&mut tx)
        .publish(&OrderPlaced {
            id,
            customer: customer.into(),
            total_cents,
        })
        .await?;
    tx.commit().await?;
    println!("orders: placed order {id}");
    Ok(id)
}

/// Waits until billing has invoiced the order.
async fn invoiced(pool: &PgPool, order: i64) -> Result<(), sqlx::Error> {
    loop {
        let found: Option<i64> =
            sqlx::query_scalar("SELECT order_id FROM invoices WHERE order_id = $1")
                .bind(order)
                .fetch_optional(pool)
                .await?;
        if found.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| DATABASE_URL.to_owned());
    let pool = PgPool::connect(&database_url).await?;
    let mut nats = NatsSettings::default();
    if let Ok(url) = env::var("NATS_URL") {
        nats.url = url;
    }
    let transport = NatsTransport::connect(nats).await?;

    // The outbox comes before the endpoint with the inbox, so the handlers' publishes go to it.
    let bus = Bus::builder("/orders", transport)
        .outbox(PostgresOutbox::new(pool.clone()))
        .partitions(
            PostgresPartitions::outbox(pool.clone()),
            PartitionSettings::default(),
        )
        .partitions(
            PostgresPartitions::inbox(pool.clone()),
            PartitionSettings::default(),
        )
        .endpoint("billing", |endpoint| {
            endpoint
                .inbox(PostgresInbox::new(pool.clone()))
                .handle::<OrderPlaced>(bill_order)
        })
        .start()
        .await?;

    let order = place_order(&bus, &pool, "Ada", 4_200).await?;
    let billed = tokio::time::timeout(Duration::from_secs(10), invoiced(&pool, order)).await;
    bus.shutdown().await;
    match billed {
        Ok(result) => Ok(result?),
        Err(_) => Err(format!("order {order} wasn't invoiced within ten seconds").into()),
    }
}
