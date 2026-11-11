# sisa-messaging-rs

[![CI](https://github.com/sisaio/sisa-messaging-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sisaio/sisa-messaging-rs/actions/workflows/ci.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/sisaio/sisa-messaging-rs/badge)](https://scorecard.dev/viewer/?uri=github.com/sisaio/sisa-messaging-rs)
[![crates.io](https://img.shields.io/crates/v/sisa-messaging.svg)](https://crates.io/crates/sisa-messaging)
[![docs.rs](https://img.shields.io/docsrs/sisa-messaging)](https://docs.rs/sisa-messaging)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue)](https://github.com/sisaio/sisa-messaging-rs/blob/main/Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/sisaio/sisa-messaging-rs/blob/main/LICENSE)

Messaging for Rust services: publish and consume over the transport you pick, with an optional
transactional outbox and inbox in your own database.

## What you pick

| Part | 0.1.0 | Later |
| --- | --- | --- |
| Transport | NATS JetStream | Kafka, Redpanda, WarpStream, RabbitMQ, LavinMQ, Apache Iggy |
| Persistence | PostgreSQL on sqlx | MySQL, SQL Server |
| Codec | JSON | MessagePack, Avro |

The outbox and the inbox are optional: use plain messaging, add the outbox so a message is
published if and only if its database transaction commits, and add the inbox so a redelivered
message isn't handled twice. Messages travel as CloudEvents 1.0 in each transport's binary mode,
so services in other languages read and write them too.

## Quick start

```toml
[dependencies]
sisa-messaging = { version = "0.1", features = ["nats", "postgres"] }
serde = { version = "1", features = ["derive"] }
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "postgres"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

A message, a handler that bills it in the delivery's transaction, and a bus that publishes
through the outbox. The `billing` endpoint's inbox records each message, so a redelivery doesn't
bill twice. This is the core of [`examples/orders`](https://github.com/sisaio/sisa-messaging-rs/blob/main/examples/orders/src/main.rs):

```rust,ignore
use serde::{Deserialize, Serialize};
use sisa_messaging::{
    Bus, HandlerContext, HandlerError, Message, NatsSettings, NatsTransport, PartitionSettings,
    PostgresInbox, PostgresOutbox, PostgresPartitions, Publisher,
};
use sqlx::{PgPool, PgTransaction};

#[derive(Debug, Serialize, Deserialize)]
struct OrderPlaced {
    id: i64,
    customer: String,
    total_cents: i64,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

async fn bill_order(
    order: OrderPlaced,
    context: &mut HandlerContext<PgTransaction<'static>>,
) -> Result<(), HandlerError> {
    sqlx::query("INSERT INTO invoices (order_id, total_cents) VALUES ($1, $2)")
        .bind(order.id)
        .bind(order.total_cents)
        .execute(context.transaction())
        .await?;
    Ok(())
}

let transport = NatsTransport::connect(NatsSettings::default()).await?;
let bus = Bus::builder("/orders", transport)
    .outbox(PostgresOutbox::new(pool.clone()))
    .partitions(PostgresPartitions::outbox(pool.clone()), PartitionSettings::default())
    .partitions(PostgresPartitions::inbox(pool.clone()), PartitionSettings::default())
    .endpoint("billing", |endpoint| {
        endpoint
            .inbox(PostgresInbox::new(pool.clone()))
            .handle::<OrderPlaced>(bill_order)
    })
    .start()
    .await?;

// The order and its message commit together, or neither does.
let mut tx = pool.begin().await?;
let id: i64 = sqlx::query_scalar(
    "INSERT INTO orders (customer, total_cents) VALUES ($1, $2) RETURNING id",
)
.bind("Ada")
.bind(4_200)
.fetch_one(&mut *tx)
.await?;
bus.outbox(&mut tx)
    .publish(&OrderPlaced { id, customer: "Ada".into(), total_cents: 4_200 })
    .await?;
tx.commit().await?;
```

### Run the example

It needs [sqlx-cli](https://crates.io/crates/sqlx-cli) for the migrations
(`cargo install sqlx-cli --no-default-features --features postgres,rustls`), and Docker for a
local NATS and PostgreSQL:

```sh
docker run -d --name orders-nats -p 4222:4222 nats:2.15-alpine -js
docker run -d --name orders-postgres -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=orders postgres:18-alpine
until docker exec orders-postgres pg_isready -h localhost -U postgres -d orders; do sleep 1; done

# The library's tables in a `messaging` schema, the example's own in `public`.
docker exec orders-postgres psql -U postgres -d orders -c 'CREATE SCHEMA messaging'
DATABASE_URL='postgres://postgres:postgres@localhost:5432/orders?options=-c%20search_path%3Dmessaging' \
    sqlx migrate run --source migrations/postgresql
DATABASE_URL='postgres://postgres:postgres@localhost:5432/orders' \
    sqlx migrate run --source examples/orders/migrations

cargo run -p orders
```

It places an order, and billing invoices it once the dispatcher has published the message:

```text
orders: placed order 1
billing: invoiced order 1 for Ada (4200 cents), message 01a1a97f-3ef5-7079-80e3-bf7a852d2639
```

`NATS_URL` and `DATABASE_URL` point it elsewhere; the database URL's `search_path` must name the
`messaging` schema and the application's.

## More

- [Docs](https://github.com/sisaio/sisa-messaging-rs/blob/main/docs/00-overview.md): an overview,
  then architecture, flows and research, in reading order.
- [Roadmap](https://github.com/sisaio/sisa-messaging-rs/blob/main/ROADMAP.md) and
  [changelog](https://github.com/sisaio/sisa-messaging-rs/blob/main/CHANGELOG.md).
- [Contributing](https://github.com/sisaio/sisa-messaging-rs/blob/main/CONTRIBUTING.md): building,
  testing and conventions.
- [Security](https://github.com/sisaio/sisa-messaging-rs/blob/main/SECURITY.md): reporting a
  vulnerability.

Rust 1.95 or newer. MIT licensed.
