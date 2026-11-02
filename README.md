# sisa-messaging-rs

[![CI](https://github.com/sisaio/sisa-messaging-rs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sisaio/sisa-messaging-rs/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.94-blue)](https://github.com/sisaio/sisa-messaging-rs/blob/main/Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/sisaio/sisa-messaging-rs/blob/main/LICENSE)

Messaging for Rust services: publish and consume over the transport you pick, with an optional
transactional outbox and inbox in your own database.

> [!NOTE]
> Work in progress: the first release, 0.1.0, is being built. The
> [architecture](https://github.com/sisaio/sisa-messaging-rs/blob/main/docs/01-architecture.md)
> describes what it will do.

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

The API below is the design for 0.1.0, not yet released:

```toml
[dependencies]
sisa-messaging = { version = "0.1", features = ["nats", "postgres"] }
```

```rust,ignore
use sisa_messaging::{Bus, Message, NatsTransport, PostgresOutbox};

#[derive(serde::Serialize, serde::Deserialize)]
struct OrderPlaced { id: u64 }

impl Message for OrderPlaced {
    const TYPE: &'static str = "orders.order_placed.v1";
}

let bus = Bus::builder("/orders", NatsTransport::connect("nats://127.0.0.1:4222", &nats).await?)
    .outbox(PostgresOutbox::new(pool.clone(), &postgres).await?)
    .start()
    .await?;

let mut tx = pool.begin().await?;
sqlx::query("insert into orders (id) values ($1)").bind(42).execute(&mut *tx).await?;
bus.outbox(&mut tx).publish(&OrderPlaced { id: 42 }).await?;
tx.commit().await?;
```

## More

- [Docs](https://github.com/sisaio/sisa-messaging-rs/blob/main/docs/00-overview.md): an overview,
  then architecture, flows and research, in reading order.
- [Roadmap](https://github.com/sisaio/sisa-messaging-rs/blob/main/ROADMAP.md) and
  [changelog](https://github.com/sisaio/sisa-messaging-rs/blob/main/CHANGELOG.md).
- [Contributing](https://github.com/sisaio/sisa-messaging-rs/blob/main/CONTRIBUTING.md): building,
  testing and conventions.
- [Security](https://github.com/sisaio/sisa-messaging-rs/blob/main/SECURITY.md): reporting a
  vulnerability.

Rust 1.94 or newer. MIT licensed.
