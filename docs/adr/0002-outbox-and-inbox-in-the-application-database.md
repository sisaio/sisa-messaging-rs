# ADR-0002: The outbox and the inbox live in the application's database

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

A service that changes its database and publishes a message needs both to happen or neither, and
a handler that a broker redelivers to must not do its database work twice. The .NET messaging
frameworks have solved this for years; this is what they do, and what this library takes from
each:

| Framework | Transports | Outbox | Inbox | What we take |
| --- | --- | --- | --- | --- |
| [MassTransit](https://masstransit.massient.com/concepts/outbox) | RabbitMQ, Azure Service Bus, Amazon SQS, ActiveMQ, Kafka and Event Hubs as "riders", SQL databases | **Bus outbox**: publishes outside a consumer go to `OutboxMessage` in the application's `DbContext` transaction; a delivery service sends them, in order, tracked in `OutboxState`. **Consumer outbox**: a consumer's publishes are held in the outbox until it completes | Part of the consumer outbox: `InboxState` per message id and consumer, kept for a duplicate detection window | The two outbox uses (application code and handlers), the per-consumer inbox key, ordered dispatch, `FOR UPDATE SKIP LOCKED` on PostgreSQL so several delivery services can run |
| [NServiceBus](https://docs.particular.net/nservicebus/outbox/) | RabbitMQ, Azure Service Bus, SQS, SQL Server, Learning | Outgoing messages are stored in the handler's transaction, then dispatched and marked sent before the incoming message is acked | The same outbox record deduplicates by incoming `MessageId`; records are kept for a retention period and purged | Ack only after the work and its outgoing messages are durable; retention must exceed the longest redelivery; dedup needs ids that stay stable across resends; side effects outside the transaction aren't covered |
| [Brighter](https://github.com/BrighterCommand/Docs/blob/master/contents/9/BrighterOutboxSupport.md) | RabbitMQ, Kafka, SQS/SNS, Azure Service Bus, Redis | `DepositPost` writes to the outbox in the caller's transaction; `ClearOutbox` sends after commit; a **sweeper** sends whatever is left (every 5 s by default, rows older than 5 s, batches of 100) | Opt-in per handler, with a choice to warn or throw on a duplicate | Write and dispatch as separate steps; a background sweeper as the safety net; a minimum age so the sweeper and an eager send don't race; inbox chosen per handler |
| [Wolverine](https://wolverinefx.net/) | RabbitMQ, Azure Service Bus, SQS, Kafka, Pulsar, MQTT, PostgreSQL and SQL Server queues | Durable sending: envelopes are stored with the transaction and sent after commit; a node that dies has its envelopes recovered by another | Durable receiving: incoming envelopes are stored before they are handled | The envelope as the unit that crosses every boundary; recovery of a dead node's work through leases instead of an owner node |

In Rust, crates on crates.io cover parts of this: several transactional outbox crates on sqlx
(such as `outbox-core`, `outbox-kit`, `outbox-postgres`, `hexeract-outbox`) and per-broker
clients. None covers a bus with endpoints, an optional inbox and optional outbox, several
transports and codecs, and a documented cross-language format together.

## Decision

- The outbox and the inbox are optional, and each is a table in the application's own database.
- An outbox row is written on the caller's connection, inside the caller's transaction, never on
  a connection of the library's own. A dispatcher publishes the row after the commit.
- The inbox records a receipt per endpoint and message id, in the handler's transaction.
- The outbox orders by its UUID version 7 id; it has no sequence column.

## Consequences

- Only the caller's transaction makes the row commit or roll back with the business data, so a
  message is published if and only if its transaction commits.
- Ids already sort by creation, so the table has one column fewer, and MySQL and SQL Server will
  need no identity column on a partitioned table. Rows from two instances in the same
  millisecond may swap, which the guarantees already allow.
- A dead dispatcher's rows come back through leases, so no instance owns the outbox.
- Side effects outside the transaction aren't covered: every mode is at least once.

## Alternatives considered

- **A connection of the library's own for the outbox write:** the row could commit when the
  business data rolls back, or the other way round.
- **A sequence column for the dispatch order:** one more column, and an identity column on a
  partitioned table in MySQL and SQL Server.
- **Storing incoming messages before handling them** (Wolverine's durable inbox): the broker
  already holds them until the ack, so recording the id is enough for deduplication. That holds
  only while the broker keeps them: a stream or topic with an age or size limit drops unacked
  messages too, so its retention must outlast the longest consumer outage.
