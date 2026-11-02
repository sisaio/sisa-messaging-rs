# Research

What the .NET messaging frameworks do about transports, the outbox and the inbox, and what this
library takes from each. The [architecture](01-architecture.md) is the result.

## The frameworks

| Framework | Transports | Outbox | Inbox | What we take |
| --- | --- | --- | --- | --- |
| [MassTransit](https://masstransit.massient.com/concepts/outbox) | RabbitMQ, Azure Service Bus, Amazon SQS, ActiveMQ, Kafka and Event Hubs as "riders", SQL databases | **Bus outbox**: publishes outside a consumer go to `OutboxMessage` in the application's `DbContext` transaction; a delivery service sends them, in order, tracked in `OutboxState`. **Consumer outbox**: a consumer's publishes are held in the outbox until it completes | Part of the consumer outbox: `InboxState` per message id and consumer, kept for a duplicate detection window | The two outbox uses (application code and handlers), the per-consumer inbox key, ordered dispatch, `FOR UPDATE SKIP LOCKED` on PostgreSQL so several delivery services can run |
| [NServiceBus](https://docs.particular.net/nservicebus/outbox/) | RabbitMQ, Azure Service Bus, SQS, SQL Server, Learning | Outgoing messages are stored in the handler's transaction, then dispatched and marked sent before the incoming message is acked | The same outbox record deduplicates by incoming `MessageId`; records are kept for a retention period and purged | Ack only after the work and its outgoing messages are durable; retention must exceed the longest redelivery; dedup needs ids that stay stable across resends; side effects outside the transaction aren't covered |
| [Brighter](https://github.com/BrighterCommand/Docs/blob/master/contents/9/BrighterOutboxSupport.md) | RabbitMQ, Kafka, SQS/SNS, Azure Service Bus, Redis | `DepositPost` writes to the outbox in the caller's transaction; `ClearOutbox` sends after commit; a **sweeper** sends whatever is left (every 5 s by default, rows older than 5 s, batches of 100) | Opt-in per handler, with a choice to warn or throw on a duplicate | Write and dispatch as separate steps; a background sweeper as the safety net; a minimum age so the sweeper and an eager send don't race; inbox chosen per handler |
| [Wolverine](https://wolverinefx.net/) | RabbitMQ, Azure Service Bus, SQS, Kafka, Pulsar, MQTT, PostgreSQL and SQL Server queues | Durable sending: envelopes are stored with the transaction and sent after commit; a node that dies has its envelopes recovered by another | Durable receiving: incoming envelopes are stored before they are handled | The envelope as the unit that crosses every boundary; recovery of a dead node's work through leases instead of an owner node |

## What the Rust ecosystem has

Crates on crates.io cover parts of this: several transactional outbox crates on sqlx (such as
`outbox-core`, `outbox-kit`, `outbox-postgres`, `hexeract-outbox`) and per-broker clients. None
covers a bus with endpoints, an optional inbox and optional outbox, several transports and
codecs, and a documented cross-language format together. The clients we build on:

| Part | Crate | Note |
| --- | --- | --- |
| NATS | `async-nats` 0.51 | JetStream publish acks, durable pull consumers, `Nats-Msg-Id` deduplication; MSRV 1.89 |
| PostgreSQL | `sqlx` 0.9 | MSRV 1.94, which sets ours |
| Kafka, Redpanda, WarpStream | `rdkafka` or `rskafka` | `rdkafka` wraps librdkafka (C, complete); `rskafka` is pure Rust but has fewer features. To decide in that milestone |
| RabbitMQ, LavinMQ | `lapin` | AMQP 0-9-1 with publisher confirms |
| Iggy | `iggy` | Iggy has its own protocol; it isn't Kafka-compatible, so it is a transport of its own |
| MySQL | `sqlx` | `FOR UPDATE SKIP LOCKED` needs MySQL 8.0 or newer |
| SQL Server | `tiberius` | sqlx dropped SQL Server in 0.7, so this crate won't share sqlx code; `READPAST` and `UPDLOCK` replace `SKIP LOCKED` |
| MessagePack | `rmp-serde` | Serde, like JSON |
| Avro | `apache-avro` | Needs a schema per message type, and usually a schema registry |

## How each transport maps

| Concept | NATS JetStream | Kafka family | RabbitMQ, LavinMQ | Iggy |
| --- | --- | --- | --- | --- |
| Publish | Publish to `{prefix}.{name}`, wait for the ack | Produce to a topic, wait for the ack | Publish to an exchange with publisher confirms | Send to a topic |
| Endpoint | Durable pull consumer | Consumer group | Queue bound to the exchange | Consumer group |
| Fan-out | One consumer per endpoint on the stream | One group per endpoint | One queue per endpoint | One group per endpoint |
| Ack | `ack` | Commit the offset | `basic.ack` | Store the offset |
| Retry later | `nak` with a delay | No per-message delay: a retry topic, or pause and seek | Reject to a delay queue, or a dead letter exchange with a TTL | A retry topic |
| Give up | `term`, then the error subject | The error topic | `basic.reject` without requeue, to a dead letter exchange | The error topic |
| Broker dedup | `Nats-Msg-Id` within the duplicate window | Idempotent producer, within one session only | None | None |
| Order | Per subject, while one consumer holds the messages | Per partition | Per queue, without redelivery | Per partition |

Kafka has no per-message retry delay and acks by offset, so a failed message holds back the ones
behind it in its partition. The `Delivery` contract hides this: the Kafka transport will retry
through a retry topic. This is the main reason the core contract talks about `retry(delay)` and
`reject`, not about offsets.

## What we leave out at first

- **Sagas and state machines** (MassTransit, NServiceBus): useful, but a layer on top of the bus.
- **Request and response**: needs reply addresses on every transport; later, if users ask.
- **Scheduled messages**: the outbox's `available_at` makes it cheap later.
- **Storing incoming messages before handling them** (Wolverine's durable inbox): the broker
  already holds them until the ack, so recording the id is enough for deduplication. That holds
  only while the broker keeps them: a stream or topic with an age or size limit drops unacked
  messages too, so its retention must outlast the longest consumer outage.
