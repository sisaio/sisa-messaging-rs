# ADR-0006: Ack, retry after a delay, and reject; a transport builds what its broker lacks

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

Brokers settle messages in different ways. The clients we build on, as checked for 0.1.0:

| Part | Crate | Note |
| --- | --- | --- |
| NATS | `async-nats` 0.51 | JetStream publish acks, durable pull consumers, `Nats-Msg-Id` deduplication; MSRV 1.89 |
| PostgreSQL | `sqlx` 0.9 | MSRV 1.94 |
| Kafka, Redpanda, WarpStream | `rdkafka` or `rskafka` | `rdkafka` wraps librdkafka (C, complete); `rskafka` is pure Rust but has fewer features. To decide in that milestone |
| RabbitMQ, LavinMQ | `lapin` | AMQP 0-9-1 with publisher confirms |
| Redis, Valkey, Dragonfly | `redis` 1.8 | Streams with consumer groups; the same client and commands for all three. Garnet has no consumer groups yet, so it can't run the transport |
| Iggy | `iggy` 0.12 | Iggy has its own protocol; it isn't Kafka-compatible, so it is a transport of its own. MSRV 1.95, which sets ours |
| MySQL | `sqlx` | `FOR UPDATE SKIP LOCKED` needs MySQL 8.0 or newer |
| SQL Server | `tiberius` | sqlx dropped SQL Server in 0.7, so this crate won't share sqlx code; `READPAST` and `UPDLOCK` replace `SKIP LOCKED` |
| MessagePack | `rmp-serde` | Serde, like JSON |
| Avro | `apache-avro` | Needs a schema per message type, and usually a schema registry |

How each maps onto the contracts:

| Concept | NATS JetStream | Kafka family | RabbitMQ, LavinMQ | Iggy | Redis Streams |
| --- | --- | --- | --- | --- | --- |
| Publish | Publish to `{prefix}.{name}`, wait for the ack | Produce to a topic, wait for the ack | Publish to an exchange with publisher confirms | Send to a topic | `XADD` to `{prefix}:{name}` |
| Endpoint | Durable pull consumer | Consumer group | Queue bound to the exchange | Consumer group | Consumer group, a consumer per instance |
| Fan-out | One consumer per endpoint on the stream | One group per endpoint | One queue per endpoint | One group per endpoint | One group per endpoint |
| Ack | `ack` | Commit the offset | `basic.ack` | Store the offset | `XACK` |
| Retry later | `nak` with a delay | No per-message delay: a retry topic, or pause and seek | Reject to a delay queue, or a dead letter exchange with a TTL | Hold the partition and seek back to the message after the delay; later messages of its partition wait | Leave it pending and claimed by the instance until the delay has passed, then claim it again |
| Give up | `term`, then the error subject | The error topic | `basic.reject` without requeue, to a dead letter exchange | The error topic | `XACK`, after the error stream |
| Broker dedup | `Nats-Msg-Id` within the duplicate window | Idempotent producer, within one session only | None | None | None in Valkey and Dragonfly; Redis 8.6 added `XADD IDMP` |
| Order | Per subject, while one consumer holds the messages | Per partition | Per queue, without redelivery | Per partition | Per stream, while one consumer reads it |

Kafka has no per-message retry delay and acks by offset, so a failed message holds back the ones
behind it in its partition. The `Delivery` contract hides this: the Kafka transport will retry
through a retry topic. This is the main reason the core contract talks about `retry(delay)` and
`reject`, not about offsets.

## Decision

- `Delivery` has `ack`, `retry(delay)` and `reject`, and a transport builds whichever of them
  its broker lacks.
- NATS uses JetStream, not core NATS.
- A Redis retry leaves its entry pending, held by its instance until the delay has passed.
  Garnet isn't supported.
- An Iggy retry holds its partition and reads it again from the message's offset. Retry topics
  come with the Kafka family.

## Consequences

- Core NATS has no acknowledgement or redelivery, so it can't be at least once; JetStream can.
- Redis has no negative acknowledgement with a delay, and an entry acked and appended again
  would reach every endpoint's group, not only the one retrying. The pending list already gives
  the takeover after a crash and counts the deliveries.
- A log commits positions, not messages, so a later message can't be committed past a failed
  one without losing it on a crash. Holding the partition keeps the order and needs no extra
  topics, but the messages behind a retried one wait.
- Iggy has its own protocol; it isn't Kafka-compatible, so it is a transport of its own.

## Alternatives considered

- **Core NATS:** no acknowledgement or redelivery.
- **Acking a Redis entry and appending it again for a retry:** every endpoint's group would get
  the copy.
- **`XCLAIM ... IDLE` so the Redis server holds the delay:** Dragonfly doesn't honor it.
- **Garnet:** it has no consumer groups yet, so it can't run the transport.
- **A contract in terms of offsets:** it would fit logs and nothing else.
