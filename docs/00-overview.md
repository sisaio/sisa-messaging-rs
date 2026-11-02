# Overview

sisa-messaging lets a Rust service publish and consume messages over a transport it picks at
startup, with an optional transactional outbox and inbox in its own database. Add the
`sisa-messaging` crate with the features of the parts you use; none is on by default.

| Part | First milestone | Later |
| --- | --- | --- |
| Transport | NATS JetStream (`nats`) | Kafka, Redpanda, WarpStream; RabbitMQ, LavinMQ; Apache Iggy |
| Persistence, for the outbox and inbox | PostgreSQL on sqlx (`postgres`) | MySQL; SQL Server |
| Codec | JSON | MessagePack; Avro |

Each mode builds on the one before:

| Mode | What you get | What you need |
| --- | --- | --- |
| Plain messaging | Publish, and handlers on durable endpoints, at least once | A transport |
| With the outbox | A message is published if and only if the transaction that wrote it commits | A persistence |
| With the inbox | A redelivered message doesn't run its handler's transaction twice | A persistence |

The words used everywhere are in [Architecture](01-architecture.md#words-used-everywhere).

## Reading order

1. [Architecture](01-architecture.md): the crates, the contracts, how each mode behaves, the wire
   format and tables other languages share, and the rules the design depends on.
2. [Flows](02-flows.md): diagrams of publishing, dispatching, receiving and retrying.
3. [Research](03-research.md): what MassTransit, NServiceBus, Brighter and Wolverine do, and how
   each transport maps onto the contracts.

What comes next is in the [roadmap](../ROADMAP.md). To change the code, read
[CONTRIBUTING.md](../CONTRIBUTING.md) after the architecture.
