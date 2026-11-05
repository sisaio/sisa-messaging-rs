# Roadmap

What shipped is in the [changelog](CHANGELOG.md). The issues and their milestones show the work in
progress. Each milestone is a release.

| Milestone | Goal |
| --- | --- |
| 0.1.0 | Done. NATS JetStream, PostgreSQL and JSON: publish, consume on durable endpoints, an optional outbox and inbox, retries and an error destination. Simple, but enough to run a service on |
| 0.2.0 | Production readiness: OpenTelemetry metrics and trace propagation, dispatch woken by `LISTEN`/`NOTIFY` instead of polling alone, sending a command to one endpoint, graceful shutdown, benchmarks and a chaos test |
| 0.3.0 | The Kafka family: Kafka, Redpanda and WarpStream, with retry topics for per-message delays |
| 0.4.0 | AMQP and Iggy: RabbitMQ and LavinMQ over AMQP 0-9-1, and Apache Iggy |
| 0.5.0 | More databases: MySQL 8 on sqlx, and SQL Server on tiberius |
| 0.6.0 | More codecs: MessagePack, and Avro with a schema registry |

## Later

Ideas that wait until users ask for them:

- Ordering by a partition key, through the outbox and on every transport.
- Scheduled messages, through the outbox's due time.
- Request and response.
- Sagas, on top of the bus.
