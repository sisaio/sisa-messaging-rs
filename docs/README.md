# sisa-messaging docs

sisa-messaging lets a Rust service publish and consume messages over a transport it picks at
startup, with an optional transactional outbox and inbox in its own database. Add the
`sisa-messaging` crate with the features of the parts you use; none is on by default.

| Part | 0.1.0 | Later |
| --- | --- | --- |
| Transport | NATS JetStream (`nats`); Redis Streams on Redis, Valkey or Dragonfly (`redis`); Apache Iggy (`iggy`, experimental) | Kafka, Redpanda, WarpStream; RabbitMQ, LavinMQ |
| Persistence, for the outbox and inbox | PostgreSQL on sqlx (`postgres`) | MySQL; SQL Server |
| Codec | JSON | MessagePack; Avro |

Each mode builds on the one before:

| Mode | What you get | What you need |
| --- | --- | --- |
| Plain messaging | Publish, and handlers on durable endpoints, at least once | A transport |
| With the outbox | A message is published if and only if the transaction that wrote it commits | A persistence |
| With the inbox | A redelivered message doesn't run its handler's transaction twice | A persistence |

## Layout

```text
docs/
├── README.md         this page: the index and the words used everywhere
├── architecture.md   how the crates fit: goals, crate map, modes, guarantees, rules
├── formats.md        the wire format and the tables other languages share
├── flows.md          diagrams of publishing, dispatching, receiving and retrying
├── specs/            what each crate does: one page per crate
├── adr/              architecture decision records: why it is this way
└── templates/        the crate spec and the flow; the ADR template is in adr/
```

At the repository root: [ROADMAP.md](../ROADMAP.md) for what comes next,
[CHANGELOG.md](../CHANGELOG.md) for what shipped, [CONTRIBUTING.md](../CONTRIBUTING.md) for the
checks, code style and Git rules, [SECURITY.md](../SECURITY.md), and [AGENTS.md](../AGENTS.md) for
coding agents. What each type does for users is in its Rust docs, not here.

## Where to start

| You want to | Read |
| --- | --- |
| See how the parts fit and what the library promises | [architecture.md](architecture.md) |
| Change or use one crate | Its page in [specs/](specs/README.md) |
| Publish or consume the messages from another language, or read the tables | [formats.md](formats.md) |
| Follow a publish, a dispatch, a delivery or a retry step by step | [flows.md](flows.md) |
| Know why something was chosen, before proposing another way | [adr/](adr/README.md) |
| Know what's next | [ROADMAP.md](../ROADMAP.md) |
| Contribute, commit, release | [CONTRIBUTING.md](../CONTRIBUTING.md) |

## Words used everywhere

| Word | Meaning |
| --- | --- |
| **Message** | A Rust type that implements `Message`: serde for its data, and a `TYPE` such as `orders.order_placed.v1` |
| **Message type** | The CloudEvents `type`: levels of `[a-z0-9_]` joined by `.`, ending with the version of the message's shape. It is valid as a NATS subject, a Kafka topic and an AMQP routing key, so it is the default destination |
| **Envelope** | A message on its way, as a CloudEvent: its id, type, source, time, content type, partition key, correlation, trace context, other extensions, and the encoded data |
| **Codec** | Turns a message into bytes and back, and names the content type, such as `application/json` |
| **Transport** | Moves envelopes: publishes them to the broker and delivers them to endpoints |
| **Endpoint** | A named, durable subscription, such as `billing`. Every instance of a service shares its endpoint (competing consumers), and each endpoint gets its own copy of every message it subscribes to (fan-out) |
| **Handler** | Application code that handles one message type on one endpoint |
| **Bus** | What the application holds: it publishes, and it runs the endpoints, their handlers and the workers |
| **Publisher** | The trait with `publish`. The bus publishes straight to the broker, the outbox publisher writes a row in the caller's transaction, and a handler's context does what its endpoint is set up for |
| **Store** | A persistence's side of the outbox, the inbox or the partitions: only the SQL. The runtime holds the logic around it |
| **Outbox** | The `outbox_messages` table in the application's database. `bus.outbox(&mut tx).publish(..)` writes a row there in the caller's transaction, and the **dispatcher** publishes the row after the commit and marks it dispatched |
| **Inbox** | The `inbox_receipts` table: a receipt per `(endpoint, message id)` handled, written in the handler's transaction, so a redelivery is acknowledged without running the handler again |
| **Partition** | One time range of a table, a day by default. Both tables are partitioned, so old data leaves by whole partitions, never by `DELETE` |
| **Retention** | How long a partition is kept before it is detached (kept as a table of its own, for archiving) or dropped. Off by default: outbox and inbox rows are business data |

## How docs grow

Docs change in the same PR as the code they describe.

1. **A change to what a crate does:** update its page in [specs/](specs/README.md), and the
   diagram in [flows.md](flows.md) if a step changes.
2. **A change to a header, a name on the broker or a table:** update [formats.md](formats.md).
   It is part of the public API, so the change is breaking.
3. **A new crate:** copy [templates/crate-spec.md](templates/crate-spec.md) to
   `specs/<crate>.md`, and add it to the [specs index](specs/README.md) and the
   [crate map](architecture.md#crates).
4. **A new flow across crates:** add a section to [flows.md](flows.md), in the shape of
   [templates/flow-spec.md](templates/flow-spec.md).
5. **A choice that is hard to reverse, or that changes a rule or a dependency:** write an
   [ADR](adr/README.md). An accepted ADR is never rewritten; a new one supersedes it.

A spec's `Status` moves from Draft to Accepted to Implemented.
