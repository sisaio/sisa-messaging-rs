# Specs

What each crate does, written so it can be built, reviewed and tested against. One page per
crate, all with the same sections: purpose, public contract, behaviour, guarantees and limits,
errors, and formats.

| Crate | Spec | Holds |
| --- | --- | --- |
| `sisa-messaging` | [facade.md](facade.md) | The re-exports and the features that pick the parts |
| `sisa-messaging-core` | [core.md](core.md) | The contracts: message, envelope, codec, transport, stores, publisher |
| `sisa-messaging-runtime` | [runtime.md](runtime.md) | The bus, receiving, dispatching the outbox, partitions and retention |
| `sisa-messaging-nats` | [nats.md](nats.md) | The NATS JetStream transport |
| `sisa-messaging-iggy` | [iggy.md](iggy.md) | The Apache Iggy transport |
| `sisa-messaging-redis` | [redis.md](redis.md) | The Redis Streams transport, for Redis, Valkey and Dragonfly |
| `sisa-messaging-postgres` | [postgres.md](postgres.md) | The PostgreSQL outbox, inbox and partition stores |
| `sisa-messaging-testing` | [testing.md](testing.md) | The test servers and helpers; not published |

What spans the crates is one level up: how they fit in [Architecture](../architecture.md), the
wire format and tables in [Formats](../formats.md), and the steps in [Flows](../flows.md).

Changing what a crate does: update its page in the same PR. A new crate starts from
[templates/crate-spec.md](../templates/crate-spec.md). A choice that is hard to reverse gets an
[ADR](../adr/README.md). Planned work goes in the [roadmap](../../ROADMAP.md), not here: a spec
says what the code does, and marks what is still open as an open question.
