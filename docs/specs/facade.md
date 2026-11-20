# Crate: sisa-messaging

Status: **Implemented** in 0.1.0 · Published: yes · Label: `area:facade` · Commit scope: `facade`

## Purpose

The crate applications depend on. It re-exports what an application uses from the other crates
and has no code of its own, so each part is also usable as its own crate.

## Public contract

Everything an application uses from [core](core.md) and the [runtime](runtime.md), and, behind a
feature each, the transports and the persistence. No feature is on by default: an application
names the transport and the persistence it uses.

| Feature | Adds |
| --- | --- |
| `nats` | `NatsTransport`, the [NATS JetStream transport](nats.md) |
| `iggy` | `IggyTransport`, the [Apache Iggy transport](iggy.md) |
| `redis` | `RedisTransport`, the [Redis Streams transport](redis.md), for Redis, Valkey and Dragonfly |
| `postgres` | `PostgresOutbox`, `PostgresInbox` and `PostgresPartitions`, the [PostgreSQL stores](postgres.md) on sqlx |

## Behaviour

None of its own. CI builds the crate alone with each feature, so one feature never needs
another.

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits). The
async-nats and sqlx types that reach the API through the re-exports are part of the public API:
moving to a release of theirs that breaks their own API is breaking here too.

## Errors

None of its own: it re-exports the [core](core.md#errors) and [runtime](runtime.md#errors) error
types.

## Formats

None of its own: see [Formats](../formats.md).
