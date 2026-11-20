# Crate: sisa-messaging-testing

Status: **Implemented** · Published: no · Label: `area:testing` · Commit scope: `testing`

## Purpose

The test helpers the other crates' tests share: a server in Docker for one test, and a way to
wait for another task. Never published, so it has no public API to keep.

## Public contract

| Item | Gives |
| --- | --- |
| `Nats` | A fresh NATS server with JetStream for one test, removed when the test ends |
| `Postgres` | The same for a PostgreSQL server, with an empty database |
| `Iggy` | The same for an Apache Iggy server |
| `Redis` | The same for a Redis server, or a Valkey or Dragonfly one |
| `eventually` | Waits for something another task does, instead of sleeping |

Each server type has `start`, and `start_restartable` with `stop` and `restart` for a test that
takes the server away, and hands out its URL and a connected client.

## Behaviour

Every test starts a server of its own, so tests run side by side and share nothing. The image
comes from `NATS_IMAGE`, `POSTGRES_IMAGE`, `IGGY_IMAGE` and `REDIS_IMAGE`, with the defaults
[CONTRIBUTING.md](../../CONTRIBUTING.md#tests) lists; CI sets `REDIS_IMAGE` to run the Redis
transport's tests on Valkey and Dragonfly.

## Guarantees and limits

The suite needs Docker and nothing else. The Iggy server runs on io_uring, so its container gets
the `SYS_NICE` capability and no seccomp profile; a Docker that refuses those can't run the Iggy
tests.

## Errors

None: a helper that can't start its server panics, which fails the test.

## Formats

None.
