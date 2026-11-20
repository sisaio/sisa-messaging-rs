# Crate: sisa-messaging-redis

Status: **Implemented** in 0.1.0 · Published: yes · Label: `area:redis` · Commit scope: `redis`

## Purpose

Both transport halves on Redis Streams, for Redis, Valkey and Dragonfly: a stream per message
type, a consumer group per endpoint, and the delayed retry and takeover built from the pending
entries. The same client and commands serve all three servers.

## Public contract

Main types: `RedisTransport`, `RedisSettings` and `RedisDelivery`. The transport implements both halves of core's
[transport contract](core.md#transport), `TransportPublisher` and `TransportSubscriber`.

## Behaviour

| Step | What the transport does |
| --- | --- |
| Publish | `XADD` to `{prefix}:{message type}` |
| Endpoint | A consumer group, with a consumer per instance |
| Fan-out | One group per endpoint |
| Ack | `XACK` |
| Retry later | Leaves the entry pending and claimed by the instance until the delay has passed, then claims it again |
| Give up | `XACK`, after the copy to the error stream |
| Broker deduplication | None in Valkey and Dragonfly; Redis 8.6 added `XADD IDMP` |
| Order | Per stream, while one consumer reads it |

An instance reads only when its endpoint asks for the next delivery, so no entry waits read in a
buffer while its ack wait runs. A read over several streams returns up to one entry of each; those
beyond the first wait in the instance, which claims them again with `XCLAIM ... JUSTID` every half
ack wait. Every entry a read returns, the first too, is claimed once more just before the handoff,
which checks that it is still the instance's and restarts its ack wait. The wait restarts when the
server runs the claim, so a slow reply takes from the handler's share, and nothing checks again
after the reply. Field names are read as bytes, so an entry another client wrote with a name that
isn't UTF-8 still reaches the error stream, its names as they came. `RedisSettings::connect_timeout` bounds each command with the reconnecting before it,
and the blocking read by that plus its wait.

Redis acknowledges one entry at a time but has no negative acknowledgement with a delay, so
`RedisDelivery` builds the redelivery from the group's pending entries. `ack` and `reject` run
`XACK`. Each instance looks every second, or every quarter of `RedisSettings::ack_wait` when that
is shorter, for entries no consumer has touched for the ack wait (`XPENDING ... IDLE`), and takes
each over with `XCLAIM`, which only one instance wins and which counts the delivery, so the attempt
survives a restart. That brings back a delivery left unsettled, such as one of an instance that
stopped. `retry` leaves the entry pending, and the instance holds it in memory: every half ack
wait it claims the entry again with `XCLAIM ... JUSTID`, which resets the idle time without
counting a delivery, so no other instance takes it over, and once the delay has passed it claims it
with `XCLAIM`, which counts the delivery, and delivers it again. Both claims run in a Lua script
that first checks with `XPENDING` that the entry is still the instance's, so one that stalled past
the ack wait doesn't take back an entry another instance has taken over. An instance that stops
with held entries leaves them to be taken over after the ack wait, so their delay may come out
shorter or longer. `XCLAIM ... IDLE` would let the server hold the delay instead, but Dragonfly
doesn't honor it. A retried message goes behind the ones published after it, and a handler that
runs longer than the ack wait has its message taken over by another instance.

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits). On
Redis: an `XTRIM`, or an `XADD` with `MAXLEN` or `MINID`, removes unacked messages too; the
streams `RedisTransport` creates have no limits. A retried message goes behind the ones published
after it, and a handler that runs longer than the ack wait has its message taken over by another
instance. Garnet isn't supported: it has no consumer groups yet
([ADR-0006](../adr/0006-how-each-transport-maps.md)). Redis Cluster and TLS are on the
[roadmap](../../ROADMAP.md).

## Errors

The transport returns core's [`TransportError`](core.md#errors) and has no error type of its
own.

## Formats

[Redis streams and groups](../formats.md#redis-streams-and-groups), and the header names in
[Envelope on the wire](../formats.md#envelope-on-the-wire).
