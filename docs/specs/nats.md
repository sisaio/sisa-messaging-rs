# Crate: sisa-messaging-nats

Status: **Implemented** in 0.1.0 · Published: yes · Label: `area:nats` · Commit scope: `nats`

## Purpose

Both transport halves on NATS JetStream: a stream per prefix, and a durable pull consumer per
endpoint. JetStream and not core NATS, which has no acknowledgement or redelivery
([ADR-0006](../adr/0006-how-each-transport-maps.md)).

## Public contract

Main types: `NatsTransport`, `NatsSettings` and `NatsDelivery`. The transport implements both halves of core's
[transport contract](core.md#transport), `TransportPublisher` and `TransportSubscriber`.

## Behaviour

| Step | What the transport does |
| --- | --- |
| Publish | Publishes to `{prefix}.{message type}` and waits for the JetStream acknowledgement |
| Endpoint | A durable pull consumer |
| Fan-out | One consumer per endpoint on the stream |
| Ack | `ack` |
| Retry later | `nak` with a delay |
| Give up | `term`, after the copy to the error subject |
| Broker deduplication | `Nats-Msg-Id` within the stream's duplicate window |
| Order | Per subject, while one consumer holds the messages |

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits). On
NATS: a stream with `max_age`, `max_msgs` or `max_bytes` removes unacked messages too, so keep it
longer than the longest consumer outage. The stream `NatsTransport` creates has no limits, and
connecting updates the stream to `NatsSettings`, which also clears limits set on it by hand.

## Errors

The transport returns core's [`TransportError`](core.md#errors) and has no error type of its
own.

## Formats

[NATS subjects and streams](../formats.md#nats-subjects-and-streams), and the header names in
[Envelope on the wire](../formats.md#envelope-on-the-wire).
