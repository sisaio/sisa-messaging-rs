# Crate: sisa-messaging-iggy

Status: **Implemented, experimental** in 0.1.0 · Published: yes · Label: `area:iggy` · Commit scope: `iggy`

## Purpose

Both transport halves on Apache Iggy: a topic per message type in one stream, a consumer group
per endpoint and topic, and the per-message retry a log doesn't have.

## Public contract

Main types: `IggyTransport`, `IggySettings` and `IggyDelivery`. The transport implements both halves of core's
[transport contract](core.md#transport), `TransportPublisher` and `TransportSubscriber`.

## Behaviour

| Step | What the transport does |
| --- | --- |
| Publish | Sends to the message type's topic |
| Endpoint | A consumer group |
| Fan-out | One group per endpoint |
| Ack | Stores the offset |
| Retry later | Holds the partition and seeks back to the message after the delay; later messages of its partition wait |
| Give up | The error topic |
| Broker deduplication | None |
| Order | Per partition |

Iggy acknowledges a position in a partition, not one message, so `IggyDelivery` builds the rest:
`ack` and `reject` commit the message's offset; `retry` holds its partition and reads it again from
that offset once the delay has passed, so the messages behind it wait and the partition keeps its
order; a delivery dropped unsettled is retried at once. The attempt is counted in memory, so it
starts from 1 again after a restart or a rebalance. An endpoint that stops handling a type stops
reading that topic, where NATS keeps the type in its consumer's filter.

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits). On
Iggy: a topic's `message_expiry` or `max_topic_size` removes unacked messages too, so keep it
longer than the longest consumer outage; the topics `IggyTransport` creates have no limits. Iggy
has no deduplication by id, so it keeps every publish. An Iggy header value holds at most 255
bytes: a publish with a longer attribute fails, and an error copy cuts it there. Retry topics, for
a per-message delay that doesn't hold the partition, come with the Kafka family
([ADR-0006](../adr/0006-how-each-transport-maps.md)).

## Errors

The transport returns core's [`TransportError`](core.md#errors) and has no error type of its
own.

## Formats

[Iggy streams and topics](../formats.md#iggy-streams-and-topics), and the header names in
[Envelope on the wire](../formats.md#envelope-on-the-wire).
