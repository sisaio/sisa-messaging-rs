# ADR-0001: CloudEvents 1.0 in binary mode is the envelope

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

Services in other languages must publish and consume the same messages, without a library of
ours. The envelope needs an id that stays the same across outbox retries and broker redeliveries,
which the inbox and broker deduplication depend on, and a routing name that is valid on every
broker.

## Decision

- The envelope is [CloudEvents 1.0](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)
  in each transport's binary content mode, with the standard tracing and partitioning extensions
  and our own `correlationid` and `causationid`.
- Message ids are UUID version 7, made by the publisher.
- The message's routing name is its CloudEvents `type`: levels of `[a-z0-9_]` joined by `.`,
  ending with the version of the message's shape.

The format itself is in [Formats](../formats.md#envelope-on-the-wire).

## Consequences

- One documented format that NATS, Kafka and AMQP each have a binding for, so other languages and
  tools read our messages. In binary mode the body is the encoded message, so consumers that
  don't know CloudEvents still read a plain body.
- Ids stay stable across outbox retries and broker redeliveries, and a version 7 id sorts by
  time, which keeps the inbox index compact.
- The routing name is one word from the Rust trait to the wire to the table, and it is valid as
  a NATS subject, a Kafka topic and an AMQP routing key.
- Receivers read binary mode only: a message in structured mode goes to the endpoint's error
  destination.
- Iggy and Redis have no CloudEvents binding, so they follow Kafka's, the closest one.

## Alternatives considered

- **An envelope format of our own:** other languages and tools would need a library of ours to
  read it.
- **CloudEvents structured mode:** the body would no longer be the plain encoded message.
