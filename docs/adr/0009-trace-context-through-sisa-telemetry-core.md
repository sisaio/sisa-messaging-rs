# ADR-0009: Trace context goes through sisa-telemetry-core, and a handling links to its publish

- Status: **Proposed**
- Date: 2026-11-21
- Deciders: Sang Au

## Context

The envelope has carried `traceparent` and `tracestate` since 0.1.0, but the runtime only passed
them on: an application had to read its own span's trace context and write it into an envelope,
and to turn the one on a delivery into a parent itself. `sisa-telemetry-core` 0.1.0, published
on 2026-11-21, gives library crates that part: `inject` and `link_from`, on the OpenTelemetry API
alone (`opentelemetry` 0.33, with no SDK and no exporter), doing nothing until an application
installs OpenTelemetry. It follows the OpenTelemetry messaging conventions, where a consumed
message starts a new trace with a link to its producer.

## Decision

`sisa-messaging-runtime` depends on `sisa-telemetry-core`. A publish writes the trace context of
the span it runs in into the envelope. Handling a delivery opens the `handle` span as the root
of a new trace, with a link to the span in the message's `traceparent`. A handler's publishes
carry the `handle` span's trace context.

Without OpenTelemetry installed nothing changes: a publish writes no trace context, and a
handler's publishes pass the handled message's on unchanged, as before.

## Consequences

- An application gets linked traces across a broker and the outbox with no code of its own.
- A handling is no longer in its publisher's trace. Following a flow means following links, or
  searching by the correlation id.
- With OpenTelemetry installed, a handler's publish no longer carries the trace context of the
  message it handled, but that of its own `handle` span. A consumer that read the original
  `traceparent` two hops later sees the previous hop instead.
- The runtime, and so every crate built on it, now holds the `opentelemetry` API. Every sisa
  crate in one binary must be on the same `opentelemetry` minor, which CI checks, and an
  OpenTelemetry upgrade follows sisa-telemetry's.
- Baggage isn't carried: the envelope has no attribute for it.

## Alternatives considered

- **The `handle` span as a child of the publishing span:** a message that waits for hours would
  make a trace that long.
