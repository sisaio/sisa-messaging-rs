# Observability design

## 1. Ownership and signal APIs

Libraries instrument behavior; applications configure collection and export.

| Signal | Library API | Application integration |
|---|---|---|
| Traces/spans | `tracing` | `tracing-subscriber` and optionally `tracing-opentelemetry` |
| Structured logs/events | `tracing` | formatting/JSON layer and optionally `opentelemetry-appender-tracing` |
| Metrics | direct `opentelemetry` Metrics API | global `SdkMeterProvider`, reader and exporter |

Library crates do not depend on the OpenTelemetry SDK, OTLP exporter, Prometheus exporter, or
collector-contrib components. They do not install a global subscriber/provider and do not shut one
down.

Metrics are not represented by an injected trait, no-op implementation, dispatcher generic, or
`Arc<M>`. `tracing` is not used as a metric facade: translating specially named events adds an
extra lookup and couples measurements to subscriber configuration. Direct OTel instruments are
clearer and cheaper for hot paths.

## 2. Names

No observability name contains a product brand or Cargo crate name.

Service identity belongs in OTel resource attributes such as `service.name`,
`service.namespace`, and `service.version`. Library identity is an instrumentation scope, for
example `messaging.outbox`.

Because `tracing` otherwise defaults the target to the Rust module path, every public callsite sets
an explicit target:

```rust,ignore
tracing::debug!(
    target: "messaging.outbox",
    message.id = %message_id,
    "message claimed"
);
```

Targets:

```text
messaging
messaging.outbox
messaging.inbox
messaging.consumer
messaging.postgres
messaging.nats
messaging.rabbitmq
```

Span names:

```text
outbox.dispatch
outbox.claim
outbox.publish
outbox.persist_outcome
inbox.process
inbox.claim
inbox.complete
process {destination template}
receive {destination template}
ack {destination template}
postgres.query
publish {destination template}
```

The destination suffix is omitted when no stable, low-cardinality name or template is available.
Messaging-facing receive, process, publish, and settlement spans follow the current OTel messaging
span convention; internal outbox/inbox spans retain fixed library operation names.

## 3. Direct global OpenTelemetry metrics

Library crates depend only on the OTel API:

```toml
opentelemetry = {
    workspace = true,
    default-features = false,
    features = ["metrics"]
}
```

Each instrumented crate owns one private `OnceLock<Instruments>`. Handles are created once and
reused for the process lifetime. Enum attributes map to static strings; emission does not allocate
labels dynamically.

The application must retain its `SdkMeterProvider` handle and pass a clone to
`opentelemetry::global::set_meter_provider` before starting messaging components. Instruments
obtained from the no-op provider before that call remain attached to that provider when cached.
Required startup order:

```text
load application settings
  → construct OTel providers/exporters/resources
  → retain SdkMeterProvider and set its clone as the global meter provider
  → install tracing subscriber/layers
  → construct and start messaging tasks
```

During shutdown, the application first stops and awaits all messaging tasks, then calls
`SdkMeterProvider::shutdown()` on the retained handle. This flushes pending metrics and releases
provider resources; messaging tasks must not emit measurements after that call.

When no provider is installed, calls use the OTel no-op provider. No custom no-op type is needed.

Global providers make parallel tests share process state. Tests that replace the provider must be
serialized or isolated in subprocesses. Most behavior tests should assert the internal outcome
that triggers a measurement rather than mutating global telemetry.

## 4. Metric catalog

Names use dot-separated lowercase words, no brand prefix, no `_total`, and no unit suffix. Units
are instrument metadata.

### Outbox lifecycle

| Instrument | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `outbox.claimed.messages` | Counter | `{message}` | none | Rows returned by successful claims |
| `outbox.published.messages` | Counter | `{message}` | none by default | Broker-acknowledged messages |
| `outbox.retried.messages` | Counter | `{message}` | `failure.kind` | Database-confirmed retry transitions |
| `outbox.dead.messages` | Counter | `{message}` | `dead.reason` | Database-confirmed dead transitions observed by this process |
| `outbox.publish.duration` | Histogram | `s` | `error.type` only on failure | Duration of one publisher attempt |
| `outbox.in.flight` | UpDownCounter | `{message}` | none | Local publishes currently unresolved |

`message.type` is not a default metric attribute. It is application-defined and can be unbounded.
Applications needing it must supply a closed registry or aggregate outside the library.

### Database-authoritative levels

| Instrument | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `outbox.message.count` | Gauge | `{message}` | `state=pending\|expired\|dead` | Current database backlog |
| `outbox.pending.oldest_age` | Gauge | `s` | none | Age of oldest currently claimable row |

Record `outbox.pending.oldest_age = 0` when no pending row exists; do not leave a stale value.
Consumers use the pending count to distinguish an empty queue.

One observer records these gauges per database/schema. Multiple dispatchers must not each run the
same queries and export duplicate snapshots.

### Transport metrics

Where practical, the NATS and RabbitMQ providers use OTel messaging instruments:

- `messaging.client.sent.messages`;
- `messaging.client.consumed.messages`;
- `messaging.client.operation.duration`.

Use `messaging.system = "nats"` or `"rabbitmq"`, a bounded `messaging.operation.name` such as
`publish`, `receive`, `ack`, `nack`, `terminate`, and `error.type` only on failure. Do not add raw
subjects, exchanges, queues, or routing keys when they may contain dynamic or sensitive tokens; use
a stable destination template when available.

Outbox lifecycle and transport metrics answer different questions and must not be summed together.

### Consumer metrics

| Instrument | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `consumer.processed.messages` | Counter | `{message}` | none | Handler transactions committed |
| `consumer.duplicate.messages` | Counter | `{message}` | `duplicate.state=completed\|in_progress` | Deliveries whose handler did not run |
| `consumer.dead.messages` | Counter | `{message}` | `dead.reason` | Deliveries observed or transitioned dead |
| `messaging.process.duration` | Histogram | `s` | OTel messaging attributes and `error.type` on failure | Handler processing duration |
| `consumer.in.flight` | UpDownCounter | `{message}` | none | Received deliveries not yet settled |

`consumer.processed.messages` is emitted only after commit confirmation. A settlement failure does
not decrement an earlier processed count; it can cause a later duplicate delivery. Message type,
scope, durable name, and subject are not default metric attributes because applications often make
them unbounded. Receive and settlement activity uses the standard transport instruments above
rather than duplicate custom counters.

The OTel messaging semantic conventions are currently marked development. The NATS
`messaging.nats` and RabbitMQ `messaging.rabbitmq` instrumentation scopes pin
`https://opentelemetry.io/schemas/1.42.0`.
`messaging.client.sent.messages` counts publish attempts that reach the broker client, including
failed or timed-out attempts. `messaging.client.consumed.messages` counts delivered messages, not
clean empty receives. Changing the schema version or instrument semantics requires a deliberate
compatibility review. The implementation does not read `OTEL_SEMCONV_STABILITY_OPT_IN` from the
environment.

## 5. Correctness and cardinality

- Counters report events observed by this process and can undercount a committed transition whose
  acknowledgement was lost.
- PostgreSQL gauges are the source for authoritative backlog alerts.
- Emit retry/dead counters only for IDs confirmed by the outcome write.
- Expiry and operator transitions are instrumented at their own execution boundary if included in
  a transition counter.
- Never label metrics with message ID, record ID, claim token, correlation ID, tenant ID, worker
  ID, raw subject, error message, or custom header.
- Attribute vocabularies are closed enums or bounded transport codes.
- Use monotonic time for durations and database time for database ages.
- Metric emission never changes correctness or turns a successful operation into an error.

## 6. Tracing levels and fields

| Level | Use |
|---|---|
| `trace` | Poll ticks, empty claims, renewal details, internal scheduling |
| `debug` | Successful claim/publish/store operations and per-message details |
| `info` | Dispatcher start/stop and explicit lifecycle milestones |
| `warn` | Transient failure being swallowed/backed off, dead batch, fencing shortfall, drain timeout |
| `error` | Worker exits unexpectedly, invariant failure, or likely data loss |

Per-message publish and claim spans default to `debug`, not `info`.

The consumer creates one `process {destination template}` span with consumer span kind. For the
single-message typed path, it captures the extracted remote context and any valid ambient HTTP or
scheduler context before creating the span. When the extracted remote `SpanContext` is valid, the
consumer creates the span with that remote context as parent and supplies the valid ambient context
as a creation-time link. Otherwise, it creates the span with the valid ambient context as parent,
or starts a new trace when neither context contains a valid span. This permitted remote-parent
choice is documented by the instrumentation. Database processing and handler execution are
children, while broker settlement uses its own client-kind settle span.

Safe structured fields include:

```text
message.id
message.type
outbox.record.id
inbox.receipt.id
inbox.scope
operation.outcome
error.type
failure.kind
dead.reason
messaging.system
messaging.operation.name
```

IDs are acceptable in spans/logs for correlation but not as metric attributes. Payload, custom
header values, credentials, connection strings, and rejected subject text are never recorded.

## 7. Logging

Log once at the layer that makes a decision:

- If the library catches and retries, backs off, dead-letters, or suppresses an error, it logs the
  decision.
- If the library returns an error, it normally does not log it first; the caller owns the terminal
  log.
- Error constructors, SQLSTATE classifiers, redaction helpers, and truncation helpers are pure.
- Poison/dead storms produce one bounded batch warning. Individual IDs are debug-level and capped.
- Fencing shortfalls log counts and operation, not an unbounded vector.

Use structured fields rather than interpolated prose. Record foreign error text only when its type
is known safe; otherwise record a stable category/type and retain the original as an error source.

## 8. Context propagation

The NATS mapper forwards W3C `traceparent` and `tracestate` in framework-owned headers. Consumer
integration captures both contexts before creating the single-message `process` span. A valid
remote `SpanContext` becomes the parent, with any valid ambient HTTP or scheduler context supplied
as a creation-time link. Without a valid remote span, the valid ambient context remains the parent,
or processing starts a new trace if neither context contains a valid span. `inbox.claim`, handler
work, and commit are children, while settlement is a related client operation.

The transport does not install a global propagator. Provider/exporter setup and sampling remain
application responsibilities.

## 9. Standards references

- [OpenTelemetry messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
- [OpenTelemetry messaging client metrics](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/)
- [OpenTelemetry Rust global metrics API](https://docs.rs/opentelemetry/latest/opentelemetry/global/)
