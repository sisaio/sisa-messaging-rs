# API and code conventions

## 1. Public API budget

Add a public item only for an active caller, correctness invariant, or real capability boundary.
Symmetry, hypothetical future use, and one-line field assignment are not enough.

Crate-root re-exports provide stable user paths. Internal module layout remains private.

## 2. Construction and settings

Use one construction style per type:

- Plain public data: struct literal.
- Public data with defaults: struct literal plus `..Default::default()`.
- Required dependencies or invariants: one `new(...)`.
- Fallible invariant: `new(...) -> Result<Self, Error>`.
- Builder: only when staged construction materially improves a common call site.

Do not combine public fields with one setter per field. Avoid positional constructors containing
several arguments of the same type.

```rust,ignore
let purge = InboxPurgeRequest {
    dead_retention: Some(Duration::from_secs(30 * 24 * 60 * 60)),
    batch_size: NonZeroU32::new(500).unwrap(),
    ..InboxPurgeRequest::default()
};
```

Do not use `#[non_exhaustive]` on request, settings, and report structs intended for literals. Use
it on public error enums and externally matched state enums where future variants are expected.

Library settings are typed data. They may derive Serde behind a feature with
`#[serde(default, deny_unknown_fields)]`, but never read environment variables, files, arguments,
or secret stores. The application loads configuration and passes settings to the canonical
constructor.

Settings are separated by responsibility:

- `DispatcherSettings`: worker behavior.
- `OutboxRetention`: published/dead retention and pass size.
- `InboxSettings`: recorded-failure limit.
- `InboxRetention`: completed/dead retention and pass size.
- `ConsumerSettings`: receive concurrency, source/database/settlement bounds, negative-ack delay,
  heartbeat interval, and drain behavior.
- `NatsPublisherSettings`: publish timeout and behavior not owned by the resolver/context.

There is no NATS consumer settings duplicate. `NatsDeliverySource` reads the durable consumer's
already configured acknowledgement wait and delivery bound; generic concurrency, database,
settlement, negative-ack delay, heartbeat, and drain policy belong to `ConsumerSettings`.

PostgreSQL connection policy belongs to the application-owned pool, so there is no provider
settings type used only for statement timeout. `max_attempts` belongs to portable
`InboxSettings`; PostgreSQL applies it atomically in SQL.

## 3. Validation

Validate settings once in the runtime object's constructor. Real cross-field checks include:

- non-zero publish, poll, idle-poll, store, source, database, settlement, and drain timeouts;
- non-zero capacity;
- `store_timeout < lease / 2`;
- retry base delay not greater than maximum delay;
- consumer heartbeat interval, when enabled, shorter than half the configured broker ack wait;
- inbox `max_attempts` not greater than a finite broker `max_deliver`.

Use types for local invariants: `NonZeroU32` for limits and attempts, `NonZeroUsize` for
concurrency, and validated domain string types. Do not add a validator that repeats what a field
type already proves.

Validate strings according to their boundary:

- Header/wire identifiers reject empty strings, excessive byte length, ASCII control bytes, CR,
  LF, and DEL.
- Header names use transport-neutral name grammar and reject the framework-reserved namespace.
- Header values reject ASCII control bytes (including CR and LF), plus excess size; they need not be
  limited to visible ASCII.
- Custom-header collections retain at most 64 distinct canonical names and 65,536 decoded UTF-8
  bytes across retained canonical names plus values. Count every retained canonical name and value
  exactly once; exclude JSON syntax and escaping, allocator or map overhead, framework headers, and
  other metadata fields. Replacements use the canonical name, do not consume another count slot,
  and must leave the collection unchanged if their resulting aggregate exceeds the byte bound.
- NATS subject rules live in `sisa-messaging-nats`.

Do not apply `char::is_control()` indiscriminately to all application strings. The purpose is to
prevent injection, invalid wire data, and unbounded storage—not generic text hygiene. Already
validated values are not rechecked in the dispatcher loop.

## 4. IDs

Keep a newtype when mixing values would be a correctness bug: `MessageId`, row IDs,
`ConversationId`, `RequestId`, a claim token, `InboxScope`, and `OrderingKey`.

Do not expose a downstream ID-generation macro. Use a private macro internally only if worthwhile.
Public behavior stays conventional: mint only where the layer owns minting, reconstruct through
`from_uuid`, expose `as_uuid`/`into_uuid`, and implement `Display`, `FromStr`, and optional Serde.

Claim tokens are minted by the store on claim. Row IDs are minted by PostgreSQL. Constructors must
not suggest that dispatcher/application code owns either operation.

## 5. Requests and reports

Request/report structs are data carriers with named public fields:

- `InboxPurgeRequest`: terminal retention and pass size fields plus `Default`; no incomplete-row
  retention or fluent setters.
- Inbox failure recording receives an explicit `FailureKind`; the PostgreSQL provider never
  rediscovers retryability from rendered error text.
- Outbox purge request: named fields plus `Default`; no fluent setters.
- Acquire request: keep one constructor only if worker identity is a required public input.
- Purge reports: named fields, not `new(u64, u64, u64)`.
- Dead-letter queries: named fields plus `Default`; no setter per filter.

Private SQL parameter structs remain useful because they name bindings and prevent positional
mistakes. They require no builder or doctest.

### PostgreSQL provider SQLx conventions

- Every runtime SQLx statement uses a compile-time checked SQLx query macro, including a checked
  typed form such as `query_as!` or `query_scalar!`. The selected form must validate SQL plus bind
  and result shape at compile time, whether it reads a live development database or SQLx offline
  metadata. Runtime-only `query`/`query_as` APIs, dynamically assembled SQL, and unchecked row
  decoding do not meet this rule for provider operations.
- Format SQL as readable multiline statements. Add a concise SQL comment only when the correctness
  reason is not apparent from the statement itself, such as fencing, locking, ordering, or index
  intent.
- Private models that supply SQL binds end in `Params`; private models decoded from SQL rows end in
  `Record`. These names make the input/output boundary visible at each query call site.
- A statement helper neither accepts nor calls a store. Its first argument is the SQLx executor—a
  pool reference or the transaction's underlying mutable connection/executor form required by SQLx
  (for example, `&mut *transaction`)—followed by exactly one typed `Params` value; it returns typed
  `Record` values.
- The store/provider is the orchestration layer: it selects the pool or opens/uses a transaction,
  invokes statement helpers, maps `Record` values into portable contract types, and owns
  multi-statement transaction boundaries. Helpers do not call back into the store, preventing a
  circular store-to-helper-to-store dependency.
- Keep tests and test-only modules outside production `src/**`. Provider and database coverage
  belongs under `tests/**`, where it runs against the documented PostgreSQL baseline rather than
  relying on production-module test scaffolding.
- Regenerate SQLx offline metadata whenever a checked query or its schema contract changes. Review
  verifies that the metadata is current, query typing remains checked, statements follow these
  presentation rules, and provider tests are outside production source.

## 6. Errors

Use one error per boundary, not a global `MessagingError`:

- validation errors live with the validated type;
- dispatcher errors describe construction or terminal worker failure;
- consumer errors distinguish invalid construction, fatal source exit, and per-delivery outcomes;
- PostgreSQL errors interpret and source `sqlx::Error`;
- NATS errors classify transport failures without leaking data.

Use `thiserror` for mechanical `Display`, `Error`, and source implementations. Declare it directly
in the workspace and in every crate using its derive; its transitive presence through SQLx or
async-nats is insufficient. `thiserror` does not replace explicit retry classification or careful
redaction.

The small, source-free header validation errors in `sisa-messaging` retain manual `Display` and
`Error` implementations so this core boundary does not add a dependency solely for those enums.

Error mapping rules:

- One pure SQLSTATE classifier in `sisa-messaging-postgres`.
- Match structured variants/codes, never foreign message text.
- Conversion, classification, truncation, and redaction helpers do not log.
- Log at the layer that consumes, retries, suppresses, or terminates because of an error.
- Returning an error normally means the caller owns its terminal log.
- Persist only a redacted, UTF-8-boundary-truncated summary.
- Share error-chain formatting/truncation once inside the PostgreSQL crate.
- Handler error `Display` and sources must be safe to persist; the consumer passes structured
  `FailureKind` separately and never parses rendered text.
- Fencing shortfalls are outcomes, not errors; summarize counts rather than an unbounded ID list.

## 7. Async and cancellation

- Native async trait methods; no `async-trait` in library crates.
- No boxed futures on the hot path.
- No database transaction across broker I/O.
- The consumer framework commits or rolls back before terminal broker settlement.
- No externally visible I/O in a `tokio::select!` branch future.
- Bound calls explicitly with the owning timeout.
- Cancellation stops new work and then follows the documented drain/release sequence.

## 8. Modules and documentation

- Split by cohesive responsibility, not arbitrary line count.
- `lib.rs` contains only crate documentation, module declarations, and re-exports. It contains no
  structs, enums, traits, functions, implementations, or runtime logic.
- Do not create `mod.rs`. A non-leaf module uses a same-level root file beside a directory of the
  same name: `dispatcher.rs` with `dispatcher/claim.rs`, `dispatcher/publish.rs`, and other focused
  children.
- The module root file may contain real code. It owns the capability's public façade, primary
  struct or trait, and high-level delegation; its directory owns the internal responsibilities.
- Apply this paired file/directory pattern to any capability that becomes complex, including a
  dispatcher, publisher, consumer, maintenance implementation, or dead-letter implementation.
- Do not split a small cohesive module merely to satisfy symmetry. Split it when it has multiple
  independently understandable responsibilities.
- Avoid wrappers that only rename another private function.
- Public docs state guarantees, ownership, failure behavior, and caller obligations—not project
  archaeology.
- Tests are named for behavior and risk, not ADR numbers.
- Doctests demonstrate common APIs; they do not test every setter/getter.

```text
src/
├── lib.rs                 # docs, mod declarations, re-exports only
├── dispatcher.rs          # OutboxDispatcher façade and top-level coordination
└── dispatcher/
    ├── claim.rs
    ├── publish.rs
    ├── outcomes.rs
    ├── leases.rs
    └── shutdown.rs
```

### Rust presentation

Use the same grouping convention in production source, tests, examples, and benchmarks. A
declaration group is either a comment, documentation, and attributes together with the declaration
they describe, or consecutive declarations that serve one purpose. Put one blank line between
declaration groups; do not separate documentation or attributes from their declaration.

Treat each struct field as its own declaration group, including private fields. Keep a field's
documentation and attributes attached to that field, and put one blank line before the next field.
Apply the same rule to individually documented enum variants. This makes each property boundary
visible even when adjacent fields or variants have related roles:

```rust,ignore
pub struct DeliveryContext {
    /// Stable identity used for deduplication.
    pub message_id: MessageId,

    /// Contract resolved by the mapper.
    pub message_type: MessageType,

    /// Attempt reported by the transport.
    pub delivery_attempt: NonZeroU32,
}
```

Within a function, keep consecutive local declarations for one purpose together, then put one
blank line before the action, loop, assertion, or returned expression that consumes them. Do not
insert blank lines inside one expression merely to create visual symmetry:

```rust,ignore
let message_id = MessageId::new();
let metadata = Metadata::default();

let envelope = Envelope::new(message_id, ExampleMessage, metadata)?;

assert_eq!(envelope.message_id(), message_id);
```

Tests use visibly distinct arrange, act, and assert/result phases. Benchmarks use the equivalent
fixture/setup, measurement, and result phases. Keep repeated fixtures and measurement scaffolding
in consistent groups; comments are useful only when spacing and names do not already explain a
phase.

Apply these rules by semantic review. Do not add an automated blank-line or source-shape rule unless
it can distinguish semantic groups without noisy false positives.

### Rust documentation

Every public API item must have useful rustdoc, including public fields, variants, associated items,
and semantic contracts exposed through traits. Document guarantees, ownership, failure behavior,
and caller obligations where they apply. Documentation is not complete merely because an item has
a summary sentence.

Private helpers, trait implementation methods, test functions, and benchmark functions do not
require documentation. Document them only when they carry a non-obvious invariant or constraint
that names and structure cannot communicate. Never add boilerplate comments solely to raise a
documentation percentage.

CodeRabbit review follows this public-surface policy. Any docstring metric that measures the broader
private, test, or benchmark surface is advisory and non-blocking; actionable findings must identify
a missing or inadequate public contract rather than cite the percentage alone.

### Cargo manifest presentation

In workspace and crate manifests, group related features and dependencies by role. Introduce each
group with one concise comment and separate groups with one blank line. Keep the comment about why
the group exists rather than repeating package names:

```toml
[dependencies]
# Core domain representation.
uuid = { workspace = true }

# Optional serialization surfaces.
serde = { workspace = true, optional = true }
serde_json = { workspace = true, optional = true }
```

## 9. Test layers

- Unit: validation, retry math, codecs, mapping, and redaction.
- Compile: `Send` bounds and capability composition.
- PostgreSQL: atomicity, fencing, concurrency, retention, poison rows, and plans.
- NATS: acknowledgement, dedup headers, payload limits, timeout, and classification.
- System: outbox-to-JetStream, direct-publish rollback behavior, and inbox commit/ack windows.

Do not use a large in-memory store to claim proof of PostgreSQL transaction behavior.
