# ADR-0005: Open traits, one engine, and adapters that hold only what differs

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

The library supports several transports, databases and codecs, and users asked to bring their
own. The dispatcher, the inbox step and partition maintenance are the same whatever the
database; only the SQL differs. Domain code that publishes shouldn't change with the mode.

## Decision

- Transports, persistences and codecs are open traits, in `sisa-messaging-core`.
- The engine lives in `sisa-messaging-runtime`. Database crates implement only the store traits,
  and the facade has no code.
- There is one `Publisher` trait. The outbox publisher borrows the caller's transaction, and
  exists only on a bus built with an outbox.
- The transport is two traits, publish and subscribe.
- JSON lives in core; other codecs get crates of their own.

## Consequences

- Users can bring their own transport, persistence or codec; sealing the traits would rule that
  out. Their docs say which changes a minor release may make.
- The engine isn't copied into every database crate: MySQL and SQL Server add only their SQL.
- One signature for domain code, while the guarantee stays visible where the transaction lives,
  and a missing outbox fails to compile instead of silently sending direct.
- The dispatcher and publish-only services need only the publish half of a transport.
- Every message is serde already, so JSON in core adds no dependency a message doesn't have.
- `Codec` has generic methods, so the codec is a type parameter of the bus, and picking a codec
  by the envelope's content type at run time isn't possible yet.

## Alternatives considered

- **Sealed traits:** rules out a user's own transport, persistence or codec.
- **The dispatcher and the inbox step in each database crate:** the same logic copied where only
  the SQL differs.
- **A shared SQL crate for the databases:** the claim, the partition DDL and the lock differ in
  every dialect, and SQL Server uses tiberius rather than sqlx.
- **One transport trait:** a publish-only service would have to implement or carry the subscribe
  half.
