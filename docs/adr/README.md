# Architecture decision records

Short records of significant decisions: why, not only what. The rules:

1. Copy [0000-template.md](0000-template.md) to the next number, such as `0009-short-title.md`.
2. Open it as **Proposed** in the PR that needs it. It becomes **Accepted** when the PR merges.
3. Never rewrite an accepted ADR. Write a new one and mark the old one **Superseded by
   ADR-XXXX**.
4. Write an ADR when a choice is hard to reverse, changes a rule, a data format or a dependency,
   or someone will ask "why did we do this?" in six months.

Don't re-propose an accepted decision without new evidence.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-cloudevents-envelope.md) | CloudEvents 1.0 in binary mode is the envelope; ids are UUID version 7; the routing name is the CloudEvents `type` | Accepted |
| [0002](0002-outbox-and-inbox-in-the-application-database.md) | The outbox and the inbox live in the application's database; the outbox is written on the caller's connection and ordered by its id | Accepted |
| [0003](0003-rows-leave-by-partition.md) | Outbox and inbox rows leave by whole partitions, never one by one; the inbox is partitioned by the envelope's `time`; an expired partition is detached by default | Accepted |
| [0004](0004-plain-sql-migrations.md) | The tables come from plain SQL migrations that name no schema | Accepted (its folder is `crates/postgres/migrations` since [ADR-0010](0010-migrations-also-ship-as-a-rust-migrator.md)) |
| [0005](0005-open-traits-and-where-the-code-lives.md) | Open traits in core, one engine in the runtime, database crates with only SQL, one `Publisher` trait, a transport in two halves, JSON in core | Accepted |
| [0006](0006-how-each-transport-maps.md) | The delivery contract is ack, retry after a delay, and reject; JetStream for NATS, pending entries for a Redis retry, a held partition for an Iggy retry | Accepted |
| [0007](0007-what-the-library-leaves-out.md) | No sagas, request and response, scheduled messages or encryption at first | Accepted |
| [0008](0008-minimum-rust-version.md) | The minimum Rust version is 1.95 | Accepted |
| [0009](0009-trace-context-through-sisa-telemetry-core.md) | The runtime sets and reads trace context through `sisa-telemetry-core`; a handling starts a new trace that links to its publish | Proposed |
| [0010](0010-migrations-also-ship-as-a-rust-migrator.md) | The migrations move into `crates/postgres/migrations`, and an opt-in `migrate` feature ships them as a sqlx `Migrator` (`migrator()`) with the same checksums | Accepted |
