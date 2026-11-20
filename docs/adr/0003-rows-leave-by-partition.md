# ADR-0003: Outbox and inbox rows leave by whole partitions, never one by one

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

Outbox and inbox rows are business data: the application's record of what it sent and handled.
The tables still grow without end unless old rows leave. Deleting rows needs vacuum and slows the
claim. On a partitioned table, a primary key only holds inside one partition.

## Decision

- No code deletes outbox or inbox rows one by one. Both tables are range-partitioned by time, and
  whole partitions expire, and only when a retention is set. Retention is off by default.
- The inbox is partitioned by the envelope's `time`, not by when the receipt was written.
- An expired partition is detached by default, and dropped only when asked.
- An outbox partition that still holds pending or failed rows never expires.

## Consequences

- Expiring a partition is one cheap statement that leaves no bloat.
- A redelivery carries the same envelope, so its receipt lands in the same partition as the
  first one and the unique constraint on endpoint, message id and message time
  (`uq_inbox_receipts_endpoint_message`) catches it, while that partition is kept. The primary
  key can't: each receipt gets a new id.
- Detaching keeps the data as a table to archive or query; dropping it is the step nobody can
  undo.
- The inbox retention must be longer than the longest redelivery, or a very late redelivery is
  handled again. A message whose `time` was changed between deliveries isn't deduplicated.
- A row whose time has no partition yet lands in the default partition, which the maintenance
  worker warns about.

## Alternatives considered

- **Deleting dispatched and old rows with `DELETE`:** needs vacuum, slows the claim, and removes
  business data by default.
- **Partitioning the inbox by when the receipt was written:** a redelivery on another day would
  land in another partition, where the unique constraint can't see the first receipt.
- **Dropping an expired partition by default:** it can't be undone.
