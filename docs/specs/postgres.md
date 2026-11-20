# Crate: sisa-messaging-postgres

Status: **Implemented** in 0.1.0 · Published: yes · Label: `area:postgres` · Commit scope: `postgres`

## Purpose

The three stores on PostgreSQL, through sqlx: the row writes, the claim, the receipt, the
partition DDL and the advisory lock. Only SQL: when to claim, how long a lease is, the backoff
and when to create or expire a partition are the [runtime's](runtime.md#behaviour). The tables
come from the migrations in [`migrations/postgresql`](../../migrations/postgresql), which the
application runs.

## Public contract

Main types: `PostgresOutbox`, `PostgresInbox` and `PostgresPartitions`, which implement core's
[store contracts](core.md#stores): `OutboxStore`, `InboxStore` and `PartitionStore`.
`PostgresPartitions::outbox(pool)` names the table a maintenance worker keeps.

## Behaviour

The outbox orders by `id` instead of a sequence column: a UUID version 7 starts with its
millisecond, and one process makes them in order inside a millisecond. Rows from two instances in
the same millisecond may swap, which the guarantees already allow.

A claim is one statement. The partial index holds only pending rows, so old partitions cost the
claim next to nothing. `RETURNING` has no order of its own, so the outer query sorts the claimed
rows:

```sql
WITH claimed AS (
    UPDATE outbox_messages
    SET available_at = now() + $2
    WHERE (created_at, id) IN (
        SELECT created_at, id FROM outbox_messages
        WHERE status = 'pending' AND available_at <= now()
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
SELECT * FROM claimed ORDER BY id;
```

A receipt is `INSERT ... ON CONFLICT DO NOTHING`; no row inserted means a duplicate.

The partition store runs the DDL the runtime's maintenance worker asks for, one instance at a
time through a session advisory lock on the table:
[Partitions and retention](runtime.md#partitions-and-retention).

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits). The
SQL never names a schema: the connection's `search_path` picks it. No code deletes outbox or
inbox rows; data leaves only by expiring a partition
([ADR-0003](../adr/0003-rows-leave-by-partition.md)).

## Errors

The stores return core's [`StoreError`](core.md#errors) and have no error type of their own.

## Formats

[PostgreSQL tables](../formats.md#postgresql-tables): the tables, the constraint and index
names, and how to run the migrations.
