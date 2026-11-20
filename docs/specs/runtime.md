# Crate: sisa-messaging-runtime

Status: **Implemented** in 0.1.0 · Published: yes · Label: `area:runtime` · Commit scope: `runtime`

## Purpose

The engine, the same for every transport and database: the bus, the receive pipeline with the
inbox step, the dispatcher, the partition maintenance scheduler, retries, and an in-memory
transport for tests. A transport or database crate brings no logic of its own to these.

## Public contract

Main types: `Bus`, `BusBuilder`, `EndpointBuilder`, `OutboxPublisher`, `HandlerContext`,
`Handler`, `RetryPolicy`, `DispatchSettings`, `PartitionSettings`, `PartitionMode` and
`InMemoryTransport`.

Three types implement core's [`Publisher`](core.md#publisher), and the code that owns the
transaction picks one:

| Publisher | Where it sends | When to use it |
| --- | --- | --- |
| `Bus` | Straight to the broker; returns when the broker has it | Outside a database transaction: before it begins or after it commits |
| `OutboxPublisher`, from `bus.outbox(&mut tx)` | A row in `outbox_messages`, in the caller's transaction; the dispatcher sends it after the commit | Inside the caller's transaction, so the message goes out only if it commits. A committed row that fails every attempt stays `failed` and isn't sent until it is set back to `pending` |
| `HandlerContext` | As the endpoint is set up: a row in the handler's transaction on an endpoint with an inbox and a bus with an outbox, otherwise held and sent after the handler succeeds | Inside a handler |

```rust,ignore
type AppBus = Bus<NatsTransport, PostgresOutbox>;

let bus: AppBus = Bus::builder("/orders", NatsTransport::connect(NatsSettings::default()).await?)
    .outbox(PostgresOutbox::new(pool.clone())) // optional; changes the type
    .partitions(PostgresPartitions::outbox(pool.clone()), PartitionSettings::default())
    .endpoint("billing", |endpoint| {
        endpoint.inbox(PostgresInbox::new(pool.clone())) // optional; after the outbox
            .handle::<OrderPlaced>(charge_order)
    })
    .start()
    .await?;

// Direct, outside a transaction.
bus.publish(&OrderPlaced { id }).await?;

// Through the outbox: the application opens and commits its own transaction, as usual.
let mut tx = pool.begin().await?;
sqlx::query("insert into orders ...").execute(&mut *tx).await?;
bus.outbox(&mut tx).publish(&OrderPlaced { id }).await?;
tx.commit().await?;

// Domain code doesn't care which one it gets.
async fn place_order<P: Publisher>(publisher: &mut P, order: Order) -> Result<(), P::Error> { .. }
```

The bus carries its outbox in its type. `outbox` exists only on a bus built with one, and
accepts only that store's connection type, so a missing outbox or a transaction from another
database is a compile error, never a silent direct send. `bus.outbox(&mut tx)` borrows the
transaction only for the call, so the application keeps using `tx` between publishes.

A handler gets the message and a `HandlerContext`. The context publishes with the handled message
as the causation, and its correlation id and trace context carried over unchanged. On an endpoint
with an inbox, it also hands out the delivery's transaction: the handler takes `&mut HandlerContext<PgTransaction<'static>>` and runs its
queries on `context.transaction()`.

```rust,ignore
async fn charge_order(
    order: OrderPlaced,
    context: &mut HandlerContext<PgTransaction<'static>>,
) -> Result<(), HandlerError> {
    sqlx::query("insert into charges ...").execute(context.transaction()).await?;
    context.publish(&InvoiceSent { order: order.id }).await?; // an outbox row in the same transaction
    Ok(())
}
```

An endpoint's inbox must write in a transaction the bus's outbox can write in too, so an inbox and
an outbox on different databases don't compile. The bus checks this when the endpoint is added,
so the outbox comes first; an endpoint with an inbox added before it fails `start`.

## Behaviour

What each mode gives is in [Architecture](../architecture.md#how-each-mode-behaves), and
[Flows](../flows.md) draws the steps below.

### Receiving

For each delivery, an endpoint:

1. Decodes the data with the bus's codec. Data of another content type or that doesn't decode, and a type the
   endpoint has no handler for, go to the error destination at once, without retries, since no
   retry can fix them; the delivery is rejected once that publish succeeds, as in step 5.
2. With an inbox, begins a transaction and records the message. A message recorded before is
   committed and acked here.
3. Runs the handler.
4. On success, writes the handler's publishes to the outbox (or holds them), commits, sends any
   held publishes, then acks. With an inbox and no outbox, the held publishes are sent after the
   commit, which can't be rolled back: if the process stops or a send fails, the redelivery finds
   the receipt and is acked without running the handler, so those publishes are lost.
5. On an error, rolls back and asks the transport to retry after the retry policy's delay: 5
   attempts by default, 1 second after the first failure and twice as long after each next one,
   at most a minute. A handler that panics fails like one that returns an error. Past
   the policy's last attempt, it publishes the envelope to the endpoint's error destination with
   the error in its headers, then rejects the delivery. If that publish fails, the delivery is
   retried after the policy's delay instead of rejected, so a message is never dropped without a
   copy in the error destination.

### Dispatching the outbox

The dispatcher is a runtime worker the bus starts when it has an outbox; a store only claims and
marks rows. Each round it claims a batch of due rows, publishes them in id order (UUID version 7,
so creation order), waiting for the broker each time, and marks them
`dispatched` with the time. A row that fails is put back with a later due time and its error, and
its `attempts` counts one more failure; past the last attempt it is marked `failed` and left for a
person to look at; setting it back to `pending` sends it again. Claiming moves the due
time forward by a lease, so the rows of a dispatcher that dies come back once the lease runs out,
and several instances claim different rows. A dispatcher publishes no row of a batch whose lease
ran out, and gives up waiting on a publish when the lease runs out, since another instance may hold
the row by then. The envelope keeps its id in the outbox, so a
row published twice (a crash between the publish and the mark) reaches the broker with the same
id: JetStream drops it inside its duplicate window, Iggy and Redis keep both, and an inbox drops it after. A row that doesn't
read back as an envelope is marked `failed` by the claim, so it can't hold up the others.

`DispatchSettings` sets the poll interval (1 second), the batch size (100), the lease (30
seconds) and the retry policy (10 attempts, 1 second apart at first, at most a minute apart). A
round that published a full batch is followed by the next at once.

### Partitions and retention

Rows are never deleted one by one: the outbox and inbox are the application's record of what it
sent and handled. Both tables are range-partitioned by time, one partition per day by default.

- **The outbox** is partitioned by `created_at`, when the row was written.
- **The inbox** is partitioned by `message_time`, the envelope's own `time`, not by when the
  receipt was written. A redelivery carries the same envelope, so its receipt falls in the same
  partition and the unique constraint on endpoint, message id and message time catches it, while
  that partition is kept. Once the partition has expired, a redelivery is handled again, so keep
  the inbox longer than the longest redelivery.
- **Maintenance** is a runtime worker the bus starts for each table given to
  `BusBuilder::partitions`, such as `PostgresPartitions::outbox(pool)`; the store only runs the
  DDL. Once an hour, with one instance at a time through a session advisory lock on the table, it
  creates the partitions for the current interval and the next 7 (`PartitionSettings::ahead`), a
  day, week or month each (`interval`), and applies the retention. `PartitionMode::External` in
  `partitions` turns it off for teams that use pg_partman or their own jobs.
- **Retention** is off by default. When set, such as `retention_days = Some(90)`, a partition older
  than that is detached, which keeps it as an ordinary table for archiving, or dropped with
  `on_expiry = Expiry::Drop`. An outbox partition that still holds `pending` or `failed` rows is never
  expired; the worker warns instead. Keep the inbox retention longer than the longest redelivery, or a very
  late redelivery is handled again.
- **A default partition** catches a row whose time has no partition yet, such as a message from a
  publisher with a clock far off. The worker warns while it holds rows, since PostgreSQL can't
  create a partition over rows already sitting in the default one.

## Guarantees and limits

The library's guarantees are in [Architecture](../architecture.md#guarantees-and-limits): they
are this crate's, since the engine is what acks, dispatches and expires. The rules it must keep
are in [Rules that hold the design together](../architecture.md#rules-that-hold-the-design-together).

## Errors

| Type | Says |
| --- | --- |
| `PublishError` | Why a `Bus`, an `OutboxPublisher` or a `HandlerContext` couldn't publish a message |
| `StartError` | Why a bus couldn't start |
| `HandlerError` | Why a handler failed: an error's type and message, which go to the error destination as the `errortype` and `errormessage` extensions once the endpoint gives up. Any error converts into it, so a handler uses `?` on its own errors |

## Formats

The runtime sets the envelope's attributes and the error extensions:
[Envelope on the wire](../formats.md#envelope-on-the-wire). The tables it dispatches from and
records in: [PostgreSQL tables](../formats.md#postgresql-tables).
