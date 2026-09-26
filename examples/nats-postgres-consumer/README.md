# NATS JetStream consumer with a PostgreSQL inbox

This example combines `NatsDeliverySource`, `NatsMapper`, and the generic typed `Consumer`
directly; there is no NATS-specific consumer façade. The handler writes an order projection row
inside the transaction that completes the inbox receipt. The consumer acknowledges the JetStream
message only after that transaction commits.

Before running it:

1. Use PostgreSQL 18 or newer and apply the versioned Sisa inbox migrations with Atlas.
2. Create this application table in the same PostgreSQL schema selected by the pool's
   `search_path`:

   ```sql
   CREATE TABLE order_projection (
       message_id uuid NOT NULL,
       order_id text NOT NULL,
       amount_cents bigint NOT NULL
   );
   ```

3. Provision a JetStream stream `ORDERS` that captures `orders.>`, and a durable pull consumer
   `orders-projection` on it. The consumer needs explicit acknowledgement and either an unlimited
   `max_deliver` or one of at least the inbox `max_attempts`. Publish through `NatsPublisher` with a
   `TypeSubjectResolver` over the `orders` prefix, so messages carry the headers and subject that
   `NatsMapper` decodes. `OrderCreated` is the type in `src/main.rs`.
4. Set `SISA_NATS_URL` and `SISA_POSTGRES_URL` in the environment.

Run from the repository root:

```sh
cargo +1.95.0 run -p nats-postgres-consumer
```

The example uses the default `SettlementMode::Broker`. It acknowledges completed work, negatively
acknowledges retryable failures with a delay, and terminates malformed or dead deliveries. Press
Ctrl-C to cancel and drain the consumer for at most its drain timeout. A second Ctrl-C aborts the
remaining work, leaves unfinished deliveries unacknowledged for broker redelivery, and exits with
status 130.

The application owns connection policy, stream and consumer provisioning, migration execution,
monitoring, and restarts. It must keep the durable name and the inbox scope stable for this
handler.
