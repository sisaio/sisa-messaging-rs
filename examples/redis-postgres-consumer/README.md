# Redis Streams consumer with a PostgreSQL inbox

This example combines `RedisDeliverySource`, `RedisMapper`, and the generic typed `Consumer`
directly. The handler inserts an order receipt inside the transaction that completes the inbox
receipt. The consumer acknowledges the Redis entry only after that transaction commits.

Before running it:

1. Use PostgreSQL 18 or newer and apply the versioned Sisa inbox migrations with Atlas.
2. Create this application table in the same PostgreSQL schema selected by the pool's
   `search_path`:

   ```sql
   CREATE TABLE example_order_receipts (order_id text PRIMARY KEY);
   ```

3. Provision a Redis stream and consumer group on Redis 7, Valkey 8, or Dragonfly 2.0.0.
   Publish through `RedisPublisher`, which writes the required `v`, `h`, and `p` stream fields.
   For example, with the application's Redis `client` and provisioned `stream` in scope:

   ```rust
   use sisa_messaging::{Envelope, JsonSerializer, MessageId, Metadata, Serializer};
   use sisa_messaging_redis::RedisPublisher;
   use std::time::Duration;

   let publisher = RedisPublisher::new(
       client.get_multiplexed_async_connection().await?,
       stream.clone(),
       Duration::from_secs(2),
   )?;
   let envelope = Envelope::new(
       MessageId::new(),
       OrderCreated { order_id: "sample-order".to_owned() },
       Metadata::default(),
   )?;
   publisher.append(&JsonSerializer.serialize(&envelope)?).await?;
   ```

   `OrderCreated` is the type in `src/main.rs`. Garnet 2.1.8 does not support the required `XADD`
   command.
4. Set `SISA_POSTGRES_URL`, `SISA_REDIS_URL`, `SISA_REDIS_STREAM`, `SISA_REDIS_GROUP`, and
   `SISA_REDIS_CONSUMER` in the environment. Use a distinct consumer name for each live process.

Run from the repository root:

```sh
cargo +1.95.0 run -p redis-postgres-consumer
```

Press Ctrl-C to cancel and drain the consumer. The example uses
`SettlementMode::PendingRecovery`: unresolved entries stay pending and become eligible for a later
bounded reclaim. Malformed entries or durable dead results stop the consumer for operator action.
The application owns connection policy, stream and group provisioning, migration execution,
monitoring, and restarts. It must keep the inbox scope stable for this handler.
