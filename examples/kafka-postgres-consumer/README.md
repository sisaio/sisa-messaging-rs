# Kafka partitioned consumer with a PostgreSQL inbox

This example combines `KafkaDeliverySource`, `KafkaEnvelopeMapper`, and the generic typed
partitioned `Consumer` directly; there is no Kafka-specific consumer façade, and the application
never imports rdkafka. The handler writes an order projection row inside the transaction that
completes the inbox receipt. The source advances a partition's committed offset only after that
transaction commits, through a transactional offset commit bound to the consumer-group generation
that assigned the partition.

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

3. Create the `orders` topic. The source checks that it exists when it opens and never creates
   it. Publish through `KafkaPublisher` so records carry the headers `KafkaEnvelopeMapper`
   decodes. `OrderCreated` is the type in `src/main.rs`.
4. Grant the consumer group `orders-projection`, the topic, and the transactional identity
   `sisa.17.orders-projection.<instance id>` (the group id prefixed by its byte length) to the
   principal the client authenticates as. The broker must support transactions.
5. Set `SISA_KAFKA_BOOTSTRAP_SERVERS`, `SISA_KAFKA_INSTANCE_ID`, and `SISA_POSTGRES_URL` in the
   environment. Give each running instance a distinct, stable instance id; a second live
   instance with the same id fences the first.

Run from the repository root:

```sh
cargo +1.95.0 run -p kafka-postgres-consumer
```

Each partition has at most one record in flight, and other partitions continue while one is
blocked. A record whose handler fails, or whose inbox commit fails, stops the consumer with its
offset unadvanced; the application's supervisor restarts it, and the record replays from the
committed offset. Press Ctrl-C to cancel and drain the consumer for at most its drain timeout.
The Kafka consumer then closes within its shutdown bound. A second Ctrl-C aborts the remaining
work, leaves unfinished records unadvanced for replay, and exits with status 130.

The application owns connection, authentication, and TLS policy (as advanced client
properties), topic provisioning, migration execution, monitoring, and restarts. It must keep the
group, the instance ids, and the inbox scope stable for this handler.
