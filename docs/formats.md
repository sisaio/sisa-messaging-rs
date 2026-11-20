# Formats

What goes over the wire and into the tables. Services in other languages share messages and
tables by following these formats. They change only with a version bump that the changelog calls
out. How each crate uses them is in its [spec](specs/README.md).

## Envelope on the wire

[CloudEvents 1.0](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md) in binary
content mode: the attributes go in the transport's headers and the body is the encoded message, so
consumers that don't know CloudEvents still read a plain body.

| Attribute | Required | Value |
| --- | --- | --- |
| `specversion` | Yes | `1.0` |
| `id` | Yes | The message id, a UUID version 7 |
| `source` | Yes | A URI reference naming the publishing service, set once per bus, such as `/orders` |
| `type` | Yes | The message type, such as `orders.order_placed.v1` |
| `time` | Yes | When it was published, RFC 3339 with a year from 0000 to 9999 and no leap second |
| `datacontenttype` | Yes | The codec's content type, such as `application/json` |
| `subject` | No | What the message is about within the source, such as an order id |
| `dataschema` | No | The schema's URI, for Avro and other schema codecs |
| `partitionkey` | No | Messages with one key keep their order; the CloudEvents Partitioning extension. The Kafka message key; a NATS subject token later |
| `traceparent`, `tracestate` | No | W3C Trace Context, per the CloudEvents Distributed Tracing extension |
| `correlationid` | No | Our extension: the id that ties a conversation together |
| `causationid` | No | Our extension: the id of the message whose handler published this one |
| `errorendpoint`, `errortype`, `errormessage`, `errorattempts` | Error destination only | Our extensions: where and why handling gave up |

Our extension names follow the CloudEvents rules (lowercase letters and digits, at most 20
characters), so other CloudEvents SDKs carry them. Receivers read binary mode only: on NATS, Iggy and Redis, a
message whose `ce-` or `ce_` headers or fields don't make an envelope, such as one in structured mode, goes to the
endpoint's error destination as it came, with `errorendpoint`, `errortype` and `errormessage`, and
isn't handled.

In 0.1.0 the publishers set neither `subject`, `dataschema` nor `partitionkey`, and a message
published outside a handler has no `correlationid`; envelopes from other producers keep the ones
they carry, through the outbox and the inbox too.

Each transport names the headers as its CloudEvents binding says. Iggy has no binding, so it
follows Kafka's, the closest one, and keeps every attribute in a header:

| Transport | Header names | Example |
| --- | --- | --- |
| NATS | `ce-` and the attribute, its value percent-encoded as the binding says | `ce-id`, `ce-type`, `ce-datacontenttype`; `ce-subject: Euro%20%E2%82%AC` |
| Kafka family | `ce_` and the attribute | `ce_id`, `ce_type`; `content-type` for the content type |
| Iggy | `ce_` and the attribute, as string user headers; the Iggy message id is the message id. A message without `ce_datacontenttype` takes its `content-type` header, as Kafka's binding has it | `ce_id`, `ce_type`, `ce_datacontenttype` |
| Redis | `ce_` and the attribute, as fields of the stream entry, with the data in the field `data`, as the Kafka binding has it since CloudEvents has none for Redis. A message without `ce_datacontenttype` takes its `content-type` field | `ce_id`, `ce_type`, `data` |
| AMQP | `cloudEvents:` and the attribute, from the AMQP 1.0 binding, since AMQP 0-9-1 has none | `cloudEvents:id`; the content type in the `content_type` property |

On NATS a space, `"`, `%` and every character outside printable ASCII becomes `%XY` of its UTF-8
bytes, so a line break can't end a header early; a receiver decodes each value once and sends one
that doesn't decode to UTF-8 to the error destination. On NATS the publish also sets `Nats-Msg-Id` to the message id, so JetStream drops a duplicate
publish inside the stream's duplicate window. A copy to the error subject goes without it, so the
stream keeps it next to the original of the same id. Iggy has no deduplication by id, so it keeps
every publish. An Iggy header value holds at most 255 bytes: a publish with a longer attribute
fails, and an error copy cuts it there.

## NATS subjects and streams

| Thing | Name | Example |
| --- | --- | --- |
| Subject | `{prefix}.{message type}` | `sisa.orders.order_placed.v1` |
| Stream | One per prefix, capturing `{prefix}.>` | `SISA`, on `sisa.>` |
| Durable consumer | The endpoint name, filtered on the subjects of the messages it handles. A new one starts with the messages published after it is created; an existing one keeps its subjects and gains the missing ones | `billing` |
| Error subject | `{prefix}._error.{endpoint}` | `sisa._error.billing` |

The prefix keeps deployments that share a server apart, such as `prod` and `staging`. Give each
its own stream too: connecting sets the stream's subjects to its prefix's.

## Iggy streams and topics

| Thing | Name | Example |
| --- | --- | --- |
| Stream | `IggySettings::stream`, created on connect | `sisa` |
| Topic | The message type, created on the first publish or subscribe with `IggySettings::partitions` partitions (4 by default) | `orders.order_placed.v1` |
| Consumer group | The endpoint name, on the topic of each message type it handles. A new group reads each partition from its first message, so a new endpoint also gets what is already in its topics | `billing` on `orders.order_placed.v1` |
| Error topic | `_error.{endpoint}`, one partition | `_error.billing` |

The stream keeps deployments that share a server apart, such as `prod` and `staging`. An
endpoint's instances share its group's partitions, so a topic's partitions cap how many of them
work on it at once. A message with a `partitionkey` goes to the partition its key picks.

How `IggyDelivery` builds an ack, a retry and a reject on a log is in the
[Iggy spec](specs/iggy.md#behaviour).

## Redis streams and groups

The same names and commands on Redis, Valkey and Dragonfly.

| Thing | Name | Example |
| --- | --- | --- |
| Stream | `{prefix}:{message type}`, created by the first publish or subscribe | `sisa:orders.order_placed.v1` |
| Consumer group | The endpoint name, on the stream of each message type it handles. A new one starts with the messages published after it is created, and a stream deleted since gets its group back the same way on the endpoint's next look, after any read or refresh in progress; messages published before then don't reach it | `billing` |
| Consumer | One per instance and subscription, named with a UUID version 7. One with nothing pending that hasn't read for an hour is removed by another instance, in a Lua script that checks again that nothing is pending | `019a9b5e-...` |
| Error stream | `{prefix}:_error:{endpoint}` | `sisa:_error:billing` |

The prefix keeps deployments that share a server apart, such as `prod` and `staging`. A stream has
no partitions: an endpoint's instances read each stream as competing consumers, and a
`partitionkey` is carried but picks nothing.

How an instance reads, and how `RedisDelivery` builds a delayed retry and a takeover from the
group's pending entries, is in the [Redis spec](specs/redis.md#behaviour).

## PostgreSQL tables

Created by the SQL migrations in [`migrations/postgresql`](../migrations/postgresql), which the
application runs itself: with `sqlx migrate run --source migrations/postgresql` in its deploy
pipeline, or by hand with psql, into a schema created beforehand. Pick one way per schema: a psql
run leaves no row in `_sqlx_migrations`, so a later `sqlx migrate run` would run the migration again
and fail. The SQL names no schema, so the connection's `search_path` picks it, for the migrations
and for the queries alike, such as `options=-c search_path=messaging` in the URL. A schema of its
own also keeps sqlx's `_sqlx_migrations` table for these migrations apart from the application's.
Columns that hold a CloudEvents attribute take its name. A primary key on a partitioned table must
hold the partition key, so each one starts with it.

Every constraint and index is named in the SQL, never left to PostgreSQL or a tool to generate,
so the names stay the same across versions and the next migration can refer to them. A name is a
two-letter prefix and the table, then the columns or purpose, within PostgreSQL's 63 characters:

| Prefix | Object | Example |
| --- | --- | --- |
| `pk_` | Primary key | `pk_outbox_messages` |
| `uq_` | Unique constraint | `uq_inbox_receipts_endpoint_message` |
| `fk_` | Foreign key | none yet |
| `ck_` | Check constraint | `ck_outbox_messages_status` |
| `ix_` | Index | `ix_outbox_messages_due` |

Constraints go inside `CREATE TABLE`. PostgreSQL has no inline index syntax, so an index that
isn't backing a constraint, such as the partial one for the claim, is a `CREATE INDEX` right after
its table. A partition's own copies of these get names PostgreSQL derives from the partition
table; nothing refers to them.

```sql
CREATE TABLE outbox_messages (
    id                uuid        NOT NULL,              -- the message id, UUID v7: claim order
    created_at        timestamptz NOT NULL DEFAULT now(), -- the partition key
    type              text        NOT NULL,              -- the message type, which picks the destination
    source            text        NOT NULL,
    subject           text,
    partition_key     text,
    data_content_type text        NOT NULL,
    data_schema       text,
    attributes        jsonb       NOT NULL,              -- time, correlation, trace, extensions
    data              bytea       NOT NULL,
    status            text        NOT NULL DEFAULT 'pending',
    available_at      timestamptz NOT NULL DEFAULT now(),     -- due time, moved forward by a claim
    attempts          integer     NOT NULL DEFAULT 0,
    last_error        text,
    dispatched_at     timestamptz,
    CONSTRAINT pk_outbox_messages PRIMARY KEY (created_at, id),
    CONSTRAINT ck_outbox_messages_status CHECK (status IN ('pending', 'dispatched', 'failed'))
) PARTITION BY RANGE (created_at);
CREATE INDEX ix_outbox_messages_due ON outbox_messages (available_at, id)
    WHERE status = 'pending';
CREATE INDEX ix_outbox_messages_id ON outbox_messages (id);

CREATE TABLE inbox_receipts (
    id             uuid        NOT NULL,                 -- the receipt's own id, UUID v7
    message_time   timestamptz NOT NULL,                 -- the envelope's time: the partition key
    endpoint       text        NOT NULL,
    message_id     uuid        NOT NULL,
    type           text        NOT NULL,
    source         text        NOT NULL,
    correlation_id uuid,
    attributes     jsonb       NOT NULL,
    data           bytea,                                -- kept only with `keep_data(true)`
    received_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_inbox_receipts PRIMARY KEY (message_time, id),
    CONSTRAINT uq_inbox_receipts_endpoint_message        -- the deduplication
        UNIQUE (endpoint, message_id, message_time)
) PARTITION BY RANGE (message_time);

-- For each table, the partitions for today and the next 7 intervals and a default partition,
-- made by the migration so the first write has a partition, then kept ahead by the maintenance
-- worker. Days are UTC days: the bounds carry `+00`, so the session's time zone doesn't move them.
CREATE TABLE outbox_messages_p20261102 PARTITION OF outbox_messages
    FOR VALUES FROM ('2026-11-02 00:00+00') TO ('2026-11-03 00:00+00');
CREATE TABLE outbox_messages_default PARTITION OF outbox_messages DEFAULT;
CREATE TABLE inbox_receipts_p20261102 PARTITION OF inbox_receipts
    FOR VALUES FROM ('2026-11-02 00:00+00') TO ('2026-11-03 00:00+00');
CREATE TABLE inbox_receipts_default PARTITION OF inbox_receipts DEFAULT;
```

The claim and the receipt, as statements, are in the
[PostgreSQL spec](specs/postgres.md#behaviour).
