-- Version 1 of the outbox and inbox tables, in the default schema `sisa_messaging`. `migrate` runs
-- this with the configured schema in place of `sisa_messaging`, then creates the daily partitions.

CREATE TABLE sisa_messaging.outbox_messages (
    id                uuid        NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    type              text        NOT NULL,
    source            text        NOT NULL,
    subject           text,
    partition_key     text,
    data_content_type text        NOT NULL,
    data_schema       text,
    attributes        jsonb       NOT NULL,
    data              bytea       NOT NULL,
    status            text        NOT NULL DEFAULT 'pending',
    available_at      timestamptz NOT NULL DEFAULT now(),
    attempts          integer     NOT NULL DEFAULT 0,
    last_error        text,
    dispatched_at     timestamptz,
    CONSTRAINT pk_outbox_messages PRIMARY KEY (created_at, id),
    CONSTRAINT ck_outbox_messages_status CHECK (status IN ('pending', 'dispatched', 'failed'))
) PARTITION BY RANGE (created_at);
CREATE INDEX ix_outbox_messages_due ON sisa_messaging.outbox_messages (available_at, id)
    WHERE status = 'pending';
CREATE INDEX ix_outbox_messages_id ON sisa_messaging.outbox_messages (id);
CREATE TABLE sisa_messaging.outbox_messages_default PARTITION OF sisa_messaging.outbox_messages DEFAULT;

CREATE TABLE sisa_messaging.inbox_receipts (
    id             uuid        NOT NULL,
    message_time   timestamptz NOT NULL,
    endpoint       text        NOT NULL,
    message_id     uuid        NOT NULL,
    type           text        NOT NULL,
    source         text        NOT NULL,
    correlation_id uuid,
    attributes     jsonb       NOT NULL,
    data           bytea,
    received_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_inbox_receipts PRIMARY KEY (message_time, id),
    CONSTRAINT uq_inbox_receipts_endpoint_message UNIQUE (endpoint, message_id, message_time)
) PARTITION BY RANGE (message_time);
CREATE TABLE sisa_messaging.inbox_receipts_default PARTITION OF sisa_messaging.inbox_receipts DEFAULT;

INSERT INTO sisa_messaging.schema_version (version) VALUES (1);
