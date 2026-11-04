-- The outbox and inbox tables. No name is qualified with a schema: the connection's search_path
-- picks it, such as `options=-c search_path=messaging` in the URL. Run it with
-- `sqlx migrate run --source migrations/postgresql`, or by hand with psql.

CREATE TABLE outbox_messages (
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
CREATE INDEX ix_outbox_messages_due ON outbox_messages (available_at, id) WHERE status = 'pending';
CREATE INDEX ix_outbox_messages_id ON outbox_messages (id);
CREATE TABLE outbox_messages_default PARTITION OF outbox_messages DEFAULT;

CREATE TABLE inbox_receipts (
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
CREATE TABLE inbox_receipts_default PARTITION OF inbox_receipts DEFAULT;

-- A partition per UTC day for today and the next 7 days, so the first writes have one; the
-- maintenance worker keeps them ahead after this. The bounds carry `+00`, so the session's time
-- zone doesn't move them.
DO $$
DECLARE
    day date;
    parent text;
BEGIN
    FOR day IN
        SELECT (now() AT TIME ZONE 'UTC')::date + ahead FROM generate_series(0, 7) AS ahead
    LOOP
        FOREACH parent IN ARRAY ARRAY['outbox_messages', 'inbox_receipts'] LOOP
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                parent || '_p' || to_char(day, 'YYYYMMDD'),
                parent,
                day || ' 00:00+00',
                day + 1 || ' 00:00+00'
            );
        END LOOP;
    END LOOP;
END
$$;
