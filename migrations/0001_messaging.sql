-- Sisa Messaging: fresh PostgreSQL 18+ baseline.
--
-- Atlas Community Edition applies this versioned migration to the schema selected by the
-- PostgreSQL connection search_path. Runtime SQL uses the same unqualified, fixed table names.
-- Every supporting index, including a unique supporting index, is created explicitly with an
-- ix_<table>_<purpose> name. Primary-key indexes retain their constraint-derived pk_* names. Once
-- released, this migration is immutable.
-- See docs/database.md for state transitions, query shapes, and retention rules.

-- Durable publication requests written in the application's business transaction. Workers claim
-- rows here, publish outside the transaction, and fence every outcome with claim_token.
CREATE TABLE outbox_messages (
    -- Internal UUIDv7 row identity; also the deterministic tiebreaker within an ordering key.
    id              uuid        NOT NULL DEFAULT uuidv7(),
    -- Stable envelope identity used to detect duplicate enqueue and for broker deduplication.
    message_id      uuid        NOT NULL,

    -- Contract identity used to select and version the message decoder.
    message_type    text        NOT NULL,
    message_version integer     NOT NULL,
    -- Serialized body format and bytes. The library does not interpret payload bytes in SQL.
    content_type    text        NOT NULL,
    payload         bytea       NOT NULL,
    -- Additive envelope metadata; constrained to a JSON object for predictable key lookup.
    metadata        jsonb       NOT NULL,

    -- Optional resolved business key used to prevent concurrent publication within one key.
    ordering_key    text,

    -- Creation time is audit data; claimable_at is initial availability, lease, or retry backoff.
    created_at      timestamptz NOT NULL DEFAULT now(),
    claimable_at    timestamptz NOT NULL DEFAULT now(),
    -- Deadline for starting a new claim; an already in-flight publication may still finish.
    expires_at      timestamptz,
    -- Per-claim fencing value. Outcome updates must match it to reject stale workers.
    claim_token     uuid,
    -- Diagnostic worker identity; ownership correctness comes from claim_token, not this value.
    locked_by       text,

    -- Number of recorded publication outcomes, not the number of claims.
    attempts        integer     NOT NULL DEFAULT 0,
    -- Mutually exclusive terminal timestamps. Null in both columns means non-terminal.
    published_at    timestamptz,
    dead_at         timestamptz,
    -- Stable terminal category and a bounded/redacted diagnostic summary.
    dead_reason     text,
    last_error      text,

    -- Structural checks validate counters, versions, and metadata shape. State checks reject
    -- impossible lease and terminal combinations at the write boundary.
    CONSTRAINT pk_outbox_messages                 PRIMARY KEY (id),
    CONSTRAINT ck_outbox_messages_attempts        CHECK (attempts >= 0),
    CONSTRAINT ck_outbox_messages_message_version CHECK (message_version >= 0),
    CONSTRAINT ck_outbox_messages_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT ck_outbox_messages_claim           CHECK ((claim_token IS NULL) = (locked_by IS NULL)),
    CONSTRAINT ck_outbox_messages_terminal_claim  CHECK (
        (published_at IS NULL AND dead_at IS NULL)
        OR (claim_token IS NULL AND locked_by IS NULL)
    ),
    CONSTRAINT ck_outbox_messages_terminal        CHECK (published_at IS NULL OR dead_at IS NULL),
    CONSTRAINT ck_outbox_messages_dead_reason     CHECK ((dead_at IS NULL) = (dead_reason IS NULL))
);

-- Enforces one durable outbox row per envelope identity.
CREATE UNIQUE INDEX ix_outbox_messages_message_id
    ON outbox_messages (message_id);

-- Main worker scan: due non-terminal rows ordered by availability and stable row identity.
-- expires_at is carried in the index for the claim query's new-claim deadline check.
CREATE INDEX ix_outbox_messages_claimable
    ON outbox_messages (claimable_at, id) INCLUDE (expires_at)
    WHERE published_at IS NULL AND dead_at IS NULL;

-- Supports the per-key head check; terminal rows cannot block a later message for the same key.
CREATE INDEX ix_outbox_messages_ordering_key
    ON outbox_messages (ordering_key, id)
    WHERE ordering_key IS NOT NULL AND published_at IS NULL AND dead_at IS NULL;

-- Supports bounded expiry sweeps over non-terminal rows that have an expiry deadline.
CREATE INDEX ix_outbox_messages_expires
    ON outbox_messages (expires_at)
    WHERE expires_at IS NOT NULL AND published_at IS NULL AND dead_at IS NULL;

-- Supports retention scans and bounded purges of successfully published rows.
CREATE INDEX ix_outbox_messages_published
    ON outbox_messages (published_at)
    WHERE published_at IS NOT NULL;

-- Supports stable dead-letter listing and retention cursors ordered by death time then row ID.
CREATE INDEX ix_outbox_messages_dead_cursor
    ON outbox_messages (dead_at, id)
    WHERE dead_at IS NOT NULL;

-- Durable deduplication receipts. Claim and completion participate in the caller's business
-- transaction so in-transaction side effects and completed_at become visible atomically.
CREATE TABLE inbox_receipts (
    -- Internal UUIDv7 operator handle and deterministic dead-letter cursor tiebreaker.
    id              uuid        NOT NULL DEFAULT uuidv7(),
    -- Consumer-defined namespace; together with message_id it identifies one durable receipt.
    scope           text        NOT NULL,
    -- Stable envelope identity; uniqueness is enforced together with scope below.
    message_id      uuid        NOT NULL,

    -- Contract identity retained for diagnostics and operator tooling; inbox stores no payload.
    message_type    text        NOT NULL,
    message_version integer     NOT NULL,
    -- Additive envelope metadata used for diagnostics and trace continuity.
    metadata        jsonb       NOT NULL,

    -- First durable observation time; it is not a non-terminal retention deadline.
    received_at     timestamptz NOT NULL DEFAULT now(),
    -- Number of failures successfully recorded after the handler transaction rolled back.
    attempts        integer     NOT NULL DEFAULT 0,
    -- Mutually exclusive terminal timestamps. Both null means pending or retrying.
    completed_at    timestamptz,
    dead_at         timestamptz,
    -- Stable terminal category and a bounded/redacted diagnostic summary.
    dead_reason     text,
    last_error      text,

    -- Structural checks validate counters, versions, metadata shape, and namespace size. State
    -- checks reject contradictory terminal outcomes at the write boundary.
    CONSTRAINT pk_inbox_receipts                 PRIMARY KEY (id),
    CONSTRAINT ck_inbox_receipts_attempts        CHECK (attempts >= 0),
    CONSTRAINT ck_inbox_receipts_message_version CHECK (message_version >= 0),
    CONSTRAINT ck_inbox_receipts_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT ck_inbox_receipts_scope_len       CHECK (octet_length(scope) BETWEEN 1 AND 128),
    CONSTRAINT ck_inbox_receipts_terminal        CHECK (completed_at IS NULL OR dead_at IS NULL),
    CONSTRAINT ck_inbox_receipts_dead_reason     CHECK ((dead_at IS NULL) = (dead_reason IS NULL))
);

-- Rejects a second receipt for the same message identity within one consumer scope.
CREATE UNIQUE INDEX ix_inbox_receipts_scope_message_id
    ON inbox_receipts (scope, message_id);

-- Supports retention scans and bounded purges of successfully completed receipts.
CREATE INDEX ix_inbox_receipts_completed
    ON inbox_receipts (completed_at)
    WHERE completed_at IS NOT NULL;

-- Supports stable dead-letter listing and retention cursors ordered by death time then row ID.
CREATE INDEX ix_inbox_receipts_dead
    ON inbox_receipts (dead_at, id)
    WHERE dead_at IS NOT NULL;
