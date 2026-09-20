use std::{error::Error, fmt, str::FromStr};

use sisa_messaging::{
    ContentType, Envelope, ErrorClassifier, FailureKind, Message, MessageId, Metadata,
    SerializedEnvelope, Serializer,
};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TestMessage;

impl Message for TestMessage {
    const TYPE: &'static str = "postgres.test-message";
    const VERSION: u32 = 1;
}

#[derive(Debug)]
pub(super) struct TestSerializerError;

impl fmt::Display for TestSerializerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("test serializer failed")
    }
}

impl Error for TestSerializerError {}

impl ErrorClassifier for TestSerializerError {
    fn classify(&self) -> FailureKind {
        FailureKind::Permanent
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct TestSerializer;

impl Serializer<TestMessage> for TestSerializer {
    type Error = TestSerializerError;

    fn serialize(
        &self,
        envelope: &Envelope<TestMessage>,
    ) -> Result<SerializedEnvelope, Self::Error> {
        Ok(SerializedEnvelope {
            message_id: envelope.message_id(),
            message_type: envelope.message_type().clone(),
            message_version: envelope.message_version(),
            content_type: ContentType::new("application/test").map_err(|_| TestSerializerError)?,
            payload: Vec::new(),
            metadata: envelope.metadata().clone(),
            ordering_key: envelope.ordering_key().cloned(),
        })
    }

    fn deserialize(
        &self,
        envelope: SerializedEnvelope,
    ) -> Result<Envelope<TestMessage>, Self::Error> {
        Envelope::new(envelope.message_id, TestMessage, envelope.metadata)
            .map_err(|_| TestSerializerError)
    }
}

pub(super) fn test_envelope(message_id: MessageId, metadata: Metadata) -> Envelope<TestMessage> {
    Envelope::new(message_id, TestMessage, metadata)
        .unwrap_or_else(|_| panic!("test envelope construction failed"))
}

pub(super) fn connect_options() -> PgConnectOptions {
    match std::env::var("DATABASE_URL") {
        Ok(url) => PgConnectOptions::from_str(&url)
            .unwrap_or_else(|_| panic!("PostgreSQL integration connection configuration failed")),
        Err(std::env::VarError::NotPresent) => {
            let host = required_postgres_component("PGHOST");
            let port = required_postgres_component("PGPORT")
                .parse::<u16>()
                .unwrap_or_else(|_| {
                    panic!("PostgreSQL integration connection configuration failed")
                });
            let user = required_postgres_component("PGUSER");
            let database = required_postgres_component("PGDATABASE");
            PgConnectOptions::new()
                .host(&host)
                .port(port)
                .username(&user)
                .database(&database)
        }
        Err(_) => panic!("PostgreSQL integration connection configuration failed"),
    }
}

fn required_postgres_component(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("PostgreSQL integration connection configuration failed"))
}

pub(super) async fn pool() -> PgPool {
    PgPoolOptions::new()
        .min_connections(1)
        .max_connections(4)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(connect_options())
        .await
        .unwrap_or_else(|_| panic!("PostgreSQL integration connection failed"))
}

pub(super) async fn isolated_outbox_pool() -> PgPool {
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(connect_options())
        .await
        .unwrap_or_else(|_| panic!("PostgreSQL integration connection failed"));
    sqlx::query!(
        r#"
            -- Copy production semantics and restore generated index names for isolated plans.
            DO $$
            BEGIN
                CREATE TEMPORARY TABLE outbox_messages
                (LIKE public.outbox_messages INCLUDING ALL);
                ALTER INDEX outbox_messages_message_id_idx
                    RENAME TO ix_outbox_messages_message_id;
                ALTER INDEX outbox_messages_claimable_at_id_expires_at_idx
                    RENAME TO ix_outbox_messages_claimable;
                ALTER INDEX outbox_messages_ordering_key_id_idx
                    RENAME TO ix_outbox_messages_ordering_key;
                ALTER INDEX outbox_messages_expires_at_idx
                    RENAME TO ix_outbox_messages_expires;
                ALTER INDEX outbox_messages_published_at_idx
                    RENAME TO ix_outbox_messages_published;
                ALTER INDEX outbox_messages_dead_at_id_idx
                    RENAME TO ix_outbox_messages_dead_cursor;
            END
            $$
        "#
    )
    .execute(&pool)
    .await
    .unwrap_or_else(|_| panic!("outbox index setup failed"));
    pool
}

pub(super) struct ConcurrentOutboxFixture {
    pub(super) pool: PgPool,

    schema: String,
}

impl ConcurrentOutboxFixture {
    pub(super) async fn cleanup(self) {
        self.pool.close().await;
        let control = pool().await;
        // The generated identifier contains only a fixed prefix and UUID hex digits.
        let statement = format!("DROP SCHEMA {} CASCADE", self.schema);
        sqlx::query(AssertSqlSafe(statement))
            .execute(&control)
            .await
            .unwrap_or_else(|_| panic!("concurrent outbox schema cleanup failed"));
    }
}

pub(super) async fn isolated_concurrent_outbox_pool() -> ConcurrentOutboxFixture {
    let control = pool().await;
    let schema = format!("outbox_concurrency_{}", Uuid::now_v7().simple());
    // PostgreSQL identifiers cannot be query parameters. The name is generated locally from UUID
    // hex, so this fixture's only dynamic DDL is injection-safe and cannot reuse stale shape.
    let create_schema = format!("CREATE SCHEMA {schema}");
    sqlx::query(AssertSqlSafe(create_schema))
        .execute(&control)
        .await
        .unwrap_or_else(|_| panic!("concurrent outbox schema setup failed"));
    // The table shares the generated schema across all connections in this one fixture.
    let create_table = format!(
        "CREATE TABLE {schema}.outbox_messages (LIKE public.outbox_messages INCLUDING ALL)"
    );
    sqlx::query(AssertSqlSafe(create_table))
        .execute(&control)
        .await
        .unwrap_or_else(|_| panic!("concurrent outbox table setup failed"));
    let search_path = format!("{schema}, public");
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .max_connections(3)
        .idle_timeout(None)
        .max_lifetime(None)
        .after_connect(move |connection, _| {
            let search_path = search_path.clone();
            Box::pin(async move {
                sqlx::query!(
                    r#"
                        -- Every pool connection resolves the provider table name to this schema.
                        SELECT set_config('search_path', $1, false) AS "search_path!"
                    "#,
                    search_path
                )
                .fetch_one(connection)
                .await
                .map(|_| ())
            })
        })
        .connect_with(connect_options())
        .await
        .unwrap_or_else(|_| panic!("concurrent PostgreSQL integration connection failed"));
    ConcurrentOutboxFixture { pool, schema }
}

pub(super) async fn insert_outbox_row(pool: &PgPool, message_type: &str) -> Uuid {
    insert_outbox_row_with_attempts(pool, message_type, 0).await
}

pub(super) async fn insert_outbox_row_with_attempts(
    pool: &PgPool,
    message_type: &str,
    attempts: i32,
) -> Uuid {
    sqlx::query_scalar!(
        r#"
            -- Fixture rows are complete envelopes unless a test deliberately supplies poison.
            INSERT INTO outbox_messages (
                id, message_id, message_type, message_version, content_type,
                payload, metadata, created_at, claimable_at, attempts
            )
            VALUES (uuidv7(), uuidv7(), $1, 1, 'application/test',
                    ''::bytea, '{}'::jsonb, now(), now(), $2)
            RETURNING id
        "#,
        message_type,
        attempts,
    )
    .fetch_one(pool)
    .await
    .unwrap_or_else(|_| panic!("outbox test row setup failed"))
}

pub(super) struct OutboxLookupParams {
    id: Option<Uuid>,

    message_id: Option<Uuid>,
}

impl OutboxLookupParams {
    pub(super) fn by_id(id: Uuid) -> Self {
        Self {
            id: Some(id),
            message_id: None,
        }
    }

    pub(super) fn by_message_id(message_id: Uuid) -> Self {
        Self {
            id: None,
            message_id: Some(message_id),
        }
    }
}

pub(super) struct OutboxRecord {
    pub(super) id: Uuid,

    pub(super) message_id: Uuid,

    pub(super) message_type: String,

    pub(super) attempts: i32,

    pub(super) claimable_at: chrono::DateTime<chrono::Utc>,

    pub(super) published_at: Option<chrono::DateTime<chrono::Utc>>,

    pub(super) dead_at: Option<chrono::DateTime<chrono::Utc>>,

    pub(super) dead_reason: Option<String>,

    pub(super) last_error: Option<String>,

    pub(super) claim_token: Option<Uuid>,

    pub(super) locked_by: Option<String>,

    pub(super) observed_at: chrono::DateTime<chrono::Utc>,
}

pub(super) async fn outbox_record(
    pool: &PgPool,
    params: OutboxLookupParams,
) -> Option<OutboxRecord> {
    sqlx::query_as!(
        OutboxRecord,
        r#"
            -- A lookup is keyed by exactly one caller-validated durable or message identity.
            SELECT
                id AS "id!",
                message_id AS "message_id!",
                message_type AS "message_type!",
                attempts AS "attempts!",
                claimable_at AS "claimable_at!",
                published_at,
                dead_at,
                dead_reason,
                last_error,
                claim_token,
                locked_by,
                now() AS "observed_at!"
            FROM outbox_messages
            WHERE (id = $1 AND $2::uuid IS NULL)
               OR (message_id = $2 AND $1::uuid IS NULL)
        "#,
        params.id,
        params.message_id,
    )
    .fetch_optional(pool)
    .await
    .unwrap_or_else(|_| panic!("outbox record lookup failed"))
}
