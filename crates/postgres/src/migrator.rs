//! [`migrator`]: the migrations in this crate's `migrations/` folder as a sqlx [`Migrator`]
//! (feature `migrate`), for an application that runs its migrations from Rust instead of
//! sqlx-cli. It records the same versions, descriptions and checksums as
//! `sqlx migrate run --source crates/postgres/migrations`, so either way may run a schema the
//! other started.

use std::borrow::Cow;

use sqlx::SqlSafeStr as _;
use sqlx::migrate::{Migration, MigrationType, Migrator};

/// Every migration in `migrations/`, in order: the version and the description as sqlx-cli takes
/// them from the file name, and the SQL, unchanged.
const MIGRATIONS: [(i64, &str, &str); 1] = [(
    20_261_104_000_000,
    "create outbox and inbox",
    include_str!("../migrations/20261104000000_create_outbox_and_inbox.sql"),
)];

/// The outbox and inbox migrations. Run it on a connection whose `search_path` is the schema the
/// tables go in, as with sqlx-cli. Its bookkeeping goes in that schema's `_sqlx_migrations`
/// unless the caller names another table ([`Migrator::dangerous_set_table_name`]).
#[must_use]
pub fn migrator() -> Migrator {
    Migrator::with_migrations(
        MIGRATIONS
            .iter()
            .map(|&(version, description, sql)| {
                Migration::new(
                    version,
                    Cow::Borrowed(description),
                    MigrationType::Simple,
                    sql.into_sql_str(),
                    false,
                )
            })
            .collect(),
    )
}
