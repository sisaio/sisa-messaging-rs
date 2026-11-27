# ADR-0010: The migrations ship in the postgres crate, and also as a Rust migrator

- Status: **Proposed**
- Date: 2026-11-27
- Deciders: Sang Au

## Context

- [ADR-0004](0004-plain-sql-migrations.md) keeps the tables in plain SQL migrations in the repository's `migrations/postgresql` folder, run by the application with sqlx-cli or psql, and builds sqlx without its macros and migrate features.
- Some applications don't use sqlx-cli. They run their migrations from Rust: a `migrate` step calls each crate's `Migrator`, so the order and the bookkeeping are code. One such application, built on the sisa module crates, copies this repository's SQL into its own tree, byte for byte, so the checksums match, and must copy every new migration again on each upgrade.
- A crate can only embed files inside its own folder: `include_str!` on `../../migrations` builds in this repository but not from crates.io, which packages only the crate's folder.
- sqlx 0.9 builds a `Migrator` at run time from migrations given as values (`Migrator::with_migrations`, `Migration::new`). That needs its `migrate` feature, not its macros.

## Decision

- The SQL migrations move, unchanged, to `crates/postgres/migrations/`. sqlx-cli runs them with `--source crates/postgres/migrations`. Their content never changes, so their checksums stay the same.
- `sisa-messaging-postgres` gets an opt-in feature `migrate`. With it, `migrator()` returns a sqlx `Migrator` of those files, embedded with `include_str!`, with the same versions, descriptions, checksums and kinds as sqlx-cli records. The facade re-exports it as `postgres_migrator` under its own `migrate` feature.
- The default build stays as ADR-0004 has it: no sqlx macros, and no migrate feature unless an application asks for one.
- A test compares `migrator()` with the folder as sqlx reads it, so a migration added to the folder and not to the list fails the build.
- ADR-0004 otherwise stands: the SQL names no schema, the connection's `search_path` picks it, and the application chooses how to run it.

## Consequences

- An application that runs migrations from Rust calls `migrator()` instead of copying SQL. It can still name its own bookkeeping table (`Migrator::dangerous_set_table_name`) and create its schema (`Migrator::create_schema`).
- Both ways record the same rows, so an application can switch from sqlx-cli to `migrator()`, or back, on a database that is already migrated.
- A new migration needs a line in `migrator.rs` as well as its file. The test catches a missing one.
- sqlx-cli users change one path in their deploy step.
- The SQL file's own comment still names the old folder: changing it would change its checksum.

## Alternatives considered

- **`sqlx::migrate!()`:** it needs sqlx's macros, which ADR-0004 keeps out.
- **Keep the SQL at the repository root and link it into the crate:** a symlink is fragile across platforms and packaging.
- **Leave it to each application to copy the SQL:** that's what this replaces; every upgrade is a manual step that a checksum mismatch punishes.
