# ADR-0004: The tables come from plain SQL migrations that name no schema

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

The outbox and inbox tables live in the application's database, which the application already
migrates in its deploy pipeline. Deployments that share a server need their tables kept apart.

## Decision

The tables come from plain SQL migrations in `migrations/postgresql`, which the application runs
itself, with sqlx-cli or by hand. The SQL names no schema: the connection's `search_path` picks
it, for the migrations and for the queries alike. Every constraint and index is named in the SQL.

## Consequences

- Applications already run sqlx migrations in their deploy pipelines, and a schema per
  deployment needs no setting of ours.
- Run in a schema of their own, the migrations keep their own `_sqlx_migrations` table there,
  apart from the application's.
- The tables are part of the public API: a change to them is breaking, and the application's own
  migrations may depend on the constraint and index names.
- Pick one way to run them per schema: a psql run leaves no row in `_sqlx_migrations`, so a
  later `sqlx migrate run` would run the migration again and fail.
- sqlx is built without its macros and migrate features: the tests check the queries.

## Alternatives considered

- **Creating the tables from library code at startup:** the application's deploy pipeline
  wouldn't see or order the change.
- **A schema setting of ours, qualifying every table:** a second place to configure what the
  connection already says.
