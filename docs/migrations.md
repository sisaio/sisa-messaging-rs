# Migration lifecycle

## 1. Atlas's role

Atlas Community Edition is repository and deployment tooling. It is not a Rust dependency,
runtime service, or public crate API.

| Stage | Atlas responsibility | Owner |
|---|---|---|
| Development | Create or propose versioned SQL, regenerate `atlas.sum`, apply to disposable databases, and inspect the resulting schema | Contributor |
| CI | Verify migration-directory integrity, replay every migration on clean PostgreSQL 18, check final status, and run database tests | Repository CI |
| Release | Package the immutable migration directory with the release that contains the matching PostgreSQL provider | Release pipeline |
| Production | Apply the exact released directory once, before application code that requires it is deployed | Consumer's deployment pipeline/operator |

Atlas does not run inside `sisa-messaging-postgres`. A store constructor never checks or changes a
schema, and an application process never migrates on startup. SQLx remains the runtime query
client; its migration feature is not enabled.

The repository uses only Atlas Community capabilities. It does not depend on Atlas Cloud, Atlas
Registry, Pro linting, checkpoints, down migrations, hooks, Data Scripts, or managed drift
detection.

## 2. Canonical migration artifact

The root [`migrations/`](../migrations/) directory is the executable source of truth:

```text
migrations/
├── 0001_messaging.sql
├── <later-version>_<change>.sql
└── atlas.sum
```

Rules:

- Versions are linear and increasing.
- A released file is never edited, deleted, renamed, or reordered.
- `atlas.sum` is committed and regenerated only after intentionally changing an unreleased file
  or adding a migration.
- SQL controls constraint and index names explicitly. Atlas may propose SQL, but a reviewer owns
  the final file.
- The complete directory, rather than only the newest file, is the deployable artifact.
- A generated schema snapshot lives outside `migrations/`; it is not executable history.

## 3. Development workflow

The checked-in [`atlas.hcl`](../atlas.hcl) configures only a disposable PostgreSQL 18 development
database and the migration-directory location. It contains no production target or credentials.

Create a migration, edit and review its SQL, then regenerate the checksum:

```text
atlas migrate new <change-name> --dir file://migrations
atlas migrate hash --dir file://migrations
```

`atlas migrate diff` may be used to propose a migration from a desired schema during development.
The result is reviewed and committed as versioned SQL; direct declarative `schema apply` is not a
release or production workflow.

Before review, apply the complete directory to a clean PostgreSQL 18 database and confirm:

```text
atlas migrate apply --dir file://migrations --url <development-database-url>
atlas migrate status --dir file://migrations --url <development-database-url>
```

Target URLs and credentials come from contributor or deployment tooling. This does not weaken the
library rule against environment-variable configuration: no Rust library reads them. A deployment
system may obtain its Atlas credentials from its own secret manager or environment.

## 4. CI contract

CI uses a pinned Atlas Community release or container digest, never the floating
`latest-community` tag. A migration change must pass all of the following:

1. Regenerate `atlas.sum` and fail if that changes the checked-in file unexpectedly.
2. Start an empty PostgreSQL 18 database.
3. Apply the complete migration directory.
4. Require `atlas migrate status` to report the latest version and zero pending files.
5. Run PostgreSQL integration, concurrency, invariant, and representative query-plan tests.
6. Optionally generate a SQL snapshot and compare it with the expected reviewed snapshot.

Community Edition does not provide migration linting or testing commands, so ordinary repository
CI owns these checks. A generic pinned CLI or container invocation is preferred over an
Atlas-specific hosted integration.

## 5. Public release and distribution

A crates.io package cannot assume that its consumer cloned this repository or retained workspace
root files. Therefore every release that changes the database contract publishes a separate,
immutable migration bundle alongside the source tag. The bundle contains:

- the complete `migrations/` directory through that release;
- `atlas.sum`;
- this deployment guide or a link pinned to the same source tag; and
- the compatible `sisa-messaging-postgres` crate version.

The recommended asset name is
`sisa-messaging-postgres-migrations-<crate-version>.tar.gz`. Its release digest is recorded by the
release pipeline. The Git repository remains the authoring source; the tagged bundle is the
production input.

Consumers either vendor that exact bundle into their infrastructure repository or fetch it from
their approved artifact store by immutable version and digest. They must not deploy migrations
from a moving branch, an unpinned URL, or a locally regenerated snapshot.

## 6. Production deployment

The consumer's deployment pipeline runs Atlas as an explicit pre-deployment job:

```text
atlas migrate apply \
  --dir file://migrations \
  --url <production-database-url-with-selected-search-path> \
  --dry-run

atlas migrate apply \
  --dir file://migrations \
  --url <production-database-url-with-selected-search-path>

atlas migrate status \
  --dir file://migrations \
  --url <production-database-url-with-selected-search-path>
```

Production rules:

- One supervised deployment job applies migrations; application replicas never compete to do it.
- A dedicated migration role has DDL privileges. Runtime application roles do not.
- The selected `search_path` identifies the schema containing both messaging tables and Atlas's
  revision bookkeeping.
- The migration completes before application code that requires the new schema is enabled.
- Rollback means rolling application code back while retaining a compatible expanded schema, or
  shipping a new compensating forward migration. Production does not run `migrate down`.
- A later concurrent index build is isolated in its own `-- atlas:txmode none` migration and uses
  the invalid-index recovery procedure in [PostgreSQL design](database.md#8-migration-operations).

For installations that place messaging tables in multiple PostgreSQL schemas, the operator runs
the bundle once per schema and keeps revision bookkeeping in each selected schema.

## 7. Snapshot role

A full SQL snapshot is useful for review, documentation, or an operator's manual drift check:

```text
atlas migrate apply \
  --dir file://migrations \
  --url <disposable-postgresql-18-url-with-selected-search-path>

atlas migrate status \
  --dir file://migrations \
  --url <disposable-postgresql-18-url-with-selected-search-path>

atlas schema inspect \
  --url <disposable-postgresql-18-url-with-selected-search-path> \
  --format '{{ sql . }}' \
  > schema.snapshot.sql
```

The target must be a clean, disposable PostgreSQL 18 database. This procedure inspects the database
after the versioned directory has been applied; inspecting a migration directory as a schema source
would require an explicit dev database and is not the release snapshot contract. The snapshot is
never inserted into the active migration directory. It does not replace `atlas.sum`, Atlas revision
bookkeeping, or the versioned files. Use `pg_dump --schema-only` when PostgreSQL objects outside
the Atlas Community schema model must be captured.

## 8. References

- [Atlas Community Edition](https://atlasgo.io/community-edition)
- [Atlas versioned migrations](https://atlasgo.io/versioned/intro)
- [Applying versioned migrations](https://atlasgo.io/versioned/apply)
- [Atlas schema inspection](https://atlasgo.io/inspect)
