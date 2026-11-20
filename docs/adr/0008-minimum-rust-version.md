# ADR-0008: The minimum Rust version is 1.95

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

The workspace has one `rust-version` for every crate. The clients set its floor: the iggy client
0.12 needs Rust 1.95, sqlx 0.9 needs 1.94 and async-nats 1.89, as checked for 0.1.0.

## Decision

The workspace's `rust-version` is 1.95, and CI builds every crate at it.

## Consequences

- An application on an older compiler can't use any crate, the ones that would build on it
  included.
- Moving the minimum up follows the clients, and is called out in the changelog.

## Alternatives considered

- **A lower minimum for the crates that don't need Iggy:** one version for the workspace is
  simpler to test and to state.
