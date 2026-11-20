# ADR-0007: What the library leaves out at first

- Status: **Accepted**
- Date: 2026-11-20, when it was recorded here; settled before 0.1.0
- Deciders: Sang Au

## Context

The .NET frameworks this library learns from
([ADR-0002](0002-outbox-and-inbox-in-the-application-database.md)) also ship sagas, request and
response, and scheduled messages. The goal here is a library that is small and explicit.

## Decision

These stay out until users ask for them:

- **Sagas and state machines** (MassTransit, NServiceBus): useful, but a layer on top of the bus.
- **Request and response**: needs reply addresses on every transport.
- **Scheduled messages**: the outbox's `available_at` makes it cheap later.
- **Encryption**: the library encrypts nothing. An application that must encrypts the data with
  a codec that wraps another, or encrypts fields in the message itself.

## Consequences

- Building a bus is all the setup, and each part has one way in and one way out.
- An application that needs one of these builds it on top, or waits for the
  [roadmap](../../ROADMAP.md).
- With application-side encryption only the data is encrypted: the CloudEvents attributes go in
  the headers and the outbox's columns as they are.

## Alternatives considered

- **Shipping them in 0.1.0:** each needs every transport to support it before the contracts
  have settled.
