# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0](https://github.com/sisaio/sisa-messaging-rs/releases/tag/v0.1.0) - 2026-11-20

### Added

- *(facade)* [**breaking**] re-export the parts by name and add the orders example
- *(runtime)* [**breaking**] dispatch the outbox to the broker
- *(runtime)* [**breaking**] add the inbox for transactional endpoints
- *(runtime)* maintain partitions and apply the retention
- *(postgres)* write the outbox in the caller's transaction
- *(runtime)* [**breaking**] add the Bus with endpoints, handlers and retries
- *(core)* add the transport contracts and an in-memory transport
- *(core)* add the Codec trait and JsonCodec
- *(core)* add Message, MessageId and the CloudEvents Envelope

### Fixed

- *(runtime)* fence the outbox marks and bound each publish by its lease
- *(postgres)* fail the inbox commit of an aborted transaction
- *(core)* keep timestamps and extension names within the wire rules
