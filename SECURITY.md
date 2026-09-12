# Security policy

## Status

Sisa Messaging is under active development and is **not production-ready**. No stable release or
security support window is currently offered.

Do not include credentials, production URLs, message payloads, raw headers, or other sensitive data
in issues, logs, pull requests, or test fixtures.

## Reporting

For a suspected vulnerability, contact the repository maintainers privately through the project's
GitHub security advisory process. Do not open a public issue until maintainers confirm that it is
safe to disclose.

## Release and deployment boundaries

CI may build and archive rehearsal artifacts only. It does not publish crates, create tags or
releases, or execute production migrations. Production operators must use an immutable reviewed
migration bundle and a dedicated deployment identity.
