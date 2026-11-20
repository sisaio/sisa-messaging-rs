# Security

## Reporting

Report a vulnerability privately through
[GitHub security advisories](https://github.com/sisaio/sisa-messaging-rs/security/advisories/new),
not in a public issue.

## Supported versions

| Version | Security fixes |
| --- | --- |
| The latest 0.x minor release | Yes, as a patch release of that minor |
| Older releases | No: upgrade to the latest minor |

## After you report

- You get a first answer within 7 days.
- We confirm the problem with you, agree on a fix, and keep the advisory private until the fixed
  release is on crates.io.
- The fix ships as a patch release, and the advisory is published with it, crediting you unless
  you'd rather not be named.

## What counts

A message body, a connection string or a password reaching a log, an error message or a `Debug`
output is a vulnerability, and so is a way for a publisher to add or cut short another header
through an attribute's value. The library encrypts nothing: message data that sits unencrypted on
the broker or in the outbox and inbox tables is the application's choice, not a vulnerability.

## Verifying a release

Each GitHub release carries the crates' `.crate` files as crates.io serves them, checked against
a packaging of the tagged source, and a Sigstore bundle with their SLSA build provenance, made by
the release workflow. Check a downloaded crate with the GitHub CLI:

```sh
gh attestation verify sisa-messaging-<version>.crate --repo sisaio/sisa-messaging-rs \
    --signer-workflow sisaio/sisa-messaging-rs/.github/workflows/release.yml
```
