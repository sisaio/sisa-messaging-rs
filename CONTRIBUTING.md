# Contributing to Sisa Messaging

Thank you for helping improve Sisa Messaging. GitHub Issues are the authoritative record for work;
repository documentation describes the resulting design and behavior rather than acting as a
ticket system.

## Before changing the repository

Read the documentation index in [`docs/README.md`](docs/README.md) and search the open issues before
starting. Read-only investigation and discussion do not require a new issue, but every code,
configuration, migration, test, or documentation change must have one open issue that states:

- the intended outcome and motivation;
- testable acceptance criteria;
- scope and explicit non-goals;
- compatibility, schema, concurrency, security, performance, or release risks; and
- the validation expected before review.

Use a parent issue and implementation sub-issues when an initiative needs multiple PRs. Each
implementation issue gets its own Codex task, linked branch, and normally one PR. Do not add
per-ticket Markdown files to the repository.

Report security vulnerabilities with a private security advisory instead of a public issue.

## Classify the issue

Labels are namespaced by purpose:

- Apply exactly one type: `type:bug`, `type:feature`, `type:task`, or `type:docs`.
- Apply every affected area: `area:core`, `area:outbox`, `area:inbox`, `area:consumer`,
  `area:postgres`, `area:nats`, `area:ci`, `area:release`, or `area:docs`.
- Apply a risk label only when it changes the required review evidence: `risk:breaking`,
  `risk:security`, `risk:performance`, or `risk:migration`.
- Use `needs:triage`, `needs:decision`, or `blocked` only while that exceptional condition exists.

Priority and normal workflow status belong in GitHub Project fields. Do not duplicate `Todo`,
`In Progress`, `Done`, or priority values as labels.

## Approve before implementation

Creating and classifying an issue is triage, not authorization to implement it. Keep the issue in
`Todo` while the repository owner reviews its content, labels, priority, and scope. Do not create a
Codex task, linked branch, commit, or pull request until the owner explicitly approves the issue.

After approval, move the issue to `In Progress`, provision exactly one Codex task from the approved
issue, and create its linked numbered branch. If review requests changes or a decision is still
missing, leave the issue in `Todo` and use `needs:decision` when appropriate.

## Create the linked branch

Create branches through the issue so GitHub records the development link:

```shell
gh issue develop 123 \
  --base main \
  --name feat/123-short-description \
  --checkout
```

Branch names use `<type>/<issue-number>-<short-kebab-description>`. Allowed types are `feat`,
`fix`, `docs`, `refactor`, `test`, `perf`, `ci`, `build`, `chore`, `release`, and `hotfix`.
Never implement directly on `main`.

## Commit and validate

Install the repository hooks with `prek install`. Commits follow Conventional Commits 1.0.0 and
reference the issue without closing it:

```text
feat(outbox): implement bounded dispatcher

Refs #123
```

Never bypass hooks with `--no-verify`. Keep commits and PRs within the path limits documented in
[`docs/agent-team.md`](docs/agent-team.md), and include the relevant formatting, lint, test,
benchmark, query-plan, or migration evidence for the affected risk.

## Open the pull request

Complete the pull request template and include `Closes #123`, matching the issue number in the
branch. The PR contains the review discussion and validation evidence; merging it into `main`
closes the issue.

Automation and Codex agents must never enable auto-merge or merge the PR. After all required checks
and reviews pass, report that the PR is ready and wait for the repository owner to review and merge
it manually.

Update normative docs in the same PR only when the change affects a lasting guarantee, public
capability, schema invariant, ownership boundary, compatibility statement, or non-goal. Otherwise,
do not change docs merely to record that a ticket existed.
