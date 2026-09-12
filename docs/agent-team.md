# Codex agent team

## 1. Decision

This repository uses one primary Codex agent and four project-scoped custom agents:

| Role | Model / effort | Writes files | Use |
|---|---|---:|---|
| Primary delivery lead | User-selected session model | Docs only | Requirements, task packets, orchestration, acceptance, and user decisions |
| `architect` | `gpt-6-astra` / `high` | No | Architecture, public contracts, concurrency, database design, and test strategy |
| `backend_developer` | `gpt-5.6-sol` / `high` | Yes | Rust/SQL implementation, fixes, and tests |
| `reviewer` | `gpt-5.6-terra` / `high` | No | Independent code, SQL, architecture, and test review |
| `release_engineer` | `gpt-5.6-luna` / `medium` | Yes, narrowly | GitHub CI/CD, Atlas mechanics, release bundles, and crates.io preparation |

The model split is deliberate. Architecture retains the strongest model but uses `high` as its
cost-aware default; `xhigh` is a one-off escalation for unresolved, high-risk design problems.
Sustained implementation uses the agentic workhorse; review uses a different model family at
`high` reasoning to reduce correlated blind spots; repeatable delivery work uses the faster model.
The project config caps spawned agents at three concurrent threads, although the normal workflow
is mostly sequential.

## 2. Roles intentionally not created

- **BA and PO:** no standing agents. The architecture set already defines scope, guarantees,
  non-goals, user-facing capabilities, and completion gates. The primary delivery lead translates
  a request into acceptance criteria. If a request changes product semantics or compatibility,
  the primary agent asks the user instead of letting an agent invent product policy.
- **DBA:** no standing agent. Database design belongs to `architect`; runtime SQL and database tests
  belong to `backend_developer`; migration mechanics and packaging belong to `release_engineer`;
  `reviewer` provides independent database review. This preserves end-to-end ownership without an
  agent that is idle for most tasks.
- **QA:** no standing agent. The architect designs risk coverage, the developer implements every
  required test layer, and the reviewer challenges both. A separate QA handoff would fragment
  responsibility.
- **Frontend and design:** excluded because this repository publishes Rust libraries and has no UI.
- **General DevOps:** no always-on role. `release_engineer` is spawned only for repository delivery
  concerns; applications and production infrastructure are explicitly outside this library's
  ownership boundaries.

## 3. Scope and token policy

Codex custom-agent files can request read-only or workspace-write defaults, but parent-session live
permission overrides take precedence and there are no per-folder read ACLs. The scopes below are
therefore instruction-level controls for ownership and context, not security boundaries. The
primary agent supplies the exact paths in every task packet, does not spawn design/review roles
under a write-enabling override, and verifies that their handoffs introduced no worktree changes.

| Agent | Default read scope | Default write scope |
|---|---|---|
| `architect` | Relevant `docs/**`; root and crate manifests; `migrations/**` for DB work; affected public interfaces and tests | None |
| `backend_developer` | Assigned `crates/<name>/**`; named docs; related tests/examples; necessary manifests and adjacent interfaces | Only assigned implementation/test paths |
| `reviewer` | Task packet, diff, changed files, relevant docs, callers/callees, manifests, migrations, and risk-focused tests | None |
| `release_engineer` | `.github/**`, root manifests/toolchain/policy, `atlas.hcl`, `migrations/**`, release scripts, migration/release docs | Only explicitly assigned delivery paths |

Every agent must:

1. Prefix every shell command with `rtk` and batch related reads/checks.
2. Start from `rg` and targeted file ranges, not whole-repository dumps.
3. Read only the normative docs named by the task; `docs/README.md` supplies the routing order.
4. Report a concrete dependency before expanding beyond the supplied path scope.
5. Return summaries and evidence rather than raw logs.
6. Preserve unrelated user changes and never claim an unrun check passed.

Only one write-heavy agent may own overlapping files. The backend developer exclusively owns Rust,
runtime SQL, and test implementation. The release engineer exclusively owns versioned migration
SQL, `atlas.sum`, CI, and release/delivery files when assigned. The primary agent writes only team
or task documentation and orchestration artifacts. Read-only architecture or review work may run
in parallel when it is genuinely independent, but architecture must settle before implementation
and review must inspect the completed diff.

## 4. Task packet

The primary agent sends a small packet to each spawned agent:

```text
Objective:
Acceptance criteria:
Base branch:
Proposed branch: <type>/<short-kebab-description>
PR intent and target file count:
Commit rounds: exact message, intended paths, validation for each round
Normative docs/sections:
Allowed read paths:
Allowed write paths:
Non-goals:
Required test layers and completion gates:
Known decisions and unresolved questions:
Expected handoff:
```

Do not paste entire design documents into the packet. Give paths and section names so the receiving
agent loads only what it needs.

## 5. Workflow

```text
user request
  -> primary: task packet and acceptance criteria
  -> architect: design packet, when the architecture gate applies
  -> backend_developer: implementation and tests
  -> release_engineer: delivery preparation, only when applicable
  -> reviewer: isolated review of the complete diff
  -> owning writer: accepted fixes
  -> reviewer: focused re-review of every changed risk
  -> primary: verify evidence and report to user
```

The architecture gate applies only when a task needs a design decision that the normative docs do
not already answer, or when it changes high-risk schema, transaction/locking, fencing, async
concurrency/cancellation, compatibility, or cross-crate behavior. A scoped implementation fully
specified by existing docs goes directly to the developer. The architect's normal design packet is
limited to 800 words and does not repeat source documents.

Independent review is required after every material Rust, runtime SQL, versioned migration, test,
manifest or dependency, CI or release, and normative-document change. Only generated output with a
separately reviewed source, or a change explicitly classified as low-risk and non-behavioral by the
primary agent, may skip review. Release preparation never occurs after the final review: changes
from a review finding receive another focused reviewer pass.

The review gate passes only when no blocker/high finding remains, every accepted medium finding is
fixed or explicitly dispositioned, required checks have evidence, docs and behavior agree, and
there is no unresolved schema/public compatibility decision. The reviewer advises; the primary
agent owns finding disposition and final acceptance.

## 6. Database and release escalation

For a database task, the primary agent asks the architect to define transactions, invariants,
locking, indexes, query-plan proof, forward migration and compatibility. The backend developer
implements Rust, runtime SQL, and tests. The release engineer exclusively authors or updates the
versioned migration SQL, checksum, replay, and packaging from that approved design. The reviewer
then inspects the complete database diff as one behavioral change. Any later fix by either writer
receives a focused re-review.

The release engineer may prepare crates, release assets, CI, or tags, but may not publish, push,
create a GitHub release, or run production migrations without explicit user authorization.

## 7. Git and review-size policy

Every task is planned as one reviewable PR. The primary agent defines the branch and commit rounds
before implementation:

- Branches use `<type>/<short-kebab-description>` from the named base branch. Allowed types are
  `feat`, `fix`, `docs`, `refactor`, `test`, `perf`, `ci`, `build`, `chore`, `release`, and
  `hotfix`. Examples: `feat/outbox-dispatcher`, `fix/claim-fencing`, `docs/consumer-guide`, and
  `ci/pr-size-gate`. The description is lowercase kebab-case. Work never starts directly on `main`.
- Each commit represents one testable behavior and strictly follows Conventional Commits 1.0.0:
  `<type>[optional scope][optional !]: <description>`, with body and footer sections separated by
  blank lines. `feat` means a new feature, `fix` means a bug fix, and an incompatible change uses
  `!` or an uppercase `BREAKING CHANGE:` footer. Repository commit types are `build`, `chore`, `ci`,
  `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, and `test`. A `hotfix/*` branch uses
  `fix`; release mechanics normally use `chore(release)`.
- `prek` installs the versioned `.pre-commit-config.yaml` hooks. Before commit they validate the
  branch, staged diff, formatting, Clippy, and the commit message; before push they run the full
  workspace test suite. Hooks provide fast feedback, while CI remains the non-bypassable authority.
  Agents never use `--no-verify`. The branch hook permits Git's transient detached `HEAD` only while
  an active rebase directory exists; an ordinary detached-HEAD commit remains forbidden.
- A commit targets at most 10 changed paths and has a hard limit of 20.
- A task/PR targets at most 25 changed paths and has a hard limit of 90. The 90-file stop leaves a
  ten-file margin below CodeRabbit's 100-file maximum.
- A PR has a 250-commit auditability ceiling because GitHub's pull-request commits endpoint does
  not return a complete list beyond that point. CI fails closed rather than silently skipping the
  per-commit path audit; normal tasks should remain nowhere near this ceiling.
- Added, modified, deleted, renamed, generated, test, migration, checksum, and lockfile paths all
  count. Companion files are not exempt.
- Work is split by capability, dependency direction, or independently testable behavior before a
  limit is reached. A later PR may depend on an earlier one, but each must remain independently
  understandable and reviewable.
- Exceeding the 10-file commit or 25-file task/PR target requires a documented reason and user
  approval before implementation. The 20-file commit, 90-file PR, and CodeRabbit 100-file ceilings
  are non-overridable; an atomic change that cannot fit must be split into an explicit multi-commit
  or multi-PR sequence before implementation continues.

Before each commit round, the primary agent reports:

```text
Base branch:
Working branch:
PR purpose:
Commit round N:
Exact commit message:
Intended paths and predicted count:
Validation:
Current PR path count:
Next slice:
```

Agents may propose and report Git operations, but they do not stage, commit, push, tag, publish, or
create a release unless the user explicitly requests that action. At handoff, the owning agent and
reviewer compare predicted and actual path counts and report both.

## 8. Configuration files

- `.codex/config.toml` enables the team and limits concurrency.
- `.codex/agents/architect.toml` defines the read-only architecture role.
- `.codex/agents/backend-developer.toml` defines the implementation role.
- `.codex/agents/reviewer.toml` defines the isolated read-only review role.
- `.codex/agents/release-engineer.toml` defines the on-demand delivery role.
- `.coderabbit.yaml` configures the repository-aware CodeRabbit review layer; GitHub CI owns hard
  file-count and executable quality gates.
- `AGENTS.md` contains the small routing policy loaded for every task.

Start a new Codex task after changing these files so the project instructions and custom-agent
definitions are loaded afresh.

## 9. Lean flow and durable records

The useful workflow lessons from the earlier Reliar repository are retained without its project
management overhead:

| Lane | Expected size | Flow |
|---|---:|---|
| Patch | Usually 1 commit and at most 5 paths | Primary task packet → developer → reviewer → primary |
| Standard | At most 25 paths and normally 1–3 commits | Primary → architect only if gated → developer/release engineer → reviewer → primary |
| Multi-PR | More than 25 forecast paths | User-approved PR sequence; every PR follows the normal targets and hard limits |

Normal work lives in the active Codex task packet and is handed off by repository path and section,
not copied into messages. This repository does not maintain a separate backlog project, one
markdown card per task, separate story files, or a routine ADR stream. Git history, the PR, and the
normative documents are the durable record.

When a decision changes a lasting guarantee, public capability, schema invariant, ownership
boundary, or non-goal, update the relevant normative document in the same PR. Create an ADR only
with explicit user approval, and only when a rare irreversible cross-cutting decision needs history
that cannot be expressed clearly in the normative document. A multi-PR effort may use one
`docs/work/<slug>.md` plan after user approval; do not create per-slice cards.

Do not copy Claude-specific slash commands, settings, or skills into Codex. Add a project skill only
when a repeated workflow needs procedural detail that cannot stay concise in `AGENTS.md` and is not
already covered by the normative docs. The completion gates and Definition of Done remain in
`docs/implementation-plan.md`; they are not duplicated into another checklist.

Cross-cutting review remains concise and risk-based:

- **Security:** no unsafe code, secrets, sensitive payload/header/error leakage, untrusted-input
  unwraps, dynamic unparameterized SQL, implicit migrations, or unjustified dependency surface.
- **Performance:** bounded concurrency/queries/batches, no blocking runtime work or transaction
  across network I/O, benchmark evidence for changed hot paths, and real PostgreSQL plan evidence
  for changed queries or indexes.
- **Supply chain:** minimal features, direct dependency ownership, pinned CI/tooling, and the
  repository's `cargo deny` and release gates.
