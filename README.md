# mailroom

`mailroom` is an OSS-ready local-first repository for Gmail operations: search, triage, draft workflows, attachment capture, and eventually cleanup automation. The native center of gravity is a Rust CLI/TUI with a single local operational store. The operator loop can also use Codex Gmail/GitHub plugin workflows while the native product matures.

## Current posture

- Primary stack: Rust + `clap`
- Planned operator surfaces: CLI first, TUI second
- Local operational store: SQLite FTS5
- Versioned content: code, docs, examples, plans
- Ignored runtime content: `.mailroom/` state, caches, exports, secrets, and attachment vaults
- V1 milestone: search + triage + draft queue

## Repository layout

- [`src/`](src/): Rust entrypoints and workspace logic
- [`config/`](config/): tracked example configuration
- [`docs/`](docs/): architecture, decisions, operations, roadmap, and workflow docs
- [`.github/workflows/`](.github/workflows/): CI for formatting, linting, and tests
- `.mailroom/`: ignored runtime workspace for local state

## Local runtime workspace

`mailroom` treats the git repo as the durable source of truth for design and code, and `.mailroom/` as the local operational workspace:

- `.mailroom/auth/`
- `.mailroom/cache/`
- `.mailroom/state/`
- `.mailroom/vault/`
- `.mailroom/exports/`
- `.mailroom/logs/`

These paths are intentionally ignored from git.

## Native commands

The initial binary is a scaffold with workspace awareness:

```bash
cargo run -- workspace init
cargo run -- paths --json
cargo run -- doctor --json
cargo run -- roadmap
```

## Docs map

- [`docs/README.md`](docs/README.md): doc index
- [`docs/decisions/0001-foundation.md`](docs/decisions/0001-foundation.md): foundational architecture decision
- [`docs/architecture/system-overview.md`](docs/architecture/system-overview.md): system boundaries and responsibilities
- [`docs/operations/plugin-assisted-workflows.md`](docs/operations/plugin-assisted-workflows.md): how Codex Gmail/GitHub workflows fit alongside native commands
- [`docs/roadmap/v1-search-triage-draft-queue.md`](docs/roadmap/v1-search-triage-draft-queue.md): first milestone scope

## Near-term build plan

1. Add config loading and SQLite store initialization.
2. Implement Gmail account/auth modeling and sync contracts.
3. Build native search/triage flows.
4. Add draft queue state and reply workflows.
5. Layer in a TUI over the native command core.
