# mailroom

`mailroom` is a local-first repository for Gmail operations: search, triage, reply drafting, attachment capture, and eventually cleanup automation. The native center of gravity is a Rust CLI/TUI with a single local operational store. The operator loop can also use Codex Gmail/GitHub plugin workflows while the native product matures.

## Current posture

- Primary stack: Rust + `clap`
- Planned operator surfaces: CLI first, TUI second
- Local operational store: SQLite with migration-owned schema and future FTS5 search
- Versioned content: code, docs, examples, plans
- Ignored runtime content: `.mailroom/` state, caches, exports, secrets, and attachment vaults
- V1 milestone: search + triage + draft queue

## Repository layout

- [`src/`](src/): Rust entrypoints and workspace logic
- [`config/`](config/): tracked example configuration
- [`migrations/`](migrations/): embedded SQL schema migrations
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

Repo-local overrides also live under `.mailroom/`:

- `.mailroom/config.toml`
- `.mailroom/state/mailroom.sqlite3`

## Native commands

The current binary can resolve config and bootstrap the local store:

```bash
cargo run -- workspace init
cargo run -- paths --json
cargo run -- doctor --json
cargo run -- config show --json
cargo run -- store init --json
cargo run -- store doctor --json
cargo run -- roadmap
```

Config precedence is:

1. Built-in defaults
2. User config via `directories::ProjectDirs`
3. Repo-local `.mailroom/config.toml`
4. `MAILROOM_` environment overrides

## Docs map

- [`docs/README.md`](docs/README.md): doc index
- [`docs/decisions/0001-foundation.md`](docs/decisions/0001-foundation.md): foundational architecture decision
- [`docs/architecture/system-overview.md`](docs/architecture/system-overview.md): system boundaries and responsibilities
- [`docs/operations/local-config-and-store.md`](docs/operations/local-config-and-store.md): config precedence, store bootstrapping, and hardening
- [`docs/operations/plugin-assisted-workflows.md`](docs/operations/plugin-assisted-workflows.md): how Codex Gmail/GitHub workflows fit alongside native commands
- [`docs/roadmap/v1-search-triage-draft-queue.md`](docs/roadmap/v1-search-triage-draft-queue.md): first milestone scope

## Near-term build plan

1. Add mailbox/account modeling and Gmail auth contracts.
2. Build search primitives over the local SQLite store.
3. Add triage state and durable workflow queues.
4. Add draft/reply queue records and operator notes.
5. Layer in a TUI over the native command core.
