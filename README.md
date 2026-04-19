# mailroom

`mailroom` is a local-first repository for Gmail operations: search, thread triage, reply drafting, reviewed cleanup actions, attachment capture, and later export automation. The native center of gravity is a Rust CLI/TUI with a single local operational store. The operator loop can also use Codex Gmail/GitHub plugin workflows where live inspection or ad hoc actions are still a better fit.

## Current posture

- Primary stack: Rust + `clap`
- Planned operator surfaces: CLI first, TUI second
- Local operational store: SQLite with migration-owned schema and FTS5-backed mailbox search
- Native Gmail foundation: OAuth login, active account persistence, live profile/label reads, one-shot mailbox sync, local search, thread-scoped workflow state, remote draft sync, and reviewed cleanup actions
- Versioned content: code, docs, examples, plans
- Ignored runtime content: `.mailroom/` state, caches, exports, secrets, and attachment vaults
- V1 milestone: search + thread workflow + draft/send + reviewed cleanup

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
- `.mailroom/auth/gmail-oauth-client.json`
- `.mailroom/auth/gmail-credentials.json`
- `.mailroom/state/mailroom.sqlite3`

## Native commands

The current binary can now resolve config, bootstrap the local store, manage Gmail auth, sync mailbox metadata, search the local cache, manage thread workflows, sync remote Gmail drafts, and execute reviewed cleanup actions:

```bash
cargo run -- workspace init
cargo run -- paths --json
cargo run -- doctor --json
cargo run -- config show --json
cargo run -- auth status --json
cargo run -- auth setup
cargo run -- auth setup --credentials-file /path/to/client_secret.json
cargo run -- auth login --no-browser
cargo run -- auth logout --json
cargo run -- account show --json
cargo run -- gmail labels list --json
cargo run -- store init --json
cargo run -- store doctor --json
cargo run -- sync run --json
cargo run -- sync run --full --recent-days 30 --json
cargo run -- search "project alpha" --label INBOX --limit 10 --json
cargo run -- workflow list --json
cargo run -- workflow show thread-123 --json
cargo run -- triage set thread-123 --bucket urgent --note "reply today" --json
cargo run -- workflow promote thread-123 --to follow-up --json
cargo run -- workflow snooze thread-123 --until 2026-04-25 --json
cargo run -- draft start thread-123 --reply-all --json
cargo run -- draft body thread-123 --text "Thanks, sending details shortly." --json
cargo run -- draft attach add thread-123 --path ./notes/reply.txt --json
cargo run -- draft send thread-123 --json
cargo run -- cleanup archive thread-123 --json
cargo run -- cleanup archive thread-123 --execute --json
cargo run -- cleanup label thread-123 --add 0.To-Reply --remove INBOX --execute --json
cargo run -- roadmap
```

## JSON contract and exit codes

All `--json` commands now use one normalized envelope:

- success: `{ "success": true, "data": ... }`
- failure: `{ "success": false, "error": { "code", "message", "kind", "operation", "causes" } }`

`error.code` is stable and operator-oriented. Current exit buckets are:

- `2`: validation or config failure
- `3`: auth required
- `4`: not found
- `5`: conflict
- `6`: timeout, rate limit, or remote failure
- `7`: local storage failure
- `10`: internal failure

Mailbox sync/search behavior, cursor fallback rules, and `doctor` field meanings
live in [`docs/operations/mailbox-sync-and-search.md`](docs/operations/mailbox-sync-and-search.md).
Durable architectural ownership for the sync/search slice lives in
[`docs/decisions/0003-message-canonical-sync.md`](docs/decisions/0003-message-canonical-sync.md).
Thread workflow, remote draft, and cleanup behavior live in
[`docs/operations/thread-workflow-and-cleanup.md`](docs/operations/thread-workflow-and-cleanup.md),
with the durable design captured in
[`docs/decisions/0004-unified-thread-workflow.md`](docs/decisions/0004-unified-thread-workflow.md).

Config precedence is:

1. Built-in defaults
2. User config via `directories::ProjectDirs`
3. Repo-local `.mailroom/config.toml`
4. `MAILROOM_` environment overrides

For Gmail auth, the primary path is:

1. Run `cargo run -- auth setup`.
2. If Mailroom auto-discovers exactly one `client_secret_*.json`, select it. Otherwise paste the Client ID and optional Client Secret directly into the CLI.
3. Advanced: if you already ran `gcloud auth application-default login` with Gmail scopes, choose the ADC import option.
4. Let Mailroom import the client locally and continue into the browser consent flow or reuse the imported ADC refresh token.

Once imported, the repo-local OAuth client file becomes the authoritative Gmail
OAuth client for future login and token refresh flows. Legacy inline
`gmail.client_id` / `gmail.client_secret` config is only used when no imported
client file exists.

If you omit `--credentials-file`, Mailroom will try to auto-discover a single
`client_secret_*.json` file from the current directory or `~/Downloads`, then
offer that path inside the setup wizard. The imported file is stored in the
standard Google Desktop app `installed` JSON shape under
`.mailroom/auth/gmail-oauth-client.json`.

Advanced manual overrides still work:

- `gmail.client_id`
- optionally `gmail.client_secret`
- leave the default `gmail.modify` scope unless you are intentionally testing a narrower mock configuration

## Docs map

- [`docs/README.md`](docs/README.md): doc index
- [`docs/decisions/0001-foundation.md`](docs/decisions/0001-foundation.md): foundational architecture decision
- [`docs/architecture/system-overview.md`](docs/architecture/system-overview.md): system boundaries and responsibilities
- [`docs/decisions/0003-message-canonical-sync.md`](docs/decisions/0003-message-canonical-sync.md): mailbox sync and search design
- [`docs/decisions/0004-unified-thread-workflow.md`](docs/decisions/0004-unified-thread-workflow.md): thread workflow, drafts, and cleanup ownership
- [`docs/operations/local-config-and-store.md`](docs/operations/local-config-and-store.md): config precedence, store bootstrapping, and hardening
- [`docs/operations/gmail-auth-and-account.md`](docs/operations/gmail-auth-and-account.md): Gmail OAuth flow, credential storage, and account verification
- [`docs/operations/mailbox-sync-and-search.md`](docs/operations/mailbox-sync-and-search.md): sync commands, search filters, and cursor behavior
- [`docs/operations/thread-workflow-and-cleanup.md`](docs/operations/thread-workflow-and-cleanup.md): triage, draft/send, snooze, and reviewed cleanup commands
- [`docs/operations/plugin-assisted-workflows.md`](docs/operations/plugin-assisted-workflows.md): how Codex Gmail/GitHub workflows fit alongside native commands
- [`docs/roadmap/v1-search-triage-draft-queue.md`](docs/roadmap/v1-search-triage-draft-queue.md): first milestone scope

## Near-term build plan

1. Add attachment cataloging and intentional export/vault flows on top of the existing thread workflow state.
2. Harden draft composition ergonomics, including better operator review and richer reply helpers.
3. Add unsubscribe assistance and bulk-cleanup heuristics only after explicit review contracts exist.
4. Build a TUI over the existing command core and SQLite workflow model.
