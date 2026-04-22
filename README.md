# mailroom

`mailroom` is a local-first repository for Gmail operations: search, thread triage, reply drafting, reviewed cleanup actions, attachment cataloging, and controlled export. The native center of gravity is a Rust CLI/TUI with a single local operational store. The operator loop can also use Codex Gmail/GitHub plugin workflows where live inspection or ad hoc actions are still a better fit.

## Current posture

- Primary stack: Rust + `clap`
- Planned operator surfaces: CLI first, TUI second
- Local operational store: SQLite with migration-owned schema and FTS5-backed mailbox search
- Native Gmail foundation: OAuth login, active account persistence, live profile/label reads, one-shot mailbox sync, local search, thread-scoped workflow state, remote draft sync, reviewed cleanup actions, attachment catalog/export foundation, and review-first automation rules
- Hardening surface: read-only label audits, readiness verification, and operator runbooks for safe real-mailbox rollout
- Versioned content: code, docs, examples, plans
- Ignored runtime content: `.mailroom/` state, caches, exports, secrets, and attachment vaults
- V1 milestone: search + thread workflow + draft/send + reviewed cleanup + controlled attachment export + review-first automation

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
- `.mailroom/automation.toml`
- `.mailroom/auth/gmail-oauth-client.json`
- `.mailroom/auth/gmail-credentials.json`
- `.mailroom/state/mailroom.sqlite3`

## Native commands

The current binary can now resolve config, bootstrap the local store, manage Gmail auth, sync mailbox metadata, search the local cache, catalog inbound attachments, manage thread workflows, sync remote Gmail drafts, and execute reviewed cleanup actions:

```bash
cargo run -- workspace init
cargo run -- paths --json
cargo run -- doctor --json
cargo run -- audit labels --json
cargo run -- audit verification --json
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
cargo run -- sync run --profile deep-audit --json
cargo run -- search "project alpha" --label INBOX --limit 10 --json
cargo run -- attachment list --json
cargo run -- attachment show m-1:1.2 --json
cargo run -- attachment fetch m-1:1.2 --json
cargo run -- attachment export m-1:1.2 --json
cargo run -- attachment export m-1:1.2 --to ./exports/statement.pdf --json
cargo run -- automation rules validate --json
cargo run -- automation run --json
cargo run -- automation run --rule archive-newsletters --limit 25 --json
cargo run -- automation show 42 --json
cargo run -- automation apply 42 --execute --json
cargo run -- workflow list --json
cargo run -- workflow show thread-123 --json
cargo run -- triage set thread-123 --bucket urgent --note "reply today" --json
cargo run -- workflow promote thread-123 --to follow_up --json
cargo run -- workflow snooze thread-123 --until 2026-04-25 --json
cargo run -- draft start thread-123 --reply-all --json
cargo run -- draft body thread-123 --text "Thanks, sending details shortly." --json
cargo run -- draft attach add thread-123 --path ./notes/reply.txt --json
cargo run -- draft send thread-123 --json
cargo run -- cleanup archive thread-123 --json
cargo run -- cleanup archive thread-123 --execute --json
cargo run -- cleanup label thread-123 --add 0.To-Reply --remove INBOX --execute --json
cargo run -- cleanup trash thread-123 --json
cargo run -- roadmap
```

## JSON contract and exit codes

All `--json` commands now use one normalized envelope:

- success: `{ "success": true, "data": ... }`
- failure: `{ "success": false, "error": { "code": "validation_failed", "message": "use --until YYYY-MM-DD or --clear", "kind": "workflow.validation", "operation": "workflow.snooze", "causes": ["use --until YYYY-MM-DD or --clear"] } }`

`error.code` is stable and operator-oriented. Current exit buckets are:

- `2`: validation or config failure
- `3`: auth required
- `4`: not found
- `5`: conflict
- `6`: timeout, rate limit, or remote failure
- `7`: local storage failure
- `10`: internal failure

## Gmail sync hardening

`mailroom sync run` is quota-aware by default. Full and incremental sync now:

- use Mailroom's built-in Gmail quota limiter instead of an external generic
  rate-limiter dependency
- budget Gmail read calls by quota units, not raw request count
- use `500`-message list/history pages for fewer API round trips
- keep message payload fetch concurrency bounded by default
- retry Gmail `429`, `5xx`, and usage-limit `403` responses with truncated backoff

The named deep-audit preset is available when a deep bootstrap needs extra
headroom:

```bash
cargo run -- sync run --profile deep-audit --json
```

Equivalent explicit flags:

```bash
cargo run -- sync run --full --recent-days 365 --quota-units-per-minute 9000 --message-fetch-concurrency 3 --json
```

Operator input mistakes on the workflow surface now stay in the validation
bucket. Examples:

- `workflow snooze` requires exactly one of `--until` or `--clear`
- `draft body` requires exactly one of `--text`, `--file`, or `--stdin`
- `draft attach add` treats missing attachment files as validation failures
- `draft attach remove` rejects ambiguous filename-only matches; use the stored
  attachment path when multiple attachments share a filename

Mailbox sync/search behavior, cursor fallback rules, and `doctor` field meanings
live in [`docs/operations/mailbox-sync-and-search.md`](docs/operations/mailbox-sync-and-search.md).
Durable architectural ownership for the sync/search slice lives in
[`docs/decisions/0003-message-canonical-sync.md`](docs/decisions/0003-message-canonical-sync.md).
Attachment catalog, vault, and export behavior live in
[`docs/operations/attachment-catalog-and-export.md`](docs/operations/attachment-catalog-and-export.md),
with the durable ownership captured in
[`docs/decisions/0005-attachment-canonical-model.md`](docs/decisions/0005-attachment-canonical-model.md).
Thread workflow, remote draft, and cleanup behavior live in
[`docs/operations/thread-workflow-and-cleanup.md`](docs/operations/thread-workflow-and-cleanup.md),
with the durable design captured in
[`docs/decisions/0004-unified-thread-workflow.md`](docs/decisions/0004-unified-thread-workflow.md).
Review-first automation rules and persisted bulk-action snapshots live in
[`docs/operations/automation-rules-and-bulk-actions.md`](docs/operations/automation-rules-and-bulk-actions.md),
with the durable design captured in
[`docs/decisions/0006-review-first-automation-rules.md`](docs/decisions/0006-review-first-automation-rules.md).
Read-only verification and hardening guidance live in
[`docs/operations/verification-and-hardening.md`](docs/operations/verification-and-hardening.md),
with the durable design captured in
[`docs/decisions/0007-verification-audit-hardening.md`](docs/decisions/0007-verification-audit-hardening.md).

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
- [`docs/decisions/0005-attachment-canonical-model.md`](docs/decisions/0005-attachment-canonical-model.md): attachment catalog, vault, and export ownership
- [`docs/decisions/0006-review-first-automation-rules.md`](docs/decisions/0006-review-first-automation-rules.md): review-first automation rules and persisted bulk-action snapshots
- [`docs/decisions/0007-verification-audit-hardening.md`](docs/decisions/0007-verification-audit-hardening.md): read-only audit ownership and real-mailbox rollout posture
- [`docs/operations/local-config-and-store.md`](docs/operations/local-config-and-store.md): config precedence, store bootstrapping, and hardening
- [`docs/operations/gmail-auth-and-account.md`](docs/operations/gmail-auth-and-account.md): Gmail OAuth flow, credential storage, and account verification
- [`docs/operations/mailbox-sync-and-search.md`](docs/operations/mailbox-sync-and-search.md): sync commands, search filters, and cursor behavior
- [`docs/operations/attachment-catalog-and-export.md`](docs/operations/attachment-catalog-and-export.md): attachment listing, vault fetch, and export commands
- [`docs/operations/thread-workflow-and-cleanup.md`](docs/operations/thread-workflow-and-cleanup.md): triage, draft/send, snooze, and reviewed cleanup commands
- [`docs/operations/automation-rules-and-bulk-actions.md`](docs/operations/automation-rules-and-bulk-actions.md): rule validation, persisted run snapshots, and review-first bulk apply
- [`docs/operations/verification-and-hardening.md`](docs/operations/verification-and-hardening.md): deep-sync audit, label canonicalization, canary tests, and first-wave ruleset rollout
- [`docs/operations/plugin-assisted-workflows.md`](docs/operations/plugin-assisted-workflows.md): how Codex Gmail/GitHub workflows fit alongside native commands
- [`docs/roadmap/v1-search-triage-draft-queue.md`](docs/roadmap/v1-search-triage-draft-queue.md): first milestone scope

## Near-term build plan

1. Use the verification and hardening runbook to canonicalize labels, deepen the local audit corpus, and stage the first real personal ruleset.
2. Expand automation ergonomics only after a few low-surprise micro-batch archive/label runs land cleanly.
3. Expand unsubscribe assistance only after the deeper sync proves out list-header coverage in the local cache.
4. Build a TUI over the existing command core, audit surfaces, and SQLite workflow model.
