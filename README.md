# mailroom

`mailroom` is a local-first repository for Gmail operations: search, triage, reply drafting, attachment capture, and eventually cleanup automation. The native center of gravity is a Rust CLI/TUI with a single local operational store. The operator loop can also use Codex Gmail/GitHub plugin workflows while the native product matures.

## Current posture

- Primary stack: Rust + `clap`
- Planned operator surfaces: CLI first, TUI second
- Local operational store: SQLite with migration-owned schema and FTS5-backed mailbox search
- Native Gmail foundation: OAuth login, active account persistence, live profile/label reads, one-shot mailbox sync, and local search
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
- `.mailroom/auth/gmail-oauth-client.json`
- `.mailroom/auth/gmail-credentials.json`
- `.mailroom/state/mailroom.sqlite3`

## Native commands

The current binary can now resolve config, bootstrap the local store, manage Gmail auth, sync mailbox metadata, and search the local cache:

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
cargo run -- roadmap
```

Mailbox sync/search behavior, cursor fallback rules, and `doctor` field meanings
live in [`docs/operations/mailbox-sync-and-search.md`](docs/operations/mailbox-sync-and-search.md).
Durable architectural ownership for the sync/search slice lives in
[`docs/decisions/0003-message-canonical-sync.md`](docs/decisions/0003-message-canonical-sync.md).

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
- [`docs/operations/local-config-and-store.md`](docs/operations/local-config-and-store.md): config precedence, store bootstrapping, and hardening
- [`docs/operations/gmail-auth-and-account.md`](docs/operations/gmail-auth-and-account.md): Gmail OAuth flow, credential storage, and account verification
- [`docs/operations/mailbox-sync-and-search.md`](docs/operations/mailbox-sync-and-search.md): sync commands, search filters, and cursor behavior
- [`docs/operations/plugin-assisted-workflows.md`](docs/operations/plugin-assisted-workflows.md): how Codex Gmail/GitHub workflows fit alongside native commands
- [`docs/roadmap/v1-search-triage-draft-queue.md`](docs/roadmap/v1-search-triage-draft-queue.md): first milestone scope

## Near-term build plan

1. Introduce triage state and durable workflow queues on top of the synced message store.
2. Implement draft/reply queue records and operator notes.
3. Add attachment catalog and intentional export flows.
4. Provide safe reviewed mailbox mutations such as archive, label, and trash.
5. Build a TUI over the native command core.
