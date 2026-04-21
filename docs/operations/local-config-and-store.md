# Local Config And Store

## Purpose

`mailroom` keeps code and durable design in the git tree, and keeps operational
state under the ignored `.mailroom/` runtime workspace. This document defines
how config is resolved and how the local SQLite store is created and inspected.

## Config precedence

Resolved config is built in this order:

1. Built-in defaults derived from the repo root
2. User config via `directories::ProjectDirs`
3. Repo-local `.mailroom/config.toml`
4. `MAILROOM_` environment overrides

On Linux and WSL, the user config path is typically `~/.config/mailroom/config.toml`.

Environment overrides use `__` to separate nested keys. Example:

```bash
MAILROOM_STORE__BUSY_TIMEOUT_MS=10000 cargo run -- config show --json
```

Path defaults are derived in two stages:

- `workspace.runtime_root` is the parent default for `auth_dir`, `cache_dir`, `state_dir`, `vault_dir`, `exports_dir`, and `logs_dir`
- `store.database_path` defaults to `workspace.state_dir/mailroom.sqlite3`
- `gmail` auth defaults to Google-installed-app endpoints, localhost loopback auth, and a repo-local credential file at `workspace.auth_dir/gmail-credentials.json`
- imported Google Desktop app client metadata defaults to `workspace.auth_dir/gmail-oauth-client.json`
- relative configured filesystem paths are resolved from the repo root so command behavior stays stable from subdirectories
- explicit child path or `store.database_path` overrides still win when set

## Default paths

The repo-local defaults are:

- `.mailroom/auth/`
- `.mailroom/auth/gmail-oauth-client.json`
- `.mailroom/auth/gmail-credentials.json`
- `.mailroom/cache/`
- `.mailroom/state/`
- `.mailroom/vault/`
- `.mailroom/exports/`
- `.mailroom/logs/`
- `.mailroom/state/mailroom.sqlite3`

Initialize the runtime workspace with:

```bash
cargo run -- workspace init
```

Inspect resolved config with:

```bash
cargo run -- config show --json
```

Inspect auth state with:

```bash
cargo run -- auth status --json
```

Guided Gmail auth setup starts with:

```bash
cargo run -- auth setup
```

`mailroom auth setup` can:

- auto-discover a downloaded Desktop app JSON
- import `--credentials-file /path/to/client_secret.json`
- prompt for Client ID and optional Client Secret
- import an existing gcloud ADC authorized-user session

## Store bootstrap

Initialize the local database with:

```bash
cargo run -- store init --json
```

This command:

- ensures the database parent directory exists
- opens SQLite through `rusqlite`
- applies embedded SQL migrations through `rusqlite_migration`
- hardens the database connection defaults
- reports schema version, migration counts, and active pragma values
- creates mailbox sync/search tables once the current migration set is applied

Inspect store state without creating the database with:

```bash
cargo run -- store doctor --json
```

`store doctor` opens an existing database read-only, applies the non-persistent
connection hardening settings used by `store init`, and reports the effective
pragma state without rewriting persisted database settings.

Once the mailbox tables exist (schema v3), `doctor` and `store doctor` also
report:

- mailbox message count
- label count
- indexed message count
- latest stored sync cursor and status
- last attempted sync epoch
- last successful full-sync epoch
- last successful incremental-sync epoch
- active full-bootstrap checkpoint state when a staged full sync is in progress
- persisted adaptive sync pacing state for the active account when present

Full-bootstrap resume state is stored in SQLite, alongside the staged mailbox
rows, not in a sidecar file under `.mailroom/`. The checkpoint row records the
bootstrap query, current `nextPageToken`, highest observed history cursor,
progress counters, and staged row counts for the active account.

Adaptive sync pacing is also stored in SQLite. The pacing row records the
learned quota budget, learned message-fetch concurrency, clean-run streak, last
observed pressure kind, and update time for the active account. The runtime
sync flags still act as per-run ceilings over those learned values.

## Hardening defaults

The store currently enforces:

- `foreign_keys=ON`
- `trusted_schema=OFF`
- `journal_mode=WAL`
- `synchronous=NORMAL`
- nonzero busy timeout
- fixed SQLite `application_id`

On Unix-like systems, `mailroom` also hardens the database file to `0600` after creation.

The Gmail credential file is also hardened on Unix-like systems:

- `.mailroom/auth/` is kept owner-only
- `.mailroom/auth/gmail-oauth-client.json` is written as `0600`
- `.mailroom/auth/gmail-credentials.json` is written as `0600`

## Migration ownership

Schema migrations live under tracked `migrations/` and are embedded into the
binary at compile time. The current naming contract follows `rusqlite_migration`
directory loading rules:

- `01-name/up.sql`
- optional `01-name/down.sql`
- consecutive numeric prefixes

`build.rs` marks `migrations/` as a rebuild trigger so embedded migration state
stays current when SQL files change.

The current mailbox-oriented schema adds:

- `gmail_messages`
- `gmail_labels`
- `gmail_message_labels`
- `gmail_sync_state`
- `gmail_message_search`
- `gmail_full_sync_stage_*`
- `gmail_full_sync_checkpoint`
- `gmail_sync_pacing_state`
