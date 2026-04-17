# Mailbox Sync And Search

## Purpose

This runbook covers the native mailbox metadata sync and local search surface in
`mailroom`.

The current native flow owns:

- label refresh from Gmail
- recent-window bootstrap sync
- incremental sync from stored Gmail history IDs
- local metadata + snippet indexing in SQLite FTS5
- local search with explicit filters

It does not yet own:

- full body indexing
- attachment extraction
- triage queues
- draft/reply workflows
- destructive mailbox actions

## Commands

Run a normal sync:

```bash
cargo run -- sync run --json
```

Force a fresh bootstrap over a specific recent window:

```bash
cargo run -- sync run --full --recent-days 30 --json
```

Run a local search:

```bash
cargo run -- search "project alpha" --json
```

Search with filters:

```bash
cargo run -- search "invoice" --label INBOX --from billing@example.com --after 2026-01-01 --before 2026-02-01 --limit 20 --json
```

Inspect sync state through doctor output:

```bash
cargo run -- doctor --json
```

## Sync behavior

The sync flow is one-shot and local-first:

1. refresh the active account from Gmail profile data
2. refresh the label catalog
3. decide between full bootstrap and incremental replay
4. fetch Gmail message metadata with bounded concurrency
5. persist mailbox rows, label joins, FTS rows, and sync cursor state

Default bootstrap behavior:

- query: `in:anywhere -in:spam -in:trash newer_than:{N}d`
- default `N`: `90`
- storage: metadata plus snippet only

Incremental sync behavior:

- starts from the stored `cursor_history_id`
- uses `users.history.list`
- refetches changed messages by message ID
- removes locally cached messages when Gmail reports deletion or when a changed
  message now lives in spam or trash
- falls back to a full bootstrap when Gmail reports a stale history cursor

Persisted sync state behavior:

- `last_sync_epoch_s` is the last attempted sync, whether it succeeded or failed
- `last_full_sync_success_epoch_s` is updated only after a successful full bootstrap
- `last_incremental_sync_success_epoch_s` is updated only after a successful incremental replay
- failed syncs update status and error details without overwriting the last successful timestamps

## Search behavior

Local search uses SQLite FTS5 over:

- subject
- from header
- recipient summary
- snippet
- normalized label names

Supported first-class filters:

- `--label`
- `--from`
- `--after`
- `--before`
- `--limit`

Search intentionally does not attempt to emulate the full Gmail `q` language.
The contract is local full-text search plus explicit structured filters.

## Local state

The mailbox sync/search slice adds these SQLite objects:

- `gmail_messages`
- `gmail_labels`
- `gmail_message_labels`
- `gmail_sync_state`
- `gmail_message_search`

`doctor` and `store doctor` report mailbox counts and sync state when the store
already exists.

Relevant sync fields in JSON output include:

- `last_sync_mode`
- `last_sync_status`
- `last_sync_epoch_s`
- `last_full_sync_success_epoch_s`
- `last_incremental_sync_success_epoch_s`
- `cursor_history_id`

## Safety boundaries

- sync is read-only against Gmail
- search is fully local
- Mailroom does not mutate mailbox state in this slice
- repo-local `.mailroom/` remains the only runtime storage location by default
