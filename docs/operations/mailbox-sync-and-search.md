# Mailbox Sync And Search

## Purpose

This runbook covers the native mailbox metadata sync and local search surface in
`mailroom`.

The current native flow owns:

- label refresh from Gmail
- recent-window bootstrap sync
- incremental sync from stored Gmail history IDs
- local metadata + snippet indexing in SQLite FTS5
- attachment metadata cataloging from Gmail message payloads
- local search with explicit filters

It does not yet own:

- full-body indexing
- attachment content indexing
- bulk attachment export or document processing

Thread workflow, draft/send, and cleanup behavior live in
`docs/operations/thread-workflow-and-cleanup.md`.
Attachment fetch/export behavior lives in
`docs/operations/attachment-catalog-and-export.md`.

## Commands

Run a normal sync:

```bash
cargo run -- sync run --json
```

Lower the adaptive ceilings if a deep resync needs extra headroom:

```bash
cargo run -- sync run --full --recent-days 365 --quota-units-per-minute 9000 --message-fetch-concurrency 3 --json
```

Force a fresh bootstrap over a specific recent window:

```bash
cargo run -- sync run --full --recent-days 30 --json
```

Run the same sync surface in benchmarking mode when you want explicit
throughput and pipeline telemetry for tuning:

```bash
cargo run -- sync benchmark --full --recent-days 365 --json
```

For real-mailbox hardening before the first production ruleset, use one deeper
audit sync once:

```bash
cargo run -- sync run --full --recent-days 365 --json
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

All `--json` commands in this slice return the normalized Mailroom envelope:

- success: `{ "success": true, "data": ... }`
- failure: `{ "success": false, "error": { code, message, kind, operation, causes } }`

## Sync behavior

The sync flow is one-shot and local-first:

1. refresh the active account from Gmail profile data
2. refresh the label catalog
3. decide between full bootstrap and incremental replay
4. fetch Gmail message payloads with bounded concurrency and quota-aware pacing
5. normalize metadata into bounded write batches
6. persist mailbox rows, attachment rows, label joins, FTS rows, and sync cursor state through a single writer lane

Default bootstrap behavior:

- query: `in:anywhere -in:spam -in:trash newer_than:{N}d`
- default `N`: `90`
- default quota budget: `12000` Gmail quota units per minute
- default message fetch concurrency: `4`
- default list/history page size: `500`
- storage: metadata, snippet, and attachment rows
- adaptive pacing: default-on, persisted per account, and bounded by the current
  CLI ceilings

Incremental sync behavior:

- starts from the stored `cursor_history_id`
- uses `users.history.list`
- refetches changed messages by message ID
- removes locally cached messages when Gmail reports deletion or when a changed
  message now lives in spam or trash
- falls back to a full bootstrap when Gmail reports a stale history cursor
- keeps the history walk serial so later-page deletes still dominate before any
  message fetch work is staged

Persisted sync state behavior:

- `last_sync_epoch_s` is the last attempted sync, whether it succeeded or failed
- `last_full_sync_success_epoch_s` is updated only after a successful full bootstrap
- `last_incremental_sync_success_epoch_s` is updated only after a successful incremental replay
- failed syncs update status and error details without overwriting the last successful timestamps

Full bootstrap checkpoint behavior:

- full bootstraps stage labels and message pages in SQLite before the live mailbox tables are replaced
- the active checkpoint stores the bootstrap query, current Gmail `nextPageToken`, progress counters, and staged row counts
- if a full sync dies mid-stream and the next run uses the same bootstrap query, Mailroom resumes from the saved checkpoint instead of replaying the whole bootstrap window
- if the requested bootstrap query changes, Mailroom discards the old checkpoint and restarts from page 1
- if Gmail rejects a saved `pageToken`, Mailroom clears the staged checkpoint and restarts the full bootstrap safely
- the live mailbox cache remains unchanged until the staged full sync finalizes successfully

Bounded pipeline behavior:

- full and incremental sync now run through a bounded pipeline: list work, fetch
  message catalogs, normalize, then hand batches to one writer lane
- the writer lane is the only stage that advances durable progress or touches
  sync state
- full sync stages page manifests and page-message membership in SQLite so a
  large listed page can be written in bounded sub-batches while checkpoint
  ownership still stays page-ordered
- full-sync checkpoint progress only advances after a whole listed page is
  durably staged in page order
- any in-flight work that was fetched or normalized but not yet written is
  intentionally replayed after restart
- incremental sync now stages delete IDs and upsert batches before one final
  atomic apply to the live mailbox tables

Quota hardening behavior:

- Gmail read calls are budgeted by documented quota units instead of raw request count
- `users.messages.list` and `users.messages.get` are paced under one shared limiter
- GET retries respect `Retry-After` when present
- Gmail usage-limit `403` responses (`rateLimitExceeded`, `userRateLimitExceeded`) are retried like `429`
- adaptive pacing downshifts quota ceilings on quota-pressure retries and steps
  message-fetch concurrency down on Gmail concurrent-request pressure
- adaptive pacing only upshifts learned pacing targets after later clean successful
  runs; one-off lower CLI ceilings do not permanently ratchet learned state down
- persisted pacing rows are clamped to supported bounds on load so corrupt local
  state cannot abort sync startup
- sync reports now include estimated reserved quota units, pressure-classified retry
  counts, `Retry-After` wait totals, and both the capped and effective fetch pacing

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
- `gmail_message_attachments`
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
- `full_sync_checkpoint`
- `sync_pacing_state`
- `pipeline_enabled`
- `pipeline_list_queue_high_water`
- `pipeline_write_queue_high_water`
- `pipeline_write_batch_count`
- `pipeline_writer_wait_ms`
- `pipeline_fetch_batch_count`
- `pipeline_fetch_batch_avg_ms`
- `pipeline_fetch_batch_max_ms`
- `pipeline_writer_tx_count`
- `pipeline_writer_tx_avg_ms`
- `pipeline_writer_tx_max_ms`
- `pipeline_reorder_buffer_high_water`
- `pipeline_staged_message_count`
- `pipeline_staged_delete_count`
- `pipeline_staged_attachment_count`

Relevant `sync run` output fields now also include:

- `resumed_from_checkpoint`
- `checkpoint_reused_pages`
- `checkpoint_reused_messages_upserted`
- `pipeline_enabled`
- `pipeline_list_queue_high_water`
- `pipeline_write_queue_high_water`
- `pipeline_write_batch_count`
- `pipeline_writer_wait_ms`
- `pipeline_fetch_batch_count`
- `pipeline_fetch_batch_avg_ms`
- `pipeline_fetch_batch_max_ms`
- `pipeline_writer_tx_count`
- `pipeline_writer_tx_avg_ms`
- `pipeline_writer_tx_max_ms`
- `pipeline_reorder_buffer_high_water`
- `pipeline_staged_message_count`
- `pipeline_staged_delete_count`
- `pipeline_staged_attachment_count`
- `adaptive_pacing_enabled`
- `quota_units_cap_per_minute`
- `message_fetch_concurrency_cap`
- `starting_quota_units_per_minute`
- `starting_message_fetch_concurrency`
- `effective_quota_units_per_minute`
- `effective_message_fetch_concurrency`
- `adaptive_downshift_count`
- `quota_pressure_retry_count`
- `concurrency_pressure_retry_count`
- `backend_retry_count`
- `retry_after_wait_ms`
- `duration_ms`
- `pages_per_second`
- `messages_per_second`

## Safety boundaries

- sync is read-only against Gmail
- search is fully local
- Mailroom mutates mailbox state only through the explicit thread workflow
  cleanup and draft/send commands, not through sync/search itself
- repo-local `.mailroom/` remains the only runtime storage location by default

If the goal is first-time real-mailbox rollout rather than daily operations,
continue with [`verification-and-hardening.md`](verification-and-hardening.md)
after sync health is green.
