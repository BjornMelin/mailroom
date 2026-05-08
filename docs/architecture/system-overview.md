# System Overview

## Goal

Build a local-first mailbox operations system that can:

- index and search mailbox state quickly
- support triage decisions with durable local state
- stage and manage drafts/replies
- locate and export attachments intentionally
- support review-first cleanup automation without hiding destructive actions

## Core boundaries

### Native core

The native Rust core owns:

- workspace paths and local runtime layout
- config loading and precedence resolution
- storage initialization, migration ownership, and pragma hardening
- Gmail OAuth login, credential persistence, and token refresh
- active account verification against Gmail profile data
- live label reads used for operator inspection and future sync validation
- mailbox metadata sync and cursor management
- mailbox state and attachment catalog modeling
- local search over synced mailbox state
- content-addressed attachment vaulting and deliberate export
- thread-scoped workflow state and append-only workflow events
- remote Gmail draft projection and send orchestration
- reviewed cleanup actions for archive, label, and trash
- typed automation rule parsing, snapshot persistence, and thread-first bulk apply
- read-only label audit, readiness verification, and ruleset-hardening guidance
- read-only Ratatui operator shell over existing diagnostics, search, workflow, and automation reports
- structured CLI output

### TUI layer

The TUI is a thin operator shell over the native core. It must not create a
second rules engine or storage model.

The current `mailroom tui` shell is read-only and renders:

- workspace, auth, store, mailbox, and readiness diagnostics
- local SQLite FTS search results
- thread workflow queue rows
- automation rollout readiness and candidate previews

Mutation-oriented TUI work must reuse the existing CLI/service actions and add
explicit confirmation flows before exposing any Gmail write.

### Plugin-assisted operator path

Codex Gmail and GitHub capabilities remain useful for:

- live mailbox inspection beyond the current native metadata surface
- reply drafting and thread understanding
- repo and PR workflows
- comparing native behavior against a known-good operator loop

### Runtime workspace

`.mailroom/` is the local operational root:

- `auth/`: OAuth material and account wiring
- `cache/`: transient fetch and derivation caches
- `state/`: SQLite database, WAL files, and local workflow state
- `vault/`: intentional file retention area
- `exports/`: generated exports and review artifacts
- `logs/`: runtime logs

The repo-local runtime root can also contain `.mailroom/config.toml`, which
overrides the user-level config location but remains ignored from git.

## Current substrate

The current native substrate is intentionally narrow but now usable:

- typed config resolution with `Figment`
- user config discovery via `directories::ProjectDirs`
- repo-local config overrides under `.mailroom/config.toml`
- SQLite bootstrap through `rusqlite`
- migration application through embedded SQL files and `rusqlite_migration`
- Gmail installed-app OAuth using PKCE and a loopback localhost callback
- repo-local credential storage under `.mailroom/auth/`
- active account persistence in SQLite
- live Gmail profile and label inspection through the native client
- one-shot mailbox sync over Gmail message metadata and history replay
- local SQLite FTS5 search over metadata and snippet text
- attachment metadata cataloging plus on-demand vault/export flows
- thread-scoped workflow state with triage buckets, snooze, and stage promotion
- local draft revisions with file attachments and remote Gmail draft synchronization
- explicit draft send and reviewed cleanup actions that resync the mailbox afterward
- typed TOML automation rules and persisted review snapshots for bulk cleanup
- read-only audit commands for label taxonomy drift, header coverage, and rollout readiness
- read-only terminal shell over diagnostics, search, workflows, and automation rollout
- hardened connection defaults: `foreign_keys=ON`, `trusted_schema=OFF`, `journal_mode=WAL`, `synchronous=NORMAL`, and a nonzero busy timeout

This substrate now covers the first complete operator loop for search, thread
triage, drafting, sending, and reviewed cleanup without inventing a second
config, auth, or storage path later.

Detailed sync/search ownership, including the “last attempt” versus “last successful
full or incremental sync” contract, is defined in
`docs/decisions/0003-message-canonical-sync.md` and
`docs/operations/mailbox-sync-and-search.md`.

Detailed thread workflow ownership is defined in
`docs/decisions/0004-unified-thread-workflow.md` and
`docs/operations/thread-workflow-and-cleanup.md`.

Detailed attachment ownership is defined in
`docs/decisions/0005-attachment-canonical-model.md` and
`docs/operations/attachment-catalog-and-export.md`.

Detailed automation ownership is defined in
`docs/decisions/0006-review-first-automation-rules.md` and
`docs/operations/automation-rules-and-bulk-actions.md`.

Detailed verification and hardening ownership is defined in
`docs/decisions/0007-verification-audit-hardening.md` and
`docs/operations/verification-and-hardening.md`.

Detailed TUI ownership is defined in
`docs/decisions/0008-read-only-tui-foundation.md` and
`docs/operations/tui-operator-shell.md`.

## Non-goals for v1

- full mailbox mirroring by default
- full body and MIME indexing in the first native search slice
- autonomous background rule execution or one-click unsubscribe as the primary feature
- external search infrastructure
- shared multi-user collaboration
