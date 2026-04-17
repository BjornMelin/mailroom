# System Overview

## Goal

Build a local-first mailbox operations system that can:

- index and search mailbox state quickly
- support triage decisions with durable local state
- stage and manage drafts/replies
- locate and export attachments intentionally
- support later cleanup automation without hiding destructive actions

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
- mailbox state modeling
- local search over synced mailbox state
- future triage and draft queue workflows
- structured CLI output

### TUI layer

The TUI should be a thin operator shell over the native core. It must not create a second rules engine or storage model.

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
- hardened connection defaults: `foreign_keys=ON`, `trusted_schema=OFF`, `journal_mode=WAL`, `synchronous=NORMAL`, and a nonzero busy timeout

This substrate exists to support later search, sync, triage, and draft
workflows without inventing a second config, auth, or storage path later.

Detailed sync/search ownership, including the “last attempt” versus “last successful
full or incremental sync” contract, is defined in
`docs/decisions/0003-message-canonical-sync.md` and
`docs/operations/mailbox-sync-and-search.md`.

## Non-goals for v1

- full mailbox mirroring by default
- full body and MIME indexing in the first native search slice
- immediate unsubscribe automation as the primary feature
- external search infrastructure
- shared multi-user collaboration
