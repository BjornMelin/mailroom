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
- mailbox state modeling
- search, triage, and draft queue workflows
- structured CLI output

### TUI layer

The TUI should be a thin operator shell over the native core. It must not create a second rules engine or storage model.

### Plugin-assisted operator path

Codex Gmail and GitHub capabilities remain useful for:

- live mailbox inspection before native sync tooling is complete
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

The current native substrate is intentionally narrow:

- typed config resolution with `Figment`
- user config discovery via `directories::ProjectDirs`
- repo-local config overrides under `.mailroom/config.toml`
- SQLite bootstrap through `rusqlite`
- migration application through embedded SQL files and `rusqlite_migration`
- hardened connection defaults: `foreign_keys=ON`, `trusted_schema=OFF`, `journal_mode=WAL`, `synchronous=NORMAL`, and a nonzero busy timeout

This substrate exists to support later mailbox/account modeling, search, triage,
and draft workflows without inventing a second config or storage path later.

## Non-goals for v1

- full mailbox mirroring by default
- immediate unsubscribe automation as the primary feature
- external search infrastructure
- shared multi-user collaboration
