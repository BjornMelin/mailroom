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
- config loading
- storage initialization and schema ownership
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
- `state/`: SQLite and local workflow state
- `vault/`: intentional file retention area
- `exports/`: generated exports and review artifacts
- `logs/`: runtime logs

## Non-goals for v1

- full mailbox mirroring by default
- immediate unsubscribe automation as the primary feature
- external search infrastructure
- shared multi-user collaboration

