# V1: Search, Triage, Draft Queue

## Objective

Deliver the first complete operational slice:

- local mailbox metadata store
- fast local search
- triage-oriented workflow state
- draft/reply queue state
- reviewed cleanup actions
- CLI flows first, TUI surfaces second

## Included

- workspace initialization
- config contract
- SQLite schema ownership
- SQLite bootstrap and diagnostics
- mailbox/account modeling
- mailbox metadata sync
- local search primitives
- thread-scoped triage and status tracking
- draft queue records, remote Gmail draft sync, and operator notes
- reviewed archive, label, and trash actions
- plugin-assisted operator documentation

## Current status

The substrate layer is in place:

- repo-local runtime initialization under `.mailroom/`
- typed config resolution with defaults, user config, repo config, and env overrides
- local SQLite bootstrap with embedded migrations
- store diagnostics that expose schema version and active pragma state
- Gmail OAuth login with PKCE and loopback localhost callback
- repo-local Gmail credential storage under `.mailroom/auth/`
- active account persistence from `users.getProfile`
- native Gmail label reads for live mailbox verification
- one-shot mailbox sync with recent-window bootstrap and incremental history replay
- local SQLite FTS5 search over subject, sender, recipients, snippet, and labels

The first workflow slice is now in place:

- thread-scoped workflow state backed by SQLite
- fixed triage buckets and stage promotion
- snooze and follow-up timing fields
- local draft revisions with file attachments
- Gmail draft create/update/send integration
- reviewed archive, label, and trash actions with post-action resync

The next implementation slice should build attachment catalog/export and richer
operator review ergonomics, not re-open auth, account, config, store, sync, or
workflow ownership.

## Deferred

- unsubscribe automation
- bulk cleanup heuristics
- attachment catalog and export flows
- advanced semantic/vector search
- external search engines
- multi-account support
- shared/team workflows

## Success condition

An operator can set up the workspace, sync mailbox metadata locally, search it
quickly, classify thread work for follow-up, stage and send replies in a durable
local system, and execute reviewed cleanup actions intentionally.
