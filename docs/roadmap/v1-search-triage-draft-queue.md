# V1: Search, Triage, Draft Queue

## Objective

Deliver the first complete operational slice:

- local mailbox metadata store
- fast local search
- triage-oriented workflow state
- draft/reply queue state
- CLI flows first, TUI surfaces second

## Included

- workspace initialization
- config contract
- SQLite schema ownership
- SQLite bootstrap and diagnostics
- mailbox/account modeling
- mailbox metadata sync
- local search primitives
- triage queues and status tracking
- draft queue records and operator notes
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

The next implementation slice should start at triage queues and durable follow-up state, not re-open auth, account, config, store, or mailbox sync ownership.

## Deferred

- unsubscribe automation
- bulk cleanup heuristics
- advanced semantic/vector search
- external search engines
- multi-account support
- shared/team workflows

## Success condition

An operator can set up the workspace, sync mailbox metadata locally, search it quickly, classify work for follow-up, and stage reply/draft actions in a durable local system.
