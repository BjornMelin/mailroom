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
- mailbox/account modeling
- search primitives
- triage queues and status tracking
- draft queue records and operator notes
- plugin-assisted operator documentation

## Deferred

- unsubscribe automation
- bulk cleanup heuristics
- advanced semantic/vector search
- external search engines
- multi-account support
- shared/team workflows

## Success condition

An operator can set up the workspace, inspect mailbox-derived state, search it quickly, classify work for follow-up, and stage reply/draft actions in a durable local system.

