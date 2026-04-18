# 0003: Message-Canonical Mailbox Sync And Search

## Status

Accepted

## Decision

`mailroom` stores mailbox state with messages as the single canonical durable
unit.

- `gmail_messages` is the primary mailbox table.
- labels and message-label joins are stored separately.
- thread views are derived at query time instead of stored as a second
  canonical mailbox model.
- the local search index covers Gmail metadata plus snippet text, not full MIME
  bodies.
- the first sync surface is one-shot CLI execution, not a daemon or push
  subscriber.
- the default bootstrap scope is recent mail from All Mail excluding spam and
  trash.

## Why

- Gmail history and change replay are message-oriented.
- a single canonical unit keeps deletes, relabeling, and incremental sync
  simpler to reason about.
- metadata plus snippet gives fast local search value without committing to full
  MIME parsing, attachment extraction, or body redaction policy too early.
- one-shot sync keeps the operational contract reviewable while the product is
  still single-user and CLI-first.

## Consequences

### Positive

- one clear local truth for sync and search
- incremental replay can use Gmail history IDs directly
- search is fast and local without external infrastructure
- thread grouping can evolve later without rewriting storage ownership

### Negative

- thread summaries are computed, not cached as a first-class table
- body-only terms are not searchable yet
- initial sync is intentionally bounded to recent mail unless the operator asks
  for a broader bootstrap

## Rejected alternatives

### Canonical thread store first

Rejected because Gmail sync and history replay are fundamentally message-level,
which would force extra reconciliation logic between thread and message truth.

### Full-message and MIME indexing in the first sync slice

Rejected because it would widen the branch into content parsing, attachment
policy, and sensitive-data handling before local metadata sync proved itself.

### Background daemon or push-watch as the first sync contract

Rejected because the repo needed a reliable, explicit CLI primitive before
adding long-lived runtime behavior.
