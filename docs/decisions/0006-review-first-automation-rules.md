# 0006: Review-First Automation Rules

## Status

Accepted

## Context

Mailroom already owns:

- local mailbox sync and search
- thread-scoped workflow state
- explicit draft/send flows
- reviewed archive, label, and trash actions
- attachment cataloging and export

The next operator need is higher-leverage cleanup without turning Mailroom into
an autonomous Gmail daemon. The design pressure was:

- how to express automation safely
- whether bulk actions should operate on messages or threads
- whether apply should re-evaluate rules live or operate from a frozen review
  snapshot
- whether unsubscribe should become a first-class mutation path immediately

## Decision

Mailroom adds a local, typed, review-first automation layer.

- `.mailroom/automation.toml` is the authoritative local rule file.
- `config/automation.example.toml` is the tracked template and documentation
  contract.
- rules are typed TOML, not a custom DSL or embedded scripting language.
- the local mailbox store remains canonical; automation reads synced mailbox
  state instead of calling Gmail search directly.
- automation matches the latest synced message per thread and plans thread-first
  actions.
- `mailroom automation run` persists a review snapshot in SQLite.
- `mailroom automation apply <run-id> --execute` applies that saved snapshot
  instead of recomputing live candidates.
- the first action set is intentionally narrow: `archive`, `trash`, and
  `label(add/remove)`.
- unsubscribe stays assistance-only through header visibility in the review
  snapshot; this slice does not execute one-click unsubscribe flows.

## Why

- typed TOML keeps the operator contract readable, testable, and easy to diff.
- thread-first actions align with Mailroom’s existing cleanup semantics and the
  workflow model from 0004.
- persisted snapshots preserve the review boundary. Operators apply the exact
  candidate set they reviewed, even if the mailbox changes afterward.
- keeping unsubscribe assistance read-only avoids expanding the trust boundary
  into arbitrary third-party `mailto:` or HTTPS unsubscribe endpoints.

## Consequences

Positive:

- one local review loop for bulk cleanup instead of ad hoc live Gmail actions
- repeatable, auditable automation runs with stable candidate IDs and event logs
- reuse of the existing Gmail thread mutation layer and post-apply resync
- no second rules engine for a future TUI

Negative:

- rule evaluation depends on local sync freshness
- thread-first planning can intentionally coarsen message-level distinctions
- the first rule surface is constrained compared with Gmail’s server-side filter
  model or a scripting engine

## Rejected alternatives

### Gmail server-side filters as the primary automation model

Rejected because filter ownership would move into Gmail settings APIs, operate
at the message level, and split Mailroom’s local workflow truth across two
systems.

### Rule re-evaluation at apply time

Rejected because recomputing live candidates erodes the review boundary. Apply
must use the saved snapshot the operator already inspected.

### Scriptable rules with an embedded engine

Rejected because TOML plus fixed predicates solves the current use case with
much lower maintenance and cognitive load.
