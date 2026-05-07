# 0008: Read-Only TUI Foundation

## Status

Accepted

## Context

Mailroom already has CLI-first surfaces for:

- local SQLite mailbox search
- thread workflow inspection and mutation
- draft/send and reviewed cleanup commands
- review-first automation rules and rollout reports
- read-only label and verification audits

The next operator need is a faster terminal cockpit for inspecting those
surfaces together. The risk is creating a second workflow engine, rules engine,
or mailbox representation just to make the UI feel richer.

## Decision

Mailroom adds `mailroom tui` as a Ratatui-based, read-only operator shell.

- The TUI is a thin presentation layer over existing services and reports.
- Startup loads `doctor`, `audit verification`, `workflow list`, and
  `automation rollout` data from the existing Rust core.
- The Search pane runs local SQLite FTS queries through the same mailbox search
  service as `mailroom search`.
- No TUI view exposes Gmail mutations, draft send, cleanup execution,
  attachment export, automation snapshot creation, or automation apply.
- `ratatui` is compiled with default features disabled and only the Crossterm
  backend enabled to avoid pulling extra widget/backend surface.

## Why

- Operators get one screen for readiness, search, workflow queue inspection, and
  automation rollout posture without leaving the terminal.
- Read-only scope keeps the first TUI branch safe for a real Gmail account.
- Reusing existing service functions preserves the single SQLite store and the
  existing CLI JSON contracts as the source of truth.
- A small TUI foundation gives later mutation-oriented branches a stable place
  to add confirmation flows without mixing them into the first shell.

## Consequences

Positive:

- lower-friction daily inspection flow
- no new persisted TUI state
- no new rules engine, query model, or workflow ownership
- clear seam for future production TUI actions

Negative:

- the first shell is intentionally not a full mailbox client
- searches still require a prior local sync
- startup report freshness is bounded by the local cache and current rules file

## Rejected alternatives

### TUI-first mutation controls

Rejected for the first TUI slice. Mutation controls need richer confirmation,
diff, canary, and audit affordances than the first shell should carry.

### Separate TUI store or view models persisted to disk

Rejected because the existing SQLite store and service reports already own the
needed facts. Persisting TUI-specific state would create drift.

### Full default Ratatui feature set

Rejected because Mailroom only needs core widgets and the Crossterm backend for
this shell. Extra default features do not earn their dependency cost yet.
