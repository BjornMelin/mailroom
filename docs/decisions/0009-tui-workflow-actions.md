# 0009: TUI Workflow Actions

## Status

Accepted

## Context

Decision 0008 established `mailroom tui` as a read-only shell so the first
terminal UI branch could be safe against a real Gmail account. The next operator
need is fast triage from the same queue view without creating a second workflow
engine or bypassing the CLI/service contracts.

The existing workflow service already owns local triage, stage promotion,
snooze, draft, send, and cleanup behavior. The TUI should reuse that ownership
instead of issuing SQLite writes directly.

## Decision

The TUI Workflows pane may expose local workflow actions behind explicit
confirmation prompts:

- set triage bucket
- promote to `follow_up`
- promote to `ready_to_send`
- snooze until an operator-entered `YYYY-MM-DD`
- clear snooze

The TUI must call the existing workflow service functions for those actions and
refresh the workflow list after a successful action.

Promotion to `closed` stays CLI-only in this slice because that service path can
retire and delete a remote Gmail draft. Draft editing/sending, cleanup
execution, attachment export, automation run creation, automation apply, and
rules editing remain outside this decision.

## Why

- Local triage and follow-up management are the lowest-risk workflow mutations.
- Reusing the workflow service preserves the single SQLite store owner and the
  append-only workflow event trail.
- Confirmation prompts keep mutations deliberate while avoiding high-friction
  command switching for routine queue management.
- Excluding `closed` keeps Gmail draft deletion out of the first TUI mutation
  slice.

## Consequences

Positive:

- operators can inspect and classify workflow rows from one terminal surface
- workflow list refreshes immediately after successful actions
- no duplicate workflow persistence or Gmail adapter logic is introduced

Negative:

- the TUI is no longer globally read-only
- the Workflows pane needs explicit modal state and tests
- closed promotion still requires the CLI until draft/cleanup safety screens
  land in a later slice

## Rejected alternatives

### Direct SQLite writes from TUI code

Rejected because `src/workflows/` owns workflow mutation semantics, validation,
event logging, and account resolution.

### Add all workflow and cleanup actions at once

Rejected because draft send, cleanup execution, and closed promotion include
Gmail mutations and need stronger confirmation surfaces than the first local
workflow action slice.

