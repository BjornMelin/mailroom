# 0010: TUI Draft And Cleanup Flows

## Status

Accepted

## Context

Decision 0009 allowed the Workflows pane to run low-risk local workflow actions
through existing workflow services. The next operator gap is draft and cleanup
work from the same selected-thread context without reimplementing Gmail or store
logic in the TUI.

The CLI service layer already owns draft creation, draft body replacement, draft
send, cleanup preview, cleanup execute, post-mutation sync, and workflow event
recording. The TUI should reuse that layer and add only terminal interaction
state, confirmation text, and rendering.

## Decision

The TUI Workflows pane may expose draft and cleanup flows for the selected
workflow:

- inspect current draft detail with `workflow show`
- start a Gmail reply or reply-all draft
- replace the current draft body with plain text
- send the current Gmail draft
- preview archive, label, and trash cleanup
- execute archive, label, and trash cleanup

All actions must call the existing workflow service functions. The TUI must not
write workflow rows, draft rows, labels, Gmail messages, or mailbox sync state
directly.

Draft send requires typing `SEND` exactly before confirmation. Cleanup execute
requires toggling out of preview mode, typing `APPLY` exactly, and confirming.
Cleanup preview remains the default mode.

After a successful draft or cleanup action, the TUI refreshes the mailbox-backed
snapshot, preserves the selected workflow when possible, refreshes active local
search results, and reloads selected draft detail.

## Why

- Operators can complete the single-thread draft and cleanup loop without
  switching between TUI inspection and CLI commands.
- Reusing workflow services preserves the single operational store owner,
  existing Gmail adapters, post-action sync behavior, and error handling.
- High-friction confirmation keeps irreversible Gmail mutations deliberate.
- Keeping preview as the cleanup default preserves the review-first safety
  model.

## Consequences

Positive:

- selected-thread draft and cleanup work can happen from one terminal surface
- draft send and cleanup execute remain explicit Gmail mutations
- existing service tests and CLI contracts continue to own mutation semantics
- TUI tests cover modal state, cancellation, text input, and confirmation gates

Negative:

- the TUI now has more modal state and key routing complexity
- plain-text draft body editing is intentionally basic
- automation apply and rules editing still require CLI workflows

## Rejected alternatives

### Direct Gmail calls from the TUI

Rejected because Gmail auth, draft projection, cleanup mutation, sync refresh,
and error classification already belong to the workflow service layer.

### One-key send or execute actions

Rejected because sending drafts, archiving mail, labeling mail, and trashing
mail are real Gmail mutations. The TUI should make the operator restate intent
before executing.

### Add automation apply and rules editing in the same slice

Rejected because automation apply and rules editing are bulk/workspace-level
operations, not selected-thread flows. They need separate review surfaces and
should remain CLI-only until the automation TUI slice.
