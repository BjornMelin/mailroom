# 0004: Unified Thread Workflow

## Status

Accepted

## Decision

`mailroom` models operator work at the Gmail thread level while keeping mailbox
sync canonical at the message level.

- `gmail_messages` remains the canonical synced mailbox store.
- `thread_workflows` is the canonical operator state for triage, follow-up,
  drafting, ready-to-send, sent, and closed stages.
- `thread_workflow_events` is an append-only event log for workflow transitions.
- local draft revisions and attachments are stored in SQLite and remain the
  source of truth for operator workflow state.
- remote Gmail drafts are treated as projections of the local draft state, not
  as the workflow source of truth.
- cleanup actions stay explicit and reviewed. Archive, label, and trash require
  an operator command with `--execute`.
- the first compose surface is plain text plus file attachments.

## Why

- operators reason about replies, waiting states, and cleanup as thread-level
  work, not as individual message rows.
- keeping sync canonical at the message level avoids reopening the storage
  ownership decision from 0003.
- storing the workflow and draft history locally makes the system resilient to
  Gmail draft drift, logout, or partial failures between local edits and remote
  mutation calls.
- explicit cleanup execution preserves the repo’s local-first and review-first
  safety posture.

## Consequences

### Positive

- one durable local workflow model for CLI and later TUI surfaces
- thread-level triage and drafting on top of a message-canonical sync layer
- durable local draft revisions and attachment manifests
- reviewed cleanup actions share the same thread context and event log

### Negative

- thread workflows add a second layer of modeling on top of synced messages
- remote Gmail drafts can drift and must be refreshed from local state
- plain-text-first composition intentionally omits richer authoring features for
  now

## Rejected alternatives

### Canonical Gmail drafts as workflow truth

Rejected because Gmail drafts are remote mutable state and do not capture the
full operator workflow model. Local workflow ownership makes retries and audit
history simpler.

### Message-scoped workflow rows

Rejected because reply, waiting, and cleanup work are naturally thread-oriented.
Using message rows as workflow truth would create noisy duplicates for the same
conversation.

### Immediate destructive cleanup without preview

Rejected because archive, relabel, and trash must remain explicit operator
actions with a clear review boundary.
