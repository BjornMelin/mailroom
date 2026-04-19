# Thread Workflow And Cleanup

## Purpose

This runbook covers the native thread-scoped workflow surface in `mailroom`.

The current native flow owns:

- thread-scoped workflow state
- triage buckets and stage promotion
- snooze / follow-up timing
- local draft revisions with file attachments
- remote Gmail draft create/update/send
- reviewed archive, label, and trash actions

It builds on the existing auth/account and mailbox sync/search substrate. It
does not yet own:

- attachment catalog/export workflows
- unsubscribe automation
- bulk cleanup heuristics
- rich HTML composition

## Prerequisites

Before using workflow commands:

```bash
cargo run -- auth status --json
cargo run -- sync run --json
```

Workflow commands require:

- an authenticated active Gmail account
- a locally synced mailbox entry for the target thread

If a thread is not in the local cache yet, run `mailroom sync run` first.

All workflow commands with `--json` return the normalized Mailroom envelope:

- success: `{ "success": true, "data": ... }`
- failure: `{ "success": false, "error": { code, message, kind, operation, causes } }`

## Workflow stages

- `triage`
- `follow_up`
- `drafting`
- `ready_to_send`
- `sent`
- `closed`

## Triage buckets

- `urgent`
- `needs_reply_soon`
- `waiting`
- `fyi`

## Inspect and classify work

List all workflow items:

```bash
cargo run -- workflow list --json
```

Filter by stage or bucket:

```bash
cargo run -- workflow list --stage triage --triage-bucket urgent --json
```

Show one workflow item in detail:

```bash
cargo run -- workflow show thread-123 --json
```

Set a triage bucket and optional note:

```bash
cargo run -- triage set thread-123 --bucket urgent --note "reply before 3pm" --json
```

Promote a workflow item:

```bash
cargo run -- workflow promote thread-123 --to follow-up --json
cargo run -- workflow promote thread-123 --to ready-to-send --json
```

Snooze or clear snooze:

```bash
cargo run -- workflow snooze thread-123 --until 2026-04-25 --json
cargo run -- workflow snooze thread-123 --clear --json
```

## Draft workflow

Start a reply draft:

```bash
cargo run -- draft start thread-123 --json
```

Start a reply-all draft:

```bash
cargo run -- draft start thread-123 --reply-all --json
```

Replace the draft body:

```bash
cargo run -- draft body thread-123 --text "Thanks, I will send the update shortly." --json
```

Load the draft body from a file:

```bash
cargo run -- draft body thread-123 --file ./reply.txt --json
```

Load the draft body from stdin:

```bash
printf 'Thanks for the note.\n' | cargo run -- draft body thread-123 --stdin --json
```

Add or remove attachments:

```bash
cargo run -- draft attach add thread-123 --path ./notes/reply.txt --json
cargo run -- draft attach remove thread-123 --path ./notes/reply.txt --json
```

Send the active draft:

```bash
cargo run -- draft send thread-123 --json
```

Behavior notes:

- local draft revisions are stored first
- Gmail draft state is refreshed from the local revision after each edit
- `draft send` sends the current remote draft, marks the workflow as `sent`, and
  runs a mailbox sync afterward

## Reviewed cleanup actions

Cleanup commands default to preview mode. Nothing is mutated unless
`--execute` is present.

Preview archive:

```bash
cargo run -- cleanup archive thread-123 --json
```

Execute archive:

```bash
cargo run -- cleanup archive thread-123 --execute --json
```

Preview relabeling:

```bash
cargo run -- cleanup label thread-123 --add 0.To-Reply --remove INBOX --json
```

Execute relabeling:

```bash
cargo run -- cleanup label thread-123 --add 0.To-Reply --remove INBOX --execute --json
```

Preview trash:

```bash
cargo run -- cleanup trash thread-123 --json
```

Execute trash:

```bash
cargo run -- cleanup trash thread-123 --execute --json
```

Behavior notes:

- archive removes `INBOX` at the Gmail thread level
- label cleanup resolves local label names to synced Gmail label IDs first
- trash uses Gmail thread trash, not hard delete
- cleanup execution marks the workflow `closed`, appends an event, and runs a
  mailbox sync afterward
- post-send and post-cleanup sync failures are reported as warnings while the
  completed mutation still returns success

## Local state

This slice adds these SQLite objects:

- `thread_workflows`
- `thread_workflow_events`
- `thread_draft_revisions`
- `thread_draft_attachments`

`doctor` and `store doctor` report workflow counts when the store already
exists.

Relevant fields include:

- `workflow_count`
- `open_workflow_count`
- `draft_workflow_count`
- `event_count`
- `draft_revision_count`

## Safety boundaries

- workflow state is local-first
- Gmail drafts are projections of local draft state
- send and cleanup actions are explicit operator commands
- cleanup commands preview by default
- the local event log preserves an audit trail for workflow transitions
