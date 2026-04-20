# Automation Rules And Bulk Actions

## Purpose

This runbook covers Mailroom’s review-first automation surface.

The current automation slice owns:

- a local typed rules file at `.mailroom/automation.toml`
- rule validation and preview snapshots
- persisted automation runs and append-only run events
- reviewed thread-first bulk actions for archive, label, and trash
- unsubscribe assistance through visible list headers in the review output

It does not yet own:

- autonomous background execution
- direct one-click unsubscribe execution
- Gmail server-side filter management
- body-content or semantic classification

## Prerequisites

Before using automation commands:

```bash
cargo run -- auth status --json
cargo run -- sync run --json
cp config/automation.example.toml .mailroom/automation.toml
```

Automation commands require:

- an authenticated active Gmail account
- a locally synced mailbox
- an existing `.mailroom/automation.toml` file

If you use label actions, the referenced label names must already exist in the
local Gmail label cache. If you created labels recently, run `mailroom sync run`
again first.

## Rule file

Mailroom reads only one active rule file:

- `.mailroom/automation.toml`

Use `config/automation.example.toml` as the tracked template.

Supported match fields:

- `from_address`
- `subject_contains`
- `label_any`
- `older_than_days`
- `has_attachments`
- `has_list_unsubscribe`
- `list_id_contains`
- `precedence`

Supported actions:

- `archive`
- `trash`
- `label` with `add = []` and `remove = []`

Example:

```toml
[[rules]]
id = "archive-newsletters"
description = "Archive older list mail that still sits in INBOX."
enabled = true
priority = 200

[rules.match]
label_any = ["INBOX"]
older_than_days = 14
has_list_unsubscribe = true

[rules.action]
kind = "archive"
```

Rule semantics:

- rules are evaluated in priority order, highest first
- `--limit` truncates after that priority ordering, with mailbox recency only
  breaking ties between rules at the same priority
- only enabled rules participate in `automation run`
- each thread can appear at most once per run snapshot
- matching uses the latest synced message for each thread
- `older_than_days` compares against the latest synced message timestamp

## Commands

Validate the active file:

```bash
cargo run -- automation rules validate --json
```

Create a review snapshot across all enabled rules:

```bash
cargo run -- automation run --json
```

Restrict preview to specific rule IDs:

```bash
cargo run -- automation run --rule archive-newsletters --rule trash-bulk-notices --json
```

Limit the snapshot size:

```bash
cargo run -- automation run --limit 50 --json
```

Inspect a saved snapshot:

```bash
cargo run -- automation show 42 --json
```

Apply a saved snapshot:

```bash
cargo run -- automation apply 42 --execute --json
```

Without `--execute`, `automation apply` returns a validation-style error and
does not mutate Gmail.

## Output model

`--json` uses the standard Mailroom envelope.

Plain output includes:

- run metadata
- candidate count
- event count
- one TSV row per candidate

Candidate rows expose:

- `candidate_id`
- `rule_id`
- `thread_id`
- `action`
- `apply_status`
- `has_unsubscribe`
- `subject`

The JSON payload also includes header-derived unsubscribe hints:

- `list_id_header`
- `list_unsubscribe_header`
- `list_unsubscribe_post_header`
- `precedence_header`
- `auto_submitted_header`

## Safety model

- `automation run` is preview-only and only writes a local snapshot
- `automation apply` mutates Gmail only when `--execute` is present
- `automation apply --execute` requires working Gmail auth up front and aborts
  before persisting apply results if credentials are missing or expired
- apply uses the stored snapshot, not a live recompute
- thread mutations reuse the same Gmail thread cleanup path as the manual
  cleanup commands
- successful apply runs trigger a best-effort mailbox resync afterward

If a run applies zero candidates, Mailroom records the run transition locally
but does not issue Gmail mutations.

## Local state

This slice adds:

- `automation_runs`
- `automation_run_candidates`
- `automation_run_events`

`doctor` and `store doctor` now report:

- `automation_run_count`
- `automation_previewed_run_count`
- `automation_applied_run_count`
- `automation_apply_failed_run_count`
- `automation_candidate_count`

## Recommended operator loop

1. Sync local mailbox metadata.
2. Validate the active rules file.
3. Run a preview snapshot.
4. Inspect the saved run by ID.
5. Apply only the reviewed run with `--execute`.
6. Re-run sync or `doctor` if you want to inspect the reconciled local state.
