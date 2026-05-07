# Automation Rules And Bulk Actions

## Purpose

This runbook covers Mailroom’s review-first automation surface.

The current automation slice owns:

- a local typed rules file at `.mailroom/automation.toml`
- read-only starter rule suggestions from recurring local mailbox evidence
- rule validation and preview snapshots
- read-only rollout checks for first-wave micro-batch readiness
- persisted automation runs and append-only run events
- stale local snapshot pruning after dry-run review
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
cargo run -- audit verification --json
cargo run -- audit labels --json
cp config/automation.example.toml .mailroom/automation.toml
```

Automation commands other than `automation rules suggest` require:

- an authenticated active Gmail account
- a locally synced mailbox
- an existing `.mailroom/automation.toml` file

`automation rules suggest` still requires an authenticated active account and
local sync evidence, but it does not require an existing rules file.

If you use label actions, the referenced label names must already exist in the
local Gmail label cache. If you created labels recently, run `mailroom sync run`
again first.

## Rule file

Mailroom reads only one active rule file:

- `.mailroom/automation.toml`

Use `config/automation.example.toml` as the tracked template.

You can generate disabled starter rules from recurring older `INBOX` list or
bulk sender evidence in the local cache:

```bash
cargo run -- automation rules suggest --json
cargo run -- automation rules suggest --limit 5 --min-thread-count 4 --older-than-days 21 --json
```

Suggestions are read-only. They do not write `.mailroom/automation.toml` and do
not mutate Gmail. Review the disabled TOML snippets, copy only low-surprise
rules into `.mailroom/automation.toml`, then enable one rule at a time.

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

For `label` actions, at least one of `add` or `remove` must be non-empty after
normalization, and the same normalized label name cannot appear in both lists.

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

Generate disabled starter rules before editing the active file:

```bash
cargo run -- automation rules suggest --json
```

Check first-wave rollout readiness without saving a run:

```bash
cargo run -- automation rollout --limit 10 --json
cargo run -- automation rollout --rule archive-newsletters --limit 10 --json
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

Prune stale local review snapshots after inspecting the dry-run counts:

```bash
cargo run -- automation prune --older-than-days 30 --json
cargo run -- automation prune --older-than-days 30 --status previewed --execute --json
cargo run -- automation prune --older-than-days 90 --status applied --status apply-failed --json
```

`automation prune` deletes only local SQLite automation snapshot rows. It never
mutates Gmail and it never targets in-progress `applying` runs.

## Output model

`--json` uses the standard Mailroom envelope.

Plain output includes:

- run metadata
- selected rule IDs
- candidate count
- event count
- one TSV row per candidate
- a second TSV section with per-candidate match reasoning and label/action detail

Candidate rows expose:

- `candidate_id`
- `rule_id`
- `thread_id`
- `action`
- `apply_status`
- `has_unsubscribe`
- `subject`

Candidate detail rows expose:

- `from_address`
- `attachment_count`
- `labels`
- `matched_predicates`
- `action_add_labels`
- `action_remove_labels`
- `list_id_header`
- `precedence_header`

The JSON payload also includes header-derived unsubscribe hints:

- `list_id_header`
- `list_unsubscribe_header`
- `list_unsubscribe_post_header`
- `precedence_header`
- `auto_submitted_header`

`automation rollout` returns the same standard JSON envelope with:

- `verification` readiness from `audit verification`
- optional `rules` validation detail
- preview-only candidate summaries
- blockers, warnings, next steps, and exact follow-up commands

Missing or invalid rules are reported as rollout blockers rather than as a
persisted run.

## Safety model

- `automation run` is preview-only and only writes a local snapshot
- `automation rollout` is read-only and writes no automation snapshot
- `automation apply` mutates Gmail only when `--execute` is present
- `automation apply --execute` requires working Gmail auth up front and aborts
  before persisting apply results if credentials are missing, expired, or point
  at a different Gmail account than the saved run snapshot
- apply uses the stored snapshot, not a live recompute
- thread mutations reuse the same Gmail thread cleanup path as the manual
  cleanup commands
- successful apply runs trigger a best-effort mailbox resync afterward
- `automation prune` is dry-run by default and deletes only local snapshot
  history when `--execute` is present

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
3. Run `automation rollout --limit 10` to check readiness and preview matching
   candidates without saving a run.
4. Run a preview snapshot.
5. Inspect the saved run by ID.
6. Apply only the reviewed run with `--execute`.
7. Re-run sync or `doctor` if you want to inspect the reconciled local state.
8. Periodically prune stale preview snapshots after a dry-run count review.

For the real-mailbox hardening sequence, do not jump straight from rule editing
to `automation apply --execute`. Follow
[`verification-and-hardening.md`](verification-and-hardening.md) first so the
label taxonomy, deep-sync coverage, and canary mutation path are already proven.
