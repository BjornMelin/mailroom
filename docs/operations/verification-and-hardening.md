# Verification And Hardening

## Purpose

This runbook is the intended path from “Mailroom features compile and basic auth
works” to “real mailbox mutations are safe enough to trust.”

Use it before:

- building the first personal ruleset
- applying automation snapshots to more than a handful of threads
- trusting list-header rules such as `has_list_unsubscribe`
- broad archive/label cleanup waves

The goal is to prove the local cache, label taxonomy, canary write path, and
first-wave rules before any large live mutation.

## Core stance

- Mailroom workflow state is canonical locally.
- Gmail labels are durable category/routing signals, not workflow truth.
- verification is read-only first, mutation second.
- the first real automation wave should be `archive` plus `label` only.
- `trash` and unsubscribe execution stay out of the first rollout wave.

## Phase 0: Baseline health

Start by verifying the local workspace, store, and auth state:

```bash
cargo run -- auth status --json
cargo run -- doctor --json
cargo run -- audit verification --json
```

You want to see:

- an active authenticated Gmail account
- an initialized local database
- nonzero mailbox message counts
- no sync health warning

If `audit verification` reports a shallow bootstrap window or zero
`messages_with_list_unsubscribe`, that is expected for a first pass. It means
the next step is the deep audit sync, not that Mailroom is broken.

## Phase 1: Deep audit sync

Keep the normal operational window narrow, but do one deeper sync before
finalizing the first personal ruleset:

```bash
cargo run -- sync run --profile deep-audit --json
cargo run -- audit verification --json
```

The preset currently expands to:

```bash
cargo run -- sync run --full --recent-days 365 --quota-units-per-minute 9000 --message-fetch-concurrency 3 --json
```

Why:

- rare-but-important mail appears in the local review corpus
- newly cached automation headers become visible
- list-driven rules stop guessing from a shallow slice

Keep `90d` as the normal operating posture. The deeper sync is an audit step,
not the permanent default.

## Phase 2: Label taxonomy audit

Inspect the current local label surface:

```bash
cargo run -- audit labels --json
```

Read this report in three parts:

1. `numbered_overlap_groups`
   Use this to find numbered canonical labels that still overlap with legacy
   labels such as `0. To Reply` versus `To Reply`.
2. `empty_user_labels`
   These are strong candidates for cleanup or deliberate preservation.
3. `top_user_labels`
   These show the most active user labels in the local cache and help you pick
   the first ruleset bundle.

Before high-volume automation, clean up obvious numbered-vs-legacy duplicates.

## Phase 3: Manual canary mutations

Do not start with automation. Prove the native write path on a tiny canary set.

### Draft/send canary

Use a self-addressed thread or another harmless canary thread:

```bash
cargo run -- search "from:me" --limit 5 --json
cargo run -- workflow show <thread-id> --json
cargo run -- draft start <thread-id> --json
cargo run -- draft body <thread-id> --text "Mailroom canary reply." --json
cargo run -- draft send <thread-id> --json
```

### Cleanup canary

Use a tiny handpicked set of threads:

```bash
cargo run -- cleanup archive <thread-id> --json
cargo run -- cleanup archive <thread-id> --execute --json

cargo run -- cleanup label <thread-id> --add "1. Newsletters" --remove INBOX --json
cargo run -- cleanup label <thread-id> --add "1. Newsletters" --remove INBOX --execute --json
```

Keep this canary phase small. Do not introduce `trash` yet.

## Phase 4: First-wave ruleset construction

The first ruleset should be intentionally conservative.

Recommended first-wave shape:

- exact sender rules first
- existing category labels second
- age thresholds third
- list-header matchers only after the deep sync proves nonzero header coverage

Recommended first proving bundle:

- Dev
- Newsletters
- Jobs

Good early candidates are notification-heavy senders with low downside:

- `notifications@github.com`
- `notifications@vercel.com`
- newsletter senders you already trust
- job alert senders you already classify the same way

Avoid automating anything costly or high-context yet:

- finance
- health
- legal
- housing
- school
- personal threads that still need judgment

## Phase 5: Preview-only rules

Validate the rules file and preview a bounded run:

```bash
cp config/automation.example.toml .mailroom/automation.toml
cargo run -- automation rules suggest --json
$EDITOR .mailroom/automation.toml

cargo run -- automation rules validate --json
cargo run -- automation rollout --limit 10 --json
cargo run -- automation run --limit 10 --json
cargo run -- automation show <run-id> --json
```

Inspect:

- `blockers`
- `warnings`
- preview-only `candidates`
- `selected_rule_ids`
- `candidate_count`
- `candidate_details`
- `matched_predicates`
- label add/remove deltas

If the preview is surprising, fix the rules before any live apply.

## Phase 6: Micro-batch live apply

Start with micro-batches only:

```bash
cargo run -- automation rollout --rule <rule-id> --limit 10 --json
cargo run -- automation run --rule <rule-id> --limit 10 --json
cargo run -- automation show <run-id> --json
cargo run -- automation apply <run-id> --execute --json
```

First live mutation rules:

- prefer `archive`
- allow `label(add/remove)`
- keep the batch small
- avoid `trash`
- keep unsubscribe assistance read-only

After every apply:

```bash
cargo run -- doctor --json
cargo run -- audit verification --json
```

Look for:

- sync reconciliation still healthy
- no unexpected label drift
- no surprising candidate spillover when you run the next preview

## Suggested acceptance checklist

Treat the first real ruleset as accepted only when all of these are true:

- deep audit sync completed successfully
- label overlap report is understood and obvious duplicates are handled
- self-canary draft/send succeeded
- canary archive/label mutations succeeded
- at least one preview-only automation run looked exactly right
- at least one micro-batch `archive` or `label` automation apply completed with
  low surprise
- `audit verification` no longer warns about the specific rule family you plan
  to expand next

## Commands recap

```bash
cargo run -- auth status --json
cargo run -- doctor --json
cargo run -- audit labels --json
cargo run -- audit verification --json
cargo run -- sync run --profile deep-audit --json
cargo run -- automation rules validate --json
cargo run -- automation rollout --limit 10 --json
cargo run -- automation run --limit 10 --json
cargo run -- automation show <run-id> --json
cargo run -- automation apply <run-id> --execute --json
cargo run -- automation prune --older-than-days 30 --json
```
