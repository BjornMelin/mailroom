# V1: Search, Triage, Draft Queue

## Objective

Deliver the first complete operational slice:

- local mailbox metadata store
- fast local search
- triage-oriented workflow state
- draft/reply queue state
- reviewed cleanup actions
- review-first automation rules and bulk actions
- CLI flows first, TUI operator flows second

## Included

- workspace initialization
- config contract
- SQLite schema ownership
- SQLite bootstrap and diagnostics
- mailbox/account modeling
- mailbox metadata sync
- local search primitives
- thread-scoped triage and status tracking
- draft queue records, remote Gmail draft sync, and operator notes
- reviewed archive, label, and trash actions
- review-first automation rules with persisted snapshots
- TUI dashboard/search/workflow/automation inspection shell with confirmed
  selected-thread workflow, draft, and cleanup actions
- plugin-assisted operator documentation

## Current status

The substrate layer is in place:

- repo-local runtime initialization under `.mailroom/`
- typed config resolution with defaults, user config, repo config, and env overrides
- local SQLite bootstrap with embedded migrations
- store diagnostics that expose schema version and active pragma state
- Gmail OAuth login with PKCE and loopback localhost callback
- repo-local Gmail credential storage under `.mailroom/auth/`
- active account persistence from `users.getProfile`
- native Gmail label reads for live mailbox verification
- one-shot mailbox sync with recent-window bootstrap and incremental history replay
- local SQLite FTS5 search over subject, sender, recipients, snippet, and labels

The first workflow slice is now in place:

- thread-scoped workflow state backed by SQLite
- fixed triage buckets and stage promotion
- snooze and follow-up timing fields
- local draft revisions with file attachments
- Gmail draft create/update/send integration
- reviewed archive, label, and trash actions with post-action resync

The attachment catalog/export foundation is now in place too:

- attachment metadata rows derived from synced Gmail message payloads
- explicit vault fetch into `.mailroom/vault/`
- deliberate export into `.mailroom/exports/` or an explicit destination
- append-only attachment export event tracking

The review-first automation slice is now in place too:

- typed TOML rules under `.mailroom/automation.toml`
- disabled starter rule suggestions from recurring local mailbox evidence
- persisted automation run snapshots and append-only run events
- thread-first archive, label, and trash bulk actions gated behind `--execute`
- unsubscribe assistance through list headers in candidate inspection output

The verification and hardening slice is now in place too:

- read-only `audit labels` output for overlap, numbered-vs-legacy, and empty-label review
- read-only `audit verification` output for deep-sync readiness, header coverage, and first-wave rollout posture
- operator runbooks for deep audit syncs, self-canary send tests, and micro-batch archive/label rollout

The TUI foundation, local workflow action slice, and selected-thread
draft/cleanup slice are now in place too:

- Ratatui shell at `mailroom tui`
- Dashboard, Search, Workflows, Automation, and Help panes
- local search through the existing SQLite-backed search service
- workflow and automation inspection through the existing service reports
- selected workflow detail plus confirmed local triage, promote, and snooze
  actions through existing workflow services
- selected workflow current-draft inspection
- confirmed draft start, draft body replacement, and high-friction draft send
  through existing workflow services
- cleanup archive, label, and trash preview by default, with high-friction
  execute confirmations through existing workflow services
- no attachment export, automation apply, rules editing, or direct Gmail adapter
  calls from TUI code

The next implementation slices should focus on automation action flows and the
real personal ruleset rollout on top of the shipped audit surface, not re-open
auth, account, config, store, sync, workflow, draft, cleanup, attachment, or
automation ownership.

## Deferred

- direct one-click unsubscribe execution
- autonomous background automation
- attachment content indexing or OCR
- advanced semantic/vector search
- external search engines
- multi-account support
- shared/team workflows

## Success condition

An operator can set up the workspace, sync mailbox metadata locally, search it
quickly, classify thread work for follow-up, stage and send replies in a durable
local system, catalog/export attachments intentionally, and execute reviewed
manual or automation-driven cleanup actions intentionally. The same operator can
also open a terminal shell to inspect readiness, search, workflow queue,
automation rollout posture, and confirmed selected-thread workflow, draft, and
cleanup actions from the existing local state.
