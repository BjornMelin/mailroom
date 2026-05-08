# 0011: TUI Automation Apply and Rules Editing

## Status

Accepted.

## Context

Mailroom already has a review-first automation model:

- `.mailroom/automation.toml` is the local typed rules file.
- `automation rollout` is read-only readiness and candidate preview.
- `automation run` persists a frozen review snapshot in SQLite.
- `automation apply <run-id> --execute` mutates Gmail from that saved snapshot.

The TUI previously showed only rollout readiness. Issue #26 adds operator flows
for rules review, persisted run creation, saved candidate inspection, and
guarded apply without creating a second rules engine or bypassing the CLI safety
model.

## Decision

The TUI automation pane remains a thin operator surface over existing automation
services.

- Rules validation calls the same rules validation service as
  `automation rules validate`.
- Starter suggestions call the same suggestion service as
  `automation rules suggest` and remain disabled snippets for operator review.
- Persisted preview creation calls `automation run` semantics and writes a local
  review snapshot only.
- Candidate inspection loads a saved run by ID and renders saved candidate
  details from SQLite.
- Apply is only available for a loaded persisted run and requires typing
  `APPLY` exactly.
- The TUI never applies live `automation rollout` output.

Rules editing uses `$VISUAL` or `$EDITOR` against `.mailroom/automation.toml`
instead of a constrained in-TUI TOML form. If the active rules file is missing,
the TUI seeds it from `config/automation.example.toml` before opening the
editor. After the editor exits, the TUI validates the file and refreshes the
automation rollout report.

## Consequences

- Operators can complete the automation review loop without leaving the TUI for
  routine validation, suggestion review, snapshot creation, run inspection, and
  guarded apply.
- Operators still use their normal editor for TOML, preserving comments,
  formatting, and multi-line edits without adding a second TOML writer.
- Terminal lifecycle must temporarily restore the terminal before launching the
  editor, then re-enter Ratatui after the editor exits.
- Gmail mutation risk stays bounded because apply targets only a persisted run
  snapshot and requires exact high-friction confirmation.

## Rejected Options

### In-TUI Structured Rules Form

Rejected for this slice. A constrained form would need to represent the full
rules schema, ordering, comments, labels, and future predicates. That duplicates
rules ownership and increases the chance of silently rewriting operator TOML.

### Apply Rollout Directly

Rejected. Rollout output is a live preview, not a reviewed snapshot. Applying it
would violate the persisted review boundary from ADR 0006.

### TUI-Specific Automation Store

Rejected. Saved runs, candidates, and events already live in the canonical
SQLite automation tables. A TUI-specific store would create drift.
