# AGENTS.md

Local contract for `mailroom`.

## Purpose

`mailroom` is an OSS-ready, local-first Gmail operations repository. It exists to support:

- fast mailbox search and triage
- reply and draft queue workflows
- attachment discovery and controlled export
- mailbox cleanup workflows after review
- read-only verification, audit, and ruleset hardening before bulk apply
- durable docs and operational discipline for email management

## Architecture defaults

- Native center of gravity: Rust
- Primary interfaces: CLI first, TUI second
- Single operational store: SQLite FTS5
- Scope: single user, single mailbox first
- Runtime workspace: `.mailroom/`
- Sensitive state must remain out of git

## Data handling

- Never commit live mailbox caches, OAuth credentials, tokens, or exported attachments by default.
- Keep all runtime state under `.mailroom/` unless there is a deliberate reason to introduce a new ignored path.
- Prefer one canonical representation for local mail metadata rather than duplicating state across multiple stores.
- Treat delete/archive/label/send actions as deliberate operator actions; keep analysis and mutations clearly separated in the code and docs.

## Implementation guidance

- Keep the Rust surface modular: command parsing, workspace paths, storage, Gmail adapters, and TUI should stay separate.
- Prefer repo-local paths over hidden global machine state when reasonable.
- Add new dependencies only when they earn their keep for the current milestone.
- Preserve structured output for agent and shell workflows.
- Prefer read-only audit and verification commands before broad mailbox mutations or high-volume automation apply runs.
- If a plugin-assisted Codex workflow exists for an operation, document it alongside native commands rather than pretending the repo already implements it.

## Error handling

- Treat `src/lib.rs` as the application boundary: use `anyhow` there for command dispatch and top-level context, and prefer typed `thiserror` errors in Gmail, workflow, and store layers.
- Keep error enums local to the layer that owns the failure semantics; do not introduce a repo-wide catch-all error enum unless it clearly reduces total code and cognitive load.
- For new CLI JSON contracts, normalize success and failure to one top-level shape:
  - success: `{ "success": true, "data": ... }`
  - failure: `{ "success": false, "error": { "code": ..., "message": ..., "kind": ..., "operation": ..., "causes": [...] } }`
- Keep `error.code` stable and operator-oriented, keep `error.kind` for deeper subsystem detail, and keep `error.causes` to an ordered message chain only.
- Do not include debug or backtrace payloads in the JSON error contract; use stderr and Rust backtrace env vars for deep diagnostics instead.
- Preserve existing human-facing error text unless it is misleading, ambiguous, or missing required operator action.
- When adding or changing CLI failures, keep exit codes in a small stable bucket set rather than creating one-off codes per variant.
- Keep blocking SQLite and filesystem work behind `tokio::task::spawn_blocking`; do not treat running `spawn_blocking` work as abortable.
- Add focused error-path tests for every new failure class. If CLI JSON or exit-code behavior changes, add contract tests for the new output and exit mapping in the same pass.

## Verification

Run the narrowest useful checks first, then the full local gate:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

If CLI behavior changes, also run:

```bash
cargo run -- paths --json
cargo run -- doctor --json
```

If CLI JSON or exit-code contracts change, also verify the affected command paths in both human and `--json` modes.

## Docs discipline

- Update docs when architecture, storage boundaries, or command surfaces change.
- Put durable architecture choices in `docs/decisions/`.
- Put operator procedures in `docs/operations/` or `docs/workflows/`.
- Keep docs concrete and aligned with the current binary surface.
- Update operator-facing docs in the same change whenever CLI error contracts, JSON envelopes, or exit-code behavior change.
