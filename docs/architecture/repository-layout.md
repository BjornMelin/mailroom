# Repository Layout

## Tracked tree

- `src/`: native Rust code
- `config/`: tracked examples and default config contracts
- `migrations/`: SQL schema migrations embedded into the binary
- `docs/`: durable documentation
- `build.rs`: migration change invalidation for compile-time embedding
- `.github/workflows/`: CI

## Ignored tree

- `.mailroom/config.toml`
- `.mailroom/auth/`
- `.mailroom/cache/`
- `.mailroom/state/`
- `.mailroom/vault/`
- `.mailroom/exports/`
- `.mailroom/logs/`

## Current code ownership

- `src/cli.rs`: CLI command surface and flags
- `src/config.rs`: typed config resolution and source reporting
- `src/auth/`: Gmail OAuth flow and credential persistence
- `src/gmail/`: native Gmail HTTP client, label reads, metadata reads, history replay, and draft/thread mutation primitives
- `src/attachments.rs`: attachment listing, vault fetch, and export orchestration
- `src/automation/`: rules parsing, snapshot planning, and bulk-apply orchestration
- `src/mailbox.rs`: sync/search orchestration over Gmail and SQLite
- `src/workflows/`: thread-scoped triage, draft/send, snooze, and cleanup orchestration
- `src/doctor.rs`: combined workspace/store/auth health reporting
- `src/workspace.rs`: repo-root runtime path layout and initialization
- `src/store/`: SQLite connection policy, embedded migrations, account persistence, mailbox persistence, workflow persistence, automation persistence, and store diagnostics

## Expected code evolution

As the codebase grows, prefer a layout along these lines:

- `src/cli/`: command parsing and output shaping
- `src/workspace/`: repo-local path and runtime initialization
- `src/auth/`: OAuth and credential-store abstractions
- `src/store/`: SQLite schema, queries, mailbox persistence, and search primitives
- `src/gmail/`: Gmail auth and API adapters
- `src/attachments/` or `src/attachments.rs`: inbound attachment catalog and export flows
- `src/automation/`: review-first automation rules and bulk action snapshots
- `src/workflows/`: triage, drafting, and cleanup flows
- `src/tui/`: ratatui application shell

Do not introduce duplicate ownership of workflow or automation rules between CLI, TUI, and adapters.
