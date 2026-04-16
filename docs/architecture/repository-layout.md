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
- `src/gmail/`: native Gmail HTTP client and live mailbox reads
- `src/doctor.rs`: combined workspace/store/auth health reporting
- `src/workspace.rs`: repo-root runtime path layout and initialization
- `src/store/`: SQLite connection policy, embedded migrations, account persistence, and store diagnostics

## Expected code evolution

As the codebase grows, prefer a layout along these lines:

- `src/cli/`: command parsing and output shaping
- `src/workspace/`: repo-local path and runtime initialization
- `src/auth/`: OAuth and credential-store abstractions
- `src/store/`: SQLite schema, queries, and search primitives
- `src/gmail/`: Gmail auth and API adapters
- `src/workflows/`: triage, drafting, cleanup flows, export flows
- `src/tui/`: ratatui application shell

Do not introduce duplicate ownership of workflow rules between CLI, TUI, and adapters.
