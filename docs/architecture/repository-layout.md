# Repository Layout

## Tracked tree

- `src/`: native Rust code
- `config/`: tracked examples and default config contracts
- `docs/`: durable documentation
- `.github/workflows/`: CI

## Ignored tree

- `.mailroom/auth/`
- `.mailroom/cache/`
- `.mailroom/state/`
- `.mailroom/vault/`
- `.mailroom/exports/`
- `.mailroom/logs/`

## Expected code evolution

As the codebase grows, prefer a layout along these lines:

- `src/cli/`: command parsing and output shaping
- `src/workspace/`: repo-local path and runtime initialization
- `src/store/`: SQLite schema and queries
- `src/gmail/`: Gmail auth and API adapters
- `src/workflows/`: triage, drafting, cleanup flows, export flows
- `src/tui/`: ratatui application shell

Do not introduce duplicate ownership of workflow rules between CLI, TUI, and adapters.
