# TUI Operator Shell

`mailroom tui` opens the native read-only terminal shell.

It is designed for fast inspection after `workspace init`, auth setup, and a
local sync. It does not replace the CLI JSON contract; it renders the same
underlying service reports for human operation.

## Run

```bash
cargo run -- tui
```

Seed the Search pane with an initial local query:

```bash
cargo run -- tui --search "project alpha"
```

## Views

- Dashboard: workspace, database, auth, account, mailbox count, and readiness
  flags from `doctor` plus `audit verification`.
- Search: local SQLite FTS search through the same service as `mailroom search`.
- Workflows: read-only `workflow list` queue overview.
- Automation: read-only `automation rollout` readiness and candidate preview.
- Help: key bindings and safety posture.

## Keys

- `q` or `Esc`: quit
- `Tab` / `Shift+Tab`: move between views
- `1` through `5`: jump to a view
- `/`: activate search input
- `Enter`: submit search input
- `r`: refresh dashboard, workflow, and automation reports
- `Ctrl-C`: quit

## Safety Contract

The first TUI shell is read-only.

It does not:

- send drafts
- create or update Gmail drafts
- archive, label, or trash mail
- apply automation snapshots
- create automation run snapshots
- fetch or export attachments
- edit `.mailroom/automation.toml`

Use the existing CLI commands for deliberate mutation flows:

```bash
cargo run -- draft send <thread-id> --json
cargo run -- cleanup archive <thread-id> --execute --json
cargo run -- automation run --limit 10 --json
cargo run -- automation apply <run-id> --execute --json
```

## Troubleshooting

If the Dashboard has no account, run:

```bash
cargo run -- auth status --json
cargo run -- account show --json
```

If Search is empty or reports no active account, run:

```bash
cargo run -- sync run --json
cargo run -- search "known term" --json
```

If Automation reports missing rules, create or copy a rules file:

```bash
cp config/automation.example.toml .mailroom/automation.toml
cargo run -- automation rules validate --json
```

The TUI intentionally reports these conditions rather than trying to repair or
mutate local state.
