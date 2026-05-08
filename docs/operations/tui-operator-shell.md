# TUI Operator Shell

`mailroom tui` opens the native terminal operator shell.

It is designed for fast inspection and deliberate local workflow actions after
`workspace init`, auth setup, and a local sync. It does not replace the CLI JSON
contract; it renders the same underlying reports for human operation and uses
existing service owners for every action.

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
- Search: local SQLite FTS search through the mailbox read model.
- Workflows: `workflow list` queue overview, selected-row detail, and confirmed
  local workflow actions.
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

Workflow view keys:

- `j` / `Down`: select next workflow row
- `k` / `Up`: select previous workflow row
- `t`: open a triage-bucket confirmation
- `p`: open a workflow-promotion confirmation
- `z`: open a snooze / clear-snooze confirmation

Workflow confirmation keys:

- `Enter`: confirm the displayed action
- `Esc` or `q`: cancel the confirmation
- `Ctrl-C`: quit the TUI
- `Tab` / `Shift+Tab`: cycle triage bucket or promotion target
- `1` through `4`: choose `urgent`, `needs_reply_soon`, `waiting`, or `fyi`
  in a triage confirmation
- `f` / `r`: choose `follow_up` or `ready_to_send` in a promotion confirmation
- text input: type `YYYY-MM-DD` in a snooze confirmation; leave empty to clear
  the snooze

## Safety Contract

The TUI shell is still review-first. It exposes only local workflow mutation
actions from the existing workflow service layer:

- `triage set`
- `workflow promote` to `follow_up` or `ready_to_send`
- `workflow snooze` or clear snooze

Promotion to `closed` remains CLI-only in this slice because that service path
can retire and delete a remote Gmail draft.

It does not:

- send drafts
- create or update Gmail drafts
- archive, label, or trash mail
- apply automation snapshots
- create automation run snapshots
- fetch or export attachments
- edit `.mailroom/automation.toml`

Use the existing CLI commands for mutation flows that are intentionally outside
this TUI slice:

```bash
cargo run -- draft send <thread-id> --json
cargo run -- workflow promote <thread-id> --to closed --json
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
