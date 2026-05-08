# TUI Operator Shell

`mailroom tui` opens the native terminal operator shell.

It is designed for fast inspection and deliberate workflow, draft, cleanup, and
automation actions after `workspace init`, auth setup, and a local sync. It does
not replace the CLI JSON contract; it renders the same underlying reports for
human operation and uses existing service owners for every action.

## Run

```bash
cargo run -- tui
```

Seed the Search pane with an initial local query:

```bash
cargo run -- tui --search "project alpha"
```

The supported minimum terminal size is `80x24`. Smaller terminals render a
non-mutating "Terminal too small" guard instead of the main layout.

## Views

- Dashboard: workspace, database, auth, account, mailbox count, and readiness
  flags from `doctor` plus `audit verification`.
- Search: local SQLite FTS search through the mailbox read model.
- Workflows: `workflow list` queue overview, selected-row detail, current draft
  inspection, confirmed local workflow actions, Gmail draft actions, and cleanup
  preview/execute flows.
- Automation: `automation rollout` readiness, rule validation, disabled starter
  suggestion review, persisted run creation, saved-run candidate inspection, and
  guarded saved-run apply.
- Help: key bindings and safety posture.

## Keys

- `q`: quit when no confirmation modal, search input, or help overlay is active;
  when the help overlay is active, `q` closes the overlay instead
- `?` / `F1`: open the help overlay without leaving the current view
- `Esc`: quit when search editing, confirmation modals, and the help overlay are
  inactive; while search editing is active, `Esc` exits the input instead; while
  the help overlay is active, `Esc` closes the overlay
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
- `i`: inspect the selected workflow and current local draft detail
- `d`: start a Gmail reply draft for the selected workflow; in the modal,
  `Tab` / `Shift+Tab` toggles Reply or Reply-All
- `b`: edit the current Gmail draft body
- `s`: send the current Gmail draft after high-friction confirmation
- `a`: preview or execute archive cleanup
- `l`: preview or execute label cleanup
- `x`: preview or execute trash cleanup

Automation view keys:

- `j` / `Down`: select next saved-run candidate after a run is loaded
- `k` / `Up`: select previous saved-run candidate after a run is loaded
- `v`: validate `.mailroom/automation.toml`
- `g`: review disabled starter suggestions from local mailbox evidence
- `n`: create a persisted automation preview run snapshot
- `o`: load a persisted automation run by ID
- `a`: apply the loaded persisted run after high-friction confirmation
- `e`: open `.mailroom/automation.toml` in `$VISUAL`, `$EDITOR`, or `vi`

Workflow confirmation keys:

- `Enter`: confirm the displayed action
- `Esc`: cancel the confirmation
- `Ctrl-C`: quit the TUI
- `Tab` / `Shift+Tab`: cycle triage bucket, promotion target, Reply or
  Reply-All, or label cleanup fields depending on the active modal
- `1` through `4`: choose `urgent`, `needs_reply_soon`, `waiting`, or `fyi`
  in a triage confirmation
- `f` / `r`: choose `follow_up` or `ready_to_send` in a promotion confirmation
- text input: type `YYYY-MM-DD` in a snooze confirmation; leave empty to clear
  the snooze without changing the current workflow stage
- draft body input: type the replacement plain-text draft body; `Esc` cancels
  without changing the draft
- draft send input: type `SEND` exactly, then `Enter`
- cleanup input: use `Ctrl-E` to toggle from preview to execute; execute mode
  requires typing `APPLY` exactly, then `Enter`
- label cleanup input: labels are comma-separated; `Tab` / `Shift+Tab` switches
  between add, remove, and confirmation fields

Automation confirmation keys:

- `Enter`: confirm the displayed automation action
- `Esc` / `q`: cancel the automation confirmation
- run creation input: type a positive candidate limit; this creates only a
  local persisted review snapshot
- saved-run input: type a positive run ID to inspect candidates
- apply input: type `APPLY` exactly, then `Enter`

## Safety Contract

The TUI shell is still review-first. It exposes workflow, draft, and cleanup
actions only through the existing workflow service layer:

- `triage set`
- `workflow promote` to `follow_up` or `ready_to_send`
- `workflow snooze` or clear snooze
- `workflow show` for selected-workflow and current-draft inspection
- `draft start`
- `draft body`
- `draft send`
- cleanup archive, label, and trash preview
- cleanup archive, label, and trash execute

Promotion to `closed` remains CLI-only in this slice because that service path
can retire and delete a remote Gmail draft.

Draft send and cleanup execute are high-friction Gmail mutations:

- `draft send` requires the current workflow to have a synced Gmail draft ID and
  requires typing `SEND` exactly before `Enter`
- cleanup preview is the default for archive, label, and trash
- cleanup execute requires toggling execute mode with `Ctrl-E`, typing `APPLY`
  exactly, and then pressing `Enter`
- after successful actions, the TUI refreshes the workflow list, selected draft
  detail, and any active local search report through existing services

Automation actions use the existing automation service layer:

- rules validation uses `automation rules validate`
- starter suggestions use `automation rules suggest` and stay disabled snippets
  for operator review
- run creation uses `automation run` and writes a local persisted snapshot
- saved-run inspection uses `automation show`
- apply uses `automation apply <run-id> --execute` only for the loaded persisted
  run

Automation apply is a high-friction Gmail mutation:

- the TUI never applies live `automation rollout` output
- a run must be loaded before apply can open
- the confirmation summarizes the run ID, candidate count, action mix, blocked
  rollout rules when present, and a Gmail mutation warning
- apply requires typing `APPLY` exactly before `Enter`
- after successful automation actions, the TUI refreshes rollout readiness

Rules editing uses the operator's editor instead of an in-TUI TOML form. Press
`e` to open `.mailroom/automation.toml` in `$VISUAL`, `$EDITOR`, or `vi`. If the
rules file is missing, the TUI seeds it from `config/automation.example.toml`.
When the editor exits, the TUI validates the file and refreshes automation
readiness.

The footer status always includes a textual severity label in addition to color:
`OK`, `WARN`, or `ERROR`. Operators should rely on the text label rather than
color alone.

It still does not:

- promote workflows to `closed`
- fetch or export attachments
- apply rollout previews directly

Use the existing CLI commands for flows that remain intentionally outside this
TUI slice:

```bash
cargo run -- workflow promote <thread-id> --to closed --json
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

If the terminal remains in raw mode after a crash or killed process, restore the
TTY with:

```bash
reset
stty sane
```

Run PTY smoke checks before shipping TUI changes. These commands intentionally
time out after opening the TUI and write transcripts under `/tmp` with
restrictive permissions and unpredictable names because rendered mailbox data
may appear in the session log:

```bash
umask 077
SMOKE_MAIN="$(mktemp /tmp/mailroom-tui-smoke.XXXXXX.txt)"
SMOKE_SEARCH="$(mktemp /tmp/mailroom-tui-search-smoke.XXXXXX.txt)"
SMOKE_NARROW="$(mktemp /tmp/mailroom-tui-narrow-smoke.XXXXXX.txt)"
script -qefc 'timeout 2s cargo run -- tui' "$SMOKE_MAIN"
script -qefc 'timeout 2s cargo run -- tui --search "known term"' "$SMOKE_SEARCH"
script -qefc 'stty cols 40 rows 10; timeout 2s cargo run -- tui' "$SMOKE_NARROW"
```
