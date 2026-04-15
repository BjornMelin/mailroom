# Plugin-Assisted Workflows

`mailroom` is native-first, but plugin-assisted operation is part of the design.

## Why this exists

The Codex Gmail plugin already provides strong live-mailbox capabilities:

- search
- thread reads
- draft creation
- send
- labeling
- archive and delete actions

The GitHub plugin already provides:

- repository inspection
- PR/issue workflows
- repo publishing support

Documenting these alongside the native tool keeps the repo useful while the Rust implementation catches up.

## Expected usage split

### Use plugin-assisted workflows for

- live mailbox triage before native sync exists
- reply drafting against current threads
- one-off labeling or archival actions
- validating future native behavior against real mailbox results

### Use native `mailroom` workflows for

- repo-owned local search/index state
- repeatable triage workflows
- durable draft queue state
- attachment vaulting and export policy
- long-term automation and TUI experiences

## Rule

If an operation exists only in the plugin path today, document it plainly. Do not claim the native binary already owns it.

