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

- live mailbox inspection outside the local sync window
- ad hoc thread understanding against current Gmail state
- one-off operations not yet modeled in the native CLI
- one-off labeling or archival actions
- validating future native behavior against real mailbox results

### Use native `mailroom` workflows for

- Gmail login, local credential persistence, and active account tracking
- live mailbox profile verification and label reads
- repo-owned local search/index state
- repeatable thread triage workflows
- durable local draft/reply state
- reviewed archive, label, and trash actions
- attachment vaulting and export policy
- long-term automation and TUI experiences

## Rule

If an operation exists only in the plugin path today, document it plainly. Do
not claim the native binary already owns it.
