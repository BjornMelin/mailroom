# Inbox Triage Workflow

The current operator loop for inbox triage is:

1. collect or inspect candidate messages
2. search and shortlist
3. classify into buckets
4. promote selected items into draft or follow-up state
5. execute cleanup actions only after review

Preferred buckets:

- urgent
- needs reply soon
- waiting
- fyi

The native CLI now supports this flow directly:

```bash
cargo run -- sync run --json
cargo run -- search "project alpha" --json
cargo run -- triage set thread-123 --bucket urgent --json
cargo run -- workflow promote thread-123 --to follow_up --json
cargo run -- draft start thread-123 --json
cargo run -- cleanup archive thread-123 --json
cargo run -- cleanup archive thread-123 --execute --json
```

The same local workflow state is intended to back the later TUI.
Codex Gmail/GitHub plugin-assisted reads remain useful for live inspection, but
keep the write path in the native CLI so the local store stays authoritative.
