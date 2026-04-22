# Inbox Triage Workflow

The current operator loop for inbox triage is:

1. verify local mailbox readiness and label taxonomy first
2. collect or inspect candidate messages
3. search and shortlist
4. classify into buckets
5. promote selected items into draft or follow-up state
6. execute cleanup actions only after review

Preferred buckets:

- urgent
- needs reply soon
- waiting
- fyi

The native CLI now supports this flow directly:

```bash
cargo run -- audit verification --json
cargo run -- audit labels --json
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
