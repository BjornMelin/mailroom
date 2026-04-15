# ADR 0001: Mailroom Foundation

## Status

Accepted

## Decision

`mailroom` uses the following foundation:

- Rust as the native implementation language
- `clap` for the initial CLI surface
- a future `ratatui` TUI layered over the same command core
- SQLite FTS5 as the single local operational store
- repo-local ignored runtime state under `.mailroom/`
- plugin-assisted Gmail/GitHub workflows documented alongside native commands

## Context

The repo is intended to become a durable mailbox operating system rather than a thin wrapper around one-off scripts. It needs:

- strong local search and triage
- durable reply/draft queue state
- careful handling of sensitive data
- agent-friendly structured output
- a path to a richer terminal UI without forcing premature infrastructure

The design also needs to stay reviewable and low-friction for a single-user mailbox while remaining clean enough to publish as a reusable OSS tool.

## Consequences

### Positive

- one clear operational store
- no external search service required for v1
- clean separation between versioned repo content and local secrets/state
- native command surface can mature incrementally while plugin-assisted workflows remain useful
- TUI work can build on the same storage and workflow primitives

### Negative

- Gmail API integration in Rust will require deliberate auth and client work
- some high-end search or analytics ideas remain deferred until the core store proves insufficient
- the first version optimizes correctness and operability over maximal feature breadth

