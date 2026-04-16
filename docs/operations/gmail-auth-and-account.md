# Gmail Auth And Account

## Purpose

This runbook covers the native Gmail auth and account surface in `mailroom`.

The current native flow owns:

- Gmail OAuth login
- repo-local credential persistence
- active account persistence in SQLite
- live profile verification
- live label reads

It does not yet own full mailbox sync, search indexing, or destructive mailbox
operations.

## Required config

At minimum, configure:

```toml
[gmail]
client_id = "your-installed-app-client-id.apps.googleusercontent.com"
```

Optional:

- `client_secret`
- `listen_host`
- `listen_port`
- `open_browser`
- endpoint overrides for tests and mocks

## Native commands

```bash
cargo run -- auth status --json
cargo run -- auth login --no-browser
cargo run -- auth logout --json
cargo run -- account show --json
cargo run -- gmail labels list --json
```

## Plugin-assisted workflow (Codex)

Codex Gmail capabilities are the operator-assisted inspection path before native
sync and search workflows are ready.

- inspect mailbox and thread context before local sync tooling is complete
- compare plugin-assisted reads with `cargo run -- account show --json` and
  `cargo run -- gmail labels list --json`
- keep final mutation decisions in native Mailroom commands so inspection and
  mutation stay clearly separated

## Login flow

`mailroom auth login`:

1. resolves config and ensures runtime directories exist
2. initializes the SQLite store if needed
3. binds a localhost callback listener
4. creates a PKCE authorization request
5. opens the browser when enabled, while also printing the URL
6. exchanges the returned code for tokens
7. verifies the mailbox with `users.getProfile`
8. upserts the active account row
9. writes credentials to `.mailroom/auth/gmail-credentials.json`

## Local storage

- credentials live in `.mailroom/auth/gmail-credentials.json`
- the active account record lives in SQLite `accounts`
- logout removes the credential file and marks active accounts inactive

## Scope policy

Mailroom defaults to `gmail.modify`.

That scope is intentionally broader than read-only mailbox metadata because the
long-term product includes search, labeling, archive/delete review workflows,
and draft/reply operations. The current branch only uses the read side of that
scope.
