# Gmail Auth And Account

## Purpose

This runbook covers the native Gmail auth and account surface in `mailroom`.

The current native flow owns:

- guided Google desktop-app client setup
- Gmail OAuth login
- repo-local credential persistence
- active account persistence in SQLite
- live profile verification
- live label reads

It owns authentication and live Gmail reads that feed the native mailbox sync
and local search commands.
Workflow state, draft revisions, and cleanup actions are owned by the thread
workflow layer, not this auth/account runbook.

## Required config

Primary path:

```bash
cargo run -- auth setup
```

`mailroom auth setup` will:

1. reuse an existing imported OAuth client or inline config when one is already configured
2. otherwise offer one of these setup lanes:
   - import a downloaded Google Desktop app JSON
   - auto-discover a single `client_secret_*.json` from the current directory or `~/Downloads`
   - prompt for Client ID and optional Client Secret directly in the CLI
   - import an existing `gcloud auth application-default login` authorized-user session
3. save the imported OAuth client in `.mailroom/auth/gmail-oauth-client.json`
4. continue directly into the existing PKCE loopback login flow, or reuse the imported ADC refresh token to finish login without a browser round-trip

If you already have the downloaded JSON and want to skip the wizard selection,
this remains valid:

```bash
cargo run -- auth setup --credentials-file /path/to/client_secret.json
```

Manual config still works when you want to bypass the setup wizard:

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
cargo run -- auth setup
cargo run -- auth setup --credentials-file /path/to/client_secret.json
cargo run -- auth login --no-browser
cargo run -- auth logout --json
cargo run -- account show --json
cargo run -- gmail labels list --json
```

All `--json` commands in this slice return the normalized Mailroom envelope:

- success: `{ "success": true, "data": ... }`
- failure: `{ "success": false, "error": { code, message, kind, operation, causes } }`

## Plugin-assisted workflow (Codex)

Codex Gmail capabilities remain useful as a live inspection and comparison path.

- inspect mailbox and thread context beyond the local sync window
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

If `.mailroom/auth/gmail-oauth-client.json` exists, `mailroom auth login`
resolves that imported Desktop app client first. Legacy inline `gmail.client_id`
config is only used when no imported client file is present.

Mailroom stores the imported client in the standard Google Desktop app
`installed` JSON shape so the file is human-readable, familiar, and durable.

## Local storage

- imported OAuth client metadata lives in `.mailroom/auth/gmail-oauth-client.json`
- credentials live in `.mailroom/auth/gmail-credentials.json`
- the active account record lives in SQLite `accounts`
- logout removes the credential file and marks active accounts inactive

The imported OAuth client file and token file stay separate on purpose:

- the OAuth client file is stable setup state
- the credential file is revocable mailbox auth state

This keeps `auth setup` idempotent and avoids forcing operators to re-import the
Google client every time they need to re-authorize.

## Google-side setup boundary

Mailroom does not attempt to automate Google Cloud Console itself.

The intended operator path is:

1. enable Gmail API
2. create a Desktop app OAuth client
3. either download the Google JSON once, or copy the Client ID and Client Secret from the console UI
4. run `mailroom auth setup`

Mailroom prints the relevant Google Console URLs in setup guidance when the
credentials file is missing or cannot be auto-discovered.

If Google Cloud Console only shows copyable Client ID / Client Secret values,
that is still sufficient: choose the paste lane in `mailroom auth setup` and
complete the rest from the CLI.

## ADC import

`mailroom auth setup` can also import an existing gcloud Application Default
Credentials authorized-user session when:

- `GOOGLE_APPLICATION_CREDENTIALS` points at an authorized-user JSON file, or
- `~/.config/gcloud/application_default_credentials.json` exists

This path is intentionally secondary and advanced:

- it only supports authorized-user ADC, not service accounts
- it assumes the stored refresh token was created with Gmail-compatible scopes
- it does not replace the normal Desktop app client path for new users

## Scope policy

Mailroom defaults to `gmail.modify`.

That scope is intentionally broader than read-only mailbox metadata because the
native product now owns search, sync, draft/reply send flows, and reviewed
archive/label/trash actions.
