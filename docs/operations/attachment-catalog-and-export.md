# Attachment Catalog And Export

## Purpose

This runbook covers the native attachment surface in `mailroom`.

The current native flow owns:

- attachment metadata cataloging during mailbox sync
- local attachment listing and detail inspection
- on-demand byte fetch into `.mailroom/vault/`
- deliberate export into `.mailroom/exports/` or an explicit destination
- append-only export event tracking

It does not yet own:

- attachment content indexing
- OCR or document extraction
- automatic bulk export

## Commands

List cataloged attachments:

```bash
cargo run -- attachment list --json
```

Filter by message, thread, filename, MIME type, or fetched state:

```bash
cargo run -- attachment list --thread-id thread-123 --filename invoice --mime-type application/pdf --fetched-only --json
```

Show one attachment in detail:

```bash
cargo run -- attachment show m-1:1.2 --json
```

Fetch bytes into the local vault:

```bash
cargo run -- attachment fetch m-1:1.2 --json
```

Export to the default repo-local export location:

```bash
cargo run -- attachment export m-1:1.2 --json
```

Export to an explicit file path or existing directory:

```bash
cargo run -- attachment export m-1:1.2 --to ./exports/statement.pdf --json
```

## Local state

Attachment metadata is stored in SQLite:

- `gmail_message_attachments`
- `attachment_export_events`

Attachment identity and lookup rules:

- attachment keys are account-scoped (`account_id`, `attachment_key`)
- vault-state updates are account-scoped and fail if no row is updated
- export-event lookups are indexed by (`account_id`, `attachment_key`)

Fetched bytes are stored outside SQLite under `.mailroom/vault/`.

Vault behavior:

- bytes are fetched only when the operator asks for them
- vault files are content-addressed with `blake3`
- previously fetched vault linkage is preserved across later mailbox resyncs
- a previously fetched vault entry is reused only when path exists and hash/size
  still match the cataloged vault state
- when vault integrity checks fail, Mailroom re-downloads bytes from Gmail and
  rewrites vault linkage

Export behavior:

- exports copy from the vault, not directly from Gmail
- default destinations live under `.mailroom/exports/`
- if the destination already exists with different content, Mailroom fails with
  a conflict instead of overwriting it silently
- destination conflict checks hash existing files via streaming reads to avoid
  loading full files into memory

## Error contract

Attachment commands keep stable JSON failure kinds:

- not found: `attachment.not_found`
- validation: `attachment.validation`
- destination conflict: `attachment.destination_conflict`
- storage failures (filesystem + mailbox store): `attachment.storage`

Stale vault-state races (zero-row update after read) are reported as
`attachment.not_found`, not as a generic storage failure.

## Sanitizer guardrail

Attachment export naming uses `sanitize-filename` `0.7.0-beta` behind local
guardrails:

- sanitized names must be non-empty
- empty-sanitized values fall back to deterministic defaults

If stable dependency behavior reaches parity with these guards, migrate away
from beta without changing operator-visible filename behavior.

## Safety boundaries

- mailbox sync remains read-only and metadata-first
- `attachment fetch` is read-only against Gmail
- `attachment export` is local filesystem work only
- vault and export directories remain ignored from git by default
