# 0005: Attachment Canonical Model

## Status

Accepted

## Context

Mailroom already owns message-canonical mailbox sync, local search, thread
workflow state, remote Gmail drafts, and reviewed cleanup actions. The next
native slice needed inbound attachment support without reintroducing a second
mailbox store, full MIME parsing, or automatic bulk export.

The core design questions were:

- what unit owns attachment metadata locally
- whether attachment bytes belong in SQLite
- whether sync should download bytes eagerly
- where durable fetched bytes should live

## Decision

Mailroom keeps the existing message-canonical mailbox model and adds attachment
rows beneath synced Gmail messages.

- `gmail_messages` remains the canonical durable mailbox record.
- `gmail_message_attachments` stores parsed Gmail attachment metadata keyed to
  `message_rowid` with account-scoped identity (`account_id`, `attachment_key`).
- sync catalogs attachment metadata only; it does not fetch bytes eagerly.
- attachment bytes are fetched on demand from Gmail into the repo-local vault at
  `.mailroom/vault/`.
- vault paths are content-addressed with `blake3`.
- exports are explicit operator actions that copy from the vault into
  `.mailroom/exports/` or a chosen destination and append an export event.
- SQLite does not store attachment bytes.

## Hardening Notes (2026-04-20)

- vault reuse now requires hash and size verification before returning
  `downloaded=false`; mismatches trigger a re-download path.
- export conflict checks hash existing destination files using streaming reads
  (no full-file in-memory load).
- vault-state writes now fail fast when no attachment row is updated, and that
  stale-key race maps to the stable `attachment.not_found` JSON contract.
- the filename sanitizer dependency remains `sanitize-filename` `0.7.0-beta`,
  with repo-owned non-empty wrapper guardrails and regression tests; this stays
  intentional until a stable line provides equivalent behavior.
- downgrade check policy: if stable crate behavior reaches parity with current
  guardrails, move off beta in the same change that preserves the tests.

## Consequences

Positive:

- no second mailbox truth source
- sync remains bounded and mostly metadata-oriented
- fetched bytes survive later resyncs through preserved vault linkage
- exports are reviewable, local, and auditable

Negative:

- first fetch of an attachment still needs a live Gmail read
- full attachment content search is deferred
- inline or malformed MIME parts are cataloged only as far as Gmail’s parsed
  payload surface exposes them

## Explicit Non-Goals

- bulk export by default
- OCR or document text extraction
- full RFC822/MIME parsing in this slice
- storing attachment bytes in SQLite
