# 0007: Verification Audit Hardening

## Status

Accepted

## Context

Mailroom already owns the first complete local mailbox loop:

- Gmail auth and active account persistence
- one-shot metadata sync plus local FTS search
- thread-scoped workflow state
- remote draft projection and explicit send
- reviewed cleanup actions
- attachment catalog, vault, and export
- review-first automation rules with persisted snapshots

That is enough power to mutate a real mailbox at scale. The next risk is no
longer missing substrate. The next risk is operator surprise:

- label taxonomy drift between numbered canonical labels and older legacy labels
- shallow sync windows hiding rare but important mail before rules are finalized
- list-header coverage looking absent until a fresh deep sync populates the new
  cached header fields
- jumping from implementation to large live bulk actions without a narrow
  verification lane

## Decision

Mailroom adds a native, read-only verification and hardening surface.

- `mailroom audit labels` is the canonical read-only report for label overlap,
  numbered-vs-legacy drift, top local user labels, and empty local user labels.
- `mailroom audit verification` is the canonical read-only report for rollout
  readiness: sync window, header coverage, local store counts, rules-file
  presence, and next-step guidance.
- these audit commands read the existing local store and auth state only; they
  do not introduce a second mailbox representation or new persisted state.
- the audit model is explicitly local-cache scoped. It reports what Mailroom can
  safely prove from the synced SQLite store, not hidden Gmail server state.
- real-mailbox rollout stays two-phase:
  - keep the normal operational sync window narrow
  - do one deeper audit sync before finalizing the first personal ruleset
- operator runbooks become part of the native contract. The repo documents deep
  syncs, label canonicalization, self-canary send tests, and micro-batch
  archive/label rollout as the intended path to first production use.

## Why

- read-only audit commands let operators verify state before mutation without
  widening the trust boundary or adding background automation.
- label overlap reporting addresses the concrete failure mode where local rules
  and Gmail taxonomy drift apart.
- readiness reporting keeps the rollout logic close to the native store that
  already owns sync, attachments, workflows, and automation snapshots.
- keeping this schema-free avoids inventing yet another state layer for
  verification when the needed signals already exist in SQLite.

## Consequences

Positive:

- safer real-mailbox rollout for personal automation rules
- durable documentation for canary sends, deep syncs, and micro-batch bulk apply
- no extra write-path or background worker complexity
- a clearer native inspection contract for the future TUI

Negative:

- audit reports are only as complete as the local sync window and current cache
- label usage counts are cache-local, not guaranteed full-mailbox truth
- operators must still do some deliberate manual work, especially around label
  cleanup and canary validation

## Rejected alternatives

### Fold all readiness guidance into `doctor`

Rejected because `doctor` is the low-level health report for workspace, store,
and auth. Ruleset hardening, label overlap, and rollout guidance are a second
operator concern and deserve a distinct read-only audit surface.

### Persist a separate verification database or materialized audit tables

Rejected because the needed signals already live in the canonical SQLite store.
Persisting duplicate audit state would create a second source of truth.

### Automate label cleanup immediately

Rejected because label canonicalization is a high-context operator decision. The
first hardening lane should report drift clearly, not mutate taxonomy for the
operator.
