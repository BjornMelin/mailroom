# Security And Storage

## Storage rules

- Code and docs are versioned.
- Operational state stays under `.mailroom/`.
- Secrets, tokens, attachment caches, and exports stay out of git by default.
- Any example data checked into the repo should be sanitized first.

## Sensitive material

Treat the following as sensitive:

- OAuth credentials
- refresh and access tokens
- repo-local Gmail credential files under `.mailroom/auth/`
- mailbox-derived SQLite state
- attachment exports
- generated message bodies containing personal or confidential content

## Default policy

- Prefer references and reproducible commands over checked-in mailbox artifacts.
- If you intentionally preserve exported content, make that a deliberate workflow with explicit review.
- Avoid spreading runtime state across hidden directories outside the repo unless there is a clear reason.
- Keep Gmail credential storage owner-only; the default native file store is a local fallback, not a reason to commit or share tokens.
