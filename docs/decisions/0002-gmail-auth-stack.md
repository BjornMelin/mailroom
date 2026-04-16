# 0002: Gmail Auth Stack

## Status

Accepted

## Decision

`mailroom` uses a single native async auth stack for Gmail:

- `tokio`
- `reqwest`
- `oauth2`
- `secrecy`
- repo-local file-backed credential storage under `.mailroom/auth/`

The OAuth flow is Google installed-app authorization code with PKCE and a
loopback localhost callback listener. The default requested scope is
`https://www.googleapis.com/auth/gmail.modify`.

## Why

- It keeps the native client on one HTTP/runtime stack instead of mixing Gmail
  API and OAuth stacks.
- It matches Google’s native-app guidance for installed apps.
- It keeps the first auth/account slice local-first and reviewable.
- It leaves room for alternative credential-store backends later without
  changing the command surface.

## Rejected alternatives

### `yup-oauth2` as the primary auth layer

Rejected because it would introduce a second client/runtime surface compared to
the native `reqwest` Gmail client we want to keep as the long-term center.

### Generated Gmail SDK as the primary integration surface

Rejected for the first native slice because the generated surface is broader and
heavier than the narrow operator-oriented Gmail client Mailroom needs today.

### Device flow as the default operator path

Rejected because Mailroom is a normal desktop/CLI operator tool, not a
limited-input device workflow.
