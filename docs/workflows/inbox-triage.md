# Inbox Triage Workflow

The target operator loop for inbox triage is:

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

This workflow should eventually exist in both CLI and TUI forms, backed by the same local state.

