CREATE TABLE thread_workflows (
    workflow_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    current_stage TEXT NOT NULL
        CHECK (current_stage IN ('triage', 'follow_up', 'drafting', 'ready_to_send', 'sent', 'closed')),
    triage_bucket TEXT
        CHECK (triage_bucket IS NULL OR triage_bucket IN ('urgent', 'needs_reply_soon', 'waiting', 'fyi')),
    note TEXT NOT NULL DEFAULT '',
    snoozed_until_epoch_s INTEGER,
    follow_up_due_epoch_s INTEGER,
    latest_message_id TEXT,
    latest_message_internal_date_epoch_ms INTEGER,
    latest_message_subject TEXT NOT NULL DEFAULT '',
    latest_message_from_header TEXT NOT NULL DEFAULT '',
    latest_message_snippet TEXT NOT NULL DEFAULT '',
    current_draft_revision_id INTEGER,
    gmail_draft_id TEXT,
    gmail_draft_message_id TEXT,
    gmail_draft_thread_id TEXT,
    last_remote_sync_epoch_s INTEGER,
    last_sent_message_id TEXT,
    last_cleanup_action TEXT
        CHECK (last_cleanup_action IS NULL OR last_cleanup_action IN ('archive', 'label', 'trash')),
    created_at_epoch_s INTEGER NOT NULL,
    updated_at_epoch_s INTEGER NOT NULL,
    UNIQUE (account_id, thread_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX thread_workflows_account_stage_idx
    ON thread_workflows (account_id, current_stage, updated_at_epoch_s DESC);

CREATE INDEX thread_workflows_account_bucket_idx
    ON thread_workflows (account_id, triage_bucket, updated_at_epoch_s DESC);

CREATE INDEX thread_workflows_account_updated_idx
    ON thread_workflows (account_id, updated_at_epoch_s DESC);

CREATE TABLE thread_workflow_events (
    event_id INTEGER PRIMARY KEY,
    workflow_id INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    from_stage TEXT,
    to_stage TEXT,
    triage_bucket TEXT
        CHECK (triage_bucket IS NULL OR triage_bucket IN ('urgent', 'needs_reply_soon', 'waiting', 'fyi')),
    note TEXT,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    created_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES thread_workflows (workflow_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX thread_workflow_events_workflow_idx
    ON thread_workflow_events (workflow_id, created_at_epoch_s DESC);

CREATE INDEX thread_workflow_events_account_thread_idx
    ON thread_workflow_events (account_id, thread_id, created_at_epoch_s DESC);

CREATE TABLE thread_draft_revisions (
    draft_revision_id INTEGER PRIMARY KEY,
    workflow_id INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    source_message_id TEXT NOT NULL,
    reply_mode TEXT NOT NULL CHECK (reply_mode IN ('reply', 'reply_all')),
    subject TEXT NOT NULL,
    to_addresses_json TEXT NOT NULL CHECK (json_valid(to_addresses_json)),
    cc_addresses_json TEXT NOT NULL CHECK (json_valid(cc_addresses_json)),
    bcc_addresses_json TEXT NOT NULL CHECK (json_valid(bcc_addresses_json)),
    body_text TEXT NOT NULL,
    created_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES thread_workflows (workflow_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX thread_draft_revisions_workflow_idx
    ON thread_draft_revisions (workflow_id, created_at_epoch_s DESC);

CREATE TABLE thread_draft_attachments (
    attachment_id INTEGER PRIMARY KEY,
    draft_revision_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at_epoch_s INTEGER NOT NULL,
    UNIQUE (draft_revision_id, path),
    FOREIGN KEY (draft_revision_id) REFERENCES thread_draft_revisions (draft_revision_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX thread_draft_attachments_revision_idx
    ON thread_draft_attachments (draft_revision_id, created_at_epoch_s DESC);
