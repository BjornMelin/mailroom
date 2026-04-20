DROP INDEX IF EXISTS automation_run_events_run_idx;
DROP TABLE IF EXISTS automation_run_events;

DROP INDEX IF EXISTS automation_run_candidates_account_thread_idx;
DROP INDEX IF EXISTS automation_run_candidates_run_idx;
DROP TABLE IF EXISTS automation_run_candidates;

DROP INDEX IF EXISTS automation_runs_account_created_idx;
DROP TABLE IF EXISTS automation_runs;

PRAGMA foreign_keys=OFF;

ALTER TABLE gmail_message_labels RENAME TO gmail_message_labels_automation_rollback;
ALTER TABLE gmail_message_attachments RENAME TO gmail_message_attachments_automation_rollback;
ALTER TABLE gmail_messages RENAME TO gmail_messages_automation_rollback;

CREATE TABLE gmail_messages (
    message_rowid INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    history_id TEXT NOT NULL,
    internal_date_epoch_ms INTEGER NOT NULL,
    snippet TEXT NOT NULL,
    subject TEXT NOT NULL,
    from_header TEXT NOT NULL,
    from_address TEXT,
    recipient_headers TEXT NOT NULL,
    to_header TEXT NOT NULL,
    cc_header TEXT NOT NULL,
    bcc_header TEXT NOT NULL,
    reply_to_header TEXT NOT NULL,
    size_estimate INTEGER NOT NULL,
    updated_at_epoch_s INTEGER NOT NULL,
    UNIQUE (account_id, message_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_messages (
    message_rowid,
    account_id,
    message_id,
    thread_id,
    history_id,
    internal_date_epoch_ms,
    snippet,
    subject,
    from_header,
    from_address,
    recipient_headers,
    to_header,
    cc_header,
    bcc_header,
    reply_to_header,
    size_estimate,
    updated_at_epoch_s
)
SELECT
    message_rowid,
    account_id,
    message_id,
    thread_id,
    history_id,
    internal_date_epoch_ms,
    snippet,
    subject,
    from_header,
    from_address,
    recipient_headers,
    to_header,
    cc_header,
    bcc_header,
    reply_to_header,
    size_estimate,
    updated_at_epoch_s
FROM gmail_messages_automation_rollback;

DROP TABLE gmail_messages_automation_rollback;

CREATE INDEX gmail_messages_account_internal_date_idx
    ON gmail_messages (account_id, internal_date_epoch_ms DESC);

CREATE INDEX gmail_messages_account_thread_idx
    ON gmail_messages (account_id, thread_id);

CREATE INDEX gmail_messages_account_from_address_idx
    ON gmail_messages (account_id, from_address);

CREATE TABLE gmail_message_labels (
    message_rowid INTEGER NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (message_rowid, label_id),
    FOREIGN KEY (message_rowid) REFERENCES gmail_messages (message_rowid) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_message_labels (
    message_rowid,
    label_id
)
SELECT
    message_rowid,
    label_id
FROM gmail_message_labels_automation_rollback;

DROP TABLE gmail_message_labels_automation_rollback;

CREATE INDEX gmail_message_labels_label_idx
    ON gmail_message_labels (label_id);

CREATE INDEX gmail_message_labels_message_rowid_idx
    ON gmail_message_labels (message_rowid);

CREATE TABLE gmail_message_attachments (
    attachment_rowid INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    message_rowid INTEGER NOT NULL,
    attachment_key TEXT NOT NULL,
    part_id TEXT NOT NULL,
    gmail_attachment_id TEXT,
    filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    content_disposition TEXT,
    content_id TEXT,
    is_inline INTEGER NOT NULL CHECK (is_inline IN (0, 1)),
    vault_content_hash TEXT,
    vault_relative_path TEXT,
    vault_size_bytes INTEGER,
    vault_fetched_at_epoch_s INTEGER,
    updated_at_epoch_s INTEGER NOT NULL,
    UNIQUE (message_rowid, part_id),
    UNIQUE (account_id, attachment_key),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE,
    FOREIGN KEY (message_rowid) REFERENCES gmail_messages (message_rowid) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_message_attachments (
    attachment_rowid,
    account_id,
    message_rowid,
    attachment_key,
    part_id,
    gmail_attachment_id,
    filename,
    mime_type,
    size_bytes,
    content_disposition,
    content_id,
    is_inline,
    vault_content_hash,
    vault_relative_path,
    vault_size_bytes,
    vault_fetched_at_epoch_s,
    updated_at_epoch_s
)
SELECT
    attachment_rowid,
    account_id,
    message_rowid,
    attachment_key,
    part_id,
    gmail_attachment_id,
    filename,
    mime_type,
    size_bytes,
    content_disposition,
    content_id,
    is_inline,
    vault_content_hash,
    vault_relative_path,
    vault_size_bytes,
    vault_fetched_at_epoch_s,
    updated_at_epoch_s
FROM gmail_message_attachments_automation_rollback;

DROP TABLE gmail_message_attachments_automation_rollback;

CREATE INDEX gmail_message_attachments_message_rowid_idx
    ON gmail_message_attachments (message_rowid);

CREATE INDEX gmail_message_attachments_account_filename_idx
    ON gmail_message_attachments (account_id, filename);

CREATE INDEX gmail_message_attachments_account_vault_idx
    ON gmail_message_attachments (account_id, vault_content_hash);

PRAGMA foreign_keys=ON;
