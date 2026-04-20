CREATE TABLE gmail_message_attachments (
    attachment_rowid INTEGER PRIMARY KEY,
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
    UNIQUE (attachment_key),
    FOREIGN KEY (message_rowid) REFERENCES gmail_messages (message_rowid) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_message_attachments_message_rowid_idx
    ON gmail_message_attachments (message_rowid);

CREATE INDEX gmail_message_attachments_filename_idx
    ON gmail_message_attachments (filename);

CREATE INDEX gmail_message_attachments_vault_idx
    ON gmail_message_attachments (vault_content_hash);

CREATE TABLE attachment_export_events (
    export_event_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    attachment_key TEXT NOT NULL,
    message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    exported_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX attachment_export_events_account_exported_idx
    ON attachment_export_events (account_id, exported_at_epoch_s DESC);

CREATE INDEX attachment_export_events_attachment_key_idx
    ON attachment_export_events (attachment_key, exported_at_epoch_s DESC);
