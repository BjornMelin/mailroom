DROP INDEX IF EXISTS attachment_export_events_attachment_key_idx;
CREATE INDEX attachment_export_events_account_attachment_key_idx
    ON attachment_export_events (account_id, attachment_key, exported_at_epoch_s DESC);

ALTER TABLE gmail_message_attachments RENAME TO gmail_message_attachments_legacy;

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
    legacy.attachment_rowid,
    gm.account_id,
    legacy.message_rowid,
    legacy.attachment_key,
    legacy.part_id,
    legacy.gmail_attachment_id,
    legacy.filename,
    legacy.mime_type,
    legacy.size_bytes,
    legacy.content_disposition,
    legacy.content_id,
    legacy.is_inline,
    legacy.vault_content_hash,
    legacy.vault_relative_path,
    legacy.vault_size_bytes,
    legacy.vault_fetched_at_epoch_s,
    legacy.updated_at_epoch_s
FROM gmail_message_attachments_legacy AS legacy
INNER JOIN gmail_messages AS gm
  ON gm.message_rowid = legacy.message_rowid;

DROP TABLE gmail_message_attachments_legacy;

CREATE INDEX gmail_message_attachments_message_rowid_idx
    ON gmail_message_attachments (message_rowid);

CREATE INDEX gmail_message_attachments_account_filename_idx
    ON gmail_message_attachments (account_id, filename);

CREATE INDEX gmail_message_attachments_account_vault_idx
    ON gmail_message_attachments (account_id, vault_content_hash);
