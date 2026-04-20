DROP INDEX IF EXISTS attachment_export_events_account_attachment_key_idx;
CREATE INDEX attachment_export_events_attachment_key_idx
    ON attachment_export_events (attachment_key, exported_at_epoch_s DESC);

ALTER TABLE gmail_message_attachments RENAME TO gmail_message_attachments_scoped;

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

INSERT INTO gmail_message_attachments (
    attachment_rowid,
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
    dedup.attachment_rowid,
    dedup.message_rowid,
    dedup.attachment_key,
    dedup.part_id,
    dedup.gmail_attachment_id,
    dedup.filename,
    dedup.mime_type,
    dedup.size_bytes,
    dedup.content_disposition,
    dedup.content_id,
    dedup.is_inline,
    dedup.vault_content_hash,
    dedup.vault_relative_path,
    dedup.vault_size_bytes,
    dedup.vault_fetched_at_epoch_s,
    dedup.updated_at_epoch_s
FROM (
    SELECT
        scoped.*,
        ROW_NUMBER() OVER (
            PARTITION BY scoped.attachment_key
            ORDER BY scoped.updated_at_epoch_s DESC, scoped.attachment_rowid DESC
        ) AS row_number
    FROM gmail_message_attachments_scoped AS scoped
) AS dedup
WHERE dedup.row_number = 1;

DROP TABLE gmail_message_attachments_scoped;

CREATE INDEX gmail_message_attachments_message_rowid_idx
    ON gmail_message_attachments (message_rowid);

CREATE INDEX gmail_message_attachments_filename_idx
    ON gmail_message_attachments (filename);

CREATE INDEX gmail_message_attachments_vault_idx
    ON gmail_message_attachments (vault_content_hash);
