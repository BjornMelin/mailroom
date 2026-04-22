ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_enabled INTEGER NOT NULL DEFAULT 0 CHECK (pipeline_enabled IN (0, 1));

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_list_queue_high_water INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_write_queue_high_water INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_write_batch_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_writer_wait_ms INTEGER NOT NULL DEFAULT 0;

CREATE TABLE gmail_incremental_sync_stage_delete_ids (
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    PRIMARY KEY (account_id, message_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_incremental_sync_stage_delete_ids_account_idx
    ON gmail_incremental_sync_stage_delete_ids (account_id);

CREATE TABLE gmail_incremental_sync_stage_messages (
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
    list_id_header TEXT,
    list_unsubscribe_header TEXT,
    list_unsubscribe_post_header TEXT,
    precedence_header TEXT,
    auto_submitted_header TEXT,
    label_names_text TEXT NOT NULL,
    PRIMARY KEY (account_id, message_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_incremental_sync_stage_messages_account_thread_idx
    ON gmail_incremental_sync_stage_messages (account_id, thread_id);

CREATE TABLE gmail_incremental_sync_stage_message_labels (
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (account_id, message_id, label_id),
    FOREIGN KEY (account_id, message_id)
        REFERENCES gmail_incremental_sync_stage_messages (account_id, message_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_incremental_sync_stage_message_labels_account_message_idx
    ON gmail_incremental_sync_stage_message_labels (account_id, message_id);

CREATE TABLE gmail_incremental_sync_stage_attachments (
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    attachment_key TEXT NOT NULL,
    part_id TEXT NOT NULL,
    gmail_attachment_id TEXT,
    filename TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    content_disposition TEXT,
    content_id TEXT,
    is_inline INTEGER NOT NULL CHECK (is_inline IN (0, 1)),
    PRIMARY KEY (account_id, attachment_key),
    UNIQUE (account_id, message_id, part_id),
    FOREIGN KEY (account_id, message_id)
        REFERENCES gmail_incremental_sync_stage_messages (account_id, message_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_incremental_sync_stage_attachments_account_message_idx
    ON gmail_incremental_sync_stage_attachments (account_id, message_id);
