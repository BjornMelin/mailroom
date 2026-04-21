ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_fetch_batch_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_fetch_batch_avg_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_fetch_batch_max_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_writer_tx_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_writer_tx_avg_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_writer_tx_max_ms INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_reorder_buffer_high_water INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_staged_message_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_staged_delete_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE gmail_sync_state
    ADD COLUMN pipeline_staged_attachment_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE gmail_full_sync_stage_pages (
    account_id TEXT NOT NULL,
    page_seq INTEGER NOT NULL,
    listed_count INTEGER NOT NULL,
    staged_message_count INTEGER NOT NULL,
    next_page_token TEXT,
    status TEXT NOT NULL CHECK (status IN ('partial', 'complete')),
    updated_at_epoch_s INTEGER NOT NULL,
    PRIMARY KEY (account_id, page_seq),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_full_sync_stage_pages_account_status_idx
    ON gmail_full_sync_stage_pages (account_id, status, page_seq);

CREATE TABLE gmail_full_sync_stage_page_messages (
    account_id TEXT NOT NULL,
    page_seq INTEGER NOT NULL,
    message_id TEXT NOT NULL,
    PRIMARY KEY (account_id, page_seq, message_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_full_sync_stage_page_messages_account_message_idx
    ON gmail_full_sync_stage_page_messages (account_id, message_id);
