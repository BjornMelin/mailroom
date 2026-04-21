DROP INDEX IF EXISTS gmail_full_sync_stage_page_messages_account_message_idx;
DROP TABLE IF EXISTS gmail_full_sync_stage_page_messages;

DROP INDEX IF EXISTS gmail_full_sync_stage_pages_account_status_idx;
DROP TABLE IF EXISTS gmail_full_sync_stage_pages;

CREATE TABLE gmail_sync_state_prev (
    account_id TEXT PRIMARY KEY,
    cursor_history_id TEXT,
    bootstrap_query TEXT NOT NULL,
    last_sync_mode TEXT NOT NULL,
    last_sync_status TEXT NOT NULL,
    last_error TEXT,
    last_sync_epoch_s INTEGER NOT NULL,
    last_full_sync_success_epoch_s INTEGER,
    last_incremental_sync_success_epoch_s INTEGER,
    pipeline_enabled INTEGER NOT NULL DEFAULT 0,
    pipeline_list_queue_high_water INTEGER NOT NULL DEFAULT 0,
    pipeline_write_queue_high_water INTEGER NOT NULL DEFAULT 0,
    pipeline_write_batch_count INTEGER NOT NULL DEFAULT 0,
    pipeline_writer_wait_ms INTEGER NOT NULL DEFAULT 0,
    message_count INTEGER NOT NULL DEFAULT 0,
    label_count INTEGER NOT NULL DEFAULT 0,
    indexed_message_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_sync_state_prev (
    account_id,
    cursor_history_id,
    bootstrap_query,
    last_sync_mode,
    last_sync_status,
    last_error,
    last_sync_epoch_s,
    last_full_sync_success_epoch_s,
    last_incremental_sync_success_epoch_s,
    pipeline_enabled,
    pipeline_list_queue_high_water,
    pipeline_write_queue_high_water,
    pipeline_write_batch_count,
    pipeline_writer_wait_ms,
    message_count,
    label_count,
    indexed_message_count
)
SELECT
    account_id,
    cursor_history_id,
    bootstrap_query,
    last_sync_mode,
    last_sync_status,
    last_error,
    last_sync_epoch_s,
    last_full_sync_success_epoch_s,
    last_incremental_sync_success_epoch_s,
    pipeline_enabled,
    pipeline_list_queue_high_water,
    pipeline_write_queue_high_water,
    pipeline_write_batch_count,
    pipeline_writer_wait_ms,
    message_count,
    label_count,
    indexed_message_count
FROM gmail_sync_state;

DROP TABLE gmail_sync_state;

ALTER TABLE gmail_sync_state_prev RENAME TO gmail_sync_state;
