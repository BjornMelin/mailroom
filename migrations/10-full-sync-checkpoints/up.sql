CREATE TABLE gmail_full_sync_checkpoint (
    account_id TEXT PRIMARY KEY,
    bootstrap_query TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('paging', 'ready_to_finalize')),
    next_page_token TEXT,
    cursor_history_id TEXT,
    pages_fetched INTEGER NOT NULL,
    messages_listed INTEGER NOT NULL,
    messages_upserted INTEGER NOT NULL,
    labels_synced INTEGER NOT NULL,
    staged_label_count INTEGER NOT NULL,
    staged_message_count INTEGER NOT NULL,
    staged_message_label_count INTEGER NOT NULL,
    staged_attachment_count INTEGER NOT NULL,
    started_at_epoch_s INTEGER NOT NULL,
    updated_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_full_sync_checkpoint_updated_at_idx
    ON gmail_full_sync_checkpoint (updated_at_epoch_s DESC);
