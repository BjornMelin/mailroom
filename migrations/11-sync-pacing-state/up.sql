CREATE TABLE gmail_sync_pacing_state (
    account_id TEXT PRIMARY KEY,
    learned_quota_units_per_minute INTEGER NOT NULL,
    learned_message_fetch_concurrency INTEGER NOT NULL,
    clean_run_streak INTEGER NOT NULL,
    last_pressure_kind TEXT,
    updated_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_sync_pacing_state_updated_at_idx
    ON gmail_sync_pacing_state (updated_at_epoch_s DESC);
