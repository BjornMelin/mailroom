DROP INDEX IF EXISTS gmail_sync_pacing_state_updated_at_idx;

CREATE TABLE gmail_sync_pacing_state_prev (
    account_id TEXT PRIMARY KEY,
    learned_quota_units_per_minute INTEGER NOT NULL,
    learned_message_fetch_concurrency INTEGER NOT NULL,
    clean_run_streak INTEGER NOT NULL,
    last_pressure_kind TEXT,
    updated_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_sync_pacing_state_prev (
    account_id,
    learned_quota_units_per_minute,
    learned_message_fetch_concurrency,
    clean_run_streak,
    last_pressure_kind,
    updated_at_epoch_s
)
SELECT
    account_id,
    learned_quota_units_per_minute,
    learned_message_fetch_concurrency,
    clean_run_streak,
    last_pressure_kind,
    updated_at_epoch_s
FROM gmail_sync_pacing_state;

DROP TABLE gmail_sync_pacing_state;

ALTER TABLE gmail_sync_pacing_state_prev RENAME TO gmail_sync_pacing_state;

CREATE INDEX gmail_sync_pacing_state_updated_at_idx
    ON gmail_sync_pacing_state (updated_at_epoch_s DESC);
