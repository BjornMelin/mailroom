DROP INDEX IF EXISTS gmail_sync_pacing_state_updated_at_idx;

CREATE TABLE gmail_sync_pacing_state_next (
    account_id TEXT PRIMARY KEY,
    learned_quota_units_per_minute INTEGER NOT NULL
        CHECK (learned_quota_units_per_minute >= 5),
    learned_message_fetch_concurrency INTEGER NOT NULL
        CHECK (learned_message_fetch_concurrency >= 1),
    clean_run_streak INTEGER NOT NULL
        CHECK (clean_run_streak >= 0),
    last_pressure_kind TEXT
        CHECK (
            last_pressure_kind IS NULL
            OR last_pressure_kind IN ('quota', 'concurrency', 'mixed')
        ),
    updated_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_sync_pacing_state_next (
    account_id,
    learned_quota_units_per_minute,
    learned_message_fetch_concurrency,
    clean_run_streak,
    last_pressure_kind,
    updated_at_epoch_s
)
SELECT
    account_id,
    CASE
        WHEN learned_quota_units_per_minute < 5 THEN 5
        WHEN learned_quota_units_per_minute > 12000 THEN 12000
        ELSE learned_quota_units_per_minute
    END,
    CASE
        WHEN learned_message_fetch_concurrency < 1 THEN 1
        WHEN learned_message_fetch_concurrency > 4 THEN 4
        ELSE learned_message_fetch_concurrency
    END,
    MAX(clean_run_streak, 0),
    CASE
        WHEN last_pressure_kind IN ('quota', 'concurrency', 'mixed') THEN last_pressure_kind
        ELSE NULL
    END,
    updated_at_epoch_s
FROM gmail_sync_pacing_state;

DROP TABLE gmail_sync_pacing_state;

ALTER TABLE gmail_sync_pacing_state_next RENAME TO gmail_sync_pacing_state;

CREATE INDEX gmail_sync_pacing_state_updated_at_idx
    ON gmail_sync_pacing_state (updated_at_epoch_s DESC);
