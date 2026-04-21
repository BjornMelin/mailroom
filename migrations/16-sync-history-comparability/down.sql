DROP INDEX IF EXISTS gmail_sync_run_summary_updated_idx;
ALTER TABLE gmail_sync_run_summary RENAME TO gmail_sync_run_summary_new;
CREATE TEMP TABLE gmail_sync_run_summary_copy AS
SELECT *
FROM gmail_sync_run_summary_new;

ALTER TABLE gmail_sync_run_history RENAME TO gmail_sync_run_history_new;

CREATE TABLE gmail_sync_run_history (
    run_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    sync_mode TEXT NOT NULL CHECK (sync_mode IN ('full', 'incremental')),
    status TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
    started_at_epoch_s INTEGER NOT NULL,
    finished_at_epoch_s INTEGER NOT NULL,
    bootstrap_query TEXT NOT NULL,
    cursor_history_id TEXT,
    fallback_from_history INTEGER NOT NULL CHECK (fallback_from_history IN (0, 1)),
    resumed_from_checkpoint INTEGER NOT NULL CHECK (resumed_from_checkpoint IN (0, 1)),
    pages_fetched INTEGER NOT NULL,
    messages_listed INTEGER NOT NULL,
    messages_upserted INTEGER NOT NULL,
    messages_deleted INTEGER NOT NULL,
    labels_synced INTEGER NOT NULL,
    checkpoint_reused_pages INTEGER NOT NULL,
    checkpoint_reused_messages_upserted INTEGER NOT NULL,
    pipeline_enabled INTEGER NOT NULL CHECK (pipeline_enabled IN (0, 1)),
    pipeline_list_queue_high_water INTEGER NOT NULL,
    pipeline_write_queue_high_water INTEGER NOT NULL,
    pipeline_write_batch_count INTEGER NOT NULL,
    pipeline_writer_wait_ms INTEGER NOT NULL,
    pipeline_fetch_batch_count INTEGER NOT NULL,
    pipeline_fetch_batch_avg_ms INTEGER NOT NULL,
    pipeline_fetch_batch_max_ms INTEGER NOT NULL,
    pipeline_writer_tx_count INTEGER NOT NULL,
    pipeline_writer_tx_avg_ms INTEGER NOT NULL,
    pipeline_writer_tx_max_ms INTEGER NOT NULL,
    pipeline_reorder_buffer_high_water INTEGER NOT NULL,
    pipeline_staged_message_count INTEGER NOT NULL,
    pipeline_staged_delete_count INTEGER NOT NULL,
    pipeline_staged_attachment_count INTEGER NOT NULL,
    adaptive_pacing_enabled INTEGER NOT NULL CHECK (adaptive_pacing_enabled IN (0, 1)),
    quota_units_budget_per_minute INTEGER NOT NULL,
    message_fetch_concurrency INTEGER NOT NULL,
    quota_units_cap_per_minute INTEGER NOT NULL,
    message_fetch_concurrency_cap INTEGER NOT NULL,
    starting_quota_units_per_minute INTEGER NOT NULL,
    starting_message_fetch_concurrency INTEGER NOT NULL,
    effective_quota_units_per_minute INTEGER NOT NULL,
    effective_message_fetch_concurrency INTEGER NOT NULL,
    adaptive_downshift_count INTEGER NOT NULL,
    estimated_quota_units_reserved INTEGER NOT NULL,
    http_attempt_count INTEGER NOT NULL,
    retry_count INTEGER NOT NULL,
    quota_pressure_retry_count INTEGER NOT NULL,
    concurrency_pressure_retry_count INTEGER NOT NULL,
    backend_retry_count INTEGER NOT NULL,
    throttle_wait_count INTEGER NOT NULL,
    throttle_wait_ms INTEGER NOT NULL,
    retry_after_wait_ms INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    pages_per_second REAL NOT NULL,
    messages_per_second REAL NOT NULL,
    error_message TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

INSERT INTO gmail_sync_run_history (
    run_id,
    account_id,
    sync_mode,
    status,
    started_at_epoch_s,
    finished_at_epoch_s,
    bootstrap_query,
    cursor_history_id,
    fallback_from_history,
    resumed_from_checkpoint,
    pages_fetched,
    messages_listed,
    messages_upserted,
    messages_deleted,
    labels_synced,
    checkpoint_reused_pages,
    checkpoint_reused_messages_upserted,
    pipeline_enabled,
    pipeline_list_queue_high_water,
    pipeline_write_queue_high_water,
    pipeline_write_batch_count,
    pipeline_writer_wait_ms,
    pipeline_fetch_batch_count,
    pipeline_fetch_batch_avg_ms,
    pipeline_fetch_batch_max_ms,
    pipeline_writer_tx_count,
    pipeline_writer_tx_avg_ms,
    pipeline_writer_tx_max_ms,
    pipeline_reorder_buffer_high_water,
    pipeline_staged_message_count,
    pipeline_staged_delete_count,
    pipeline_staged_attachment_count,
    adaptive_pacing_enabled,
    quota_units_budget_per_minute,
    message_fetch_concurrency,
    quota_units_cap_per_minute,
    message_fetch_concurrency_cap,
    starting_quota_units_per_minute,
    starting_message_fetch_concurrency,
    effective_quota_units_per_minute,
    effective_message_fetch_concurrency,
    adaptive_downshift_count,
    estimated_quota_units_reserved,
    http_attempt_count,
    retry_count,
    quota_pressure_retry_count,
    concurrency_pressure_retry_count,
    backend_retry_count,
    throttle_wait_count,
    throttle_wait_ms,
    retry_after_wait_ms,
    duration_ms,
    pages_per_second,
    messages_per_second,
    error_message
)
SELECT
    run_id,
    account_id,
    sync_mode,
    status,
    started_at_epoch_s,
    finished_at_epoch_s,
    bootstrap_query,
    cursor_history_id,
    fallback_from_history,
    resumed_from_checkpoint,
    pages_fetched,
    messages_listed,
    messages_upserted,
    messages_deleted,
    labels_synced,
    checkpoint_reused_pages,
    checkpoint_reused_messages_upserted,
    pipeline_enabled,
    pipeline_list_queue_high_water,
    pipeline_write_queue_high_water,
    pipeline_write_batch_count,
    pipeline_writer_wait_ms,
    pipeline_fetch_batch_count,
    pipeline_fetch_batch_avg_ms,
    pipeline_fetch_batch_max_ms,
    pipeline_writer_tx_count,
    pipeline_writer_tx_avg_ms,
    pipeline_writer_tx_max_ms,
    pipeline_reorder_buffer_high_water,
    pipeline_staged_message_count,
    pipeline_staged_delete_count,
    pipeline_staged_attachment_count,
    adaptive_pacing_enabled,
    quota_units_budget_per_minute,
    message_fetch_concurrency,
    quota_units_cap_per_minute,
    message_fetch_concurrency_cap,
    starting_quota_units_per_minute,
    starting_message_fetch_concurrency,
    effective_quota_units_per_minute,
    effective_message_fetch_concurrency,
    adaptive_downshift_count,
    estimated_quota_units_reserved,
    http_attempt_count,
    retry_count,
    quota_pressure_retry_count,
    concurrency_pressure_retry_count,
    backend_retry_count,
    throttle_wait_count,
    throttle_wait_ms,
    retry_after_wait_ms,
    duration_ms,
    pages_per_second,
    messages_per_second,
    error_message
FROM gmail_sync_run_history_new;

DROP TABLE gmail_sync_run_history_new;

DROP INDEX IF EXISTS gmail_sync_run_history_account_mode_finished_idx;
DROP INDEX IF EXISTS gmail_sync_run_history_account_finished_idx;

CREATE INDEX gmail_sync_run_history_account_finished_idx
    ON gmail_sync_run_history (account_id, finished_at_epoch_s DESC, run_id DESC);

CREATE INDEX gmail_sync_run_history_account_mode_finished_idx
    ON gmail_sync_run_history (account_id, sync_mode, finished_at_epoch_s DESC, run_id DESC);

CREATE TABLE gmail_sync_run_summary (
    account_id TEXT NOT NULL,
    sync_mode TEXT NOT NULL CHECK (sync_mode IN ('full', 'incremental')),
    latest_run_id INTEGER NOT NULL,
    latest_status TEXT NOT NULL CHECK (latest_status IN ('ok', 'failed')),
    latest_finished_at_epoch_s INTEGER NOT NULL,
    best_clean_run_id INTEGER,
    best_clean_quota_units_per_minute INTEGER,
    best_clean_message_fetch_concurrency INTEGER,
    best_clean_messages_per_second REAL,
    best_clean_duration_ms INTEGER,
    recent_success_count INTEGER NOT NULL,
    recent_failure_count INTEGER NOT NULL,
    recent_failure_streak INTEGER NOT NULL,
    recent_clean_success_streak INTEGER NOT NULL,
    regression_detected INTEGER NOT NULL CHECK (regression_detected IN (0, 1)),
    regression_kind TEXT CHECK (
        regression_kind IS NULL
        OR regression_kind IN ('failure_streak', 'retry_pressure', 'throughput_drop', 'duration_spike')
    ),
    regression_run_id INTEGER,
    regression_message TEXT,
    updated_at_epoch_s INTEGER NOT NULL,
    PRIMARY KEY (account_id, sync_mode),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE,
    FOREIGN KEY (latest_run_id) REFERENCES gmail_sync_run_history (run_id) ON DELETE CASCADE,
    FOREIGN KEY (best_clean_run_id) REFERENCES gmail_sync_run_history (run_id) ON DELETE SET NULL,
    FOREIGN KEY (regression_run_id) REFERENCES gmail_sync_run_history (run_id) ON DELETE SET NULL
) STRICT;

INSERT INTO gmail_sync_run_summary (
    account_id,
    sync_mode,
    latest_run_id,
    latest_status,
    latest_finished_at_epoch_s,
    best_clean_run_id,
    best_clean_quota_units_per_minute,
    best_clean_message_fetch_concurrency,
    best_clean_messages_per_second,
    best_clean_duration_ms,
    recent_success_count,
    recent_failure_count,
    recent_failure_streak,
    recent_clean_success_streak,
    regression_detected,
    regression_kind,
    regression_run_id,
    regression_message,
    updated_at_epoch_s
)
SELECT
    account_id,
    sync_mode,
    latest_run_id,
    latest_status,
    latest_finished_at_epoch_s,
    best_clean_run_id,
    best_clean_quota_units_per_minute,
    best_clean_message_fetch_concurrency,
    best_clean_messages_per_second,
    best_clean_duration_ms,
    recent_success_count,
    recent_failure_count,
    recent_failure_streak,
    recent_clean_success_streak,
    regression_detected,
    regression_kind,
    regression_run_id,
    regression_message,
    updated_at_epoch_s
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY account_id, sync_mode
               ORDER BY latest_finished_at_epoch_s DESC, latest_run_id DESC
           ) AS row_number
    FROM gmail_sync_run_summary_copy
)
WHERE row_number = 1;

CREATE INDEX gmail_sync_run_summary_updated_idx
    ON gmail_sync_run_summary (updated_at_epoch_s DESC);

DROP TABLE gmail_sync_run_summary_copy;
DROP TABLE gmail_sync_run_summary_new;
