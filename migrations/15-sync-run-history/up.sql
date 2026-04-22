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
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE,
    UNIQUE (run_id, account_id, sync_mode)
) STRICT;

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
    FOREIGN KEY (latest_run_id, account_id, sync_mode)
        REFERENCES gmail_sync_run_history (run_id, account_id, sync_mode) ON DELETE CASCADE,
    FOREIGN KEY (best_clean_run_id, account_id, sync_mode)
        REFERENCES gmail_sync_run_history (run_id, account_id, sync_mode) ON DELETE SET NULL,
    FOREIGN KEY (regression_run_id, account_id, sync_mode)
        REFERENCES gmail_sync_run_history (run_id, account_id, sync_mode) ON DELETE SET NULL
) STRICT;

CREATE INDEX gmail_sync_run_summary_updated_idx
    ON gmail_sync_run_summary (updated_at_epoch_s DESC);
