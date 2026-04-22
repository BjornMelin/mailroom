DROP INDEX IF EXISTS gmail_sync_run_summary_updated_idx;

CREATE TEMP TABLE gmail_sync_run_summary_copy AS
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
FROM gmail_sync_run_summary;

CREATE TEMP TABLE gmail_sync_run_history_copy AS
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
FROM gmail_sync_run_history;

DROP TABLE gmail_sync_run_summary;
DROP TABLE gmail_sync_run_history;

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
    UNIQUE (run_id, account_id, sync_mode),
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
FROM gmail_sync_run_history_copy;

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
WITH ranked_history AS (
    SELECT
        history.*,
        ROW_NUMBER() OVER (
            PARTITION BY history.account_id, history.sync_mode
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
        ) AS latest_rank,
        ROW_NUMBER() OVER (
            PARTITION BY history.account_id, history.sync_mode
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
        ) AS recent_rank,
        SUM(CASE
                WHEN history.status = 'ok' AND history.messages_listed >= 100 THEN 1
                ELSE 0
            END) OVER (
            PARTITION BY history.account_id, history.sync_mode
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS meaningful_success_rank,
        SUM(CASE WHEN history.status != 'failed' THEN 1 ELSE 0 END) OVER (
            PARTITION BY history.account_id, history.sync_mode
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS non_failed_seen,
        SUM(CASE
                WHEN history.status = 'ok'
                 AND history.messages_listed > 0
                 AND history.retry_count = 0
                 AND history.quota_pressure_retry_count = 0
                 AND history.concurrency_pressure_retry_count = 0
                 AND history.backend_retry_count = 0
                THEN 0
                ELSE 1
            END) OVER (
            PARTITION BY history.account_id, history.sync_mode
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS non_clean_seen
    FROM gmail_sync_run_history AS history
),
latest_rows AS (
    SELECT *
    FROM ranked_history
    WHERE latest_rank = 1
),
latest_success_rows AS (
    SELECT *
    FROM ranked_history
    WHERE status = 'ok'
      AND meaningful_success_rank = 1
),
recent_stats AS (
    SELECT
        history.account_id,
        history.sync_mode,
        SUM(CASE
                WHEN history.recent_rank <= 10 AND history.status = 'ok' THEN 1
                ELSE 0
            END) AS recent_success_count,
        SUM(CASE
                WHEN history.recent_rank <= 10 AND history.status = 'failed' THEN 1
                ELSE 0
            END) AS recent_failure_count,
        SUM(CASE
                WHEN history.status = 'failed' AND history.non_failed_seen = 0 THEN 1
                ELSE 0
            END) AS recent_failure_streak,
        SUM(CASE
                WHEN history.status = 'ok'
                 AND history.messages_listed > 0
                 AND history.retry_count = 0
                 AND history.quota_pressure_retry_count = 0
                 AND history.concurrency_pressure_retry_count = 0
                 AND history.backend_retry_count = 0
                 AND history.non_clean_seen = 0
                THEN 1
                ELSE 0
            END) AS recent_clean_success_streak
    FROM ranked_history AS history
    GROUP BY history.account_id, history.sync_mode
),
retry_baseline AS (
    SELECT
        latest.account_id,
        latest.sync_mode,
        COUNT(*) AS baseline_count,
        SUM(CASE
                WHEN history.quota_pressure_retry_count = 0
                 AND history.concurrency_pressure_retry_count = 0
                THEN 1
                ELSE 0
            END) AS clean_baseline_count
    FROM latest_rows AS latest
    LEFT JOIN ranked_history AS history
        ON history.account_id = latest.account_id
       AND history.sync_mode = latest.sync_mode
       AND history.status = 'ok'
       AND history.recent_rank > 1
       AND history.recent_rank <= 4
    GROUP BY latest.account_id, latest.sync_mode
),
best_clean_rows AS (
    SELECT *
    FROM (
        SELECT
            history.account_id,
            history.sync_mode,
            history.run_id,
            history.effective_quota_units_per_minute,
            history.effective_message_fetch_concurrency,
            history.messages_per_second,
            history.duration_ms,
            ROW_NUMBER() OVER (
                PARTITION BY history.account_id, history.sync_mode
                ORDER BY
                    history.messages_per_second DESC,
                    history.estimated_quota_units_reserved ASC,
                    history.effective_message_fetch_concurrency ASC,
                    history.run_id DESC
            ) AS best_rank
        FROM gmail_sync_run_history AS history
        WHERE history.status = 'ok'
          AND history.messages_listed > 0
          AND history.retry_count = 0
          AND history.quota_pressure_retry_count = 0
          AND history.concurrency_pressure_retry_count = 0
          AND history.backend_retry_count = 0
    )
    WHERE best_rank = 1
),
throughput_baseline AS (
    SELECT
        latest.account_id,
        latest.sync_mode,
        COUNT(*) AS baseline_count,
        AVG(history.messages_per_second) AS avg_messages_per_second,
        AVG(history.duration_ms * 1.0) AS avg_duration_ms
    FROM latest_success_rows AS latest
    LEFT JOIN ranked_history AS history
        ON history.account_id = latest.account_id
       AND history.sync_mode = latest.sync_mode
       AND history.status = 'ok'
       AND history.messages_listed >= 100
       AND history.meaningful_success_rank BETWEEN 2 AND 6
    GROUP BY latest.account_id, latest.sync_mode
),
prior_meaningful_success AS (
    SELECT *
    FROM ranked_history
    WHERE status = 'ok'
      AND messages_listed >= 100
      AND meaningful_success_rank = 2
),
prior_baseline AS (
    SELECT
        prior.account_id,
        prior.sync_mode,
        COUNT(*) AS baseline_count,
        AVG(history.messages_per_second) AS avg_messages_per_second,
        AVG(history.duration_ms * 1.0) AS avg_duration_ms
    FROM prior_meaningful_success AS prior
    LEFT JOIN ranked_history AS history
        ON history.account_id = prior.account_id
       AND history.sync_mode = prior.sync_mode
       AND history.status = 'ok'
       AND history.messages_listed >= 100
       AND history.meaningful_success_rank BETWEEN 3 AND 7
    GROUP BY prior.account_id, prior.sync_mode
),
regression_flags AS (
    SELECT
        latest.account_id,
        latest.sync_mode,
        CASE
            WHEN recent.recent_failure_streak >= 2 THEN 'failure_streak'
            WHEN latest.status = 'ok'
             AND (latest.quota_pressure_retry_count > 0 OR latest.concurrency_pressure_retry_count > 0)
             AND retry.baseline_count = 3
             AND retry.clean_baseline_count = 3 THEN 'retry_pressure'
            WHEN latest.status = 'ok'
             AND latest.messages_listed >= 100
             AND throughput.baseline_count >= 5
             AND throughput.avg_messages_per_second > 0.0
             AND (
                latest.messages_per_second / throughput.avg_messages_per_second < 0.7
                OR (
                    latest.messages_per_second / throughput.avg_messages_per_second < 0.85
                    AND prior.baseline_count >= 5
                    AND prior.avg_messages_per_second > 0.0
                    AND prior_row.messages_per_second / prior.avg_messages_per_second < 0.85
                )
             ) THEN 'throughput_drop'
            WHEN latest.status = 'ok'
             AND latest.messages_listed >= 100
             AND throughput.baseline_count >= 5
             AND throughput.avg_duration_ms > 0.0
             AND (
                latest.duration_ms * 1.0 / throughput.avg_duration_ms > 1.5
                OR (
                    latest.duration_ms * 1.0 / throughput.avg_duration_ms > 1.25
                    AND prior.baseline_count >= 5
                    AND prior.avg_duration_ms > 0.0
                    AND prior_row.duration_ms * 1.0 / prior.avg_duration_ms > 1.25
                )
             ) THEN 'duration_spike'
            ELSE NULL
        END AS regression_kind
    FROM latest_rows AS latest
    INNER JOIN recent_stats AS recent
        ON recent.account_id = latest.account_id
       AND recent.sync_mode = latest.sync_mode
    LEFT JOIN retry_baseline AS retry
        ON retry.account_id = latest.account_id
       AND retry.sync_mode = latest.sync_mode
    LEFT JOIN throughput_baseline AS throughput
        ON throughput.account_id = latest.account_id
       AND throughput.sync_mode = latest.sync_mode
    LEFT JOIN prior_meaningful_success AS prior_row
        ON prior_row.account_id = latest.account_id
       AND prior_row.sync_mode = latest.sync_mode
    LEFT JOIN prior_baseline AS prior
        ON prior.account_id = latest.account_id
       AND prior.sync_mode = latest.sync_mode
)
SELECT
    latest.account_id,
    latest.sync_mode,
    latest.run_id,
    latest.status,
    latest.finished_at_epoch_s,
    best_clean.run_id,
    best_clean.effective_quota_units_per_minute,
    best_clean.effective_message_fetch_concurrency,
    best_clean.messages_per_second,
    best_clean.duration_ms,
    recent.recent_success_count,
    recent.recent_failure_count,
    recent.recent_failure_streak,
    recent.recent_clean_success_streak,
    CASE WHEN regression.regression_kind IS NOT NULL THEN 1 ELSE 0 END,
    regression.regression_kind,
    CASE WHEN regression.regression_kind IS NOT NULL THEN latest.run_id ELSE NULL END,
    CASE
        WHEN regression.regression_kind = 'failure_streak' THEN printf(
            '%d consecutive %s sync failures',
            recent.recent_failure_streak,
            latest.sync_mode
        )
        WHEN regression.regression_kind = 'retry_pressure' THEN printf(
            'retry pressure appeared after %d clean successful %s runs',
            3,
            latest.sync_mode
        )
        WHEN regression.regression_kind = 'throughput_drop' THEN printf(
            'messages_per_second dropped from %.3f baseline to %.3f',
            throughput.avg_messages_per_second,
            latest.messages_per_second
        )
        WHEN regression.regression_kind = 'duration_spike' THEN printf(
            'duration_ms rose from %.0f baseline to %d',
            throughput.avg_duration_ms,
            latest.duration_ms
        )
        ELSE NULL
    END,
    latest.finished_at_epoch_s
FROM latest_rows AS latest
INNER JOIN recent_stats AS recent
    ON recent.account_id = latest.account_id
   AND recent.sync_mode = latest.sync_mode
LEFT JOIN best_clean_rows AS best_clean
    ON best_clean.account_id = latest.account_id
   AND best_clean.sync_mode = latest.sync_mode
LEFT JOIN throughput_baseline AS throughput
    ON throughput.account_id = latest.account_id
   AND throughput.sync_mode = latest.sync_mode
LEFT JOIN regression_flags AS regression
    ON regression.account_id = latest.account_id
   AND regression.sync_mode = latest.sync_mode;

CREATE INDEX gmail_sync_run_summary_updated_idx
    ON gmail_sync_run_summary (updated_at_epoch_s DESC);

DROP TABLE gmail_sync_run_history_copy;
DROP TABLE gmail_sync_run_summary_copy;
