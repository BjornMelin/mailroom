ALTER TABLE gmail_sync_run_history
    ADD COLUMN comparability_kind TEXT NOT NULL
    DEFAULT 'full_query'
    CHECK (
        comparability_kind IN (
            'full_recent_days',
            'full_query',
            'incremental_workload_tier'
        )
    );

ALTER TABLE gmail_sync_run_history
    ADD COLUMN comparability_key TEXT NOT NULL DEFAULT '';

ALTER TABLE gmail_sync_run_history
    ADD COLUMN startup_seed_run_id INTEGER;

UPDATE gmail_sync_run_history
SET comparability_kind = CASE
        WHEN sync_mode = 'incremental' THEN 'incremental_workload_tier'
        WHEN instr(bootstrap_query, 'newer_than:') > 0
         AND instr(
                substr(
                    bootstrap_query,
                    instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                ),
                'd'
            ) > 1
         AND substr(
                substr(
                    bootstrap_query,
                    instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                ),
                1,
                instr(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    ),
                    'd'
                ) - 1
            ) NOT GLOB '*[^0-9]*'
         AND (
                length(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    )
                ) = instr(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    ),
                    'd'
                )
                OR substr(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    ),
                    instr(
                        substr(
                            bootstrap_query,
                            instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                        ),
                        'd'
                    ) + 1,
                    1
                ) = ' '
            ) THEN 'full_recent_days'
        ELSE 'full_query'
    END,
    comparability_key = CASE
        WHEN sync_mode = 'incremental' THEN CASE
            WHEN messages_listed + messages_deleted = 0 THEN 'zero_work'
            WHEN messages_listed + messages_deleted < 25 THEN 'tiny'
            WHEN messages_listed + messages_deleted < 100 THEN 'small'
            WHEN messages_listed + messages_deleted < 500 THEN 'medium'
            ELSE 'large'
        END
        WHEN instr(bootstrap_query, 'newer_than:') > 0
         AND instr(
                substr(
                    bootstrap_query,
                    instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                ),
                'd'
            ) > 1
         AND substr(
                substr(
                    bootstrap_query,
                    instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                ),
                1,
                instr(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    ),
                    'd'
                ) - 1
            ) NOT GLOB '*[^0-9]*'
         AND (
                length(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    )
                ) = instr(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    ),
                    'd'
                )
                OR substr(
                    substr(
                        bootstrap_query,
                        instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                    ),
                    instr(
                        substr(
                            bootstrap_query,
                            instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                        ),
                        'd'
                    ) + 1,
                    1
                ) = ' '
            ) THEN substr(
            bootstrap_query,
            instr(bootstrap_query, 'newer_than:') + length('newer_than:'),
            instr(
                substr(
                    bootstrap_query,
                    instr(bootstrap_query, 'newer_than:') + length('newer_than:')
                ),
                'd'
            ) - 1
        )
        ELSE bootstrap_query
    END;

ALTER TABLE gmail_sync_run_summary RENAME TO gmail_sync_run_summary_old;

CREATE TABLE gmail_sync_run_summary (
    account_id TEXT NOT NULL,
    sync_mode TEXT NOT NULL CHECK (sync_mode IN ('full', 'incremental')),
    comparability_kind TEXT NOT NULL CHECK (
        comparability_kind IN (
            'full_recent_days',
            'full_query',
            'incremental_workload_tier'
        )
    ),
    comparability_key TEXT NOT NULL,
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
    PRIMARY KEY (account_id, sync_mode, comparability_kind, comparability_key),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE,
    FOREIGN KEY (latest_run_id, account_id, sync_mode)
        REFERENCES gmail_sync_run_history (run_id, account_id, sync_mode) ON DELETE CASCADE,
    FOREIGN KEY (best_clean_run_id, account_id, sync_mode)
        REFERENCES gmail_sync_run_history (run_id, account_id, sync_mode) ON DELETE SET NULL,
    FOREIGN KEY (regression_run_id, account_id, sync_mode)
        REFERENCES gmail_sync_run_history (run_id, account_id, sync_mode) ON DELETE SET NULL
) STRICT;

INSERT INTO gmail_sync_run_summary (
    account_id,
    sync_mode,
    comparability_kind,
    comparability_key,
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
            PARTITION BY history.account_id, history.sync_mode, history.comparability_kind, history.comparability_key
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
        ) AS latest_rank,
        ROW_NUMBER() OVER (
            PARTITION BY history.account_id, history.sync_mode, history.comparability_kind, history.comparability_key
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
        ) AS recent_rank,
        SUM(CASE
                WHEN history.status = 'ok' AND history.messages_listed >= 100 THEN 1
                ELSE 0
            END) OVER (
            PARTITION BY history.account_id, history.sync_mode, history.comparability_kind, history.comparability_key
            ORDER BY history.finished_at_epoch_s DESC, history.run_id DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS meaningful_success_rank,
        SUM(CASE WHEN history.status != 'failed' THEN 1 ELSE 0 END) OVER (
            PARTITION BY history.account_id, history.sync_mode, history.comparability_kind, history.comparability_key
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
            PARTITION BY history.account_id, history.sync_mode, history.comparability_kind, history.comparability_key
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
        history.comparability_kind,
        history.comparability_key,
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
    GROUP BY
        history.account_id,
        history.sync_mode,
        history.comparability_kind,
        history.comparability_key
),
retry_baseline AS (
    SELECT
        latest.account_id,
        latest.sync_mode,
        latest.comparability_kind,
        latest.comparability_key,
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
       AND history.comparability_kind = latest.comparability_kind
       AND history.comparability_key = latest.comparability_key
       AND history.status = 'ok'
       AND history.recent_rank > 1
       AND history.recent_rank <= 4
    GROUP BY
        latest.account_id,
        latest.sync_mode,
        latest.comparability_kind,
        latest.comparability_key
),
best_clean_rows AS (
    SELECT *
    FROM (
        SELECT
            history.account_id,
            history.sync_mode,
            history.comparability_kind,
            history.comparability_key,
            history.run_id,
            history.effective_quota_units_per_minute,
            history.effective_message_fetch_concurrency,
            history.messages_per_second,
            history.duration_ms,
            ROW_NUMBER() OVER (
                PARTITION BY history.account_id, history.sync_mode, history.comparability_kind, history.comparability_key
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
        latest.comparability_kind,
        latest.comparability_key,
        COUNT(*) AS baseline_count,
        AVG(history.messages_per_second) AS avg_messages_per_second,
        AVG(history.duration_ms * 1.0) AS avg_duration_ms
    FROM latest_success_rows AS latest
    LEFT JOIN ranked_history AS history
        ON history.account_id = latest.account_id
       AND history.sync_mode = latest.sync_mode
       AND history.comparability_kind = latest.comparability_kind
       AND history.comparability_key = latest.comparability_key
       AND history.status = 'ok'
       AND history.messages_listed >= 100
       AND history.meaningful_success_rank BETWEEN 2 AND 6
    GROUP BY
        latest.account_id,
        latest.sync_mode,
        latest.comparability_kind,
        latest.comparability_key
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
        prior.comparability_kind,
        prior.comparability_key,
        COUNT(*) AS baseline_count,
        AVG(history.messages_per_second) AS avg_messages_per_second,
        AVG(history.duration_ms * 1.0) AS avg_duration_ms
    FROM prior_meaningful_success AS prior
    LEFT JOIN ranked_history AS history
        ON history.account_id = prior.account_id
       AND history.sync_mode = prior.sync_mode
       AND history.comparability_kind = prior.comparability_kind
       AND history.comparability_key = prior.comparability_key
       AND history.status = 'ok'
       AND history.messages_listed >= 100
       AND history.meaningful_success_rank BETWEEN 3 AND 7
    GROUP BY
        prior.account_id,
        prior.sync_mode,
        prior.comparability_kind,
        prior.comparability_key
),
regression_flags AS (
    SELECT
        latest.account_id,
        latest.sync_mode,
        latest.comparability_kind,
        latest.comparability_key,
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
       AND recent.comparability_kind = latest.comparability_kind
       AND recent.comparability_key = latest.comparability_key
    LEFT JOIN retry_baseline AS retry
        ON retry.account_id = latest.account_id
       AND retry.sync_mode = latest.sync_mode
       AND retry.comparability_kind = latest.comparability_kind
       AND retry.comparability_key = latest.comparability_key
    LEFT JOIN throughput_baseline AS throughput
        ON throughput.account_id = latest.account_id
       AND throughput.sync_mode = latest.sync_mode
       AND throughput.comparability_kind = latest.comparability_kind
       AND throughput.comparability_key = latest.comparability_key
    LEFT JOIN prior_meaningful_success AS prior_row
        ON prior_row.account_id = latest.account_id
       AND prior_row.sync_mode = latest.sync_mode
       AND prior_row.comparability_kind = latest.comparability_kind
       AND prior_row.comparability_key = latest.comparability_key
    LEFT JOIN prior_baseline AS prior
        ON prior.account_id = latest.account_id
       AND prior.sync_mode = latest.sync_mode
       AND prior.comparability_kind = latest.comparability_kind
       AND prior.comparability_key = latest.comparability_key
)
SELECT
    latest.account_id,
    latest.sync_mode,
    latest.comparability_kind,
    latest.comparability_key,
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
   AND recent.comparability_kind = latest.comparability_kind
   AND recent.comparability_key = latest.comparability_key
LEFT JOIN best_clean_rows AS best_clean
    ON best_clean.account_id = latest.account_id
   AND best_clean.sync_mode = latest.sync_mode
   AND best_clean.comparability_kind = latest.comparability_kind
   AND best_clean.comparability_key = latest.comparability_key
LEFT JOIN throughput_baseline AS throughput
    ON throughput.account_id = latest.account_id
   AND throughput.sync_mode = latest.sync_mode
   AND throughput.comparability_kind = latest.comparability_kind
   AND throughput.comparability_key = latest.comparability_key
LEFT JOIN regression_flags AS regression
    ON regression.account_id = latest.account_id
   AND regression.sync_mode = latest.sync_mode
   AND regression.comparability_kind = latest.comparability_kind
   AND regression.comparability_key = latest.comparability_key;

DROP TABLE gmail_sync_run_summary_old;

DROP INDEX IF EXISTS gmail_sync_run_summary_updated_idx;
CREATE INDEX gmail_sync_run_summary_updated_idx
    ON gmail_sync_run_summary (updated_at_epoch_s DESC);
