use super::read::{read_sync_run_history_record, row_to_sync_run_history};
use super::run_history_policy::{
    DetectedSyncRunRegression, SYNC_RUN_SUMMARY_RECENT_WINDOW, compare_best_clean_run,
    detect_sync_run_regression, is_clean_success,
};
use super::{
    SyncMode, SyncRunComparability, SyncRunHistoryRecord, SyncRunSummaryRecord, SyncStatus,
};
use anyhow::{Result, anyhow, ensure};
use rusqlite::{OptionalExtension, params};

pub(crate) fn recompute_sync_run_summary_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    sync_mode: SyncMode,
    comparability: &SyncRunComparability,
    updated_at_epoch_s: i64,
) -> Result<SyncRunSummaryRecord> {
    let history =
        read_sync_run_history_rows_for_summary(transaction, account_id, sync_mode, comparability)?;
    ensure!(
        !history.is_empty(),
        "sync run summary requires at least one history row"
    );

    let latest = &history[0];
    let recent_window = history.len().min(SYNC_RUN_SUMMARY_RECENT_WINDOW);
    let recent_rows = &history[..recent_window];
    let recent_success_count = recent_rows
        .iter()
        .filter(|row| row.status == SyncStatus::Ok)
        .count() as i64;
    let recent_failure_count = recent_rows
        .iter()
        .filter(|row| row.status == SyncStatus::Failed)
        .count() as i64;
    let recent_failure_streak = history
        .iter()
        .take_while(|row| row.status == SyncStatus::Failed)
        .count() as i64;
    let recent_clean_success_streak = history
        .iter()
        .take_while(|row| is_clean_success(row))
        .count() as i64;

    let best_clean_run = history
        .iter()
        .filter(|row| is_clean_success(row))
        .max_by(|left, right| compare_best_clean_run(left, right));

    let regression = detect_sync_run_regression(&history);

    let summary_input = SyncRunSummaryUpsert {
        account_id,
        sync_mode,
        comparability,
        latest,
        best_clean_run,
        recent_success_count,
        recent_failure_count,
        recent_failure_streak,
        recent_clean_success_streak,
        regression,
        updated_at_epoch_s,
    };

    upsert_sync_run_summary(transaction, &summary_input)?;

    read_sync_run_summary_for_comparability(transaction, account_id, sync_mode, &comparability.key)?
        .ok_or_else(|| anyhow!("sync run summary disappeared after upsert"))
}

struct SyncRunSummaryUpsert<'a> {
    account_id: &'a str,
    sync_mode: SyncMode,
    comparability: &'a SyncRunComparability,
    latest: &'a SyncRunHistoryRecord,
    best_clean_run: Option<&'a SyncRunHistoryRecord>,
    recent_success_count: i64,
    recent_failure_count: i64,
    recent_failure_streak: i64,
    recent_clean_success_streak: i64,
    regression: Option<DetectedSyncRunRegression>,
    updated_at_epoch_s: i64,
}

fn upsert_sync_run_summary(
    transaction: &rusqlite::Transaction<'_>,
    input: &SyncRunSummaryUpsert<'_>,
) -> Result<()> {
    let mut statement = transaction.prepare_cached(
        "INSERT INTO gmail_sync_run_summary (
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
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
         ON CONFLICT (account_id, sync_mode, comparability_key) DO UPDATE SET
             comparability_kind = excluded.comparability_kind,
             latest_run_id = excluded.latest_run_id,
             latest_status = excluded.latest_status,
             latest_finished_at_epoch_s = excluded.latest_finished_at_epoch_s,
             best_clean_run_id = excluded.best_clean_run_id,
             best_clean_quota_units_per_minute = excluded.best_clean_quota_units_per_minute,
             best_clean_message_fetch_concurrency = excluded.best_clean_message_fetch_concurrency,
             best_clean_messages_per_second = excluded.best_clean_messages_per_second,
             best_clean_duration_ms = excluded.best_clean_duration_ms,
             recent_success_count = excluded.recent_success_count,
             recent_failure_count = excluded.recent_failure_count,
             recent_failure_streak = excluded.recent_failure_streak,
             recent_clean_success_streak = excluded.recent_clean_success_streak,
             regression_detected = excluded.regression_detected,
             regression_kind = excluded.regression_kind,
             regression_run_id = excluded.regression_run_id,
             regression_message = excluded.regression_message,
             updated_at_epoch_s = excluded.updated_at_epoch_s",
    )?;
    statement.execute(params![
        input.account_id,
        input.sync_mode.as_str(),
        input.comparability.kind.as_str(),
        &input.comparability.key,
        input.latest.run_id,
        input.latest.status.as_str(),
        input.latest.finished_at_epoch_s,
        input.best_clean_run.map(|row| row.run_id),
        input
            .best_clean_run
            .map(|row| row.effective_quota_units_per_minute),
        input
            .best_clean_run
            .map(|row| row.effective_message_fetch_concurrency),
        input.best_clean_run.map(|row| row.messages_per_second),
        input.best_clean_run.map(|row| row.duration_ms),
        input.recent_success_count,
        input.recent_failure_count,
        input.recent_failure_streak,
        input.recent_clean_success_streak,
        if input.regression.is_some() {
            1_i64
        } else {
            0_i64
        },
        input.regression.as_ref().map(|value| value.kind.as_str()),
        input.regression.as_ref().map(|value| value.run_id),
        input
            .regression
            .as_ref()
            .map(|value| value.message.as_str()),
        input.updated_at_epoch_s,
    ])?;
    Ok(())
}

fn read_sync_run_history_rows_for_summary(
    connection: &impl std::ops::Deref<Target = rusqlite::Connection>,
    account_id: &str,
    sync_mode: SyncMode,
    comparability: &SyncRunComparability,
) -> Result<Vec<SyncRunHistoryRecord>> {
    let mut statement = connection.prepare_cached(
        "SELECT
             run_id,
             account_id,
             sync_mode,
             status,
             comparability_kind,
             comparability_key,
             startup_seed_run_id,
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
         FROM gmail_sync_run_history
         WHERE account_id = ?1
           AND sync_mode = ?2
           AND comparability_key = ?3
         ORDER BY finished_at_epoch_s DESC, run_id DESC",
    )?;
    let rows = statement
        .query_map(
            params![account_id, sync_mode.as_str(), &comparability.key],
            row_to_sync_run_history,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub(crate) fn read_sync_run_summary_for_comparability(
    connection: &rusqlite::Connection,
    account_id: &str,
    sync_mode: SyncMode,
    comparability_key: &str,
) -> rusqlite::Result<Option<SyncRunSummaryRecord>> {
    connection
        .query_row(
            "SELECT
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
             FROM gmail_sync_run_summary
             WHERE account_id = ?1
               AND sync_mode = ?2
               AND comparability_key = ?3",
            params![account_id, sync_mode.as_str(), comparability_key],
            super::read::row_to_sync_run_summary,
        )
        .optional()
}

#[allow(dead_code)]
pub(crate) fn read_best_clean_run_for_summary(
    connection: &rusqlite::Connection,
    summary: &SyncRunSummaryRecord,
) -> Result<Option<SyncRunHistoryRecord>> {
    let Some(run_id) = summary.best_clean_run_id else {
        return Ok(None);
    };
    read_sync_run_history_record(connection, run_id).map_err(Into::into)
}
