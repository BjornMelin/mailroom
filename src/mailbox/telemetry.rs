use crate::gmail::GmailQuotaMetricsSnapshot;
use crate::mailbox::model::{FinalizeSyncInput, SyncPerfExplainDrift, SyncRunReport};
use crate::mailbox::pacing::AdaptiveSyncPacingReport;
use crate::store;
use anyhow::{Result, anyhow};

#[derive(Debug, Clone)]
pub(crate) struct SyncRunContext {
    pub(crate) account_id: String,
    pub(crate) started_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct FailedSyncTelemetryContext<'a> {
    pub(crate) bootstrap_query: &'a str,
    pub(crate) mode: store::mailbox::SyncMode,
    pub(crate) comparability: store::mailbox::SyncRunComparability,
    pub(crate) startup_seed_run_id: Option<i64>,
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) fallback_from_history: bool,
    pub(crate) resumed_from_checkpoint: bool,
    pub(crate) pages_fetched: usize,
    pub(crate) messages_listed: usize,
    pub(crate) messages_deleted: usize,
    pub(crate) pipeline_report: crate::mailbox::pipeline::PipelineStatsReport,
    pub(crate) pacing_report: AdaptiveSyncPacingReport,
    pub(crate) quota_units_budget_per_minute: u32,
    pub(crate) message_fetch_concurrency: usize,
    pub(crate) metrics: GmailQuotaMetricsSnapshot,
    pub(crate) elapsed: std::time::Duration,
    pub(crate) error_message: String,
}

impl SyncRunContext {
    pub(crate) fn success_outcome_input(
        &self,
        finished_at_epoch_s: i64,
        report: &SyncRunReport,
    ) -> store::mailbox::SyncRunOutcomeInput {
        store::mailbox::SyncRunOutcomeInput {
            account_id: self.account_id.clone(),
            sync_mode: report.mode,
            status: store::mailbox::SyncStatus::Ok,
            comparability_kind: report.comparability_kind,
            comparability_key: report.comparability_key.clone(),
            startup_seed_run_id: report.startup_seed_run_id,
            started_at_epoch_s: self.started_at_epoch_s,
            finished_at_epoch_s,
            bootstrap_query: report.bootstrap_query.clone(),
            cursor_history_id: Some(report.cursor_history_id.clone()),
            fallback_from_history: report.fallback_from_history,
            resumed_from_checkpoint: report.resumed_from_checkpoint,
            pages_fetched: usize_to_i64(report.pages_fetched),
            messages_listed: usize_to_i64(report.messages_listed),
            messages_upserted: usize_to_i64(report.messages_upserted),
            messages_deleted: usize_to_i64(report.messages_deleted),
            labels_synced: usize_to_i64(report.labels_synced),
            checkpoint_reused_pages: usize_to_i64(report.checkpoint_reused_pages),
            checkpoint_reused_messages_upserted: usize_to_i64(
                report.checkpoint_reused_messages_upserted,
            ),
            pipeline_enabled: report.pipeline_enabled,
            pipeline_list_queue_high_water: usize_to_i64(report.pipeline_list_queue_high_water),
            pipeline_write_queue_high_water: usize_to_i64(report.pipeline_write_queue_high_water),
            pipeline_write_batch_count: usize_to_i64(report.pipeline_write_batch_count),
            pipeline_writer_wait_ms: u64_to_i64(report.pipeline_writer_wait_ms),
            pipeline_fetch_batch_count: usize_to_i64(report.pipeline_fetch_batch_count),
            pipeline_fetch_batch_avg_ms: u64_to_i64(report.pipeline_fetch_batch_avg_ms),
            pipeline_fetch_batch_max_ms: u64_to_i64(report.pipeline_fetch_batch_max_ms),
            pipeline_writer_tx_count: usize_to_i64(report.pipeline_writer_tx_count),
            pipeline_writer_tx_avg_ms: u64_to_i64(report.pipeline_writer_tx_avg_ms),
            pipeline_writer_tx_max_ms: u64_to_i64(report.pipeline_writer_tx_max_ms),
            pipeline_reorder_buffer_high_water: usize_to_i64(
                report.pipeline_reorder_buffer_high_water,
            ),
            pipeline_staged_message_count: usize_to_i64(report.pipeline_staged_message_count),
            pipeline_staged_delete_count: usize_to_i64(report.pipeline_staged_delete_count),
            pipeline_staged_attachment_count: usize_to_i64(report.pipeline_staged_attachment_count),
            adaptive_pacing_enabled: report.adaptive_pacing_enabled,
            quota_units_budget_per_minute: i64::from(report.quota_units_budget_per_minute),
            message_fetch_concurrency: usize_to_i64(report.message_fetch_concurrency),
            quota_units_cap_per_minute: i64::from(report.quota_units_cap_per_minute),
            message_fetch_concurrency_cap: usize_to_i64(report.message_fetch_concurrency_cap),
            starting_quota_units_per_minute: i64::from(report.starting_quota_units_per_minute),
            starting_message_fetch_concurrency: usize_to_i64(
                report.starting_message_fetch_concurrency,
            ),
            effective_quota_units_per_minute: i64::from(report.effective_quota_units_per_minute),
            effective_message_fetch_concurrency: usize_to_i64(
                report.effective_message_fetch_concurrency,
            ),
            adaptive_downshift_count: u64_to_i64(report.adaptive_downshift_count),
            estimated_quota_units_reserved: u64_to_i64(report.estimated_quota_units_reserved),
            http_attempt_count: u64_to_i64(report.http_attempt_count),
            retry_count: u64_to_i64(report.retry_count),
            quota_pressure_retry_count: u64_to_i64(report.quota_pressure_retry_count),
            concurrency_pressure_retry_count: u64_to_i64(report.concurrency_pressure_retry_count),
            backend_retry_count: u64_to_i64(report.backend_retry_count),
            throttle_wait_count: u64_to_i64(report.throttle_wait_count),
            throttle_wait_ms: u64_to_i64(report.throttle_wait_ms),
            retry_after_wait_ms: u64_to_i64(report.retry_after_wait_ms),
            duration_ms: u64_to_i64(report.duration_ms),
            pages_per_second: report.pages_per_second,
            messages_per_second: report.messages_per_second,
            error_message: None,
        }
    }

    pub(crate) fn failure_outcome_input(
        &self,
        finished_at_epoch_s: i64,
        failure: &FailedSyncTelemetryContext<'_>,
    ) -> store::mailbox::SyncRunOutcomeInput {
        let duration_ms =
            u64_to_i64(u64::try_from(failure.elapsed.as_millis()).unwrap_or(u64::MAX));
        let elapsed_seconds = failure.elapsed.as_secs_f64();
        let (pages_per_second, messages_per_second) = if elapsed_seconds > 0.0 {
            (
                failure.pages_fetched as f64 / elapsed_seconds,
                failure.messages_listed as f64 / elapsed_seconds,
            )
        } else {
            (0.0, 0.0)
        };
        store::mailbox::SyncRunOutcomeInput {
            account_id: self.account_id.clone(),
            sync_mode: failure.mode,
            status: store::mailbox::SyncStatus::Failed,
            comparability_kind: failure.comparability.kind,
            comparability_key: failure.comparability.key.clone(),
            startup_seed_run_id: failure.startup_seed_run_id,
            started_at_epoch_s: self.started_at_epoch_s,
            finished_at_epoch_s,
            bootstrap_query: failure.bootstrap_query.to_owned(),
            cursor_history_id: failure.cursor_history_id.clone(),
            fallback_from_history: failure.fallback_from_history,
            resumed_from_checkpoint: failure.resumed_from_checkpoint,
            pages_fetched: usize_to_i64(failure.pages_fetched),
            messages_listed: usize_to_i64(failure.messages_listed),
            messages_upserted: 0,
            messages_deleted: usize_to_i64(failure.messages_deleted),
            labels_synced: 0,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
            pipeline_enabled: failure.pipeline_report.pipeline_enabled,
            pipeline_list_queue_high_water: usize_to_i64(
                failure.pipeline_report.list_queue_high_water,
            ),
            pipeline_write_queue_high_water: usize_to_i64(
                failure.pipeline_report.write_queue_high_water,
            ),
            pipeline_write_batch_count: usize_to_i64(failure.pipeline_report.write_batch_count),
            pipeline_writer_wait_ms: u64_to_i64(failure.pipeline_report.writer_wait_ms),
            pipeline_fetch_batch_count: usize_to_i64(failure.pipeline_report.fetch_batch_count),
            pipeline_fetch_batch_avg_ms: u64_to_i64(failure.pipeline_report.fetch_batch_avg_ms),
            pipeline_fetch_batch_max_ms: u64_to_i64(failure.pipeline_report.fetch_batch_max_ms),
            pipeline_writer_tx_count: usize_to_i64(failure.pipeline_report.writer_tx_count),
            pipeline_writer_tx_avg_ms: u64_to_i64(failure.pipeline_report.writer_tx_avg_ms),
            pipeline_writer_tx_max_ms: u64_to_i64(failure.pipeline_report.writer_tx_max_ms),
            pipeline_reorder_buffer_high_water: usize_to_i64(
                failure.pipeline_report.reorder_buffer_high_water,
            ),
            pipeline_staged_message_count: usize_to_i64(
                failure.pipeline_report.staged_message_count,
            ),
            pipeline_staged_delete_count: usize_to_i64(failure.pipeline_report.staged_delete_count),
            pipeline_staged_attachment_count: usize_to_i64(
                failure.pipeline_report.staged_attachment_count,
            ),
            adaptive_pacing_enabled: failure.pacing_report.adaptive_pacing_enabled,
            quota_units_budget_per_minute: i64::from(failure.quota_units_budget_per_minute),
            message_fetch_concurrency: usize_to_i64(failure.message_fetch_concurrency),
            quota_units_cap_per_minute: i64::from(failure.pacing_report.quota_units_cap_per_minute),
            message_fetch_concurrency_cap: usize_to_i64(
                failure.pacing_report.message_fetch_concurrency_cap,
            ),
            starting_quota_units_per_minute: i64::from(
                failure.pacing_report.starting_quota_units_per_minute,
            ),
            starting_message_fetch_concurrency: usize_to_i64(
                failure.pacing_report.starting_message_fetch_concurrency,
            ),
            effective_quota_units_per_minute: i64::from(
                failure.pacing_report.effective_quota_units_per_minute,
            ),
            effective_message_fetch_concurrency: usize_to_i64(
                failure.pacing_report.effective_message_fetch_concurrency,
            ),
            adaptive_downshift_count: u64_to_i64(failure.pacing_report.adaptive_downshift_count),
            estimated_quota_units_reserved: u64_to_i64(failure.metrics.reserved_units),
            http_attempt_count: u64_to_i64(failure.metrics.http_attempts),
            retry_count: u64_to_i64(failure.metrics.retry_count),
            quota_pressure_retry_count: u64_to_i64(failure.metrics.quota_pressure_retry_count),
            concurrency_pressure_retry_count: u64_to_i64(
                failure.metrics.concurrency_pressure_retry_count,
            ),
            backend_retry_count: u64_to_i64(failure.metrics.backend_retry_count),
            throttle_wait_count: u64_to_i64(failure.metrics.throttle_wait_count),
            throttle_wait_ms: u64_to_i64(failure.metrics.throttle_wait_ms),
            retry_after_wait_ms: u64_to_i64(failure.metrics.retry_after_wait_ms),
            duration_ms,
            pages_per_second,
            messages_per_second,
            error_message: Some(failure.error_message.clone()),
        }
    }
}

pub(crate) fn populate_sync_report_metrics(
    report: &mut SyncRunReport,
    pacing_report: AdaptiveSyncPacingReport,
    metrics: Option<GmailQuotaMetricsSnapshot>,
    options: &crate::mailbox::SyncRunOptions,
) {
    report.adaptive_pacing_enabled = pacing_report.adaptive_pacing_enabled;
    report.quota_units_budget_per_minute = options.quota_units_per_minute;
    report.message_fetch_concurrency = options.message_fetch_concurrency;
    report.quota_units_cap_per_minute = pacing_report.quota_units_cap_per_minute;
    report.message_fetch_concurrency_cap = pacing_report.message_fetch_concurrency_cap;
    report.starting_quota_units_per_minute = pacing_report.starting_quota_units_per_minute;
    report.starting_message_fetch_concurrency = pacing_report.starting_message_fetch_concurrency;
    report.effective_quota_units_per_minute = pacing_report.effective_quota_units_per_minute;
    report.effective_message_fetch_concurrency = pacing_report.effective_message_fetch_concurrency;
    report.adaptive_downshift_count = pacing_report.adaptive_downshift_count;

    if let Some(metrics) = metrics {
        report.estimated_quota_units_reserved = metrics.reserved_units;
        report.http_attempt_count = metrics.http_attempts;
        report.retry_count = metrics.retry_count;
        report.quota_pressure_retry_count = metrics.quota_pressure_retry_count;
        report.concurrency_pressure_retry_count = metrics.concurrency_pressure_retry_count;
        report.backend_retry_count = metrics.backend_retry_count;
        report.throttle_wait_count = metrics.throttle_wait_count;
        report.throttle_wait_ms = metrics.throttle_wait_ms;
        report.retry_after_wait_ms = metrics.retry_after_wait_ms;
    }
}

pub(crate) fn populate_sync_report_timing(
    report: &mut SyncRunReport,
    elapsed: std::time::Duration,
) {
    report.duration_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    let elapsed_seconds = elapsed.as_secs_f64();
    if elapsed_seconds > 0.0 {
        report.pages_per_second = report.pages_fetched as f64 / elapsed_seconds;
        report.messages_per_second = report.messages_listed as f64 / elapsed_seconds;
    } else {
        report.pages_per_second = 0.0;
        report.messages_per_second = 0.0;
    }
}

pub(crate) fn finalize_sync(
    sync_state: store::mailbox::SyncStateRecord,
    bootstrap_query: &str,
    input: FinalizeSyncInput,
) -> Result<SyncRunReport> {
    Ok(SyncRunReport {
        run_id: 0,
        mode: input.mode,
        comparability_kind: input.comparability_kind,
        comparability_key: input.comparability_key,
        comparability_label: input.comparability_label,
        startup_seed_run_id: input.startup_seed_run_id,
        fallback_from_history: input.fallback_from_history,
        resumed_from_checkpoint: input.resumed_from_checkpoint,
        bootstrap_query: bootstrap_query.to_owned(),
        cursor_history_id: sync_state
            .cursor_history_id
            .ok_or_else(|| anyhow!("sync completed without a history cursor"))?,
        pages_fetched: input.pages_fetched,
        messages_listed: input.messages_listed,
        messages_upserted: input.messages_upserted,
        messages_deleted: input.messages_deleted,
        labels_synced: input.labels_synced,
        checkpoint_reused_pages: input.checkpoint_reused_pages,
        checkpoint_reused_messages_upserted: input.checkpoint_reused_messages_upserted,
        pipeline_enabled: input.pipeline_enabled,
        pipeline_list_queue_high_water: input.pipeline_list_queue_high_water,
        pipeline_write_queue_high_water: input.pipeline_write_queue_high_water,
        pipeline_write_batch_count: input.pipeline_write_batch_count,
        pipeline_writer_wait_ms: input.pipeline_writer_wait_ms,
        pipeline_fetch_batch_count: input.pipeline_fetch_batch_count,
        pipeline_fetch_batch_avg_ms: input.pipeline_fetch_batch_avg_ms,
        pipeline_fetch_batch_max_ms: input.pipeline_fetch_batch_max_ms,
        pipeline_writer_tx_count: input.pipeline_writer_tx_count,
        pipeline_writer_tx_avg_ms: input.pipeline_writer_tx_avg_ms,
        pipeline_writer_tx_max_ms: input.pipeline_writer_tx_max_ms,
        pipeline_reorder_buffer_high_water: input.pipeline_reorder_buffer_high_water,
        pipeline_staged_message_count: input.pipeline_staged_message_count,
        pipeline_staged_delete_count: input.pipeline_staged_delete_count,
        pipeline_staged_attachment_count: input.pipeline_staged_attachment_count,
        store_message_count: sync_state.message_count,
        store_label_count: sync_state.label_count,
        store_indexed_message_count: sync_state.indexed_message_count,
        adaptive_pacing_enabled: false,
        quota_units_budget_per_minute: 0,
        message_fetch_concurrency: 0,
        quota_units_cap_per_minute: 0,
        message_fetch_concurrency_cap: 0,
        starting_quota_units_per_minute: 0,
        starting_message_fetch_concurrency: 0,
        effective_quota_units_per_minute: 0,
        effective_message_fetch_concurrency: 0,
        adaptive_downshift_count: 0,
        estimated_quota_units_reserved: 0,
        http_attempt_count: 0,
        retry_count: 0,
        quota_pressure_retry_count: 0,
        concurrency_pressure_retry_count: 0,
        backend_retry_count: 0,
        throttle_wait_count: 0,
        throttle_wait_ms: 0,
        retry_after_wait_ms: 0,
        duration_ms: 0,
        pages_per_second: 0.0,
        messages_per_second: 0.0,
        regression_detected: false,
        regression_kind: None,
    })
}

pub(crate) fn default_gmail_quota_metrics_snapshot() -> GmailQuotaMetricsSnapshot {
    GmailQuotaMetricsSnapshot {
        reserved_units: 0,
        http_attempts: 0,
        retry_count: 0,
        quota_pressure_retry_count: 0,
        concurrency_pressure_retry_count: 0,
        backend_retry_count: 0,
        throttle_wait_count: 0,
        throttle_wait_ms: 0,
        retry_after_wait_ms: 0,
    }
}

pub(crate) fn build_sync_perf_drift(
    latest: &store::mailbox::SyncRunHistoryRecord,
    baseline: &store::mailbox::SyncRunHistoryRecord,
) -> SyncPerfExplainDrift {
    SyncPerfExplainDrift {
        messages_per_second_delta: Some(latest.messages_per_second - baseline.messages_per_second),
        duration_ms_delta: Some(latest.duration_ms - baseline.duration_ms),
        retry_count_delta: Some(latest.retry_count - baseline.retry_count),
        quota_units_delta: Some(
            latest.effective_quota_units_per_minute - baseline.effective_quota_units_per_minute,
        ),
        message_fetch_concurrency_delta: Some(
            latest.effective_message_fetch_concurrency
                - baseline.effective_message_fetch_concurrency,
        ),
    }
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
