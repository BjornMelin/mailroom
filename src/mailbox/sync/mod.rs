use crate::config::ConfigReport;
use crate::gmail::{GmailClient, GmailLabel, GmailMessageCatalog};
use crate::mailbox::model::{
    FinalizeSyncInput, SyncHistoryReport, SyncPerfExplainReport, SyncRunOptions,
};
use crate::mailbox::pacing::{AdaptiveSyncPacing, AdaptiveSyncPacingSeed};
use crate::mailbox::pipeline::{
    ListedPage, PIPELINE_LIST_QUEUE_CAPACITY, PIPELINE_WRITE_BATCH_MESSAGE_TARGET,
    PIPELINE_WRITE_QUEUE_CAPACITY, PipelineStats, PipelineStatsReport,
    page_processing_concurrency_for_fetch,
};
use crate::mailbox::telemetry::{
    FailedSyncTelemetryContext, SyncRunContext, build_sync_perf_drift,
    default_gmail_quota_metrics_snapshot, finalize_sync, populate_sync_report_metrics,
    populate_sync_report_timing,
};
use crate::mailbox::util::{
    bootstrap_query, is_invalid_resume_page_token_error, is_stale_history_error, labels_by_id,
    message_is_excluded, newest_history_id, recipient_headers,
};
use crate::mailbox::{
    DEFAULT_MESSAGE_FETCH_CONCURRENCY, DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE, FULL_SYNC_PAGE_SIZE,
    SyncRunReport,
};
use crate::store;
use crate::store::accounts::AccountRecord;
use crate::time::current_epoch_seconds;
use anyhow::{Context, Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::Instant;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{JoinSet, spawn_blocking};

mod full_sync;
mod incremental_sync;
mod pipeline;
#[cfg(test)]
mod sync_error_tests;

use self::full_sync::run_full_sync;
use self::incremental_sync::{
    failure_comparability, load_sync_state, maybe_seed_pacing_from_history, observe_latest_metrics,
    persist_sync_pacing_failure, persist_sync_state_failure, resolve_sync_history_account_id,
    run_incremental_sync,
};
use self::pipeline::{MailboxStoreHandle, preserve_sync_error};

const SYNC_PERF_INCREMENTAL_MIN_MEANINGFUL_WORK: i64 = 5;
const SYNC_PERF_INCREMENTAL_MAX_WORK_SPREAD_RATIO: i64 = 3;

pub async fn sync_run(
    config_report: &ConfigReport,
    force_full: bool,
    recent_days: u32,
) -> Result<SyncRunReport> {
    sync_run_with_options(
        config_report,
        SyncRunOptions {
            force_full,
            recent_days,
            quota_units_per_minute: DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE,
            message_fetch_concurrency: DEFAULT_MESSAGE_FETCH_CONCURRENCY,
        },
    )
    .await
}

pub async fn sync_run_with_options(
    config_report: &ConfigReport,
    options: SyncRunOptions,
) -> Result<SyncRunReport> {
    if options.recent_days == 0 {
        return Err(anyhow!("recent_days must be greater than zero"));
    }
    if options.message_fetch_concurrency == 0 {
        return Err(anyhow!(
            "message_fetch_concurrency must be greater than zero"
        ));
    }
    let sync_started_at_epoch_s = current_epoch_seconds()?;

    let gmail_client = crate::gmail_client_for_config(config_report)?
        .with_quota_budget(options.quota_units_per_minute)?;
    let account =
        crate::refresh_active_account_record_with_client(config_report, &gmail_client).await?;
    let run_context = SyncRunContext {
        account_id: account.account_id.clone(),
        started_at_epoch_s: sync_started_at_epoch_s,
    };
    let store_handle = MailboxStoreHandle::new(config_report, &account.account_id);
    let writer = MailboxWriterWorker::start(&store_handle).await?;
    let persisted_pacing_state = store_handle.load_sync_pacing_state().await?;
    let mut pacing = AdaptiveSyncPacing::new(
        persisted_pacing_state.as_ref(),
        options.quota_units_per_minute,
        options.message_fetch_concurrency,
    );
    gmail_client.update_quota_budget(pacing.starting_quota_units_per_minute())?;
    let requested_bootstrap_query = bootstrap_query(options.recent_days);
    let existing_sync_state = load_sync_state(&store_handle).await?;
    let initial_mode = sync_mode(options.force_full, existing_sync_state.as_ref());
    let mut failure_mode = initial_mode;
    let mut failure_cursor_history_id = existing_sync_state
        .as_ref()
        .and_then(|state| state.cursor_history_id.clone());
    let persisted_bootstrap_query = existing_sync_state
        .as_ref()
        .map(|state| state.bootstrap_query.as_str())
        .unwrap_or(requested_bootstrap_query.as_str());
    let initial_bootstrap_query = match initial_mode {
        store::mailbox::SyncMode::Full => requested_bootstrap_query.as_str(),
        store::mailbox::SyncMode::Incremental => persisted_bootstrap_query,
    };
    if initial_mode == store::mailbox::SyncMode::Full {
        maybe_seed_pacing_from_history(
            &store_handle,
            &mut pacing,
            &gmail_client,
            initial_mode,
            &store::mailbox::comparability_for_full_bootstrap_query(initial_bootstrap_query),
        )
        .await?;
    }
    let mut failure_bootstrap_query = initial_bootstrap_query;
    let mut failure_fallback_from_history = false;
    let mut full_sync_failure_telemetry = FullSyncFailureTelemetry::zero_work();
    let mut incremental_failure_telemetry = IncrementalFailureTelemetry::zero_work();
    let sync_started_at = Instant::now();

    let result = async {
        let labels = gmail_client.list_labels().await?;
        let label_names_by_id = labels_by_id(&labels);
        let sync_context = SyncExecutionContext {
            store_handle: &store_handle,
            writer: &writer,
            gmail_client: &gmail_client,
            account: &account,
            labels: &labels,
            label_names_by_id: &label_names_by_id,
        };
        observe_latest_metrics(&mut pacing, &gmail_client)?;

        match initial_mode {
            store::mailbox::SyncMode::Full => {
                run_full_sync(
                    &sync_context,
                    &mut pacing,
                    initial_bootstrap_query,
                    false,
                    &mut full_sync_failure_telemetry,
                )
                .await
            }
            store::mailbox::SyncMode::Incremental => {
                let sync_state = existing_sync_state
                    .as_ref()
                    .ok_or_else(|| anyhow!("incremental sync requires an existing sync state"))?;
                match run_incremental_sync(
                    &sync_context,
                    &mut pacing,
                    persisted_bootstrap_query,
                    sync_state.cursor_history_id.clone(),
                    &mut incremental_failure_telemetry,
                    &mut full_sync_failure_telemetry.pipeline_report,
                )
                .await
                {
                    Ok(report) => Ok(report),
                    Err(error) if is_stale_history_error(&error) => {
                        failure_mode = store::mailbox::SyncMode::Full;
                        failure_cursor_history_id = None;
                        failure_bootstrap_query = persisted_bootstrap_query;
                        failure_fallback_from_history = true;
                        incremental_failure_telemetry = IncrementalFailureTelemetry::zero_work();
                        maybe_seed_pacing_from_history(
                            &store_handle,
                            &mut pacing,
                            &gmail_client,
                            store::mailbox::SyncMode::Full,
                            &store::mailbox::comparability_for_full_bootstrap_query(
                                persisted_bootstrap_query,
                            ),
                        )
                        .await?;
                        run_full_sync(
                            &sync_context,
                            &mut pacing,
                            persisted_bootstrap_query,
                            true,
                            &mut full_sync_failure_telemetry,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }
    .await;
    let writer_shutdown_result = writer.shutdown().await;

    match (result, writer_shutdown_result) {
        (Ok(mut report), Ok(())) => {
            observe_latest_metrics(&mut pacing, &gmail_client)?;
            let pacing_report = pacing.report();
            let now_epoch_s = current_epoch_seconds()?;
            let _ = store_handle
                .upsert_sync_pacing_state(pacing.finalize_success(&account.account_id, now_epoch_s))
                .await?;
            let metrics = gmail_client.request_metrics_snapshot();
            populate_sync_report_metrics(&mut report, pacing_report, metrics, &options);
            populate_sync_report_timing(&mut report, sync_started_at.elapsed());
            let sync_state = store_handle
                .load_sync_state()
                .await?
                .ok_or_else(|| anyhow!("sync state disappeared after successful sync"))?;
            let (_, history, summary) = store_handle
                .persist_successful_sync_outcome(
                    &sync_state,
                    &run_context.success_outcome_input(now_epoch_s, &report),
                )
                .await?;
            report.run_id = history.run_id;
            report.regression_detected = summary.regression_detected;
            report.regression_kind = summary.regression_kind;
            Ok(report)
        }
        (Ok(report), Err(error)) => {
            Err(error.context(format!(
                "sync succeeded (run_id={}, mode={}, pages_fetched={}, messages_upserted={}) but mailbox writer shutdown failed",
                report.run_id, report.mode, report.pages_fetched, report.messages_upserted
            )))
        }
        (Err(sync_error), writer_shutdown_result) => {
            let pacing_persist_result =
                persist_sync_pacing_failure(&store_handle, &account, &mut pacing, &gmail_client)
                    .await;
            let pacing_report = pacing.report();
            let metrics = gmail_client
                .request_metrics_snapshot()
                .unwrap_or_else(default_gmail_quota_metrics_snapshot);
            let persist_result = writer_shutdown_result.and(
                persist_sync_state_failure(
                    &store_handle,
                    &run_context,
                    &account,
                    FailedSyncTelemetryContext {
                        bootstrap_query: failure_bootstrap_query,
                        mode: failure_mode,
                        comparability: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                failure_comparability(failure_mode, failure_bootstrap_query)
                            }
                            store::mailbox::SyncMode::Incremental => {
                                incremental_failure_telemetry.comparability
                            }
                        },
                        startup_seed_run_id: pacing.startup_seed_run_id(),
                        cursor_history_id: failure_cursor_history_id,
                        fallback_from_history: failure_fallback_from_history,
                        resumed_from_checkpoint: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.resumed_from_checkpoint
                            }
                            store::mailbox::SyncMode::Incremental => false,
                        },
                        pages_fetched: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.pages_fetched
                            }
                            store::mailbox::SyncMode::Incremental => {
                                incremental_failure_telemetry.pages_fetched
                            }
                        },
                        messages_listed: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.messages_listed
                            }
                            store::mailbox::SyncMode::Incremental => {
                                incremental_failure_telemetry.messages_listed
                            }
                        },
                        messages_upserted: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.messages_upserted
                            }
                            store::mailbox::SyncMode::Incremental => {
                                incremental_failure_telemetry.messages_upserted
                            }
                        },
                        messages_deleted: incremental_failure_telemetry.messages_deleted,
                        labels_synced: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.labels_synced
                            }
                            store::mailbox::SyncMode::Incremental => {
                                incremental_failure_telemetry.labels_synced
                            }
                        },
                        checkpoint_reused_pages: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.checkpoint_reused_pages
                            }
                            store::mailbox::SyncMode::Incremental => 0,
                        },
                        checkpoint_reused_messages_upserted: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.checkpoint_reused_messages_upserted
                            }
                            store::mailbox::SyncMode::Incremental => 0,
                        },
                        pipeline_report: match failure_mode {
                            store::mailbox::SyncMode::Full => {
                                full_sync_failure_telemetry.pipeline_report
                            }
                            store::mailbox::SyncMode::Incremental => {
                                full_sync_failure_telemetry.pipeline_report
                            }
                        },
                        pacing_report,
                        quota_units_budget_per_minute: options.quota_units_per_minute,
                        message_fetch_concurrency: options.message_fetch_concurrency,
                        metrics,
                        elapsed: sync_started_at.elapsed(),
                        error_message: sync_error.to_string(),
                    },
                )
                .await,
            );
            Err(preserve_sync_error(
                sync_error,
                persist_result.and(pacing_persist_result),
            ))
        }
    }
}

pub async fn sync_history(config_report: &ConfigReport, limit: usize) -> Result<SyncHistoryReport> {
    if limit == 0 {
        return Err(anyhow!("history limit must be greater than zero"));
    }
    let config_report = config_report.clone();
    let (account_id, summary, runs) = spawn_blocking(move || {
        store::init(&config_report)?;
        let account_id = resolve_sync_history_account_id(&config_report)?;
        let database_path = config_report.config.store.database_path.clone();
        let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        let summary = store::mailbox::get_latest_sync_run_summary_for_account(
            &database_path,
            busy_timeout_ms,
            &account_id,
        )?;
        let runs = store::mailbox::list_sync_run_history(
            &database_path,
            busy_timeout_ms,
            &account_id,
            limit,
        )?;
        Ok::<_, anyhow::Error>((account_id, summary, runs))
    })
    .await??;

    Ok(SyncHistoryReport {
        account_id,
        limit,
        summary,
        runs,
    })
}

pub async fn sync_perf_explain(
    config_report: &ConfigReport,
    limit: usize,
) -> Result<SyncPerfExplainReport> {
    if limit == 0 {
        return Err(anyhow!("history limit must be greater than zero"));
    }
    let config_report = config_report.clone();
    let (account_id, summary, runs, baseline_run) = spawn_blocking(move || {
        store::init(&config_report)?;
        let account_id = resolve_sync_history_account_id(&config_report)?;
        let database_path = config_report.config.store.database_path.clone();
        let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        let summary = store::mailbox::get_latest_sync_run_summary_for_account(
            &database_path,
            busy_timeout_ms,
            &account_id,
        )?;
        let runs = store::mailbox::list_sync_run_history(
            &database_path,
            busy_timeout_ms,
            &account_id,
            limit,
        )?;
        let baseline_run = match summary
            .as_ref()
            .and_then(|summary| summary.best_clean_run_id)
        {
            Some(run_id) => store::mailbox::get_sync_run_history_record(
                &database_path,
                busy_timeout_ms,
                run_id,
            )?,
            None => None,
        };
        Ok::<_, anyhow::Error>((account_id, summary, runs, baseline_run))
    })
    .await??;

    let latest_run = runs.first().cloned();
    let comparable_to_baseline = latest_run
        .as_ref()
        .zip(baseline_run.as_ref())
        .map(|(latest, baseline)| sync_perf_runs_are_comparable(latest, baseline))
        .unwrap_or(false);
    let drift = latest_run
        .as_ref()
        .zip(baseline_run.as_ref())
        .filter(|_| comparable_to_baseline)
        .map(|(latest, baseline)| build_sync_perf_drift(latest, baseline));

    Ok(SyncPerfExplainReport {
        account_id,
        limit,
        latest_run,
        summary,
        baseline_run,
        comparable_to_baseline,
        drift,
        runs,
    })
}

fn sync_perf_runs_are_comparable(
    latest: &store::mailbox::SyncRunHistoryRecord,
    baseline: &store::mailbox::SyncRunHistoryRecord,
) -> bool {
    if latest.comparability_kind != baseline.comparability_kind
        || latest.comparability_key != baseline.comparability_key
    {
        return false;
    }

    match latest.comparability_kind {
        store::mailbox::SyncRunComparabilityKind::IncrementalWorkloadTier => {
            incremental_sync_perf_runs_are_comparable(latest, baseline)
        }
        store::mailbox::SyncRunComparabilityKind::FullRecentDays
        | store::mailbox::SyncRunComparabilityKind::FullQuery => true,
    }
}

fn incremental_sync_perf_runs_are_comparable(
    latest: &store::mailbox::SyncRunHistoryRecord,
    baseline: &store::mailbox::SyncRunHistoryRecord,
) -> bool {
    let latest_work = sync_perf_incremental_total_work(latest);
    let baseline_work = sync_perf_incremental_total_work(baseline);
    if latest_work < SYNC_PERF_INCREMENTAL_MIN_MEANINGFUL_WORK
        || baseline_work < SYNC_PERF_INCREMENTAL_MIN_MEANINGFUL_WORK
    {
        return false;
    }

    let larger_work = latest_work.max(baseline_work);
    let smaller_work = latest_work.min(baseline_work);
    larger_work <= smaller_work.saturating_mul(SYNC_PERF_INCREMENTAL_MAX_WORK_SPREAD_RATIO)
}

fn sync_perf_incremental_total_work(run: &store::mailbox::SyncRunHistoryRecord) -> i64 {
    run.messages_listed
        .max(0)
        .saturating_add(run.messages_deleted.max(0))
}

fn sync_mode(
    force_full: bool,
    existing_sync_state: Option<&store::mailbox::SyncStateRecord>,
) -> store::mailbox::SyncMode {
    if force_full
        || existing_sync_state
            .and_then(|state| state.cursor_history_id.as_ref())
            .is_none()
    {
        store::mailbox::SyncMode::Full
    } else {
        store::mailbox::SyncMode::Incremental
    }
}

struct SyncExecutionContext<'a> {
    store_handle: &'a MailboxStoreHandle,
    writer: &'a MailboxWriterWorker,
    gmail_client: &'a GmailClient,
    account: &'a AccountRecord,
    labels: &'a [GmailLabel],
    label_names_by_id: &'a BTreeMap<String, String>,
}

struct FullSyncCheckpointState {
    record: store::mailbox::FullSyncCheckpointRecord,
    resumed_from_checkpoint: bool,
}

struct FullSyncFinalizeRequest<'a> {
    bootstrap_query: &'a str,
    fallback_from_history: bool,
    resumed_from_checkpoint: bool,
    startup_seed_run_id: Option<i64>,
    checkpoint: store::mailbox::FullSyncCheckpointRecord,
    checkpoint_reused_pages: usize,
    checkpoint_reused_messages_upserted: usize,
    pipeline_report: PipelineStatsReport,
}

#[derive(Debug, Clone)]
struct IncrementalFailureTelemetry {
    comparability: store::mailbox::SyncRunComparability,
    pages_fetched: usize,
    messages_listed: usize,
    messages_upserted: usize,
    messages_deleted: usize,
    labels_synced: usize,
}

#[derive(Debug, Clone)]
struct FullSyncFailureTelemetry {
    resumed_from_checkpoint: bool,
    pages_fetched: usize,
    messages_listed: usize,
    messages_upserted: usize,
    labels_synced: usize,
    checkpoint_reused_pages: usize,
    checkpoint_reused_messages_upserted: usize,
    pipeline_report: PipelineStatsReport,
}

impl IncrementalFailureTelemetry {
    fn zero_work() -> Self {
        Self {
            comparability: store::mailbox::comparability_for_incremental_workload(0, 0),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            messages_deleted: 0,
            labels_synced: 0,
        }
    }
}

impl FullSyncFailureTelemetry {
    fn zero_work() -> Self {
        Self {
            resumed_from_checkpoint: false,
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 0,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
            pipeline_report: disabled_pipeline_report(),
        }
    }
}

#[derive(Debug)]
struct PreparedFullSyncPage {
    page_seq: usize,
    chunk_seq: usize,
    listed_count: usize,
    next_page_token: Option<String>,
    cursor_history_id: Option<String>,
    upserts: Vec<store::mailbox::GmailMessageUpsertInput>,
    page_complete: bool,
    page_upserted_total: usize,
}

#[derive(Debug)]
struct PreparedIncrementalBatch {
    batch_seq: usize,
    listed_count: usize,
    upserts: Vec<store::mailbox::GmailMessageUpsertInput>,
    message_ids_to_delete: Vec<String>,
}

#[derive(Debug)]
struct RestartFullSyncFromScratch;

impl std::fmt::Display for RestartFullSyncFromScratch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("resume page token invalid; restart full sync from scratch")
    }
}

impl std::error::Error for RestartFullSyncFromScratch {}

fn disabled_pipeline_report() -> PipelineStatsReport {
    PipelineStatsReport {
        pipeline_enabled: false,
        list_queue_high_water: 0,
        write_queue_high_water: 0,
        write_batch_count: 0,
        writer_wait_ms: 0,
        fetch_batch_count: 0,
        fetch_batch_avg_ms: 0,
        fetch_batch_max_ms: 0,
        writer_tx_count: 0,
        writer_tx_avg_ms: 0,
        writer_tx_max_ms: 0,
        reorder_buffer_high_water: 0,
        staged_message_count: 0,
        staged_delete_count: 0,
        staged_attachment_count: 0,
    }
}

enum MailboxWriterCommand {
    ResetFullSyncProgress {
        reply: oneshot::Sender<Result<()>>,
    },
    PrepareFullSyncCheckpoint {
        labels: Vec<GmailLabel>,
        update: store::mailbox::FullSyncCheckpointUpdate,
        reply: oneshot::Sender<Result<store::mailbox::FullSyncCheckpointRecord>>,
    },
    UpdateFullSyncCheckpointLabels {
        labels: Vec<GmailLabel>,
        update: store::mailbox::FullSyncCheckpointUpdate,
        reply: oneshot::Sender<Result<store::mailbox::FullSyncCheckpointRecord>>,
    },
    StageFullSyncPageChunkAndMaybeUpdateCheckpoint {
        input: store::mailbox::FullSyncStagePageInput,
        messages: Vec<store::mailbox::GmailMessageUpsertInput>,
        checkpoint_update: Option<store::mailbox::FullSyncCheckpointUpdate>,
        reply: oneshot::Sender<Result<store::mailbox::FullSyncCheckpointRecord>>,
    },
    FinalizeFullSyncFromStage {
        updated_at_epoch_s: i64,
        sync_state_update: store::mailbox::SyncStateUpdate,
        reply: oneshot::Sender<Result<store::mailbox::SyncStateRecord>>,
    },
    ResetIncrementalSyncStage {
        reply: oneshot::Sender<Result<()>>,
    },
    StageIncrementalSyncBatch {
        messages: Vec<store::mailbox::GmailMessageUpsertInput>,
        message_ids_to_delete: Vec<String>,
        reply: oneshot::Sender<Result<()>>,
    },
    FinalizeIncrementalFromStage {
        labels: Vec<GmailLabel>,
        updated_at_epoch_s: i64,
        sync_state_update: store::mailbox::SyncStateUpdate,
        reply: oneshot::Sender<Result<(store::mailbox::SyncStateRecord, usize)>>,
    },
}

struct MailboxWriterWorker {
    sender: Option<std_mpsc::Sender<MailboxWriterCommand>>,
    handle: tokio::task::JoinHandle<Result<()>>,
}
