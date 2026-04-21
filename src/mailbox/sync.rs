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
    let mut incremental_failure_telemetry = IncrementalFailureTelemetry::zero_work();
    let mut failure_pipeline_report = disabled_pipeline_report();
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
                    &mut failure_pipeline_report,
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
                    &mut failure_pipeline_report,
                )
                .await
                {
                    Ok(report) => Ok(report),
                    Err(error) if is_stale_history_error(&error) => {
                        failure_mode = store::mailbox::SyncMode::Full;
                        failure_cursor_history_id = None;
                        failure_bootstrap_query = persisted_bootstrap_query;
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
                            &mut failure_pipeline_report,
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
        (Ok(_), Err(error)) => Err(error),
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
                        pages_fetched: incremental_failure_telemetry.pages_fetched,
                        messages_listed: incremental_failure_telemetry.messages_listed,
                        messages_deleted: incremental_failure_telemetry.messages_deleted,
                        pipeline_report: failure_pipeline_report,
                        pacing_report,
                        metrics,
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
    store::init(config_report)?;

    let account_id = resolve_sync_history_account_id(config_report)?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id_for_summary = account_id.clone();
    let account_id_for_history = account_id.clone();

    let (summary, runs) = spawn_blocking(move || {
        let summary = store::mailbox::get_latest_sync_run_summary_for_account(
            &database_path,
            busy_timeout_ms,
            &account_id_for_summary,
        )?;
        let runs = store::mailbox::list_sync_run_history(
            &database_path,
            busy_timeout_ms,
            &account_id_for_history,
            limit,
        )?;
        Ok::<_, anyhow::Error>((summary, runs))
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
    store::init(config_report)?;

    let account_id = resolve_sync_history_account_id(config_report)?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id_for_summary = account_id.clone();
    let account_id_for_history = account_id.clone();

    let (summary, runs, baseline_run) = spawn_blocking(move || {
        let summary = store::mailbox::get_latest_sync_run_summary_for_account(
            &database_path,
            busy_timeout_ms,
            &account_id_for_summary,
        )?;
        let runs = store::mailbox::list_sync_run_history(
            &database_path,
            busy_timeout_ms,
            &account_id_for_history,
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
        Ok::<_, anyhow::Error>((summary, runs, baseline_run))
    })
    .await??;

    let latest_run = runs.first().cloned();
    let comparable_to_baseline = latest_run
        .as_ref()
        .zip(baseline_run.as_ref())
        .map(|(latest, baseline)| {
            latest.comparability_kind == baseline.comparability_kind
                && latest.comparability_key == baseline.comparability_key
        })
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
    messages_deleted: usize,
}

impl IncrementalFailureTelemetry {
    fn zero_work() -> Self {
        Self {
            comparability: store::mailbox::comparability_for_incremental_workload(0, 0),
            pages_fetched: 0,
            messages_listed: 0,
            messages_deleted: 0,
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

impl MailboxWriterWorker {
    async fn start(store_handle: &MailboxStoreHandle) -> Result<Self> {
        let (sender, receiver) = std_mpsc::channel();
        let database_path = store_handle.database_path.clone();
        let busy_timeout_ms = store_handle.busy_timeout_ms;
        let account_id = store_handle.account_id.clone();
        let (ready_tx, ready_rx) = oneshot::channel();

        let handle = spawn_blocking(move || {
            let mut writer = match store::mailbox::MailboxWriterConnection::open(
                &database_path,
                busy_timeout_ms,
                &account_id,
            ) {
                Ok(writer) => {
                    let _ = ready_tx.send(Ok(()));
                    writer
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return Ok(());
                }
            };

            while let Ok(command) = receiver.recv() {
                match command {
                    MailboxWriterCommand::ResetFullSyncProgress { reply } => {
                        let _ = reply.send(writer.reset_full_sync_progress());
                    }
                    MailboxWriterCommand::PrepareFullSyncCheckpoint {
                        labels,
                        update,
                        reply,
                    } => {
                        let _ = reply.send(writer.prepare_full_sync_checkpoint(&labels, &update));
                    }
                    MailboxWriterCommand::UpdateFullSyncCheckpointLabels {
                        labels,
                        update,
                        reply,
                    } => {
                        let _ =
                            reply.send(writer.update_full_sync_checkpoint_labels(&labels, &update));
                    }
                    MailboxWriterCommand::StageFullSyncPageChunkAndMaybeUpdateCheckpoint {
                        input,
                        messages,
                        checkpoint_update,
                        reply,
                    } => {
                        let _ = reply.send(
                            writer.stage_full_sync_page_chunk_and_maybe_update_checkpoint(
                                &input,
                                &messages,
                                checkpoint_update.as_ref(),
                            ),
                        );
                    }
                    MailboxWriterCommand::FinalizeFullSyncFromStage {
                        updated_at_epoch_s,
                        sync_state_update,
                        reply,
                    } => {
                        let _ =
                            reply.send(writer.finalize_full_sync_from_stage(
                                updated_at_epoch_s,
                                &sync_state_update,
                            ));
                    }
                    MailboxWriterCommand::ResetIncrementalSyncStage { reply } => {
                        let _ = reply.send(writer.reset_incremental_sync_stage());
                    }
                    MailboxWriterCommand::StageIncrementalSyncBatch {
                        messages,
                        message_ids_to_delete,
                        reply,
                    } => {
                        let _ = reply.send(
                            writer.stage_incremental_sync_batch(&messages, &message_ids_to_delete),
                        );
                    }
                    MailboxWriterCommand::FinalizeIncrementalFromStage {
                        labels,
                        updated_at_epoch_s,
                        sync_state_update,
                        reply,
                    } => {
                        let _ = reply.send(writer.finalize_incremental_from_stage(
                            &labels,
                            updated_at_epoch_s,
                            &sync_state_update,
                        ));
                    }
                }
            }

            Ok(())
        });

        ready_rx
            .await
            .context("mailbox writer worker dropped before initialization")??;

        Ok(Self {
            sender: Some(sender),
            handle,
        })
    }

    async fn shutdown(mut self) -> Result<()> {
        self.sender.take();
        self.handle.await?
    }

    async fn reset_full_sync_progress(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::ResetFullSyncProgress { reply: reply_tx })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer reset-full-sync-progress reply dropped")?
    }

    async fn prepare_full_sync_checkpoint(
        &self,
        labels: &[GmailLabel],
        update: store::mailbox::FullSyncCheckpointUpdate,
    ) -> Result<store::mailbox::FullSyncCheckpointRecord> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::PrepareFullSyncCheckpoint {
                labels: labels.to_vec(),
                update,
                reply: reply_tx,
            })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer prepare-full-sync-checkpoint reply dropped")?
    }

    async fn update_full_sync_checkpoint_labels(
        &self,
        labels: &[GmailLabel],
        update: store::mailbox::FullSyncCheckpointUpdate,
    ) -> Result<store::mailbox::FullSyncCheckpointRecord> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::UpdateFullSyncCheckpointLabels {
                labels: labels.to_vec(),
                update,
                reply: reply_tx,
            })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer update-full-sync-checkpoint-labels reply dropped")?
    }

    async fn finalize_full_sync_from_stage(
        &self,
        updated_at_epoch_s: i64,
        sync_state_update: store::mailbox::SyncStateUpdate,
    ) -> Result<store::mailbox::SyncStateRecord> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::FinalizeFullSyncFromStage {
                updated_at_epoch_s,
                sync_state_update,
                reply: reply_tx,
            })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer finalize-full-sync reply dropped")?
    }

    async fn stage_full_sync_page_chunk_and_maybe_update_checkpoint(
        &self,
        input: store::mailbox::FullSyncStagePageInput,
        messages: &[store::mailbox::GmailMessageUpsertInput],
        checkpoint_update: Option<store::mailbox::FullSyncCheckpointUpdate>,
    ) -> Result<store::mailbox::FullSyncCheckpointRecord> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(
                MailboxWriterCommand::StageFullSyncPageChunkAndMaybeUpdateCheckpoint {
                    input,
                    messages: messages.to_vec(),
                    checkpoint_update,
                    reply: reply_tx,
                },
            )
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer stage-full-sync-page-chunk reply dropped")?
    }

    async fn reset_incremental_sync_stage(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::ResetIncrementalSyncStage { reply: reply_tx })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer reset-incremental-sync-stage reply dropped")?
    }

    async fn stage_incremental_sync_batch(
        &self,
        messages: &[store::mailbox::GmailMessageUpsertInput],
        message_ids_to_delete: &[String],
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::StageIncrementalSyncBatch {
                messages: messages.to_vec(),
                message_ids_to_delete: message_ids_to_delete.to_vec(),
                reply: reply_tx,
            })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer stage-incremental-sync-batch reply dropped")?
    }

    async fn finalize_incremental_from_stage(
        &self,
        labels: &[GmailLabel],
        updated_at_epoch_s: i64,
        sync_state_update: store::mailbox::SyncStateUpdate,
    ) -> Result<(store::mailbox::SyncStateRecord, usize)> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::FinalizeIncrementalFromStage {
                labels: labels.to_vec(),
                updated_at_epoch_s,
                sync_state_update,
                reply: reply_tx,
            })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer finalize-incremental reply dropped")?
    }

    fn sender(&self) -> Result<&std_mpsc::Sender<MailboxWriterCommand>> {
        self.sender
            .as_ref()
            .ok_or_else(|| anyhow!("mailbox writer worker has already shut down"))
    }
}

fn record_pipeline_failure(
    failure_pipeline_report: &mut PipelineStatsReport,
    stats: &PipelineStats,
) {
    *failure_pipeline_report = stats.report();
}

async fn run_full_sync(
    context: &SyncExecutionContext<'_>,
    pacing: &mut AdaptiveSyncPacing,
    bootstrap_query: &str,
    fallback_from_history: bool,
    failure_pipeline_report: &mut PipelineStatsReport,
) -> Result<SyncRunReport> {
    let mut checkpoint =
        initialize_full_sync_checkpoint(context, bootstrap_query, fallback_from_history).await?;
    let checkpoint_reused_pages =
        usize::try_from(checkpoint.record.pages_fetched).unwrap_or(usize::MAX);
    let checkpoint_reused_messages_upserted =
        usize::try_from(checkpoint.record.messages_upserted).unwrap_or(usize::MAX);

    if checkpoint.record.status == store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize {
        return finalize_full_sync_report(
            context,
            FullSyncFinalizeRequest {
                bootstrap_query,
                fallback_from_history,
                resumed_from_checkpoint: checkpoint.resumed_from_checkpoint,
                startup_seed_run_id: pacing.startup_seed_run_id(),
                checkpoint: checkpoint.record,
                checkpoint_reused_pages,
                checkpoint_reused_messages_upserted,
                pipeline_report: disabled_pipeline_report(),
            },
        )
        .await
        .inspect_err(|_| {
            *failure_pipeline_report = disabled_pipeline_report();
        });
    }
    let stats = PipelineStats::default();
    let fetch_concurrency = Arc::new(AtomicUsize::new(pacing.current_message_fetch_concurrency()));
    let (list_tx, list_rx) = mpsc::channel(PIPELINE_LIST_QUEUE_CAPACITY);
    let (write_tx, mut write_rx) = mpsc::channel(PIPELINE_WRITE_QUEUE_CAPACITY);

    let gmail_client = context.gmail_client.clone();
    let bootstrap_query_owned = bootstrap_query.to_owned();
    let resumed_from_checkpoint = checkpoint.resumed_from_checkpoint;
    let initial_page_token = checkpoint.record.next_page_token.clone();
    let lister_stats = stats.clone();
    let lister_handle = tokio::spawn(async move {
        run_full_sync_lister(
            gmail_client,
            bootstrap_query_owned,
            initial_page_token,
            resumed_from_checkpoint,
            checkpoint_reused_pages,
            list_tx,
            lister_stats,
        )
        .await
    });

    let processor_stats = stats.clone();
    let processor_fetch_concurrency = fetch_concurrency.clone();
    let processor_account_id = context.account.account_id.clone();
    let processor_label_names_by_id = Arc::new(context.label_names_by_id.clone());
    let processor_gmail_client = context.gmail_client.clone();
    let processor_handle = tokio::spawn(async move {
        run_full_sync_processor(
            processor_gmail_client,
            processor_account_id,
            processor_label_names_by_id,
            list_rx,
            write_tx,
            processor_stats,
            processor_fetch_concurrency,
        )
        .await
    });

    let mut next_page_seq = checkpoint_reused_pages;
    let mut next_chunk_seq = 0usize;
    let mut buffered_pages: BTreeMap<usize, BTreeMap<usize, PreparedFullSyncPage>> =
        BTreeMap::new();
    let labels_synced = i64::try_from(context.labels.len()).unwrap_or(i64::MAX);
    loop {
        let wait_started = Instant::now();
        let maybe_page = write_rx.recv().await;
        stats.record_writer_wait(wait_started.elapsed());

        let Some(page) = maybe_page else {
            break;
        };
        stats.on_write_dequeued();
        buffered_pages
            .entry(page.page_seq)
            .or_default()
            .insert(page.chunk_seq, page);
        stats.observe_reorder_buffer_depth(
            buffered_pages.values().map(BTreeMap::len).sum::<usize>(),
        );

        while let Some(page_chunks) = buffered_pages.get_mut(&next_page_seq) {
            let Some(page) = page_chunks.remove(&next_chunk_seq) else {
                break;
            };
            let page_complete = page.page_complete;
            let staged_message_count = page.upserts.len();
            let staged_attachment_count = page
                .upserts
                .iter()
                .map(|message| message.attachments.len())
                .sum::<usize>();
            let write_started = Instant::now();
            checkpoint.record = stage_prepared_full_sync_page_chunk(
                context,
                &checkpoint.record,
                bootstrap_query,
                labels_synced,
                page,
            )
            .await
            .inspect_err(|_| {
                record_pipeline_failure(failure_pipeline_report, &stats);
            })?;
            stats.record_writer_transaction(write_started.elapsed());
            stats.record_staged_messages(staged_message_count);
            stats.record_staged_attachments(staged_attachment_count);
            stats.on_write_batch_committed();
            observe_latest_metrics(pacing, context.gmail_client).inspect_err(|_| {
                record_pipeline_failure(failure_pipeline_report, &stats);
            })?;
            fetch_concurrency.store(
                pacing.current_message_fetch_concurrency(),
                Ordering::Release,
            );

            if page_complete {
                next_page_seq += 1;
                next_chunk_seq = 0;
                buffered_pages.remove(&(next_page_seq - 1));
            } else {
                next_chunk_seq += 1;
                if page_chunks.is_empty() {
                    buffered_pages.remove(&next_page_seq);
                }
            }
            stats.observe_reorder_buffer_depth(
                buffered_pages.values().map(BTreeMap::len).sum::<usize>(),
            );
        }
    }

    let lister_result = lister_handle
        .await
        .context("full sync lister task failed")
        .inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;
    let processor_result = processor_handle
        .await
        .context("full sync processor task failed")
        .inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;

    if let Err(error) = lister_result {
        record_pipeline_failure(failure_pipeline_report, &stats);
        if error.downcast_ref::<RestartFullSyncFromScratch>().is_some() {
            context.writer.reset_full_sync_progress().await?;
            return Box::pin(run_full_sync(
                context,
                pacing,
                bootstrap_query,
                fallback_from_history,
                failure_pipeline_report,
            ))
            .await;
        }
        return Err(error);
    }
    processor_result.inspect_err(|_| {
        record_pipeline_failure(failure_pipeline_report, &stats);
    })?;

    if !buffered_pages.is_empty() {
        record_pipeline_failure(failure_pipeline_report, &stats);
        return Err(anyhow!(
            "full sync pipeline terminated with {} buffered pages still waiting to commit",
            buffered_pages.len()
        ));
    }
    if checkpoint.record.status != store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize {
        record_pipeline_failure(failure_pipeline_report, &stats);
        return Err(anyhow!(
            "full sync pipeline drained before reaching a finalize-ready checkpoint"
        ));
    }

    finalize_full_sync_report(
        context,
        FullSyncFinalizeRequest {
            bootstrap_query,
            fallback_from_history,
            resumed_from_checkpoint: checkpoint.resumed_from_checkpoint,
            startup_seed_run_id: pacing.startup_seed_run_id(),
            checkpoint: checkpoint.record,
            checkpoint_reused_pages,
            checkpoint_reused_messages_upserted,
            pipeline_report: stats.report(),
        },
    )
    .await
    .inspect_err(|_| {
        record_pipeline_failure(failure_pipeline_report, &stats);
    })
}

async fn run_incremental_sync(
    context: &SyncExecutionContext<'_>,
    pacing: &mut AdaptiveSyncPacing,
    bootstrap_query: &str,
    cursor_history_id: Option<String>,
    failure_telemetry: &mut IncrementalFailureTelemetry,
    failure_pipeline_report: &mut PipelineStatsReport,
) -> Result<SyncRunReport> {
    let cursor_history_id =
        cursor_history_id.ok_or_else(|| anyhow!("incremental sync requires a history cursor"))?;
    let mut page_token = None;
    let mut pages_fetched = 0usize;
    let mut changed_message_ids = BTreeSet::new();
    let mut deleted_message_ids = BTreeSet::new();

    let latest_history_id = loop {
        let page = context
            .gmail_client
            .list_history(&cursor_history_id, page_token.as_deref())
            .await?;
        pages_fetched += 1;
        let page_history_id = page.history_id;

        for changed in page.changed_message_ids {
            changed_message_ids.insert(changed);
        }
        for deleted in page.deleted_message_ids {
            deleted_message_ids.insert(deleted);
        }

        page_token = page.next_page_token;
        if page_token.is_none() {
            break page_history_id;
        }
    };
    observe_latest_metrics(pacing, context.gmail_client)?;

    for deleted_message_id in &deleted_message_ids {
        changed_message_ids.remove(deleted_message_id);
    }

    let changed_message_ids: Vec<String> = changed_message_ids.into_iter().collect();
    let deleted_message_ids: Vec<String> = deleted_message_ids.into_iter().collect();
    let comparability = store::mailbox::comparability_for_incremental_workload(
        i64::try_from(changed_message_ids.len()).unwrap_or(i64::MAX),
        i64::try_from(deleted_message_ids.len()).unwrap_or(i64::MAX),
    );
    *failure_telemetry = IncrementalFailureTelemetry {
        comparability: comparability.clone(),
        pages_fetched,
        messages_listed: changed_message_ids.len(),
        messages_deleted: deleted_message_ids.len(),
    };
    maybe_seed_pacing_from_history(
        context.store_handle,
        pacing,
        context.gmail_client,
        store::mailbox::SyncMode::Incremental,
        &comparability,
    )
    .await?;
    if changed_message_ids.is_empty() && deleted_message_ids.is_empty() {
        let now_epoch_s = current_epoch_seconds()?;
        let finalize_input = FinalizeSyncInput {
            mode: store::mailbox::SyncMode::Incremental,
            comparability_kind: comparability.kind,
            comparability_key: comparability.key.clone(),
            comparability_label: comparability.label.clone(),
            startup_seed_run_id: pacing.startup_seed_run_id(),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            cursor_history_id: Some(latest_history_id),
            pages_fetched,
            messages_listed: 0,
            messages_upserted: 0,
            messages_deleted: 0,
            labels_synced: context.labels.len(),
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
            pipeline_enabled: false,
            pipeline_list_queue_high_water: 0,
            pipeline_write_queue_high_water: 0,
            pipeline_write_batch_count: 0,
            pipeline_writer_wait_ms: 0,
            pipeline_fetch_batch_count: 0,
            pipeline_fetch_batch_avg_ms: 0,
            pipeline_fetch_batch_max_ms: 0,
            pipeline_writer_tx_count: 0,
            pipeline_writer_tx_avg_ms: 0,
            pipeline_writer_tx_max_ms: 0,
            pipeline_reorder_buffer_high_water: 0,
            pipeline_staged_message_count: 0,
            pipeline_staged_delete_count: 0,
            pipeline_staged_attachment_count: 0,
        };
        let sync_state = context
            .store_handle
            .upsert_sync_state(success_sync_state_update(
                context.account,
                bootstrap_query,
                &finalize_input,
                now_epoch_s,
            ))
            .await?;
        return finalize_sync(sync_state, bootstrap_query, finalize_input);
    }

    let stats = PipelineStats::default();
    context
        .writer
        .reset_incremental_sync_stage()
        .await
        .inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;
    if !deleted_message_ids.is_empty() {
        let write_started = Instant::now();
        context
            .writer
            .stage_incremental_sync_batch(&[], &deleted_message_ids)
            .await
            .inspect_err(|_| {
                record_pipeline_failure(failure_pipeline_report, &stats);
            })?;
        stats.record_writer_transaction(write_started.elapsed());
        stats.record_staged_deletes(deleted_message_ids.len());
    }

    let fetch_concurrency = Arc::new(AtomicUsize::new(pacing.current_message_fetch_concurrency()));
    let (list_tx, list_rx) = mpsc::channel(PIPELINE_LIST_QUEUE_CAPACITY);
    let (write_tx, mut write_rx) = mpsc::channel(PIPELINE_WRITE_QUEUE_CAPACITY);

    let lister_stats = stats.clone();
    let lister_handle = tokio::spawn(async move {
        run_incremental_batch_lister(changed_message_ids, list_tx, lister_stats).await
    });

    let processor_stats = stats.clone();
    let processor_fetch_concurrency = fetch_concurrency.clone();
    let processor_account_id = context.account.account_id.clone();
    let processor_label_names_by_id = Arc::new(context.label_names_by_id.clone());
    let processor_gmail_client = context.gmail_client.clone();
    let processor_handle = tokio::spawn(async move {
        run_incremental_sync_processor(
            processor_gmail_client,
            processor_account_id,
            processor_label_names_by_id,
            list_rx,
            write_tx,
            processor_stats,
            processor_fetch_concurrency,
        )
        .await
    });

    let mut next_batch_seq = 0usize;
    let mut buffered_batches = BTreeMap::new();
    let mut messages_listed = 0usize;
    let mut messages_upserted = 0usize;

    loop {
        let wait_started = Instant::now();
        let maybe_batch = write_rx.recv().await;
        stats.record_writer_wait(wait_started.elapsed());

        let Some(batch) = maybe_batch else {
            break;
        };
        stats.on_write_dequeued();
        buffered_batches.insert(batch.batch_seq, batch);
        stats.observe_reorder_buffer_depth(buffered_batches.len());

        while let Some(batch) = buffered_batches.remove(&next_batch_seq) {
            messages_listed += batch.listed_count;
            messages_upserted += batch.upserts.len();
            let staged_attachment_count = batch
                .upserts
                .iter()
                .map(|message| message.attachments.len())
                .sum::<usize>();
            let write_started = Instant::now();
            context
                .writer
                .stage_incremental_sync_batch(&batch.upserts, &batch.message_ids_to_delete)
                .await
                .inspect_err(|_| {
                    record_pipeline_failure(failure_pipeline_report, &stats);
                })?;
            stats.record_writer_transaction(write_started.elapsed());
            stats.record_staged_messages(batch.upserts.len());
            stats.record_staged_deletes(batch.message_ids_to_delete.len());
            stats.record_staged_attachments(staged_attachment_count);
            next_batch_seq += 1;
            stats.on_write_batch_committed();
            observe_latest_metrics(pacing, context.gmail_client).inspect_err(|_| {
                record_pipeline_failure(failure_pipeline_report, &stats);
            })?;
            fetch_concurrency.store(
                pacing.current_message_fetch_concurrency(),
                Ordering::Release,
            );
            stats.observe_reorder_buffer_depth(buffered_batches.len());
        }
    }

    let lister_result = lister_handle
        .await
        .context("incremental sync lister task failed")
        .inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;
    lister_result.inspect_err(|_| {
        record_pipeline_failure(failure_pipeline_report, &stats);
    })?;

    let processor_result = processor_handle
        .await
        .context("incremental sync processor task failed")
        .inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;
    processor_result.inspect_err(|_| {
        record_pipeline_failure(failure_pipeline_report, &stats);
    })?;

    if !buffered_batches.is_empty() {
        record_pipeline_failure(failure_pipeline_report, &stats);
        return Err(anyhow!(
            "incremental sync pipeline terminated with {} buffered batches still waiting to commit",
            buffered_batches.len()
        ));
    }

    let now_epoch_s = current_epoch_seconds()?;
    let pipeline_report = stats.report();
    let sync_update = success_sync_state_update_with_pipeline(
        context.account,
        bootstrap_query,
        &FinalizeSyncInput {
            mode: store::mailbox::SyncMode::Incremental,
            comparability_kind: comparability.kind,
            comparability_key: comparability.key.clone(),
            comparability_label: comparability.label.clone(),
            startup_seed_run_id: pacing.startup_seed_run_id(),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            cursor_history_id: Some(latest_history_id.clone()),
            pages_fetched,
            messages_listed,
            messages_upserted,
            messages_deleted: 0,
            labels_synced: context.labels.len(),
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
            pipeline_enabled: pipeline_report.pipeline_enabled,
            pipeline_list_queue_high_water: pipeline_report.list_queue_high_water,
            pipeline_write_queue_high_water: pipeline_report.write_queue_high_water,
            pipeline_write_batch_count: pipeline_report.write_batch_count,
            pipeline_writer_wait_ms: pipeline_report.writer_wait_ms,
            pipeline_fetch_batch_count: pipeline_report.fetch_batch_count,
            pipeline_fetch_batch_avg_ms: pipeline_report.fetch_batch_avg_ms,
            pipeline_fetch_batch_max_ms: pipeline_report.fetch_batch_max_ms,
            pipeline_writer_tx_count: pipeline_report.writer_tx_count,
            pipeline_writer_tx_avg_ms: pipeline_report.writer_tx_avg_ms,
            pipeline_writer_tx_max_ms: pipeline_report.writer_tx_max_ms,
            pipeline_reorder_buffer_high_water: pipeline_report.reorder_buffer_high_water,
            pipeline_staged_message_count: pipeline_report.staged_message_count,
            pipeline_staged_delete_count: pipeline_report.staged_delete_count,
            pipeline_staged_attachment_count: pipeline_report.staged_attachment_count,
        },
        now_epoch_s,
    );
    let (sync_state, messages_deleted) = context
        .writer
        .finalize_incremental_from_stage(context.labels, now_epoch_s, sync_update)
        .await
        .inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;
    let finalize_input = FinalizeSyncInput {
        mode: store::mailbox::SyncMode::Incremental,
        comparability_kind: comparability.kind,
        comparability_key: comparability.key,
        comparability_label: comparability.label,
        startup_seed_run_id: pacing.startup_seed_run_id(),
        fallback_from_history: false,
        resumed_from_checkpoint: false,
        cursor_history_id: Some(latest_history_id),
        pages_fetched,
        messages_listed,
        messages_upserted,
        messages_deleted,
        labels_synced: context.labels.len(),
        checkpoint_reused_pages: 0,
        checkpoint_reused_messages_upserted: 0,
        pipeline_enabled: pipeline_report.pipeline_enabled,
        pipeline_list_queue_high_water: pipeline_report.list_queue_high_water,
        pipeline_write_queue_high_water: pipeline_report.write_queue_high_water,
        pipeline_write_batch_count: pipeline_report.write_batch_count,
        pipeline_writer_wait_ms: pipeline_report.writer_wait_ms,
        pipeline_fetch_batch_count: pipeline_report.fetch_batch_count,
        pipeline_fetch_batch_avg_ms: pipeline_report.fetch_batch_avg_ms,
        pipeline_fetch_batch_max_ms: pipeline_report.fetch_batch_max_ms,
        pipeline_writer_tx_count: pipeline_report.writer_tx_count,
        pipeline_writer_tx_avg_ms: pipeline_report.writer_tx_avg_ms,
        pipeline_writer_tx_max_ms: pipeline_report.writer_tx_max_ms,
        pipeline_reorder_buffer_high_water: pipeline_report.reorder_buffer_high_water,
        pipeline_staged_message_count: pipeline_report.staged_message_count,
        pipeline_staged_delete_count: pipeline_report.staged_delete_count,
        pipeline_staged_attachment_count: pipeline_report.staged_attachment_count,
    };

    finalize_sync(sync_state, bootstrap_query, finalize_input)
}

fn success_sync_state_update(
    account: &AccountRecord,
    bootstrap_query: &str,
    input: &FinalizeSyncInput,
    now_epoch_s: i64,
) -> store::mailbox::SyncStateUpdate {
    success_sync_state_update_with_pipeline(account, bootstrap_query, input, now_epoch_s)
}

fn success_sync_state_update_with_pipeline(
    account: &AccountRecord,
    bootstrap_query: &str,
    input: &FinalizeSyncInput,
    now_epoch_s: i64,
) -> store::mailbox::SyncStateUpdate {
    store::mailbox::SyncStateUpdate {
        account_id: account.account_id.clone(),
        cursor_history_id: input.cursor_history_id.clone(),
        bootstrap_query: bootstrap_query.to_owned(),
        last_sync_mode: input.mode,
        last_sync_status: store::mailbox::SyncStatus::Ok,
        last_error: None,
        last_sync_epoch_s: now_epoch_s,
        last_full_sync_success_epoch_s: (input.mode == store::mailbox::SyncMode::Full)
            .then_some(now_epoch_s),
        last_incremental_sync_success_epoch_s: (input.mode
            == store::mailbox::SyncMode::Incremental)
            .then_some(now_epoch_s),
        pipeline_enabled: input.pipeline_enabled,
        pipeline_list_queue_high_water: i64::try_from(input.pipeline_list_queue_high_water)
            .unwrap_or(i64::MAX),
        pipeline_write_queue_high_water: i64::try_from(input.pipeline_write_queue_high_water)
            .unwrap_or(i64::MAX),
        pipeline_write_batch_count: i64::try_from(input.pipeline_write_batch_count)
            .unwrap_or(i64::MAX),
        pipeline_writer_wait_ms: i64::try_from(input.pipeline_writer_wait_ms).unwrap_or(i64::MAX),
        pipeline_fetch_batch_count: i64::try_from(input.pipeline_fetch_batch_count)
            .unwrap_or(i64::MAX),
        pipeline_fetch_batch_avg_ms: i64::try_from(input.pipeline_fetch_batch_avg_ms)
            .unwrap_or(i64::MAX),
        pipeline_fetch_batch_max_ms: i64::try_from(input.pipeline_fetch_batch_max_ms)
            .unwrap_or(i64::MAX),
        pipeline_writer_tx_count: i64::try_from(input.pipeline_writer_tx_count).unwrap_or(i64::MAX),
        pipeline_writer_tx_avg_ms: i64::try_from(input.pipeline_writer_tx_avg_ms)
            .unwrap_or(i64::MAX),
        pipeline_writer_tx_max_ms: i64::try_from(input.pipeline_writer_tx_max_ms)
            .unwrap_or(i64::MAX),
        pipeline_reorder_buffer_high_water: i64::try_from(input.pipeline_reorder_buffer_high_water)
            .unwrap_or(i64::MAX),
        pipeline_staged_message_count: i64::try_from(input.pipeline_staged_message_count)
            .unwrap_or(i64::MAX),
        pipeline_staged_delete_count: i64::try_from(input.pipeline_staged_delete_count)
            .unwrap_or(i64::MAX),
        pipeline_staged_attachment_count: i64::try_from(input.pipeline_staged_attachment_count)
            .unwrap_or(i64::MAX),
    }
}

fn observe_latest_metrics(
    pacing: &mut AdaptiveSyncPacing,
    gmail_client: &GmailClient,
) -> Result<()> {
    if let Some(snapshot) = gmail_client.request_metrics_snapshot() {
        pacing.observe_metrics_snapshot(snapshot, Some(gmail_client))?;
    }

    Ok(())
}

async fn persist_sync_pacing_failure(
    store_handle: &MailboxStoreHandle,
    account: &AccountRecord,
    pacing: &mut AdaptiveSyncPacing,
    gmail_client: &GmailClient,
) -> Result<()> {
    observe_latest_metrics(pacing, gmail_client)?;
    let now_epoch_s = current_epoch_seconds()?;
    let _ = store_handle
        .upsert_sync_pacing_state(pacing.finalize_failure(&account.account_id, now_epoch_s))
        .await?;
    Ok(())
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

async fn load_sync_state(
    store_handle: &MailboxStoreHandle,
) -> Result<Option<store::mailbox::SyncStateRecord>> {
    store_handle.load_sync_state().await
}

async fn persist_sync_state_failure(
    store_handle: &MailboxStoreHandle,
    run_context: &SyncRunContext,
    account: &AccountRecord,
    failure: FailedSyncTelemetryContext<'_>,
) -> Result<()> {
    let finished_at_epoch_s = current_epoch_seconds()?;
    let sync_state_update = store::mailbox::SyncStateUpdate {
        account_id: account.account_id.clone(),
        cursor_history_id: failure.cursor_history_id.clone(),
        bootstrap_query: failure.bootstrap_query.to_owned(),
        last_sync_mode: failure.mode,
        last_sync_status: store::mailbox::SyncStatus::Failed,
        last_error: Some(failure.error_message.clone()),
        last_sync_epoch_s: finished_at_epoch_s,
        last_full_sync_success_epoch_s: None,
        last_incremental_sync_success_epoch_s: None,
        pipeline_enabled: failure.pipeline_report.pipeline_enabled,
        pipeline_list_queue_high_water: usize_to_i64(failure.pipeline_report.list_queue_high_water),
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
        pipeline_staged_message_count: usize_to_i64(failure.pipeline_report.staged_message_count),
        pipeline_staged_delete_count: usize_to_i64(failure.pipeline_report.staged_delete_count),
        pipeline_staged_attachment_count: usize_to_i64(
            failure.pipeline_report.staged_attachment_count,
        ),
    };
    let _ = store_handle
        .persist_failed_sync_outcome(
            &sync_state_update,
            &run_context.failure_outcome_input(finished_at_epoch_s, &failure),
        )
        .await?;
    Ok(())
}

fn resolve_sync_history_account_id(config_report: &ConfigReport) -> Result<String> {
    if let Some(active_account) = store::accounts::get_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )? {
        return Ok(active_account.account_id);
    }

    if let Some(mailbox) = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )? && let Some(sync_state) = mailbox.sync_state
    {
        return Ok(sync_state.account_id);
    }

    Err(anyhow!(
        "no active Gmail account found; run `mailroom auth login` first"
    ))
}

fn failure_comparability(
    mode: store::mailbox::SyncMode,
    bootstrap_query: &str,
) -> store::mailbox::SyncRunComparability {
    match mode {
        store::mailbox::SyncMode::Full => {
            store::mailbox::comparability_for_full_bootstrap_query(bootstrap_query)
        }
        store::mailbox::SyncMode::Incremental => {
            store::mailbox::comparability_for_incremental_workload(0, 0)
        }
    }
}

async fn maybe_seed_pacing_from_history(
    store_handle: &MailboxStoreHandle,
    pacing: &mut AdaptiveSyncPacing,
    gmail_client: &GmailClient,
    sync_mode: store::mailbox::SyncMode,
    comparability: &store::mailbox::SyncRunComparability,
) -> Result<()> {
    let Some(summary) = store_handle
        .load_sync_run_summary_for_comparability(sync_mode, &comparability.key)
        .await?
    else {
        return Ok(());
    };
    let Some(run_id) = summary.best_clean_run_id else {
        return Ok(());
    };
    let Some(baseline_run) = store_handle.load_sync_run_history_record(run_id).await? else {
        return Ok(());
    };
    if baseline_run.comparability_kind != comparability.kind
        || baseline_run.comparability_key != comparability.key
    {
        return Ok(());
    }
    let _ = pacing.apply_startup_seed(
        AdaptiveSyncPacingSeed {
            run_id: baseline_run.run_id,
            quota_units_per_minute: u32::try_from(
                baseline_run.effective_quota_units_per_minute.max(0),
            )
            .unwrap_or(u32::MAX),
            message_fetch_concurrency: usize::try_from(
                baseline_run.effective_message_fetch_concurrency.max(1),
            )
            .unwrap_or(usize::MAX),
        },
        Some(gmail_client),
    )?;
    Ok(())
}

async fn initialize_full_sync_checkpoint(
    context: &SyncExecutionContext<'_>,
    bootstrap_query: &str,
    _fallback_from_history: bool,
) -> Result<FullSyncCheckpointState> {
    let labels_synced = i64::try_from(context.labels.len()).unwrap_or(i64::MAX);
    let checkpoint = context.store_handle.load_full_sync_checkpoint().await?;
    let now_epoch_s = current_epoch_seconds()?;

    match checkpoint {
        Some(checkpoint) if checkpoint.bootstrap_query != bootstrap_query => {
            context.writer.reset_full_sync_progress().await?;
            let record = context
                .writer
                .prepare_full_sync_checkpoint(
                    context.labels,
                    store::mailbox::FullSyncCheckpointUpdate {
                        bootstrap_query: bootstrap_query.to_owned(),
                        status: store::mailbox::FullSyncCheckpointStatus::Paging,
                        next_page_token: None,
                        cursor_history_id: Some(context.account.history_id.clone()),
                        pages_fetched: 0,
                        messages_listed: 0,
                        messages_upserted: 0,
                        labels_synced,
                        started_at_epoch_s: now_epoch_s,
                        updated_at_epoch_s: now_epoch_s,
                    },
                )
                .await?;
            Ok(FullSyncCheckpointState {
                record,
                resumed_from_checkpoint: false,
            })
        }
        Some(checkpoint) => {
            let cursor_history_id = newest_history_id(
                checkpoint.cursor_history_id.clone(),
                &context.account.history_id,
            );
            let record = context
                .writer
                .update_full_sync_checkpoint_labels(
                    context.labels,
                    store::mailbox::FullSyncCheckpointUpdate {
                        bootstrap_query: checkpoint.bootstrap_query.clone(),
                        status: checkpoint.status,
                        next_page_token: checkpoint.next_page_token.clone(),
                        cursor_history_id,
                        pages_fetched: checkpoint.pages_fetched,
                        messages_listed: checkpoint.messages_listed,
                        messages_upserted: checkpoint.messages_upserted,
                        labels_synced,
                        started_at_epoch_s: checkpoint.started_at_epoch_s,
                        updated_at_epoch_s: now_epoch_s,
                    },
                )
                .await?;

            if full_sync_checkpoint_is_consistent(&record, labels_synced) {
                Ok(FullSyncCheckpointState {
                    record,
                    resumed_from_checkpoint: true,
                })
            } else {
                context.writer.reset_full_sync_progress().await?;
                let record = context
                    .writer
                    .prepare_full_sync_checkpoint(
                        context.labels,
                        store::mailbox::FullSyncCheckpointUpdate {
                            bootstrap_query: bootstrap_query.to_owned(),
                            status: store::mailbox::FullSyncCheckpointStatus::Paging,
                            next_page_token: None,
                            cursor_history_id: Some(context.account.history_id.clone()),
                            pages_fetched: 0,
                            messages_listed: 0,
                            messages_upserted: 0,
                            labels_synced,
                            started_at_epoch_s: now_epoch_s,
                            updated_at_epoch_s: now_epoch_s,
                        },
                    )
                    .await?;
                Ok(FullSyncCheckpointState {
                    record,
                    resumed_from_checkpoint: false,
                })
            }
        }
        None => {
            let record = context
                .writer
                .prepare_full_sync_checkpoint(
                    context.labels,
                    store::mailbox::FullSyncCheckpointUpdate {
                        bootstrap_query: bootstrap_query.to_owned(),
                        status: store::mailbox::FullSyncCheckpointStatus::Paging,
                        next_page_token: None,
                        cursor_history_id: Some(context.account.history_id.clone()),
                        pages_fetched: 0,
                        messages_listed: 0,
                        messages_upserted: 0,
                        labels_synced,
                        started_at_epoch_s: now_epoch_s,
                        updated_at_epoch_s: now_epoch_s,
                    },
                )
                .await?;
            Ok(FullSyncCheckpointState {
                record,
                resumed_from_checkpoint: false,
            })
        }
    }
}

fn full_sync_checkpoint_is_consistent(
    checkpoint: &store::mailbox::FullSyncCheckpointRecord,
    labels_synced: i64,
) -> bool {
    checkpoint.messages_upserted == checkpoint.staged_message_count
        && checkpoint.labels_synced == labels_synced
        && checkpoint.staged_label_count == labels_synced
        && (checkpoint.status != store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize
            || checkpoint.next_page_token.is_none())
}

async fn finalize_full_sync_report(
    context: &SyncExecutionContext<'_>,
    request: FullSyncFinalizeRequest<'_>,
) -> Result<SyncRunReport> {
    let comparability =
        store::mailbox::comparability_for_full_bootstrap_query(request.bootstrap_query);
    let finalize_input = FinalizeSyncInput {
        mode: store::mailbox::SyncMode::Full,
        comparability_kind: comparability.kind,
        comparability_key: comparability.key,
        comparability_label: comparability.label,
        startup_seed_run_id: request.startup_seed_run_id,
        fallback_from_history: request.fallback_from_history,
        resumed_from_checkpoint: request.resumed_from_checkpoint,
        cursor_history_id: request.checkpoint.cursor_history_id.clone(),
        pages_fetched: usize::try_from(request.checkpoint.pages_fetched).unwrap_or(usize::MAX),
        messages_listed: usize::try_from(request.checkpoint.messages_listed).unwrap_or(usize::MAX),
        messages_upserted: usize::try_from(request.checkpoint.messages_upserted)
            .unwrap_or(usize::MAX),
        messages_deleted: 0,
        labels_synced: usize::try_from(request.checkpoint.labels_synced).unwrap_or(usize::MAX),
        checkpoint_reused_pages: request.checkpoint_reused_pages,
        checkpoint_reused_messages_upserted: request.checkpoint_reused_messages_upserted,
        pipeline_enabled: request.pipeline_report.pipeline_enabled,
        pipeline_list_queue_high_water: request.pipeline_report.list_queue_high_water,
        pipeline_write_queue_high_water: request.pipeline_report.write_queue_high_water,
        pipeline_write_batch_count: request.pipeline_report.write_batch_count,
        pipeline_writer_wait_ms: request.pipeline_report.writer_wait_ms,
        pipeline_fetch_batch_count: request.pipeline_report.fetch_batch_count,
        pipeline_fetch_batch_avg_ms: request.pipeline_report.fetch_batch_avg_ms,
        pipeline_fetch_batch_max_ms: request.pipeline_report.fetch_batch_max_ms,
        pipeline_writer_tx_count: request.pipeline_report.writer_tx_count,
        pipeline_writer_tx_avg_ms: request.pipeline_report.writer_tx_avg_ms,
        pipeline_writer_tx_max_ms: request.pipeline_report.writer_tx_max_ms,
        pipeline_reorder_buffer_high_water: request.pipeline_report.reorder_buffer_high_water,
        pipeline_staged_message_count: request.pipeline_report.staged_message_count,
        pipeline_staged_delete_count: request.pipeline_report.staged_delete_count,
        pipeline_staged_attachment_count: request.pipeline_report.staged_attachment_count,
    };
    let now_epoch_s = current_epoch_seconds()?;
    let sync_state = context
        .writer
        .finalize_full_sync_from_stage(
            now_epoch_s,
            success_sync_state_update(
                context.account,
                request.bootstrap_query,
                &finalize_input,
                now_epoch_s,
            ),
        )
        .await?;

    finalize_sync(sync_state, request.bootstrap_query, finalize_input)
}

async fn run_full_sync_lister(
    gmail_client: GmailClient,
    bootstrap_query: String,
    mut page_token: Option<String>,
    resumed_from_checkpoint: bool,
    starting_page_seq: usize,
    list_tx: mpsc::Sender<ListedPage>,
    stats: PipelineStats,
) -> Result<()> {
    let mut page_seq = starting_page_seq;

    loop {
        let page = match gmail_client
            .list_message_ids(
                Some(bootstrap_query.as_str()),
                page_token.as_deref(),
                FULL_SYNC_PAGE_SIZE,
            )
            .await
        {
            Ok(page) => page,
            Err(error)
                if resumed_from_checkpoint
                    && page_token.is_some()
                    && is_invalid_resume_page_token_error(&error) =>
            {
                return Err(anyhow!(RestartFullSyncFromScratch));
            }
            Err(error) => return Err(error.into()),
        };

        let listed_count = page.messages.len();
        let listed_page = ListedPage {
            page_seq,
            message_ids: page
                .messages
                .into_iter()
                .map(|message| message.id)
                .collect(),
            next_page_token: page.next_page_token.clone(),
            listed_count,
        };
        stats.on_list_enqueued();
        if list_tx.send(listed_page).await.is_err() {
            return Ok(());
        }

        page_seq += 1;
        page_token = page.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    Ok(())
}

async fn run_full_sync_processor(
    gmail_client: GmailClient,
    account_id: String,
    label_names_by_id: Arc<BTreeMap<String, String>>,
    mut list_rx: mpsc::Receiver<ListedPage>,
    write_tx: mpsc::Sender<PreparedFullSyncPage>,
    stats: PipelineStats,
    fetch_concurrency: Arc<AtomicUsize>,
) -> Result<()> {
    let mut join_set = JoinSet::new();

    while let Some(page) = list_rx.recv().await {
        stats.on_list_dequeued();
        while join_set.len()
            >= page_processing_concurrency_for_fetch(fetch_concurrency.load(Ordering::Acquire))
        {
            let result = join_set
                .join_next()
                .await
                .context("full sync processor task set ended unexpectedly")?;
            result??;
        }
        let write_tx = write_tx.clone();
        let stats = stats.clone();
        let gmail_client = gmail_client.clone();
        let account_id = account_id.clone();
        let label_names_by_id = label_names_by_id.clone();
        let fetch_concurrency = fetch_concurrency.clone();
        join_set.spawn(async move {
            let fetch_started = Instant::now();
            let (catalogs, _) = fetch_message_catalogs(
                gmail_client,
                page.message_ids,
                fetch_concurrency.load(Ordering::Acquire),
            )
            .await?;
            stats.record_fetch_batch(fetch_started.elapsed());
            let mut cursor_history_id = None;
            for catalog in &catalogs {
                cursor_history_id =
                    newest_history_id(cursor_history_id, &catalog.metadata.history_id);
            }
            let mut chunk_seq = 0usize;
            let mut page_upserted_total = 0usize;
            let mut current_chunk = Vec::new();
            let mut sent_any_chunk = false;

            for catalog in catalogs {
                if message_is_excluded(&catalog.metadata.label_ids) {
                    continue;
                }
                current_chunk.push(build_upsert_input(
                    account_id.clone(),
                    catalog,
                    label_names_by_id.as_ref(),
                ));
                page_upserted_total += 1;
                if current_chunk.len() >= PIPELINE_WRITE_BATCH_MESSAGE_TARGET {
                    let permit = write_tx
                        .clone()
                        .reserve_owned()
                        .await
                        .context("full sync write queue closed while reserving batch slot")?;
                    stats.on_write_enqueued();
                    permit.send(PreparedFullSyncPage {
                        page_seq: page.page_seq,
                        chunk_seq,
                        listed_count: page.listed_count,
                        next_page_token: page.next_page_token.clone(),
                        cursor_history_id: cursor_history_id.clone(),
                        upserts: std::mem::take(&mut current_chunk),
                        page_complete: false,
                        page_upserted_total: 0,
                    });
                    sent_any_chunk = true;
                    chunk_seq += 1;
                }
            }

            let final_upserts = std::mem::take(&mut current_chunk);
            if !final_upserts.is_empty() || !sent_any_chunk {
                let permit = write_tx
                    .reserve_owned()
                    .await
                    .context("full sync write queue closed while reserving final batch slot")?;
                stats.on_write_enqueued();
                permit.send(PreparedFullSyncPage {
                    page_seq: page.page_seq,
                    chunk_seq,
                    listed_count: page.listed_count,
                    next_page_token: page.next_page_token,
                    cursor_history_id,
                    upserts: final_upserts,
                    page_complete: true,
                    page_upserted_total,
                });
            } else {
                let permit = write_tx
                    .reserve_owned()
                    .await
                    .context("full sync write queue closed while reserving final page marker")?;
                stats.on_write_enqueued();
                permit.send(PreparedFullSyncPage {
                    page_seq: page.page_seq,
                    chunk_seq,
                    listed_count: page.listed_count,
                    next_page_token: page.next_page_token,
                    cursor_history_id,
                    upserts: Vec::new(),
                    page_complete: true,
                    page_upserted_total,
                });
            }
            Ok::<_, anyhow::Error>(())
        });
    }

    drop(write_tx);
    while let Some(result) = join_set.join_next().await {
        result??;
    }

    Ok(())
}

async fn stage_prepared_full_sync_page_chunk(
    context: &SyncExecutionContext<'_>,
    checkpoint: &store::mailbox::FullSyncCheckpointRecord,
    bootstrap_query: &str,
    labels_synced: i64,
    page: PreparedFullSyncPage,
) -> Result<store::mailbox::FullSyncCheckpointRecord> {
    let updated_at_epoch_s = current_epoch_seconds()?;
    let cursor_history_id = match page.cursor_history_id.as_deref() {
        Some(history_id) => newest_history_id(checkpoint.cursor_history_id.clone(), history_id),
        None => checkpoint.cursor_history_id.clone(),
    };
    let checkpoint_update = page
        .page_complete
        .then(|| store::mailbox::FullSyncCheckpointUpdate {
            bootstrap_query: bootstrap_query.to_owned(),
            status: if page.next_page_token.is_some() {
                store::mailbox::FullSyncCheckpointStatus::Paging
            } else {
                store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize
            },
            next_page_token: page.next_page_token.clone(),
            cursor_history_id,
            pages_fetched: checkpoint.pages_fetched.saturating_add(1),
            messages_listed: checkpoint
                .messages_listed
                .saturating_add(i64::try_from(page.listed_count).unwrap_or(i64::MAX)),
            messages_upserted: checkpoint
                .messages_upserted
                .saturating_add(i64::try_from(page.page_upserted_total).unwrap_or(i64::MAX)),
            labels_synced,
            started_at_epoch_s: checkpoint.started_at_epoch_s,
            updated_at_epoch_s,
        });
    context
        .writer
        .stage_full_sync_page_chunk_and_maybe_update_checkpoint(
            store::mailbox::FullSyncStagePageInput {
                page_seq: i64::try_from(page.page_seq).unwrap_or(i64::MAX),
                listed_count: i64::try_from(page.listed_count).unwrap_or(i64::MAX),
                next_page_token: page.next_page_token,
                updated_at_epoch_s,
                page_complete: page.page_complete,
            },
            &page.upserts,
            checkpoint_update,
        )
        .await
}

async fn run_incremental_batch_lister(
    changed_message_ids: Vec<String>,
    list_tx: mpsc::Sender<ListedPage>,
    stats: PipelineStats,
) -> Result<()> {
    for (page_seq, message_id_chunk) in changed_message_ids
        .chunks(PIPELINE_WRITE_BATCH_MESSAGE_TARGET)
        .enumerate()
    {
        let listed_page = ListedPage {
            page_seq,
            message_ids: message_id_chunk.to_vec(),
            next_page_token: None,
            listed_count: message_id_chunk.len(),
        };
        stats.on_list_enqueued();
        if list_tx.send(listed_page).await.is_err() {
            return Ok(());
        }
    }

    Ok(())
}

async fn run_incremental_sync_processor(
    gmail_client: GmailClient,
    account_id: String,
    label_names_by_id: Arc<BTreeMap<String, String>>,
    mut list_rx: mpsc::Receiver<ListedPage>,
    write_tx: mpsc::Sender<PreparedIncrementalBatch>,
    stats: PipelineStats,
    fetch_concurrency: Arc<AtomicUsize>,
) -> Result<()> {
    let mut join_set = JoinSet::new();

    while let Some(page) = list_rx.recv().await {
        stats.on_list_dequeued();
        while join_set.len()
            >= page_processing_concurrency_for_fetch(fetch_concurrency.load(Ordering::Acquire))
        {
            let result = join_set
                .join_next()
                .await
                .context("incremental processor task set ended unexpectedly")?;
            result??;
        }
        let write_tx = write_tx.clone();
        let stats = stats.clone();
        let gmail_client = gmail_client.clone();
        let account_id = account_id.clone();
        let label_names_by_id = label_names_by_id.clone();
        let fetch_concurrency = fetch_concurrency.clone();
        join_set.spawn(async move {
            let fetch_started = Instant::now();
            let (catalogs, missing_message_ids) = fetch_message_catalogs(
                gmail_client,
                page.message_ids,
                fetch_concurrency.load(Ordering::Acquire),
            )
            .await?;
            stats.record_fetch_batch(fetch_started.elapsed());
            let (upserts, excluded_message_ids) =
                build_incremental_changes(&account_id, catalogs, label_names_by_id.as_ref());
            let message_ids_to_delete = missing_message_ids
                .into_iter()
                .chain(excluded_message_ids)
                .collect::<Vec<_>>();
            let permit = write_tx
                .reserve_owned()
                .await
                .context("incremental sync write queue closed while reserving batch slot")?;
            stats.on_write_enqueued();
            permit.send(PreparedIncrementalBatch {
                batch_seq: page.page_seq,
                listed_count: page.listed_count,
                upserts,
                message_ids_to_delete,
            });
            Ok::<_, anyhow::Error>(())
        });
    }

    drop(write_tx);
    while let Some(result) = join_set.join_next().await {
        result??;
    }

    Ok(())
}

fn preserve_sync_error(sync_error: anyhow::Error, persist_result: Result<()>) -> anyhow::Error {
    let _ = persist_result;
    sync_error
}

async fn fetch_message_catalogs(
    gmail_client: GmailClient,
    message_ids: Vec<String>,
    message_fetch_concurrency: usize,
) -> Result<(Vec<GmailMessageCatalog>, Vec<String>)> {
    if message_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let semaphore = std::sync::Arc::new(Semaphore::new(message_fetch_concurrency));
    let mut join_set = JoinSet::new();

    for (index, message_id) in message_ids.into_iter().enumerate() {
        let gmail_client = gmail_client.clone();
        let semaphore = semaphore.clone();
        join_set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .context("failed to acquire message fetch permit")?;
            let catalog = gmail_client
                .get_message_catalog_if_present(&message_id)
                .await?;
            Ok::<_, anyhow::Error>((index, message_id, catalog))
        });
    }

    let mut catalogs_by_index = Vec::new();
    while let Some(result) = join_set.join_next().await {
        catalogs_by_index.push(result??);
    }
    catalogs_by_index.sort_by_key(|(index, _, _)| *index);

    let mut catalogs = Vec::new();
    let mut missing_message_ids = Vec::new();
    for (_, message_id, maybe_catalog) in catalogs_by_index {
        match maybe_catalog {
            Some(catalog) => catalogs.push(catalog),
            None => missing_message_ids.push(message_id),
        }
    }

    Ok((catalogs, missing_message_ids))
}

fn build_incremental_changes(
    account_id: &str,
    catalogs: Vec<GmailMessageCatalog>,
    label_names_by_id: &BTreeMap<String, String>,
) -> (Vec<store::mailbox::GmailMessageUpsertInput>, Vec<String>) {
    let mut upserts = Vec::new();
    let mut excluded_message_ids = Vec::new();

    for catalog in catalogs {
        if message_is_excluded(&catalog.metadata.label_ids) {
            excluded_message_ids.push(catalog.metadata.id);
        } else {
            upserts.push(build_upsert_input(
                account_id.to_owned(),
                catalog,
                label_names_by_id,
            ));
        }
    }

    (upserts, excluded_message_ids)
}

fn build_upsert_input(
    account_id: String,
    catalog: GmailMessageCatalog,
    label_names_by_id: &BTreeMap<String, String>,
) -> store::mailbox::GmailMessageUpsertInput {
    let message = catalog.metadata;
    let label_names_text = message
        .label_ids
        .iter()
        .filter_map(|label_id| label_names_by_id.get(label_id))
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    let recipient_headers = recipient_headers(&message);

    store::mailbox::GmailMessageUpsertInput {
        account_id,
        message_id: message.id,
        thread_id: message.thread_id,
        history_id: message.history_id,
        internal_date_epoch_ms: message.internal_date_epoch_ms,
        snippet: message.snippet,
        subject: message.subject,
        from_header: message.from_header,
        from_address: message.from_address,
        recipient_headers,
        to_header: message.to_header,
        cc_header: message.cc_header,
        bcc_header: message.bcc_header,
        reply_to_header: message.reply_to_header,
        size_estimate: message.size_estimate,
        automation_headers: store::mailbox::GmailAutomationHeaders {
            list_id_header: message.automation_headers.list_id_header,
            list_unsubscribe_header: message.automation_headers.list_unsubscribe_header,
            list_unsubscribe_post_header: message.automation_headers.list_unsubscribe_post_header,
            precedence_header: message.automation_headers.precedence_header,
            auto_submitted_header: message.automation_headers.auto_submitted_header,
        },
        label_ids: message.label_ids,
        label_names_text,
        attachments: catalog
            .attachments
            .into_iter()
            .map(|attachment| store::mailbox::GmailAttachmentUpsertInput {
                attachment_key: attachment.attachment_key,
                part_id: attachment.part_id,
                gmail_attachment_id: attachment.gmail_attachment_id,
                filename: attachment.filename,
                mime_type: attachment.mime_type,
                size_bytes: attachment.size_bytes,
                content_disposition: attachment.content_disposition,
                content_id: attachment.content_id,
                is_inline: attachment.is_inline,
            })
            .collect(),
    }
}

#[derive(Debug, Clone)]
struct MailboxStoreHandle {
    database_path: PathBuf,
    busy_timeout_ms: u64,
    account_id: String,
}

impl MailboxStoreHandle {
    fn new(config_report: &ConfigReport, account_id: &str) -> Self {
        Self {
            database_path: config_report.config.store.database_path.clone(),
            busy_timeout_ms: config_report.config.store.busy_timeout_ms,
            account_id: account_id.to_owned(),
        }
    }

    async fn load_sync_state(&self) -> Result<Option<store::mailbox::SyncStateRecord>> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        Ok(spawn_blocking(move || {
            store::mailbox::get_sync_state(&database_path, busy_timeout_ms, &account_id)
        })
        .await??)
    }

    async fn load_full_sync_checkpoint(
        &self,
    ) -> Result<Option<store::mailbox::FullSyncCheckpointRecord>> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        Ok(spawn_blocking(move || {
            store::mailbox::get_full_sync_checkpoint(&database_path, busy_timeout_ms, &account_id)
        })
        .await??)
    }

    async fn load_sync_pacing_state(
        &self,
    ) -> Result<Option<store::mailbox::SyncPacingStateRecord>> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        Ok(spawn_blocking(move || {
            store::mailbox::get_sync_pacing_state(&database_path, busy_timeout_ms, &account_id)
        })
        .await??)
    }

    async fn load_sync_run_summary_for_comparability(
        &self,
        sync_mode: store::mailbox::SyncMode,
        comparability_key: &str,
    ) -> Result<Option<store::mailbox::SyncRunSummaryRecord>> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        let comparability_key = comparability_key.to_owned();
        Ok(spawn_blocking(move || {
            store::mailbox::get_sync_run_summary_for_comparability(
                &database_path,
                busy_timeout_ms,
                &account_id,
                sync_mode,
                &comparability_key,
            )
        })
        .await??)
    }

    async fn load_sync_run_history_record(
        &self,
        run_id: i64,
    ) -> Result<Option<store::mailbox::SyncRunHistoryRecord>> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        Ok(spawn_blocking(move || {
            store::mailbox::get_sync_run_history_record(&database_path, busy_timeout_ms, run_id)
        })
        .await??)
    }

    async fn upsert_sync_state(
        &self,
        update: store::mailbox::SyncStateUpdate,
    ) -> Result<store::mailbox::SyncStateRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        spawn_blocking(move || {
            store::mailbox::upsert_sync_state(&database_path, busy_timeout_ms, &update)
        })
        .await?
    }

    async fn upsert_sync_pacing_state(
        &self,
        update: store::mailbox::SyncPacingStateUpdate,
    ) -> Result<store::mailbox::SyncPacingStateRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        spawn_blocking(move || {
            store::mailbox::upsert_sync_pacing_state(&database_path, busy_timeout_ms, &update)
        })
        .await?
    }

    async fn persist_successful_sync_outcome(
        &self,
        sync_state: &store::mailbox::SyncStateRecord,
        outcome: &store::mailbox::SyncRunOutcomeInput,
    ) -> Result<(
        store::mailbox::SyncStateRecord,
        store::mailbox::SyncRunHistoryRecord,
        store::mailbox::SyncRunSummaryRecord,
    )> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let sync_state = sync_state.clone();
        let outcome = outcome.clone();
        spawn_blocking(move || {
            store::mailbox::persist_successful_sync_outcome(
                &database_path,
                busy_timeout_ms,
                &sync_state,
                &outcome,
            )
        })
        .await?
    }

    async fn persist_failed_sync_outcome(
        &self,
        sync_state_update: &store::mailbox::SyncStateUpdate,
        outcome: &store::mailbox::SyncRunOutcomeInput,
    ) -> Result<(
        store::mailbox::SyncStateRecord,
        store::mailbox::SyncRunHistoryRecord,
        store::mailbox::SyncRunSummaryRecord,
    )> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let sync_state_update = sync_state_update.clone();
        let outcome = outcome.clone();
        spawn_blocking(move || {
            store::mailbox::persist_failed_sync_outcome(
                &database_path,
                busy_timeout_ms,
                &sync_state_update,
                &outcome,
            )
        })
        .await?
    }
}

#[cfg(test)]
mod sync_error_tests {
    use super::preserve_sync_error;
    use anyhow::anyhow;

    #[test]
    fn preserve_sync_error_returns_original_error_when_failure_persistence_also_fails() {
        let error = preserve_sync_error(anyhow!("sync failed"), Err(anyhow!("persist failed")));

        assert_eq!(error.to_string(), "sync failed");
    }
}
