use super::full_sync::record_pipeline_failure;
use super::pipeline::{
    full_sync_checkpoint_is_consistent, run_incremental_batch_lister,
    run_incremental_sync_processor,
};
use super::*;

pub(super) async fn run_incremental_sync(
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
        messages_upserted: 0,
        messages_deleted: deleted_message_ids.len(),
        labels_synced: context.labels.len(),
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
            .upsert_sync_state(success_sync_state_update_with_pipeline(
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

    let mut lister_handle = Some(lister_handle);
    let mut processor_handle = Some(processor_handle);
    let result = async {
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
            .take()
            .expect("incremental sync lister handle missing")
            .await
            .context("incremental sync lister task failed")
            .inspect_err(|_| {
                record_pipeline_failure(failure_pipeline_report, &stats);
            })?;
        lister_result.inspect_err(|_| {
            record_pipeline_failure(failure_pipeline_report, &stats);
        })?;

        let processor_result = processor_handle
            .take()
            .expect("incremental sync processor handle missing")
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
        *failure_telemetry = IncrementalFailureTelemetry {
            comparability: comparability.clone(),
            pages_fetched,
            messages_listed,
            messages_upserted,
            messages_deleted: deleted_message_ids.len(),
            labels_synced: context.labels.len(),
        };
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
    .await;

    abort_pipeline_tasks(lister_handle.take(), processor_handle.take()).await;
    result
}

pub(super) async fn abort_pipeline_tasks(
    lister_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    processor_handle: Option<tokio::task::JoinHandle<Result<()>>>,
) {
    if let Some(lister_handle) = lister_handle {
        if !lister_handle.is_finished() {
            lister_handle.abort();
        }
        let _ = lister_handle.await;
    }
    if let Some(processor_handle) = processor_handle {
        if !processor_handle.is_finished() {
            processor_handle.abort();
        }
        let _ = processor_handle.await;
    }
}

pub(super) fn success_sync_state_update_with_pipeline(
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

pub(super) fn observe_latest_metrics(
    pacing: &mut AdaptiveSyncPacing,
    gmail_client: &GmailClient,
) -> Result<()> {
    if let Some(snapshot) = gmail_client.request_metrics_snapshot() {
        pacing.observe_metrics_snapshot(snapshot, Some(gmail_client))?;
    }

    Ok(())
}

pub(super) async fn persist_sync_pacing_failure(
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

pub(super) async fn load_sync_state(
    store_handle: &MailboxStoreHandle,
) -> Result<Option<store::mailbox::SyncStateRecord>> {
    store_handle.load_sync_state().await
}

pub(super) async fn persist_sync_state_failure(
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

pub(super) fn resolve_sync_history_account_id(config_report: &ConfigReport) -> Result<String> {
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

pub(super) fn failure_comparability(
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

pub(super) async fn maybe_seed_pacing_from_history(
    store_handle: &MailboxStoreHandle,
    pacing: &mut AdaptiveSyncPacing,
    gmail_client: &GmailClient,
    sync_mode: store::mailbox::SyncMode,
    comparability: &store::mailbox::SyncRunComparability,
) -> Result<()> {
    let Some(summary) = store_handle
        .load_sync_run_summary_for_comparability(sync_mode, comparability.kind, &comparability.key)
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

pub(super) async fn initialize_full_sync_checkpoint(
    context: &SyncExecutionContext<'_>,
    bootstrap_query: &str,
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
