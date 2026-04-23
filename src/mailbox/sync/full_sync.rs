use super::incremental_sync::{abort_pipeline_tasks, initialize_full_sync_checkpoint};
use super::pipeline::{
    finalize_full_sync_report, run_full_sync_lister, run_full_sync_processor,
    stage_prepared_full_sync_page_chunk,
};
use super::*;

impl MailboxWriterWorker {
    pub(super) async fn start(store_handle: &MailboxStoreHandle) -> Result<Self> {
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
                    if let Err(ready_error) = ready_tx.send(Err(error))
                        && let Err(open_error) = ready_error
                    {
                        return Err(open_error);
                    }
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

    pub(super) async fn shutdown(mut self) -> Result<()> {
        self.sender.take();
        self.handle.await?
    }

    pub(super) async fn reset_full_sync_progress(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::ResetFullSyncProgress { reply: reply_tx })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer reset-full-sync-progress reply dropped")?
    }

    pub(super) async fn prepare_full_sync_checkpoint(
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

    pub(super) async fn update_full_sync_checkpoint_labels(
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

    pub(super) async fn finalize_full_sync_from_stage(
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

    pub(super) async fn stage_full_sync_page_chunk_and_maybe_update_checkpoint(
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

    pub(super) async fn reset_incremental_sync_stage(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender()?
            .send(MailboxWriterCommand::ResetIncrementalSyncStage { reply: reply_tx })
            .context("mailbox writer worker is unavailable")?;
        reply_rx
            .await
            .context("mailbox writer reset-incremental-sync-stage reply dropped")?
    }

    pub(super) async fn stage_incremental_sync_batch(
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

    pub(super) async fn finalize_incremental_from_stage(
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

pub(super) fn record_pipeline_failure(
    failure_pipeline_report: &mut PipelineStatsReport,
    stats: &PipelineStats,
) {
    *failure_pipeline_report = stats.report();
}

pub(super) async fn run_full_sync(
    context: &SyncExecutionContext<'_>,
    pacing: &mut AdaptiveSyncPacing,
    bootstrap_query: &str,
    fallback_from_history: bool,
    failure_telemetry: &mut FullSyncFailureTelemetry,
) -> Result<SyncRunReport> {
    let mut checkpoint = initialize_full_sync_checkpoint(context, bootstrap_query).await?;
    let checkpoint_reused_pages =
        usize::try_from(checkpoint.record.pages_fetched).unwrap_or(usize::MAX);
    let checkpoint_reused_messages_upserted =
        usize::try_from(checkpoint.record.messages_upserted).unwrap_or(usize::MAX);
    *failure_telemetry = FullSyncFailureTelemetry {
        resumed_from_checkpoint: checkpoint.resumed_from_checkpoint,
        pages_fetched: usize::try_from(checkpoint.record.pages_fetched).unwrap_or(usize::MAX),
        messages_listed: usize::try_from(checkpoint.record.messages_listed).unwrap_or(usize::MAX),
        messages_upserted: usize::try_from(checkpoint.record.messages_upserted)
            .unwrap_or(usize::MAX),
        labels_synced: usize::try_from(checkpoint.record.labels_synced).unwrap_or(usize::MAX),
        checkpoint_reused_pages,
        checkpoint_reused_messages_upserted,
        pipeline_report: disabled_pipeline_report(),
    };

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
            failure_telemetry.pipeline_report = disabled_pipeline_report();
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

    let mut lister_handle = Some(lister_handle);
    let mut processor_handle = Some(processor_handle);
    let result = async {
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
                    record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
                })?;
                failure_telemetry.pages_fetched =
                    usize::try_from(checkpoint.record.pages_fetched).unwrap_or(usize::MAX);
                failure_telemetry.messages_listed =
                    usize::try_from(checkpoint.record.messages_listed).unwrap_or(usize::MAX);
                failure_telemetry.messages_upserted =
                    usize::try_from(checkpoint.record.messages_upserted).unwrap_or(usize::MAX);
                failure_telemetry.labels_synced =
                    usize::try_from(checkpoint.record.labels_synced).unwrap_or(usize::MAX);
                stats.record_writer_transaction(write_started.elapsed());
                stats.record_staged_messages(staged_message_count);
                stats.record_staged_attachments(staged_attachment_count);
                stats.on_write_batch_committed();
                observe_latest_metrics(pacing, context.gmail_client).inspect_err(|_| {
                    record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
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
            .take()
            .expect("full sync lister handle missing")
            .await
            .context("full sync lister task failed")
            .inspect_err(|_| {
                record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
            })?;
        let processor_result = processor_handle
            .take()
            .expect("full sync processor handle missing")
            .await
            .context("full sync processor task failed")
            .inspect_err(|_| {
                record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
            })?;

        if let Err(error) = lister_result {
            record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
            if error.downcast_ref::<RestartFullSyncFromScratch>().is_some() {
                context.writer.reset_full_sync_progress().await?;
                return Box::pin(run_full_sync(
                    context,
                    pacing,
                    bootstrap_query,
                    fallback_from_history,
                    failure_telemetry,
                ))
                .await;
            }
            return Err(error);
        }
        processor_result.inspect_err(|_| {
            record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
        })?;

        if !buffered_pages.is_empty() {
            record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
            return Err(anyhow!(
                "full sync pipeline terminated with {} buffered pages still waiting to commit",
                buffered_pages.len()
            ));
        }
        if checkpoint.record.status != store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize {
            record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
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
            record_pipeline_failure(&mut failure_telemetry.pipeline_report, &stats);
        })
    }
    .await;

    abort_pipeline_tasks(lister_handle.take(), processor_handle.take()).await;
    result
}
