use super::incremental_sync::success_sync_state_update;
use super::*;

pub(super) fn full_sync_checkpoint_is_consistent(
    checkpoint: &store::mailbox::FullSyncCheckpointRecord,
    labels_synced: i64,
) -> bool {
    checkpoint.messages_upserted == checkpoint.staged_message_count
        && checkpoint.labels_synced == labels_synced
        && checkpoint.staged_label_count == labels_synced
        && (checkpoint.status != store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize
            || checkpoint.next_page_token.is_none())
}

pub(super) async fn finalize_full_sync_report(
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

pub(super) async fn run_full_sync_lister(
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

pub(super) async fn run_full_sync_processor(
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
        let current_fetch_concurrency = fetch_concurrency.load(Ordering::Acquire);
        while join_set.len() >= page_processing_concurrency_for_fetch(current_fetch_concurrency) {
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
        join_set.spawn(async move {
            let fetch_started = Instant::now();
            let (catalogs, _) =
                fetch_message_catalogs(gmail_client, page.message_ids, current_fetch_concurrency)
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

pub(super) async fn stage_prepared_full_sync_page_chunk(
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

pub(super) async fn run_incremental_batch_lister(
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

pub(super) async fn run_incremental_sync_processor(
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
        let current_fetch_concurrency = fetch_concurrency.load(Ordering::Acquire);
        while join_set.len() >= page_processing_concurrency_for_fetch(current_fetch_concurrency) {
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
        join_set.spawn(async move {
            let fetch_started = Instant::now();
            let (catalogs, missing_message_ids) =
                fetch_message_catalogs(gmail_client, page.message_ids, current_fetch_concurrency)
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

pub(super) fn preserve_sync_error(
    sync_error: anyhow::Error,
    persist_result: Result<()>,
) -> anyhow::Error {
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

pub(super) fn build_incremental_changes(
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
pub(super) struct MailboxStoreHandle {
    pub(super) database_path: PathBuf,
    pub(super) busy_timeout_ms: u64,
    pub(super) account_id: String,
}

impl MailboxStoreHandle {
    pub(super) fn new(config_report: &ConfigReport, account_id: &str) -> Self {
        Self {
            database_path: config_report.config.store.database_path.clone(),
            busy_timeout_ms: config_report.config.store.busy_timeout_ms,
            account_id: account_id.to_owned(),
        }
    }

    pub(super) async fn load_sync_state(&self) -> Result<Option<store::mailbox::SyncStateRecord>> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        Ok(spawn_blocking(move || {
            store::mailbox::get_sync_state(&database_path, busy_timeout_ms, &account_id)
        })
        .await??)
    }

    pub(super) async fn load_full_sync_checkpoint(
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

    pub(super) async fn load_sync_pacing_state(
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

    pub(super) async fn load_sync_run_summary_for_comparability(
        &self,
        sync_mode: store::mailbox::SyncMode,
        comparability_kind: store::mailbox::SyncRunComparabilityKind,
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
                comparability_kind,
                &comparability_key,
            )
        })
        .await??)
    }

    pub(super) async fn load_sync_run_history_record(
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

    pub(super) async fn upsert_sync_state(
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

    pub(super) async fn upsert_sync_pacing_state(
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

    pub(super) async fn persist_successful_sync_outcome(
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
        Ok(spawn_blocking(move || {
            store::mailbox::persist_successful_sync_outcome(
                &database_path,
                busy_timeout_ms,
                &sync_state,
                &outcome,
            )
        })
        .await??)
    }

    pub(super) async fn persist_failed_sync_outcome(
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
        Ok(spawn_blocking(move || {
            store::mailbox::persist_failed_sync_outcome(
                &database_path,
                busy_timeout_ms,
                &sync_state_update,
                &outcome,
            )
        })
        .await??)
    }
}
