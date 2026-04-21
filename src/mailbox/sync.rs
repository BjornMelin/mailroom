use crate::config::ConfigReport;
use crate::gmail::{GmailClient, GmailLabel, GmailMessageCatalog};
use crate::mailbox::model::{FinalizeSyncInput, SyncRunOptions};
use crate::mailbox::pacing::{AdaptiveSyncPacing, AdaptiveSyncPacingReport};
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
use tokio::sync::Semaphore;
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

    let gmail_client = crate::gmail_client_for_config(config_report)?
        .with_quota_budget(options.quota_units_per_minute)?;
    let account =
        crate::refresh_active_account_record_with_client(config_report, &gmail_client).await?;
    let store_handle = MailboxStoreHandle::new(config_report, &account.account_id);
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
    let mut failure_bootstrap_query = initial_bootstrap_query;

    let result = async {
        let labels = gmail_client.list_labels().await?;
        let label_names_by_id = labels_by_id(&labels);
        let sync_context = SyncExecutionContext {
            store_handle: &store_handle,
            gmail_client: &gmail_client,
            account: &account,
            labels: &labels,
            label_names_by_id: &label_names_by_id,
        };
        observe_latest_metrics(&mut pacing, &gmail_client)?;

        match initial_mode {
            store::mailbox::SyncMode::Full => {
                run_full_sync(&sync_context, &mut pacing, initial_bootstrap_query, false).await
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
                )
                .await
                {
                    Ok(report) => Ok(report),
                    Err(error) if is_stale_history_error(&error) => {
                        failure_mode = store::mailbox::SyncMode::Full;
                        failure_cursor_history_id = None;
                        failure_bootstrap_query = persisted_bootstrap_query;
                        run_full_sync(&sync_context, &mut pacing, persisted_bootstrap_query, true)
                            .await
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }
    .await;

    match result {
        Ok(mut report) => {
            observe_latest_metrics(&mut pacing, &gmail_client)?;
            let pacing_report = pacing.report();
            let now_epoch_s = current_epoch_seconds()?;
            let _ = store_handle
                .upsert_sync_pacing_state(pacing.finalize_success(&account.account_id, now_epoch_s))
                .await?;
            let metrics = gmail_client.request_metrics_snapshot();
            populate_sync_report_metrics(&mut report, pacing_report, metrics, &options);
            Ok(report)
        }
        Err(sync_error) => {
            let pacing_persist_result =
                persist_sync_pacing_failure(&store_handle, &account, &mut pacing, &gmail_client)
                    .await;
            let persist_result = persist_sync_state_failure(
                &store_handle,
                &account,
                failure_bootstrap_query,
                failure_mode,
                failure_cursor_history_id,
                sync_error.to_string(),
            )
            .await;
            Err(preserve_sync_error(
                sync_error,
                persist_result.and(pacing_persist_result),
            ))
        }
    }
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
    gmail_client: &'a GmailClient,
    account: &'a AccountRecord,
    labels: &'a [GmailLabel],
    label_names_by_id: &'a BTreeMap<String, String>,
}

struct FullSyncCheckpointState {
    record: store::mailbox::FullSyncCheckpointRecord,
    resumed_from_checkpoint: bool,
}

async fn run_full_sync(
    context: &SyncExecutionContext<'_>,
    pacing: &mut AdaptiveSyncPacing,
    bootstrap_query: &str,
    fallback_from_history: bool,
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
            bootstrap_query,
            fallback_from_history,
            checkpoint.resumed_from_checkpoint,
            checkpoint.record,
            checkpoint_reused_pages,
            checkpoint_reused_messages_upserted,
        )
        .await;
    }

    let mut page_token = checkpoint.record.next_page_token.clone();
    let mut pages_fetched = checkpoint_reused_pages;
    let mut messages_listed =
        usize::try_from(checkpoint.record.messages_listed).unwrap_or(usize::MAX);
    let mut messages_upserted =
        usize::try_from(checkpoint.record.messages_upserted).unwrap_or(usize::MAX);
    let mut cursor_history_id = checkpoint.record.cursor_history_id.clone();

    loop {
        let page = match context
            .gmail_client
            .list_message_ids(
                Some(bootstrap_query),
                page_token.as_deref(),
                FULL_SYNC_PAGE_SIZE,
            )
            .await
        {
            Ok(page) => page,
            Err(error)
                if checkpoint.resumed_from_checkpoint
                    && page_token.is_some()
                    && is_invalid_resume_page_token_error(&error) =>
            {
                context.store_handle.reset_full_sync_progress().await?;
                return Box::pin(run_full_sync(
                    context,
                    pacing,
                    bootstrap_query,
                    fallback_from_history,
                ))
                .await;
            }
            Err(error) => return Err(error.into()),
        };
        pages_fetched += 1;
        messages_listed += page.messages.len();

        let message_ids = page
            .messages
            .iter()
            .map(|message| message.id.clone())
            .collect();
        let (catalogs, _) = fetch_message_catalogs(
            context.gmail_client.clone(),
            message_ids,
            pacing.current_message_fetch_concurrency(),
        )
        .await?;

        for catalog in &catalogs {
            cursor_history_id = newest_history_id(cursor_history_id, &catalog.metadata.history_id);
        }

        let page_upserts = build_upsert_inputs(
            &context.account.account_id,
            catalogs,
            context.label_names_by_id,
        );
        messages_upserted += page_upserts.len();
        let updated_at_epoch_s = current_epoch_seconds()?;
        checkpoint.record = context
            .store_handle
            .stage_full_sync_page_and_update_checkpoint(
                &page_upserts,
                store::mailbox::FullSyncCheckpointUpdate {
                    bootstrap_query: bootstrap_query.to_owned(),
                    status: match page.next_page_token {
                        Some(_) => store::mailbox::FullSyncCheckpointStatus::Paging,
                        None => store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize,
                    },
                    next_page_token: page.next_page_token.clone(),
                    cursor_history_id: cursor_history_id.clone(),
                    pages_fetched: i64::try_from(pages_fetched).unwrap_or(i64::MAX),
                    messages_listed: i64::try_from(messages_listed).unwrap_or(i64::MAX),
                    messages_upserted: i64::try_from(messages_upserted).unwrap_or(i64::MAX),
                    labels_synced: i64::try_from(context.labels.len()).unwrap_or(i64::MAX),
                    started_at_epoch_s: checkpoint.record.started_at_epoch_s,
                    updated_at_epoch_s,
                },
            )
            .await?;
        observe_latest_metrics(pacing, context.gmail_client)?;
        page_token = checkpoint.record.next_page_token.clone();
        if checkpoint.record.status == store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize {
            break;
        }
    }

    finalize_full_sync_report(
        context,
        bootstrap_query,
        fallback_from_history,
        checkpoint.resumed_from_checkpoint,
        checkpoint.record,
        checkpoint_reused_pages,
        checkpoint_reused_messages_upserted,
    )
    .await
}

async fn run_incremental_sync(
    context: &SyncExecutionContext<'_>,
    pacing: &mut AdaptiveSyncPacing,
    bootstrap_query: &str,
    cursor_history_id: Option<String>,
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
    let (catalogs, missing_message_ids) = fetch_message_catalogs(
        context.gmail_client.clone(),
        changed_message_ids,
        pacing.current_message_fetch_concurrency(),
    )
    .await?;
    observe_latest_metrics(pacing, context.gmail_client)?;
    let messages_listed = catalogs.len();
    let (upserts, excluded_message_ids) = build_incremental_changes(
        &context.account.account_id,
        catalogs,
        context.label_names_by_id,
    );
    let message_ids_to_delete = deleted_message_ids
        .into_iter()
        .chain(missing_message_ids)
        .chain(excluded_message_ids)
        .collect::<Vec<_>>();
    let messages_upserted = upserts.len();
    let now_epoch_s = current_epoch_seconds()?;
    let sync_update = store::mailbox::SyncStateUpdate {
        account_id: context.account.account_id.clone(),
        cursor_history_id: Some(latest_history_id.clone()),
        bootstrap_query: bootstrap_query.to_owned(),
        last_sync_mode: store::mailbox::SyncMode::Incremental,
        last_sync_status: store::mailbox::SyncStatus::Ok,
        last_error: None,
        last_sync_epoch_s: now_epoch_s,
        last_full_sync_success_epoch_s: None,
        last_incremental_sync_success_epoch_s: Some(now_epoch_s),
    };
    let (sync_state, messages_deleted) = context
        .store_handle
        .commit_incremental_sync(store::mailbox::IncrementalSyncCommit {
            labels: context.labels,
            messages_to_upsert: &upserts,
            message_ids_to_delete: &message_ids_to_delete,
            updated_at_epoch_s: now_epoch_s,
            sync_state_update: &sync_update,
        })
        .await?;
    let finalize_input = FinalizeSyncInput {
        mode: store::mailbox::SyncMode::Incremental,
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
    };

    finalize_sync(sync_state, bootstrap_query, finalize_input)
}

fn success_sync_state_update(
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
    }
}

fn finalize_sync(
    sync_state: store::mailbox::SyncStateRecord,
    bootstrap_query: &str,
    input: FinalizeSyncInput,
) -> Result<SyncRunReport> {
    Ok(SyncRunReport {
        mode: input.mode,
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
    })
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

fn populate_sync_report_metrics(
    report: &mut SyncRunReport,
    pacing_report: AdaptiveSyncPacingReport,
    metrics: Option<crate::gmail::GmailQuotaMetricsSnapshot>,
    options: &SyncRunOptions,
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

async fn load_sync_state(
    store_handle: &MailboxStoreHandle,
) -> Result<Option<store::mailbox::SyncStateRecord>> {
    store_handle.load_sync_state().await
}

async fn persist_sync_state_failure(
    store_handle: &MailboxStoreHandle,
    account: &AccountRecord,
    bootstrap_query: &str,
    mode: store::mailbox::SyncMode,
    cursor_history_id: Option<String>,
    error: String,
) -> Result<()> {
    let now_epoch_s = current_epoch_seconds()?;
    let _ = store_handle
        .upsert_sync_state(store::mailbox::SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id,
            bootstrap_query: bootstrap_query.to_owned(),
            last_sync_mode: mode,
            last_sync_status: store::mailbox::SyncStatus::Failed,
            last_error: Some(error),
            last_sync_epoch_s: now_epoch_s,
            last_full_sync_success_epoch_s: None,
            last_incremental_sync_success_epoch_s: None,
        })
        .await?;
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
            context.store_handle.reset_full_sync_progress().await?;
            let record = context
                .store_handle
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
                .store_handle
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
                context.store_handle.reset_full_sync_progress().await?;
                let record = context
                    .store_handle
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
                .store_handle
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
    bootstrap_query: &str,
    fallback_from_history: bool,
    resumed_from_checkpoint: bool,
    checkpoint: store::mailbox::FullSyncCheckpointRecord,
    checkpoint_reused_pages: usize,
    checkpoint_reused_messages_upserted: usize,
) -> Result<SyncRunReport> {
    let finalize_input = FinalizeSyncInput {
        mode: store::mailbox::SyncMode::Full,
        fallback_from_history,
        resumed_from_checkpoint,
        cursor_history_id: checkpoint.cursor_history_id.clone(),
        pages_fetched: usize::try_from(checkpoint.pages_fetched).unwrap_or(usize::MAX),
        messages_listed: usize::try_from(checkpoint.messages_listed).unwrap_or(usize::MAX),
        messages_upserted: usize::try_from(checkpoint.messages_upserted).unwrap_or(usize::MAX),
        messages_deleted: 0,
        labels_synced: usize::try_from(checkpoint.labels_synced).unwrap_or(usize::MAX),
        checkpoint_reused_pages,
        checkpoint_reused_messages_upserted,
    };
    let now_epoch_s = current_epoch_seconds()?;
    let sync_state = context
        .store_handle
        .finalize_full_sync_from_stage(
            now_epoch_s,
            success_sync_state_update(
                context.account,
                bootstrap_query,
                &finalize_input,
                now_epoch_s,
            ),
        )
        .await?;

    finalize_sync(sync_state, bootstrap_query, finalize_input)
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

fn build_upsert_inputs(
    account_id: &str,
    catalogs: Vec<GmailMessageCatalog>,
    label_names_by_id: &BTreeMap<String, String>,
) -> Vec<store::mailbox::GmailMessageUpsertInput> {
    catalogs
        .into_iter()
        .filter(|catalog| !message_is_excluded(&catalog.metadata.label_ids))
        .map(|catalog| build_upsert_input(account_id.to_owned(), catalog, label_names_by_id))
        .collect()
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

    async fn reset_full_sync_progress(&self) -> Result<()> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        spawn_blocking(move || {
            store::mailbox::reset_full_sync_progress(&database_path, busy_timeout_ms, &account_id)
        })
        .await?
    }

    async fn prepare_full_sync_checkpoint(
        &self,
        labels: &[GmailLabel],
        update: store::mailbox::FullSyncCheckpointUpdate,
    ) -> Result<store::mailbox::FullSyncCheckpointRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        let labels = labels.to_vec();
        spawn_blocking(move || {
            store::mailbox::prepare_full_sync_checkpoint(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &labels,
                &update,
            )
        })
        .await?
    }

    async fn update_full_sync_checkpoint_labels(
        &self,
        labels: &[GmailLabel],
        update: store::mailbox::FullSyncCheckpointUpdate,
    ) -> Result<store::mailbox::FullSyncCheckpointRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        let labels = labels.to_vec();
        spawn_blocking(move || {
            store::mailbox::update_full_sync_checkpoint_labels(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &labels,
                &update,
            )
        })
        .await?
    }

    async fn stage_full_sync_page_and_update_checkpoint(
        &self,
        messages: &[store::mailbox::GmailMessageUpsertInput],
        update: store::mailbox::FullSyncCheckpointUpdate,
    ) -> Result<store::mailbox::FullSyncCheckpointRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        let messages = messages.to_vec();
        spawn_blocking(move || {
            store::mailbox::stage_full_sync_page_and_update_checkpoint(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &messages,
                &update,
            )
        })
        .await?
    }

    async fn finalize_full_sync_from_stage(
        &self,
        updated_at_epoch_s: i64,
        sync_state_update: store::mailbox::SyncStateUpdate,
    ) -> Result<store::mailbox::SyncStateRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        spawn_blocking(move || {
            store::mailbox::finalize_full_sync_from_stage(
                &database_path,
                busy_timeout_ms,
                &account_id,
                updated_at_epoch_s,
                &sync_state_update,
            )
        })
        .await?
    }

    async fn commit_incremental_sync(
        &self,
        commit: store::mailbox::IncrementalSyncCommit<'_>,
    ) -> Result<(store::mailbox::SyncStateRecord, usize)> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        let labels = commit.labels.to_vec();
        let messages_to_upsert = commit.messages_to_upsert.to_vec();
        let message_ids_to_delete = commit.message_ids_to_delete.to_vec();
        let updated_at_epoch_s = commit.updated_at_epoch_s;
        let sync_state_update = commit.sync_state_update.clone();
        spawn_blocking(move || {
            store::mailbox::commit_incremental_sync(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &store::mailbox::IncrementalSyncCommit {
                    labels: &labels,
                    messages_to_upsert: &messages_to_upsert,
                    message_ids_to_delete: &message_ids_to_delete,
                    updated_at_epoch_s,
                    sync_state_update: &sync_state_update,
                },
            )
        })
        .await?
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
