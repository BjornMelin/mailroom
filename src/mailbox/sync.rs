use crate::config::ConfigReport;
use crate::gmail::{GmailClient, GmailLabel, GmailMessageCatalog};
use crate::mailbox::model::FinalizeSyncInput;
use crate::mailbox::util::{
    bootstrap_query, is_stale_history_error, labels_by_id, message_is_excluded, newest_history_id,
    recipient_headers,
};
use crate::mailbox::{FULL_SYNC_PAGE_SIZE, MESSAGE_FETCH_CONCURRENCY, SyncRunReport};
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
    if recent_days == 0 {
        return Err(anyhow!("recent_days must be greater than zero"));
    }

    let account = crate::refresh_active_account_record(config_report).await?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
    let store_handle = MailboxStoreHandle::new(config_report, &account.account_id);
    let requested_bootstrap_query = bootstrap_query(recent_days);
    let existing_sync_state = load_sync_state(&store_handle).await?;
    let initial_mode = sync_mode(force_full, existing_sync_state.as_ref());
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

        match initial_mode {
            store::mailbox::SyncMode::Full => {
                run_full_sync(
                    &store_handle,
                    &gmail_client,
                    &account,
                    &labels,
                    initial_bootstrap_query,
                    &label_names_by_id,
                    false,
                )
                .await
            }
            store::mailbox::SyncMode::Incremental => {
                let sync_state = existing_sync_state
                    .as_ref()
                    .ok_or_else(|| anyhow!("incremental sync requires an existing sync state"))?;
                match run_incremental_sync(
                    &store_handle,
                    &gmail_client,
                    &account,
                    &labels,
                    persisted_bootstrap_query,
                    &label_names_by_id,
                    sync_state.cursor_history_id.clone(),
                )
                .await
                {
                    Ok(report) => Ok(report),
                    Err(error) if is_stale_history_error(&error) => {
                        failure_mode = store::mailbox::SyncMode::Full;
                        failure_cursor_history_id = None;
                        failure_bootstrap_query = persisted_bootstrap_query;
                        run_full_sync(
                            &store_handle,
                            &gmail_client,
                            &account,
                            &labels,
                            persisted_bootstrap_query,
                            &label_names_by_id,
                            true,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }
    .await;

    match result {
        Ok(report) => Ok(report),
        Err(sync_error) => {
            let persist_result = persist_sync_state_failure(
                &store_handle,
                &account,
                failure_bootstrap_query,
                failure_mode,
                failure_cursor_history_id,
                sync_error.to_string(),
            )
            .await;
            Err(preserve_sync_error(sync_error, persist_result))
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

async fn run_full_sync(
    store_handle: &MailboxStoreHandle,
    gmail_client: &GmailClient,
    account: &AccountRecord,
    labels: &[GmailLabel],
    bootstrap_query: &str,
    label_names_by_id: &BTreeMap<String, String>,
    fallback_from_history: bool,
) -> Result<SyncRunReport> {
    let mut page_token = None;
    let mut pages_fetched = 0usize;
    let mut messages_listed = 0usize;
    let mut messages_upserted = 0usize;
    let mut cursor_history_id = Some(account.history_id.clone());
    let mut upserts = Vec::new();

    loop {
        let page = gmail_client
            .list_message_ids(
                Some(bootstrap_query),
                page_token.as_deref(),
                FULL_SYNC_PAGE_SIZE,
            )
            .await?;
        pages_fetched += 1;
        messages_listed += page.messages.len();

        let catalogs = if let Some(first_message) = page.messages.first() {
            let mut catalogs = Vec::new();
            if let Some(first_catalog) = gmail_client
                .get_message_catalog_if_present(&first_message.id)
                .await?
            {
                catalogs.push(first_catalog);
            }
            let remaining_ids = page
                .messages
                .iter()
                .skip(1)
                .map(|message| message.id.clone())
                .collect();
            let (remaining_catalogs, _) =
                fetch_message_catalogs(gmail_client.clone(), remaining_ids).await?;
            catalogs.extend(remaining_catalogs);
            catalogs
        } else {
            Vec::new()
        };

        for catalog in &catalogs {
            cursor_history_id = newest_history_id(cursor_history_id, &catalog.metadata.history_id);
        }

        let page_upserts = build_upsert_inputs(&account.account_id, catalogs, label_names_by_id);
        messages_upserted += page_upserts.len();
        upserts.extend(page_upserts);

        page_token = page.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    let finalize_input = FinalizeSyncInput {
        mode: store::mailbox::SyncMode::Full,
        fallback_from_history,
        cursor_history_id,
        pages_fetched,
        messages_listed,
        messages_upserted,
        messages_deleted: 0,
        labels_synced: labels.len(),
    };
    let now_epoch_s = current_epoch_seconds()?;
    let sync_state = store_handle
        .commit_full_sync(
            labels,
            upserts,
            now_epoch_s,
            success_sync_state_update(account, bootstrap_query, &finalize_input, now_epoch_s),
        )
        .await?;

    finalize_sync(sync_state, bootstrap_query, finalize_input)
}

async fn run_incremental_sync(
    store_handle: &MailboxStoreHandle,
    gmail_client: &GmailClient,
    account: &AccountRecord,
    labels: &[GmailLabel],
    bootstrap_query: &str,
    label_names_by_id: &BTreeMap<String, String>,
    cursor_history_id: Option<String>,
) -> Result<SyncRunReport> {
    let cursor_history_id =
        cursor_history_id.ok_or_else(|| anyhow!("incremental sync requires a history cursor"))?;
    let mut page_token = None;
    let mut pages_fetched = 0usize;
    let mut changed_message_ids = BTreeSet::new();
    let mut deleted_message_ids = BTreeSet::new();

    let latest_history_id = loop {
        let page = gmail_client
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

    for deleted_message_id in &deleted_message_ids {
        changed_message_ids.remove(deleted_message_id);
    }

    let changed_message_ids: Vec<String> = changed_message_ids.into_iter().collect();
    let (catalogs, missing_message_ids) =
        fetch_message_catalogs(gmail_client.clone(), changed_message_ids).await?;
    let messages_listed = catalogs.len();
    let (upserts, excluded_message_ids) =
        build_incremental_changes(&account.account_id, catalogs, label_names_by_id);
    let message_ids_to_delete = deleted_message_ids
        .into_iter()
        .chain(missing_message_ids)
        .chain(excluded_message_ids)
        .collect::<Vec<_>>();
    let messages_upserted = upserts.len();
    let now_epoch_s = current_epoch_seconds()?;
    let sync_update = store::mailbox::SyncStateUpdate {
        account_id: account.account_id.clone(),
        cursor_history_id: Some(latest_history_id.clone()),
        bootstrap_query: bootstrap_query.to_owned(),
        last_sync_mode: store::mailbox::SyncMode::Incremental,
        last_sync_status: store::mailbox::SyncStatus::Ok,
        last_error: None,
        last_sync_epoch_s: now_epoch_s,
        last_full_sync_success_epoch_s: None,
        last_incremental_sync_success_epoch_s: Some(now_epoch_s),
    };
    let (sync_state, messages_deleted) = store_handle
        .commit_incremental_sync(store::mailbox::IncrementalSyncCommit {
            labels,
            messages_to_upsert: &upserts,
            message_ids_to_delete: &message_ids_to_delete,
            updated_at_epoch_s: now_epoch_s,
            sync_state_update: &sync_update,
        })
        .await?;
    let finalize_input = FinalizeSyncInput {
        mode: store::mailbox::SyncMode::Incremental,
        fallback_from_history: false,
        cursor_history_id: Some(latest_history_id),
        pages_fetched,
        messages_listed,
        messages_upserted,
        messages_deleted,
        labels_synced: labels.len(),
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
        bootstrap_query: bootstrap_query.to_owned(),
        cursor_history_id: sync_state
            .cursor_history_id
            .ok_or_else(|| anyhow!("sync completed without a history cursor"))?,
        pages_fetched: input.pages_fetched,
        messages_listed: input.messages_listed,
        messages_upserted: input.messages_upserted,
        messages_deleted: input.messages_deleted,
        labels_synced: input.labels_synced,
        store_message_count: sync_state.message_count,
        store_label_count: sync_state.label_count,
        store_indexed_message_count: sync_state.indexed_message_count,
    })
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

fn preserve_sync_error(sync_error: anyhow::Error, persist_result: Result<()>) -> anyhow::Error {
    let _ = persist_result;
    sync_error
}

async fn fetch_message_catalogs(
    gmail_client: GmailClient,
    message_ids: Vec<String>,
) -> Result<(Vec<GmailMessageCatalog>, Vec<String>)> {
    if message_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let semaphore = std::sync::Arc::new(Semaphore::new(MESSAGE_FETCH_CONCURRENCY));
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

    async fn commit_full_sync(
        &self,
        labels: &[GmailLabel],
        messages: Vec<store::mailbox::GmailMessageUpsertInput>,
        updated_at_epoch_s: i64,
        sync_state_update: store::mailbox::SyncStateUpdate,
    ) -> Result<store::mailbox::SyncStateRecord> {
        let database_path = self.database_path.clone();
        let busy_timeout_ms = self.busy_timeout_ms;
        let account_id = self.account_id.clone();
        let labels = labels.to_vec();
        spawn_blocking(move || {
            store::mailbox::commit_full_sync(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &labels,
                &messages,
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
