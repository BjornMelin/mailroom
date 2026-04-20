use super::read::{count_indexed_messages, count_labels, count_messages, read_sync_state};
use super::{
    AttachmentExportEventInput, AttachmentVaultStateUpdate, GmailMessageUpsertInput,
    MailboxWriteError, SyncStateRecord, SyncStateUpdate, unique_sorted_strings,
};
use crate::gmail::GmailLabel;
use crate::store::connection;
use anyhow::{Result, anyhow, ensure};
use rusqlite::{ToSql, params, params_from_iter};
use std::collections::BTreeMap;
use std::path::Path;

const DELETE_BATCH_SIZE: usize = 400;

pub(crate) struct IncrementalSyncCommit<'a> {
    pub(crate) labels: &'a [GmailLabel],
    pub(crate) messages_to_upsert: &'a [GmailMessageUpsertInput],
    pub(crate) message_ids_to_delete: &'a [String],
    pub(crate) updated_at_epoch_s: i64,
    pub(crate) sync_state_update: &'a SyncStateUpdate,
}

#[cfg(test)]
pub(crate) fn replace_labels(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    updated_at_epoch_s: i64,
) -> Result<usize> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    replace_labels_in_transaction(&transaction, account_id, labels, updated_at_epoch_s)?;
    transaction.commit()?;
    Ok(labels.len())
}

#[cfg(test)]
pub(crate) fn replace_labels_and_report_reindex(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    updated_at_epoch_s: i64,
) -> Result<bool> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let reindexed =
        replace_labels_in_transaction(&transaction, account_id, labels, updated_at_epoch_s)?;
    transaction.commit()?;
    Ok(reindexed)
}

pub(crate) fn commit_full_sync(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    messages: &[GmailMessageUpsertInput],
    updated_at_epoch_s: i64,
    sync_state_update: &SyncStateUpdate,
) -> Result<SyncStateRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let preserved_attachment_vaults =
        load_attachment_vault_state_for_account(&transaction, account_id)?;

    let _should_reindex_search =
        replace_labels_in_transaction(&transaction, account_id, labels, updated_at_epoch_s)?;
    delete_account_messages(&transaction, account_id)?;
    write_messages(
        &transaction,
        account_id,
        messages,
        updated_at_epoch_s,
        Some(&preserved_attachment_vaults),
    )?;
    let record = upsert_sync_state_in_transaction(&transaction, sync_state_update)?;

    transaction.commit()?;
    Ok(record)
}

pub(crate) fn commit_incremental_sync(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    commit: &IncrementalSyncCommit<'_>,
) -> Result<(SyncStateRecord, usize)> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;

    let should_reindex_search = replace_labels_in_transaction(
        &transaction,
        account_id,
        commit.labels,
        commit.updated_at_epoch_s,
    )?;
    let deleted =
        delete_messages_in_transaction(&transaction, account_id, commit.message_ids_to_delete)?;
    write_messages(
        &transaction,
        account_id,
        commit.messages_to_upsert,
        commit.updated_at_epoch_s,
        None,
    )?;
    if should_reindex_search {
        reindex_message_search_for_account(&transaction, account_id)?;
    }
    let record = upsert_sync_state_in_transaction(&transaction, commit.sync_state_update)?;

    transaction.commit()?;
    Ok((record, deleted))
}

#[cfg(test)]
pub(crate) fn replace_messages(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    updated_at_epoch_s: i64,
) -> Result<usize> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    delete_account_messages(&transaction, account_id)?;
    let replaced = write_messages(&transaction, account_id, messages, updated_at_epoch_s, None)?;
    transaction.commit()?;
    Ok(replaced)
}

#[cfg(test)]
pub(crate) fn upsert_messages(
    database_path: &Path,
    busy_timeout_ms: u64,
    messages: &[GmailMessageUpsertInput],
    updated_at_epoch_s: i64,
) -> Result<usize> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let account_id = messages
        .first()
        .map(|message| message.account_id.as_str())
        .unwrap_or("");
    let updated = write_messages(&transaction, account_id, messages, updated_at_epoch_s, None)?;
    transaction.commit()?;
    Ok(updated)
}

#[cfg(test)]
pub(crate) fn apply_incremental_changes(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    messages_to_upsert: &[GmailMessageUpsertInput],
    message_ids_to_delete: &[String],
    updated_at_epoch_s: i64,
) -> Result<usize> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let deleted = delete_messages_in_transaction(&transaction, account_id, message_ids_to_delete)?;
    write_messages(
        &transaction,
        account_id,
        messages_to_upsert,
        updated_at_epoch_s,
        None,
    )?;
    transaction.commit()?;
    Ok(deleted)
}

#[cfg(test)]
pub(crate) fn delete_messages(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    message_ids: &[String],
) -> Result<usize> {
    if message_ids.is_empty() {
        return Ok(0);
    }

    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let deleted = delete_messages_in_transaction(&transaction, account_id, message_ids)?;
    transaction.commit()?;
    Ok(deleted)
}

fn delete_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    message_ids: &[String],
) -> Result<usize> {
    let mut deleted = 0usize;
    let message_ids = unique_sorted_strings(message_ids);

    for message_id_chunk in message_ids.chunks(DELETE_BATCH_SIZE) {
        let placeholders = repeat_sql_placeholders(message_id_chunk.len());
        let lookup_sql = format!(
            "SELECT message_rowid
             FROM gmail_messages
             WHERE account_id = ?1
               AND message_id IN ({placeholders})"
        );
        let delete_message_sql = format!(
            "DELETE FROM gmail_messages
             WHERE account_id = ?1
               AND message_id IN ({placeholders})"
        );

        let mut lookup = transaction.prepare(&lookup_sql)?;
        let mut lookup_params: Vec<&dyn ToSql> = Vec::with_capacity(message_id_chunk.len() + 1);
        lookup_params.push(&account_id);
        lookup_params.extend(
            message_id_chunk
                .iter()
                .map(|message_id| message_id as &dyn ToSql),
        );
        let rowids = lookup
            .query_map(params_from_iter(lookup_params.iter().copied()), |row| {
                row.get(0)
            })?
            .collect::<rusqlite::Result<Vec<i64>>>()?;

        if rowids.is_empty() {
            continue;
        }

        let search_placeholders = repeat_sql_placeholders(rowids.len());
        let delete_search_sql =
            format!("DELETE FROM gmail_message_search WHERE rowid IN ({search_placeholders})");
        let mut delete_search = transaction.prepare(&delete_search_sql)?;
        let search_params = rowids
            .iter()
            .map(|rowid| rowid as &dyn ToSql)
            .collect::<Vec<_>>();
        delete_search.execute(params_from_iter(search_params))?;

        let mut delete_message = transaction.prepare(&delete_message_sql)?;
        deleted += delete_message.execute(params_from_iter(lookup_params.iter().copied()))?;
    }

    Ok(deleted)
}

fn repeat_sql_placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn upsert_sync_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    update: &SyncStateUpdate,
) -> Result<SyncStateRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let record = upsert_sync_state_in_transaction(&transaction, update)?;
    transaction.commit()?;
    Ok(record)
}

fn replace_labels_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    labels: &[GmailLabel],
    updated_at_epoch_s: i64,
) -> Result<bool> {
    let should_reindex_search = label_names_changed(transaction, account_id, labels)?;
    transaction.execute(
        "DELETE FROM gmail_labels WHERE account_id = ?1",
        [account_id],
    )?;

    let mut insert = transaction.prepare_cached(
        "INSERT INTO gmail_labels (
             account_id,
             label_id,
             name,
             label_type,
             message_list_visibility,
             label_list_visibility,
             messages_total,
             messages_unread,
             threads_total,
             threads_unread,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )?;

    for label in labels {
        insert.execute(params![
            account_id,
            &label.id,
            &label.name,
            &label.label_type,
            &label.message_list_visibility,
            &label.label_list_visibility,
            label.messages_total,
            label.messages_unread,
            label.threads_total,
            label.threads_unread,
            updated_at_epoch_s,
        ])?;
    }

    drop(insert);
    Ok(should_reindex_search)
}

fn label_names_changed(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    labels: &[GmailLabel],
) -> Result<bool> {
    let existing_labels = {
        let mut query = transaction.prepare_cached(
            "SELECT label_id, name
             FROM gmail_labels
             WHERE account_id = ?1
             ORDER BY label_id, name",
        )?;
        query
            .query_map([account_id], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<(String, String)>>>()?
    };

    let mut next_labels = labels
        .iter()
        .map(|label| (label.id.clone(), label.name.clone()))
        .collect::<Vec<_>>();
    next_labels.sort_unstable();

    Ok(existing_labels != next_labels)
}

fn upsert_sync_state_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    update: &SyncStateUpdate,
) -> Result<SyncStateRecord> {
    let message_count = count_messages(transaction, Some(&update.account_id))?;
    let label_count = count_labels(transaction, Some(&update.account_id))?;
    let indexed_message_count = count_indexed_messages(transaction, Some(&update.account_id))?;

    transaction.execute(
        "INSERT INTO gmail_sync_state (
             account_id,
             cursor_history_id,
             bootstrap_query,
             last_sync_mode,
             last_sync_status,
             last_error,
             last_sync_epoch_s,
             last_full_sync_success_epoch_s,
             last_incremental_sync_success_epoch_s,
             message_count,
             label_count,
             indexed_message_count
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT (account_id) DO UPDATE SET
             cursor_history_id = excluded.cursor_history_id,
             bootstrap_query = excluded.bootstrap_query,
             last_sync_mode = excluded.last_sync_mode,
             last_sync_status = excluded.last_sync_status,
             last_error = excluded.last_error,
             last_sync_epoch_s = excluded.last_sync_epoch_s,
             last_full_sync_success_epoch_s = COALESCE(
                 excluded.last_full_sync_success_epoch_s,
                 gmail_sync_state.last_full_sync_success_epoch_s
             ),
             last_incremental_sync_success_epoch_s = COALESCE(
                 excluded.last_incremental_sync_success_epoch_s,
                 gmail_sync_state.last_incremental_sync_success_epoch_s
             ),
             message_count = excluded.message_count,
             label_count = excluded.label_count,
             indexed_message_count = excluded.indexed_message_count",
        params![
            &update.account_id,
            &update.cursor_history_id,
            &update.bootstrap_query,
            update.last_sync_mode.as_str(),
            update.last_sync_status.as_str(),
            &update.last_error,
            update.last_sync_epoch_s,
            update.last_full_sync_success_epoch_s,
            update.last_incremental_sync_success_epoch_s,
            message_count,
            label_count,
            indexed_message_count,
        ],
    )?;

    let record = read_sync_state(transaction, &update.account_id)?
        .ok_or_else(|| anyhow!("failed to read sync state for {}", update.account_id))?;
    Ok(record)
}

fn delete_account_messages(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_message_search
         WHERE rowid IN (
             SELECT message_rowid
             FROM gmail_messages
             WHERE account_id = ?1
         )",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_messages WHERE account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreservedAttachmentVaultState {
    content_hash: String,
    relative_path: String,
    size_bytes: i64,
    fetched_at_epoch_s: i64,
}

fn load_attachment_vault_state_for_account(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<BTreeMap<String, PreservedAttachmentVaultState>> {
    let mut query = transaction.prepare_cached(
        "SELECT
             gma.attachment_key,
             gma.vault_content_hash,
             gma.vault_relative_path,
             gma.vault_size_bytes,
             gma.vault_fetched_at_epoch_s
         FROM gmail_message_attachments gma
         WHERE gma.account_id = ?1
           AND gma.vault_content_hash IS NOT NULL
           AND gma.vault_relative_path IS NOT NULL
           AND gma.vault_size_bytes IS NOT NULL
           AND gma.vault_fetched_at_epoch_s IS NOT NULL",
    )?;
    let rows = query
        .query_map([account_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                PreservedAttachmentVaultState {
                    content_hash: row.get(1)?,
                    relative_path: row.get(2)?,
                    size_bytes: row.get(3)?,
                    fetched_at_epoch_s: row.get(4)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.into_iter().collect())
}

fn load_attachment_vault_state_for_message(
    transaction: &rusqlite::Transaction<'_>,
    message_rowid: i64,
) -> Result<BTreeMap<String, PreservedAttachmentVaultState>> {
    let mut query = transaction.prepare_cached(
        "SELECT
             attachment_key,
             vault_content_hash,
             vault_relative_path,
             vault_size_bytes,
             vault_fetched_at_epoch_s
         FROM gmail_message_attachments
         WHERE message_rowid = ?1
           AND vault_content_hash IS NOT NULL
           AND vault_relative_path IS NOT NULL
           AND vault_size_bytes IS NOT NULL
           AND vault_fetched_at_epoch_s IS NOT NULL",
    )?;
    let rows = query
        .query_map([message_rowid], |row| {
            Ok((
                row.get::<_, String>(0)?,
                PreservedAttachmentVaultState {
                    content_hash: row.get(1)?,
                    relative_path: row.get(2)?,
                    size_bytes: row.get(3)?,
                    fetched_at_epoch_s: row.get(4)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows.into_iter().collect())
}

fn write_messages(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    updated_at_epoch_s: i64,
    preserved_attachment_vaults: Option<&BTreeMap<String, PreservedAttachmentVaultState>>,
) -> Result<usize> {
    if messages.is_empty() {
        return Ok(0);
    }
    ensure!(
        !account_id.is_empty(),
        "mailbox messages must have a non-empty account_id"
    );

    let mut upsert_message = transaction.prepare_cached(
        "INSERT INTO gmail_messages (
             account_id,
             message_id,
             thread_id,
             history_id,
             internal_date_epoch_ms,
             snippet,
             subject,
             from_header,
             from_address,
             recipient_headers,
             to_header,
             cc_header,
             bcc_header,
             reply_to_header,
             size_estimate,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
         ON CONFLICT (account_id, message_id) DO UPDATE SET
             thread_id = excluded.thread_id,
             history_id = excluded.history_id,
             internal_date_epoch_ms = excluded.internal_date_epoch_ms,
             snippet = excluded.snippet,
             subject = excluded.subject,
             from_header = excluded.from_header,
             from_address = excluded.from_address,
             recipient_headers = excluded.recipient_headers,
             to_header = excluded.to_header,
             cc_header = excluded.cc_header,
             bcc_header = excluded.bcc_header,
             reply_to_header = excluded.reply_to_header,
             size_estimate = excluded.size_estimate,
             updated_at_epoch_s = excluded.updated_at_epoch_s
         RETURNING message_rowid",
    )?;
    let mut delete_message_labels =
        transaction.prepare_cached("DELETE FROM gmail_message_labels WHERE message_rowid = ?1")?;
    let mut delete_message_attachments = transaction
        .prepare_cached("DELETE FROM gmail_message_attachments WHERE message_rowid = ?1")?;
    let mut insert_message_label = transaction.prepare_cached(
        "INSERT INTO gmail_message_labels (message_rowid, label_id) VALUES (?1, ?2)",
    )?;
    let mut insert_attachment = transaction.prepare_cached(
        "INSERT INTO gmail_message_attachments (
             account_id,
             message_rowid,
             attachment_key,
             part_id,
             gmail_attachment_id,
             filename,
             mime_type,
             size_bytes,
             content_disposition,
             content_id,
             is_inline,
             vault_content_hash,
             vault_relative_path,
             vault_size_bytes,
             vault_fetched_at_epoch_s,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
    )?;
    let mut upsert_search = transaction.prepare_cached(
        "INSERT OR REPLACE INTO gmail_message_search (
             rowid,
             subject,
             from_header,
             recipient_headers,
             snippet,
             label_names
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;

    for message in messages {
        ensure!(
            message.account_id == account_id,
            "mailbox message account_id `{}` does not match batch account `{}`",
            message.account_id,
            account_id
        );
        let message_rowid: i64 = upsert_message.query_row(
            params![
                account_id,
                &message.message_id,
                &message.thread_id,
                &message.history_id,
                message.internal_date_epoch_ms,
                &message.snippet,
                &message.subject,
                &message.from_header,
                &message.from_address,
                &message.recipient_headers,
                &message.to_header,
                &message.cc_header,
                &message.bcc_header,
                &message.reply_to_header,
                message.size_estimate,
                updated_at_epoch_s,
            ],
            |row| row.get(0),
        )?;

        let message_attachment_vaults;
        let existing_attachment_vaults = match preserved_attachment_vaults {
            Some(preserved) => preserved,
            None => {
                message_attachment_vaults =
                    load_attachment_vault_state_for_message(transaction, message_rowid)?;
                &message_attachment_vaults
            }
        };
        delete_message_labels.execute([message_rowid])?;
        delete_message_attachments.execute([message_rowid])?;
        for label_id in unique_sorted_strings(&message.label_ids) {
            insert_message_label.execute(params![message_rowid, label_id])?;
        }
        for attachment in &message.attachments {
            let preserved = existing_attachment_vaults.get(&attachment.attachment_key);
            insert_attachment.execute(params![
                account_id,
                message_rowid,
                &attachment.attachment_key,
                &attachment.part_id,
                &attachment.gmail_attachment_id,
                &attachment.filename,
                &attachment.mime_type,
                attachment.size_bytes,
                &attachment.content_disposition,
                &attachment.content_id,
                if attachment.is_inline { 1_i64 } else { 0_i64 },
                preserved.map(|state| state.content_hash.as_str()),
                preserved.map(|state| state.relative_path.as_str()),
                preserved.map(|state| state.size_bytes),
                preserved.map(|state| state.fetched_at_epoch_s),
                updated_at_epoch_s,
            ])?;
        }

        upsert_search.execute(params![
            message_rowid,
            &message.subject,
            &message.from_header,
            &message.recipient_headers,
            &message.snippet,
            &message.label_names_text,
        ])?;
    }

    drop(upsert_search);
    drop(insert_attachment);
    drop(insert_message_label);
    drop(delete_message_attachments);
    drop(delete_message_labels);
    drop(upsert_message);
    Ok(messages.len())
}

pub(crate) fn set_attachment_vault_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    update: &AttachmentVaultStateUpdate,
) -> Result<(), MailboxWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(MailboxWriteError::from)?;
    let transaction = connection.transaction()?;
    let rows_updated = transaction.execute(
        "UPDATE gmail_message_attachments
         SET vault_content_hash = ?3,
             vault_relative_path = ?4,
             vault_size_bytes = ?5,
             vault_fetched_at_epoch_s = ?6
         WHERE account_id = ?1
           AND attachment_key = ?2",
        params![
            &update.account_id,
            &update.attachment_key,
            &update.content_hash,
            &update.relative_path,
            update.size_bytes,
            update.fetched_at_epoch_s,
        ],
    )?;
    if rows_updated == 0 {
        return Err(MailboxWriteError::AttachmentNotFound {
            account_id: update.account_id.clone(),
            attachment_key: update.attachment_key.clone(),
        });
    }
    if rows_updated != 1 {
        return Err(MailboxWriteError::Unexpected(anyhow!(
            "attachment vault update unexpectedly touched {rows_updated} rows for account `{}` and key `{}`",
            update.account_id,
            update.attachment_key
        )));
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn record_attachment_export(
    database_path: &Path,
    busy_timeout_ms: u64,
    event: &AttachmentExportEventInput,
) -> Result<()> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO attachment_export_events (
             account_id,
             attachment_key,
             message_id,
             thread_id,
             destination_path,
             content_hash,
             exported_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            &event.account_id,
            &event.attachment_key,
            &event.message_id,
            &event.thread_id,
            &event.destination_path,
            &event.content_hash,
            event.exported_at_epoch_s,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

fn reindex_message_search_for_account(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "INSERT OR REPLACE INTO gmail_message_search (
             rowid,
             subject,
             from_header,
             recipient_headers,
             snippet,
             label_names
         )
         SELECT
             gm.message_rowid,
             gm.subject,
             gm.from_header,
             gm.recipient_headers,
             gm.snippet,
             COALESCE(group_concat(gl.name, ' '), '')
         FROM gmail_messages gm
         LEFT JOIN gmail_message_labels gml
           ON gml.message_rowid = gm.message_rowid
         LEFT JOIN gmail_labels gl
           ON gl.account_id = gm.account_id
          AND gl.label_id = gml.label_id
         WHERE gm.account_id = ?1
         GROUP BY
             gm.message_rowid,
             gm.subject,
             gm.from_header,
             gm.recipient_headers,
             gm.snippet",
        [account_id],
    )?;
    Ok(())
}
