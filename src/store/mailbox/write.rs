use super::read::{count_indexed_messages, count_labels, count_messages, read_sync_state};
use super::{GmailMessageUpsertInput, SyncStateRecord, SyncStateUpdate, unique_sorted_strings};
use crate::gmail::GmailLabel;
use crate::store::connection;
use anyhow::{Result, anyhow};
use rusqlite::{ToSql, params, params_from_iter};
use std::path::Path;

const DELETE_BATCH_SIZE: usize = 400;

pub(crate) fn replace_labels(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    updated_at_epoch_s: i64,
) -> Result<usize> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
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
    reindex_message_search_for_account(&transaction, account_id)?;
    transaction.commit()?;
    Ok(labels.len())
}

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
    let replaced = write_messages(&transaction, messages, updated_at_epoch_s)?;
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
    let updated = write_messages(&transaction, messages, updated_at_epoch_s)?;
    transaction.commit()?;
    Ok(updated)
}

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
    write_messages(&transaction, messages_to_upsert, updated_at_epoch_s)?;
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
    let message_count = count_messages(&transaction, Some(&update.account_id))?;
    let label_count = count_labels(&transaction, Some(&update.account_id))?;
    let indexed_message_count = count_indexed_messages(&transaction, Some(&update.account_id))?;

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

    let record = read_sync_state(&transaction, &update.account_id)?
        .ok_or_else(|| anyhow!("failed to read sync state for {}", update.account_id))?;
    transaction.commit()?;
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

fn write_messages(
    transaction: &rusqlite::Transaction<'_>,
    messages: &[GmailMessageUpsertInput],
    updated_at_epoch_s: i64,
) -> Result<usize> {
    if messages.is_empty() {
        return Ok(0);
    }

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
    let mut insert_message_label = transaction.prepare_cached(
        "INSERT INTO gmail_message_labels (message_rowid, label_id) VALUES (?1, ?2)",
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
        let message_rowid: i64 = upsert_message.query_row(
            params![
                &message.account_id,
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

        delete_message_labels.execute([message_rowid])?;
        for label_id in unique_sorted_strings(&message.label_ids) {
            insert_message_label.execute(params![message_rowid, label_id])?;
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
    drop(insert_message_label);
    drop(delete_message_labels);
    drop(upsert_message);
    Ok(messages.len())
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
