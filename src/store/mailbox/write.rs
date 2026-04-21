use super::read::{
    count_indexed_messages, count_labels, count_messages, read_full_sync_checkpoint,
    read_sync_pacing_state, read_sync_state,
};
use super::{
    AttachmentExportEventInput, AttachmentVaultStateUpdate, FullSyncCheckpointRecord,
    FullSyncCheckpointUpdate, FullSyncStagePageInput, GmailMessageUpsertInput, MailboxWriteError,
    SyncPacingStateRecord, SyncPacingStateUpdate, SyncStateRecord, SyncStateUpdate,
    unique_sorted_strings,
};
use crate::gmail::GmailLabel;
use crate::store::connection;
use anyhow::{Result, anyhow, ensure};
use rusqlite::{Connection, ToSql, TransactionBehavior, params, params_from_iter};
#[cfg(test)]
use std::collections::BTreeMap;
use std::path::Path;

const DELETE_BATCH_SIZE: usize = 400;
const FULL_SYNC_STAGE_PAGE_STATUS_PARTIAL: &str = "partial";
const FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE: &str = "complete";

pub(crate) struct MailboxWriterConnection {
    connection: Connection,
    account_id: String,
}

impl MailboxWriterConnection {
    pub(crate) fn open(
        database_path: &Path,
        busy_timeout_ms: u64,
        account_id: &str,
    ) -> Result<Self> {
        Ok(Self {
            connection: connection::open_or_create(database_path, busy_timeout_ms)?,
            account_id: account_id.to_owned(),
        })
    }

    pub(crate) fn reset_full_sync_progress(&mut self) -> Result<()> {
        reset_full_sync_progress_with_connection(&mut self.connection, &self.account_id)
    }

    pub(crate) fn prepare_full_sync_checkpoint(
        &mut self,
        labels: &[GmailLabel],
        update: &FullSyncCheckpointUpdate,
    ) -> Result<FullSyncCheckpointRecord> {
        prepare_full_sync_checkpoint_with_connection(
            &mut self.connection,
            &self.account_id,
            labels,
            update,
        )
    }

    pub(crate) fn update_full_sync_checkpoint_labels(
        &mut self,
        labels: &[GmailLabel],
        update: &FullSyncCheckpointUpdate,
    ) -> Result<FullSyncCheckpointRecord> {
        update_full_sync_checkpoint_labels_with_connection(
            &mut self.connection,
            &self.account_id,
            labels,
            update,
        )
    }

    pub(crate) fn stage_full_sync_page_chunk_and_maybe_update_checkpoint(
        &mut self,
        input: &FullSyncStagePageInput,
        messages: &[GmailMessageUpsertInput],
        checkpoint_update: Option<&FullSyncCheckpointUpdate>,
    ) -> Result<FullSyncCheckpointRecord> {
        stage_full_sync_page_chunk_and_maybe_update_checkpoint_with_connection(
            &mut self.connection,
            &self.account_id,
            input,
            messages,
            checkpoint_update,
        )
    }

    pub(crate) fn finalize_full_sync_from_stage(
        &mut self,
        updated_at_epoch_s: i64,
        sync_state_update: &SyncStateUpdate,
    ) -> Result<SyncStateRecord> {
        finalize_full_sync_from_stage_with_connection(
            &mut self.connection,
            &self.account_id,
            updated_at_epoch_s,
            sync_state_update,
        )
    }

    pub(crate) fn reset_incremental_sync_stage(&mut self) -> Result<()> {
        reset_incremental_sync_stage_with_connection(&mut self.connection, &self.account_id)
    }

    pub(crate) fn stage_incremental_sync_batch(
        &mut self,
        messages: &[GmailMessageUpsertInput],
        message_ids_to_delete: &[String],
    ) -> Result<()> {
        stage_incremental_sync_batch_with_connection(
            &mut self.connection,
            &self.account_id,
            messages,
            message_ids_to_delete,
        )
    }

    pub(crate) fn finalize_incremental_from_stage(
        &mut self,
        labels: &[GmailLabel],
        updated_at_epoch_s: i64,
        sync_state_update: &SyncStateUpdate,
    ) -> Result<(SyncStateRecord, usize)> {
        finalize_incremental_from_stage_with_connection(
            &mut self.connection,
            &self.account_id,
            labels,
            updated_at_epoch_s,
            sync_state_update,
        )
    }
}

#[cfg(test)]
pub(crate) struct IncrementalSyncCommit<'a> {
    pub(crate) labels: &'a [GmailLabel],
    pub(crate) messages_to_upsert: &'a [GmailMessageUpsertInput],
    pub(crate) message_ids_to_delete: &'a [String],
    pub(crate) updated_at_epoch_s: i64,
    pub(crate) sync_state_update: &'a SyncStateUpdate,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn prepare_full_sync_checkpoint(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    prepare_full_sync_checkpoint_with_connection(&mut connection, account_id, labels, update)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn update_full_sync_checkpoint_labels(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    update_full_sync_checkpoint_labels_with_connection(&mut connection, account_id, labels, update)
}

#[cfg(test)]
pub(crate) fn reset_full_sync_stage(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<()> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    reset_full_sync_stage_with_connection(&mut connection, account_id)
}

#[cfg(test)]
pub(crate) fn stage_full_sync_labels(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
) -> Result<()> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    stage_full_sync_labels_with_connection(&mut connection, account_id, labels)
}

#[cfg(test)]
pub(crate) fn stage_full_sync_messages(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    stage_full_sync_messages_with_connection(&mut connection, account_id, messages)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn stage_full_sync_page_and_update_checkpoint(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    stage_full_sync_page_and_update_checkpoint_with_connection(
        &mut connection,
        account_id,
        messages,
        update,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn finalize_full_sync_from_stage(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    updated_at_epoch_s: i64,
    sync_state_update: &SyncStateUpdate,
) -> Result<SyncStateRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    finalize_full_sync_from_stage_with_connection(
        &mut connection,
        account_id,
        updated_at_epoch_s,
        sync_state_update,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn reset_incremental_sync_stage(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<()> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    reset_incremental_sync_stage_with_connection(&mut connection, account_id)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn stage_incremental_sync_batch(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    message_ids_to_delete: &[String],
) -> Result<()> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    stage_incremental_sync_batch_with_connection(
        &mut connection,
        account_id,
        messages,
        message_ids_to_delete,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn finalize_incremental_from_stage(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    labels: &[GmailLabel],
    updated_at_epoch_s: i64,
    sync_state_update: &SyncStateUpdate,
) -> Result<(SyncStateRecord, usize)> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    finalize_incremental_from_stage_with_connection(
        &mut connection,
        account_id,
        labels,
        updated_at_epoch_s,
        sync_state_update,
    )
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

#[cfg(test)]
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
    reset_full_sync_stage_with_connection(&mut connection, account_id)?;
    stage_full_sync_labels_with_connection(&mut connection, account_id, labels)?;
    stage_full_sync_messages_with_connection(&mut connection, account_id, messages)?;
    let record = finalize_full_sync_from_stage_with_connection(
        &mut connection,
        account_id,
        updated_at_epoch_s,
        sync_state_update,
    )?;
    let _ = reset_full_sync_stage_with_connection(&mut connection, account_id);
    Ok(record)
}

#[cfg(test)]
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

pub(crate) fn upsert_sync_pacing_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    update: &SyncPacingStateUpdate,
) -> Result<SyncPacingStateRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;
    let record = upsert_sync_pacing_state_in_transaction(&transaction, update)?;
    transaction.commit()?;
    Ok(record)
}

fn reset_full_sync_progress_with_connection(
    connection: &mut Connection,
    account_id: &str,
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    clear_full_sync_checkpoint_in_transaction(&transaction, account_id)?;
    reset_full_sync_stage_in_transaction(&transaction, account_id)?;
    transaction.commit()?;
    Ok(())
}

fn reset_incremental_sync_stage_with_connection(
    connection: &mut Connection,
    account_id: &str,
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox incremental staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    reset_incremental_sync_stage_in_transaction(&transaction, account_id)?;
    transaction.commit()?;
    Ok(())
}

fn upsert_sync_pacing_state_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    update: &SyncPacingStateUpdate,
) -> Result<SyncPacingStateRecord> {
    transaction.execute(
        "INSERT INTO gmail_sync_pacing_state (
             account_id,
             learned_quota_units_per_minute,
             learned_message_fetch_concurrency,
             clean_run_streak,
             last_pressure_kind,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (account_id) DO UPDATE SET
             learned_quota_units_per_minute = excluded.learned_quota_units_per_minute,
             learned_message_fetch_concurrency = excluded.learned_message_fetch_concurrency,
             clean_run_streak = excluded.clean_run_streak,
             last_pressure_kind = excluded.last_pressure_kind,
             updated_at_epoch_s = excluded.updated_at_epoch_s",
        params![
            &update.account_id,
            update.learned_quota_units_per_minute,
            update.learned_message_fetch_concurrency,
            update.clean_run_streak,
            update.last_pressure_kind.map(|kind| kind.as_str()),
            update.updated_at_epoch_s,
        ],
    )?;

    let record = read_sync_pacing_state(transaction, &update.account_id)?
        .ok_or_else(|| anyhow!("sync pacing state disappeared after upsert"))?;
    Ok(record)
}

fn clear_full_sync_checkpoint_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_full_sync_checkpoint WHERE account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

fn prepare_full_sync_checkpoint_with_connection(
    connection: &mut Connection,
    account_id: &str,
    labels: &[GmailLabel],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    ensure_checkpoint_matches_account(account_id, update)?;

    let transaction = connection.transaction()?;
    clear_full_sync_checkpoint_in_transaction(&transaction, account_id)?;
    reset_full_sync_stage_in_transaction(&transaction, account_id)?;
    stage_full_sync_labels_in_transaction(&transaction, account_id, labels)?;
    let record = upsert_full_sync_checkpoint_in_transaction(&transaction, account_id, update)?;
    transaction.commit()?;
    Ok(record)
}

fn update_full_sync_checkpoint_labels_with_connection(
    connection: &mut Connection,
    account_id: &str,
    labels: &[GmailLabel],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    ensure_checkpoint_matches_account(account_id, update)?;

    let transaction = connection.transaction()?;
    cleanup_incomplete_full_sync_stage_pages_in_transaction(
        &transaction,
        account_id,
        update.pages_fetched,
    )?;
    stage_full_sync_labels_in_transaction(&transaction, account_id, labels)?;
    let record = upsert_full_sync_checkpoint_in_transaction(&transaction, account_id, update)?;
    transaction.commit()?;
    Ok(record)
}

#[cfg(test)]
fn reset_full_sync_stage_with_connection(
    connection: &mut Connection,
    account_id: &str,
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    reset_full_sync_stage_in_transaction(&transaction, account_id)?;
    transaction.commit()?;
    Ok(())
}

fn reset_full_sync_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_page_messages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_pages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_attachments WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_message_labels WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_messages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_labels WHERE account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

fn cleanup_incomplete_full_sync_stage_pages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    completed_page_count: i64,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_attachments
         WHERE account_id = ?1
           AND message_id IN (
               SELECT message_id
               FROM gmail_full_sync_stage_page_messages
               WHERE account_id = ?1
                 AND page_seq IN (
                     SELECT page_seq
                     FROM gmail_full_sync_stage_pages
                     WHERE account_id = ?1
                       AND (status != ?2 OR page_seq >= ?3)
                 )
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_message_labels
         WHERE account_id = ?1
           AND message_id IN (
               SELECT message_id
               FROM gmail_full_sync_stage_page_messages
               WHERE account_id = ?1
                 AND page_seq IN (
                     SELECT page_seq
                     FROM gmail_full_sync_stage_pages
                     WHERE account_id = ?1
                       AND (status != ?2 OR page_seq >= ?3)
                 )
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_messages
         WHERE account_id = ?1
           AND message_id IN (
               SELECT message_id
               FROM gmail_full_sync_stage_page_messages
               WHERE account_id = ?1
                 AND page_seq IN (
                     SELECT page_seq
                     FROM gmail_full_sync_stage_pages
                     WHERE account_id = ?1
                       AND (status != ?2 OR page_seq >= ?3)
                 )
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_page_messages
         WHERE account_id = ?1
           AND page_seq IN (
               SELECT page_seq
               FROM gmail_full_sync_stage_pages
               WHERE account_id = ?1
                 AND (status != ?2 OR page_seq >= ?3)
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_pages
         WHERE account_id = ?1
           AND (status != ?2 OR page_seq >= ?3)",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    Ok(())
}

fn reset_incremental_sync_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_attachments WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_message_labels WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_messages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_delete_ids WHERE account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

#[cfg(test)]
fn stage_full_sync_labels_with_connection(
    connection: &mut Connection,
    account_id: &str,
    labels: &[GmailLabel],
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    stage_full_sync_labels_in_transaction(&transaction, account_id, labels)?;
    transaction.commit()?;
    Ok(())
}

fn stage_full_sync_labels_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    labels: &[GmailLabel],
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_labels WHERE account_id = ?1",
        [account_id],
    )?;

    let mut insert = transaction.prepare_cached(
        "INSERT INTO gmail_full_sync_stage_labels (
             account_id,
             label_id,
             name,
             label_type,
             message_list_visibility,
             label_list_visibility,
             messages_total,
             messages_unread,
             threads_total,
             threads_unread
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT (account_id, label_id) DO UPDATE SET
             name = excluded.name,
             label_type = excluded.label_type,
             message_list_visibility = excluded.message_list_visibility,
             label_list_visibility = excluded.label_list_visibility,
             messages_total = excluded.messages_total,
             messages_unread = excluded.messages_unread,
             threads_total = excluded.threads_total,
             threads_unread = excluded.threads_unread",
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
        ])?;
    }

    drop(insert);
    Ok(())
}

#[cfg(test)]
fn stage_full_sync_messages_with_connection(
    connection: &mut Connection,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }

    ensure!(
        !account_id.is_empty(),
        "mailbox staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    stage_full_sync_messages_in_transaction(&transaction, account_id, None, messages)?;
    transaction.commit()?;
    Ok(())
}

fn stage_full_sync_page_and_update_checkpoint_with_connection(
    connection: &mut Connection,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    let page_seq = update.pages_fetched.saturating_sub(1);
    stage_full_sync_page_chunk_and_maybe_update_checkpoint_with_connection(
        connection,
        account_id,
        &FullSyncStagePageInput {
            page_seq,
            listed_count: i64::try_from(messages.len()).unwrap_or(i64::MAX),
            next_page_token: update.next_page_token.clone(),
            updated_at_epoch_s: update.updated_at_epoch_s,
            page_complete: true,
        },
        messages,
        Some(update),
    )
}

fn stage_full_sync_page_chunk_and_maybe_update_checkpoint_with_connection(
    connection: &mut Connection,
    account_id: &str,
    input: &FullSyncStagePageInput,
    messages: &[GmailMessageUpsertInput],
    checkpoint_update: Option<&FullSyncCheckpointUpdate>,
) -> Result<FullSyncCheckpointRecord> {
    if input.page_complete {
        let update = checkpoint_update.ok_or_else(|| {
            anyhow!("full sync page completion requires a checkpoint update payload")
        })?;
        ensure_checkpoint_matches_account(account_id, update)?;
    }

    let transaction = connection.transaction()?;
    stage_full_sync_messages_in_transaction(
        &transaction,
        account_id,
        Some(input.page_seq),
        messages,
    )?;
    upsert_full_sync_stage_page_in_transaction(
        &transaction,
        account_id,
        input,
        i64::try_from(messages.len()).unwrap_or(i64::MAX),
    )?;
    let record = if let Some(update) = checkpoint_update {
        upsert_full_sync_checkpoint_in_transaction(&transaction, account_id, update)?
    } else {
        read_full_sync_checkpoint(&transaction, account_id)?
            .ok_or_else(|| anyhow!("full sync checkpoint disappeared while staging page chunk"))?
    };
    transaction.commit()?;
    Ok(record)
}

fn stage_full_sync_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    page_seq: Option<i64>,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    let mut upsert_message = transaction.prepare_cached(
        "INSERT INTO gmail_full_sync_stage_messages (
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             label_names_text
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
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
             list_id_header = excluded.list_id_header,
             list_unsubscribe_header = excluded.list_unsubscribe_header,
             list_unsubscribe_post_header = excluded.list_unsubscribe_post_header,
             precedence_header = excluded.precedence_header,
             auto_submitted_header = excluded.auto_submitted_header,
             label_names_text = excluded.label_names_text",
    )?;
    let mut delete_labels = transaction.prepare_cached(
        "DELETE FROM gmail_full_sync_stage_message_labels
         WHERE account_id = ?1
           AND message_id = ?2",
    )?;
    let mut delete_attachments = transaction.prepare_cached(
        "DELETE FROM gmail_full_sync_stage_attachments
         WHERE account_id = ?1
           AND message_id = ?2",
    )?;
    let mut insert_label = transaction.prepare_cached(
        "INSERT INTO gmail_full_sync_stage_message_labels (
             account_id,
             message_id,
             label_id
         )
         VALUES (?1, ?2, ?3)",
    )?;
    let mut insert_attachment = transaction.prepare_cached(
        "INSERT INTO gmail_full_sync_stage_attachments (
             account_id,
             message_id,
             attachment_key,
             part_id,
             gmail_attachment_id,
             filename,
             mime_type,
             size_bytes,
             content_disposition,
             content_id,
             is_inline
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT (account_id, attachment_key) DO UPDATE SET
             message_id = excluded.message_id,
             part_id = excluded.part_id,
             gmail_attachment_id = excluded.gmail_attachment_id,
             filename = excluded.filename,
             mime_type = excluded.mime_type,
             size_bytes = excluded.size_bytes,
             content_disposition = excluded.content_disposition,
             content_id = excluded.content_id,
             is_inline = excluded.is_inline",
    )?;
    let mut insert_page_message = page_seq
        .is_some()
        .then(|| {
            transaction.prepare_cached(
                "INSERT INTO gmail_full_sync_stage_page_messages (
                     account_id,
                     page_seq,
                     message_id
                 )
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (account_id, page_seq, message_id) DO NOTHING",
            )
        })
        .transpose()?;

    for message in messages {
        ensure!(
            message.account_id == account_id,
            "mailbox message account_id `{}` does not match batch account `{}`",
            message.account_id,
            account_id
        );

        upsert_message.execute(params![
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
            &message.automation_headers.list_id_header,
            &message.automation_headers.list_unsubscribe_header,
            &message.automation_headers.list_unsubscribe_post_header,
            &message.automation_headers.precedence_header,
            &message.automation_headers.auto_submitted_header,
            &message.label_names_text,
        ])?;
        delete_labels.execute(params![account_id, &message.message_id])?;
        delete_attachments.execute(params![account_id, &message.message_id])?;

        for label_id in unique_sorted_strings(&message.label_ids) {
            insert_label.execute(params![account_id, &message.message_id, label_id])?;
        }

        for attachment in &message.attachments {
            insert_attachment.execute(params![
                account_id,
                &message.message_id,
                &attachment.attachment_key,
                &attachment.part_id,
                &attachment.gmail_attachment_id,
                &attachment.filename,
                &attachment.mime_type,
                attachment.size_bytes,
                &attachment.content_disposition,
                &attachment.content_id,
                if attachment.is_inline { 1_i64 } else { 0_i64 },
            ])?;
        }

        if let (Some(page_seq), Some(insert_page_message)) =
            (page_seq, insert_page_message.as_mut())
        {
            insert_page_message.execute(params![account_id, page_seq, &message.message_id])?;
        }
    }

    drop(insert_page_message);
    drop(insert_attachment);
    drop(insert_label);
    drop(delete_attachments);
    drop(delete_labels);
    drop(upsert_message);
    Ok(())
}

fn upsert_full_sync_stage_page_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    input: &FullSyncStagePageInput,
    staged_message_count: i64,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO gmail_full_sync_stage_pages (
             account_id,
             page_seq,
             listed_count,
             staged_message_count,
             next_page_token,
             status,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT (account_id, page_seq) DO UPDATE SET
             listed_count = excluded.listed_count,
             staged_message_count = gmail_full_sync_stage_pages.staged_message_count + excluded.staged_message_count,
             next_page_token = excluded.next_page_token,
             status = excluded.status,
             updated_at_epoch_s = excluded.updated_at_epoch_s",
        params![
            account_id,
            input.page_seq,
            input.listed_count,
            staged_message_count,
            &input.next_page_token,
            if input.page_complete {
                FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE
            } else {
                FULL_SYNC_STAGE_PAGE_STATUS_PARTIAL
            },
            input.updated_at_epoch_s,
        ],
    )?;
    Ok(())
}

fn stage_incremental_sync_batch_with_connection(
    connection: &mut Connection,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    message_ids_to_delete: &[String],
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox incremental staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    stage_incremental_sync_messages_in_transaction(&transaction, account_id, messages)?;
    stage_incremental_sync_delete_ids_in_transaction(
        &transaction,
        account_id,
        message_ids_to_delete,
    )?;
    transaction.commit()?;
    Ok(())
}

fn stage_incremental_sync_delete_ids_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    message_ids_to_delete: &[String],
) -> Result<()> {
    let mut insert = transaction.prepare_cached(
        "INSERT INTO gmail_incremental_sync_stage_delete_ids (
             account_id,
             message_id
         )
         VALUES (?1, ?2)
         ON CONFLICT (account_id, message_id) DO NOTHING",
    )?;

    for message_id in unique_sorted_strings(message_ids_to_delete) {
        insert.execute(params![account_id, message_id])?;
    }

    drop(insert);
    Ok(())
}

fn stage_incremental_sync_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    let mut upsert_message = transaction.prepare_cached(
        "INSERT INTO gmail_incremental_sync_stage_messages (
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             label_names_text
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
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
             list_id_header = excluded.list_id_header,
             list_unsubscribe_header = excluded.list_unsubscribe_header,
             list_unsubscribe_post_header = excluded.list_unsubscribe_post_header,
             precedence_header = excluded.precedence_header,
             auto_submitted_header = excluded.auto_submitted_header,
             label_names_text = excluded.label_names_text",
    )?;
    let mut delete_labels = transaction.prepare_cached(
        "DELETE FROM gmail_incremental_sync_stage_message_labels
         WHERE account_id = ?1
           AND message_id = ?2",
    )?;
    let mut delete_attachments = transaction.prepare_cached(
        "DELETE FROM gmail_incremental_sync_stage_attachments
         WHERE account_id = ?1
           AND message_id = ?2",
    )?;
    let mut insert_label = transaction.prepare_cached(
        "INSERT INTO gmail_incremental_sync_stage_message_labels (
             account_id,
             message_id,
             label_id
         )
         VALUES (?1, ?2, ?3)",
    )?;
    let mut insert_attachment = transaction.prepare_cached(
        "INSERT INTO gmail_incremental_sync_stage_attachments (
             account_id,
             message_id,
             attachment_key,
             part_id,
             gmail_attachment_id,
             filename,
             mime_type,
             size_bytes,
             content_disposition,
             content_id,
             is_inline
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT (account_id, attachment_key) DO UPDATE SET
             message_id = excluded.message_id,
             part_id = excluded.part_id,
             gmail_attachment_id = excluded.gmail_attachment_id,
             filename = excluded.filename,
             mime_type = excluded.mime_type,
             size_bytes = excluded.size_bytes,
             content_disposition = excluded.content_disposition,
             content_id = excluded.content_id,
             is_inline = excluded.is_inline",
    )?;

    for message in messages {
        ensure!(
            message.account_id == account_id,
            "mailbox message account_id `{}` does not match batch account `{}`",
            message.account_id,
            account_id
        );

        upsert_message.execute(params![
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
            &message.automation_headers.list_id_header,
            &message.automation_headers.list_unsubscribe_header,
            &message.automation_headers.list_unsubscribe_post_header,
            &message.automation_headers.precedence_header,
            &message.automation_headers.auto_submitted_header,
            &message.label_names_text,
        ])?;
        delete_labels.execute(params![account_id, &message.message_id])?;
        delete_attachments.execute(params![account_id, &message.message_id])?;

        for label_id in unique_sorted_strings(&message.label_ids) {
            insert_label.execute(params![account_id, &message.message_id, label_id])?;
        }

        for attachment in &message.attachments {
            insert_attachment.execute(params![
                account_id,
                &message.message_id,
                &attachment.attachment_key,
                &attachment.part_id,
                &attachment.gmail_attachment_id,
                &attachment.filename,
                &attachment.mime_type,
                attachment.size_bytes,
                &attachment.content_disposition,
                &attachment.content_id,
                if attachment.is_inline { 1_i64 } else { 0_i64 },
            ])?;
        }
    }

    drop(insert_attachment);
    drop(insert_label);
    drop(delete_attachments);
    drop(delete_labels);
    drop(upsert_message);
    Ok(())
}

fn finalize_full_sync_from_stage_with_connection(
    connection: &mut Connection,
    account_id: &str,
    updated_at_epoch_s: i64,
    sync_state_update: &SyncStateUpdate,
) -> Result<SyncStateRecord> {
    ensure!(
        sync_state_update.account_id == account_id,
        "sync state account_id `{}` does not match finalize account `{}`",
        sync_state_update.account_id,
        account_id
    );
    ensure!(
        !account_id.is_empty(),
        "mailbox finalize requires a non-empty account_id"
    );

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    create_preserved_attachment_vault_stage(&transaction, account_id)?;
    replace_labels_from_stage_in_transaction(&transaction, account_id, updated_at_epoch_s)?;
    delete_account_messages(&transaction, account_id)?;
    insert_live_messages_from_stage_in_transaction(&transaction, account_id, updated_at_epoch_s)?;
    insert_live_message_labels_from_stage_in_transaction(&transaction, account_id)?;
    insert_live_attachments_from_stage_in_transaction(
        &transaction,
        account_id,
        updated_at_epoch_s,
    )?;
    insert_live_search_from_stage_in_transaction(&transaction, account_id)?;
    let record = upsert_sync_state_in_transaction(&transaction, sync_state_update)?;
    clear_full_sync_checkpoint_in_transaction(&transaction, account_id)?;
    reset_full_sync_stage_in_transaction(&transaction, account_id)?;
    transaction.commit()?;
    Ok(record)
}

fn finalize_incremental_from_stage_with_connection(
    connection: &mut Connection,
    account_id: &str,
    labels: &[GmailLabel],
    updated_at_epoch_s: i64,
    sync_state_update: &SyncStateUpdate,
) -> Result<(SyncStateRecord, usize)> {
    ensure!(
        sync_state_update.account_id == account_id,
        "sync state account_id `{}` does not match incremental finalize account `{}`",
        sync_state_update.account_id,
        account_id
    );
    ensure!(
        !account_id.is_empty(),
        "mailbox incremental finalize requires a non-empty account_id"
    );

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let should_reindex_search =
        replace_labels_in_transaction(&transaction, account_id, labels, updated_at_epoch_s)?;
    create_preserved_incremental_attachment_vault_stage(&transaction, account_id)?;
    delete_existing_incremental_stage_messages_in_transaction(&transaction, account_id)?;
    let deleted =
        delete_incremental_stage_delete_ids_from_live_in_transaction(&transaction, account_id)?;
    insert_live_messages_from_incremental_stage_in_transaction(
        &transaction,
        account_id,
        updated_at_epoch_s,
    )?;
    insert_live_message_labels_from_incremental_stage_in_transaction(&transaction, account_id)?;
    insert_live_attachments_from_incremental_stage_in_transaction(
        &transaction,
        account_id,
        updated_at_epoch_s,
    )?;
    insert_live_search_from_incremental_stage_in_transaction(&transaction, account_id)?;
    if should_reindex_search {
        reindex_message_search_for_account(&transaction, account_id)?;
    }
    let record = upsert_sync_state_in_transaction(&transaction, sync_state_update)?;
    reset_incremental_sync_stage_in_transaction(&transaction, account_id)?;
    transaction.commit()?;
    Ok((record, deleted))
}

fn ensure_checkpoint_matches_account(
    account_id: &str,
    update: &FullSyncCheckpointUpdate,
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox checkpoint requires a non-empty account_id"
    );
    ensure!(
        !update.bootstrap_query.is_empty(),
        "mailbox checkpoint requires a non-empty bootstrap_query"
    );
    Ok(())
}

fn upsert_full_sync_checkpoint_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    let staged_label_count = count_stage_rows(
        transaction,
        "SELECT COUNT(*) FROM gmail_full_sync_stage_labels WHERE account_id = ?1",
        account_id,
    )?;
    let staged_message_count = count_stage_rows(
        transaction,
        "SELECT COUNT(*) FROM gmail_full_sync_stage_messages WHERE account_id = ?1",
        account_id,
    )?;
    let staged_message_label_count = count_stage_rows(
        transaction,
        "SELECT COUNT(*) FROM gmail_full_sync_stage_message_labels WHERE account_id = ?1",
        account_id,
    )?;
    let staged_attachment_count = count_stage_rows(
        transaction,
        "SELECT COUNT(*) FROM gmail_full_sync_stage_attachments WHERE account_id = ?1",
        account_id,
    )?;

    transaction.execute(
        "INSERT INTO gmail_full_sync_checkpoint (
             account_id,
             bootstrap_query,
             status,
             next_page_token,
             cursor_history_id,
             pages_fetched,
             messages_listed,
             messages_upserted,
             labels_synced,
             staged_label_count,
             staged_message_count,
             staged_message_label_count,
             staged_attachment_count,
             started_at_epoch_s,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
         ON CONFLICT (account_id) DO UPDATE SET
             bootstrap_query = excluded.bootstrap_query,
             status = excluded.status,
             next_page_token = excluded.next_page_token,
             cursor_history_id = excluded.cursor_history_id,
             pages_fetched = excluded.pages_fetched,
             messages_listed = excluded.messages_listed,
             messages_upserted = excluded.messages_upserted,
             labels_synced = excluded.labels_synced,
             staged_label_count = excluded.staged_label_count,
             staged_message_count = excluded.staged_message_count,
             staged_message_label_count = excluded.staged_message_label_count,
             staged_attachment_count = excluded.staged_attachment_count,
             started_at_epoch_s = excluded.started_at_epoch_s,
             updated_at_epoch_s = excluded.updated_at_epoch_s",
        params![
            account_id,
            &update.bootstrap_query,
            update.status.as_str(),
            &update.next_page_token,
            &update.cursor_history_id,
            update.pages_fetched,
            update.messages_listed,
            update.messages_upserted,
            update.labels_synced,
            staged_label_count,
            staged_message_count,
            staged_message_label_count,
            staged_attachment_count,
            update.started_at_epoch_s,
            update.updated_at_epoch_s,
        ],
    )?;

    read_full_sync_checkpoint(transaction, account_id)?
        .ok_or_else(|| anyhow!("full sync checkpoint disappeared after upsert"))
}

fn count_stage_rows(connection: &Connection, sql: &str, account_id: &str) -> Result<i64> {
    Ok(connection.query_row(sql, [account_id], |row| row.get(0))?)
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
             pipeline_enabled,
             pipeline_list_queue_high_water,
             pipeline_write_queue_high_water,
             pipeline_write_batch_count,
             pipeline_writer_wait_ms,
             pipeline_fetch_batch_count,
             pipeline_fetch_batch_avg_ms,
             pipeline_fetch_batch_max_ms,
             pipeline_writer_tx_count,
             pipeline_writer_tx_avg_ms,
             pipeline_writer_tx_max_ms,
             pipeline_reorder_buffer_high_water,
             pipeline_staged_message_count,
             pipeline_staged_delete_count,
             pipeline_staged_attachment_count,
             message_count,
             label_count,
             indexed_message_count
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27)
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
             pipeline_enabled = excluded.pipeline_enabled,
             pipeline_list_queue_high_water = excluded.pipeline_list_queue_high_water,
             pipeline_write_queue_high_water = excluded.pipeline_write_queue_high_water,
             pipeline_write_batch_count = excluded.pipeline_write_batch_count,
             pipeline_writer_wait_ms = excluded.pipeline_writer_wait_ms,
             pipeline_fetch_batch_count = excluded.pipeline_fetch_batch_count,
             pipeline_fetch_batch_avg_ms = excluded.pipeline_fetch_batch_avg_ms,
             pipeline_fetch_batch_max_ms = excluded.pipeline_fetch_batch_max_ms,
             pipeline_writer_tx_count = excluded.pipeline_writer_tx_count,
             pipeline_writer_tx_avg_ms = excluded.pipeline_writer_tx_avg_ms,
             pipeline_writer_tx_max_ms = excluded.pipeline_writer_tx_max_ms,
             pipeline_reorder_buffer_high_water = excluded.pipeline_reorder_buffer_high_water,
             pipeline_staged_message_count = excluded.pipeline_staged_message_count,
             pipeline_staged_delete_count = excluded.pipeline_staged_delete_count,
             pipeline_staged_attachment_count = excluded.pipeline_staged_attachment_count,
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
            if update.pipeline_enabled {
                1_i64
            } else {
                0_i64
            },
            update.pipeline_list_queue_high_water,
            update.pipeline_write_queue_high_water,
            update.pipeline_write_batch_count,
            update.pipeline_writer_wait_ms,
            update.pipeline_fetch_batch_count,
            update.pipeline_fetch_batch_avg_ms,
            update.pipeline_fetch_batch_max_ms,
            update.pipeline_writer_tx_count,
            update.pipeline_writer_tx_avg_ms,
            update.pipeline_writer_tx_max_ms,
            update.pipeline_reorder_buffer_high_water,
            update.pipeline_staged_message_count,
            update.pipeline_staged_delete_count,
            update.pipeline_staged_attachment_count,
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

fn create_preserved_attachment_vault_stage(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DROP TABLE IF EXISTS temp.gmail_full_sync_preserved_attachment_vaults",
        [],
    )?;
    transaction.execute(
        "CREATE TEMP TABLE gmail_full_sync_preserved_attachment_vaults (
             attachment_key TEXT PRIMARY KEY,
             content_hash TEXT NOT NULL,
             relative_path TEXT NOT NULL,
             size_bytes INTEGER NOT NULL,
             fetched_at_epoch_s INTEGER NOT NULL
         ) STRICT",
        [],
    )?;
    transaction.execute(
        "INSERT INTO gmail_full_sync_preserved_attachment_vaults (
             attachment_key,
             content_hash,
             relative_path,
             size_bytes,
             fetched_at_epoch_s
         )
         SELECT
             attachment_key,
             vault_content_hash,
             vault_relative_path,
             vault_size_bytes,
             vault_fetched_at_epoch_s
         FROM gmail_message_attachments
         WHERE account_id = ?1
           AND vault_content_hash IS NOT NULL
           AND vault_relative_path IS NOT NULL
           AND vault_size_bytes IS NOT NULL
           AND vault_fetched_at_epoch_s IS NOT NULL",
        [account_id],
    )?;
    Ok(())
}

fn create_preserved_incremental_attachment_vault_stage(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DROP TABLE IF EXISTS temp.gmail_incremental_sync_preserved_attachment_vaults",
        [],
    )?;
    transaction.execute(
        "CREATE TEMP TABLE gmail_incremental_sync_preserved_attachment_vaults (
             attachment_key TEXT PRIMARY KEY,
             content_hash TEXT NOT NULL,
             relative_path TEXT NOT NULL,
             size_bytes INTEGER NOT NULL,
             fetched_at_epoch_s INTEGER NOT NULL
         ) STRICT",
        [],
    )?;
    transaction.execute(
        "INSERT INTO gmail_incremental_sync_preserved_attachment_vaults (
             attachment_key,
             content_hash,
             relative_path,
             size_bytes,
             fetched_at_epoch_s
         )
         SELECT
             gma.attachment_key,
             gma.vault_content_hash,
             gma.vault_relative_path,
             gma.vault_size_bytes,
             gma.vault_fetched_at_epoch_s
         FROM gmail_message_attachments gma
         INNER JOIN gmail_messages gm
           ON gm.message_rowid = gma.message_rowid
         INNER JOIN gmail_incremental_sync_stage_messages stage
           ON stage.account_id = gm.account_id
          AND stage.message_id = gm.message_id
         WHERE gma.account_id = ?1
           AND gma.vault_content_hash IS NOT NULL
           AND gma.vault_relative_path IS NOT NULL
           AND gma.vault_size_bytes IS NOT NULL
           AND gma.vault_fetched_at_epoch_s IS NOT NULL",
        [account_id],
    )?;
    Ok(())
}

fn replace_labels_from_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    updated_at_epoch_s: i64,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_labels WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
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
         SELECT
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
             ?2
         FROM gmail_full_sync_stage_labels
         WHERE account_id = ?1",
        params![account_id, updated_at_epoch_s],
    )?;
    Ok(())
}

fn insert_live_messages_from_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    updated_at_epoch_s: i64,
) -> Result<()> {
    transaction.execute(
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             updated_at_epoch_s
         )
         SELECT
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             ?2
         FROM gmail_full_sync_stage_messages
         WHERE account_id = ?1",
        params![account_id, updated_at_epoch_s],
    )?;
    Ok(())
}

fn insert_live_message_labels_from_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO gmail_message_labels (message_rowid, label_id)
         SELECT
             gm.message_rowid,
             stage.label_id
         FROM gmail_full_sync_stage_message_labels stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         WHERE stage.account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

fn insert_live_attachments_from_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    updated_at_epoch_s: i64,
) -> Result<()> {
    transaction.execute(
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
         SELECT
             stage.account_id,
             gm.message_rowid,
             stage.attachment_key,
             stage.part_id,
             stage.gmail_attachment_id,
             stage.filename,
             stage.mime_type,
             stage.size_bytes,
             stage.content_disposition,
             stage.content_id,
             stage.is_inline,
             preserved.content_hash,
             preserved.relative_path,
             preserved.size_bytes,
             preserved.fetched_at_epoch_s,
             ?2
         FROM gmail_full_sync_stage_attachments stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         LEFT JOIN gmail_full_sync_preserved_attachment_vaults preserved
           ON preserved.attachment_key = stage.attachment_key
         WHERE stage.account_id = ?1",
        params![account_id, updated_at_epoch_s],
    )?;
    Ok(())
}

fn insert_live_search_from_stage_in_transaction(
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
             stage.subject,
             stage.from_header,
             stage.recipient_headers,
             stage.snippet,
             stage.label_names_text
         FROM gmail_full_sync_stage_messages stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         WHERE stage.account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

fn delete_existing_incremental_stage_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    let mut query = transaction.prepare_cached(
        "SELECT message_id
         FROM gmail_incremental_sync_stage_messages
         WHERE account_id = ?1
         ORDER BY message_id ASC",
    )?;
    let message_ids = query
        .query_map([account_id], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    delete_messages_in_transaction(transaction, account_id, &message_ids)?;
    Ok(())
}

fn delete_incremental_stage_delete_ids_from_live_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<usize> {
    let mut query = transaction.prepare_cached(
        "SELECT message_id
         FROM gmail_incremental_sync_stage_delete_ids
         WHERE account_id = ?1
         ORDER BY message_id ASC",
    )?;
    let message_ids = query
        .query_map([account_id], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    delete_messages_in_transaction(transaction, account_id, &message_ids)
}

fn insert_live_messages_from_incremental_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    updated_at_epoch_s: i64,
) -> Result<()> {
    transaction.execute(
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             updated_at_epoch_s
         )
         SELECT
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             ?2
         FROM gmail_incremental_sync_stage_messages
         WHERE account_id = ?1",
        params![account_id, updated_at_epoch_s],
    )?;
    Ok(())
}

fn insert_live_message_labels_from_incremental_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO gmail_message_labels (message_rowid, label_id)
         SELECT
             gm.message_rowid,
             stage.label_id
         FROM gmail_incremental_sync_stage_message_labels stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         WHERE stage.account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

fn insert_live_attachments_from_incremental_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    updated_at_epoch_s: i64,
) -> Result<()> {
    transaction.execute(
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
         SELECT
             stage.account_id,
             gm.message_rowid,
             stage.attachment_key,
             stage.part_id,
             stage.gmail_attachment_id,
             stage.filename,
             stage.mime_type,
             stage.size_bytes,
             stage.content_disposition,
             stage.content_id,
             stage.is_inline,
             preserved.content_hash,
             preserved.relative_path,
             preserved.size_bytes,
             preserved.fetched_at_epoch_s,
             ?2
         FROM gmail_incremental_sync_stage_attachments stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         LEFT JOIN gmail_incremental_sync_preserved_attachment_vaults preserved
           ON preserved.attachment_key = stage.attachment_key
         WHERE stage.account_id = ?1",
        params![account_id, updated_at_epoch_s],
    )?;
    Ok(())
}

fn insert_live_search_from_incremental_stage_in_transaction(
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
             stage.subject,
             stage.from_header,
             stage.recipient_headers,
             stage.snippet,
             stage.label_names_text
         FROM gmail_incremental_sync_stage_messages stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         WHERE stage.account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PreservedAttachmentVaultState {
    content_hash: String,
    relative_path: String,
    size_bytes: i64,
    fetched_at_epoch_s: i64,
}

#[cfg(test)]
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

#[cfg(test)]
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
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
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
             list_id_header = excluded.list_id_header,
             list_unsubscribe_header = excluded.list_unsubscribe_header,
             list_unsubscribe_post_header = excluded.list_unsubscribe_post_header,
             precedence_header = excluded.precedence_header,
             auto_submitted_header = excluded.auto_submitted_header,
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
                &message.automation_headers.list_id_header,
                &message.automation_headers.list_unsubscribe_header,
                &message.automation_headers.list_unsubscribe_post_header,
                &message.automation_headers.precedence_header,
                &message.automation_headers.auto_submitted_header,
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
        .map_err(|source| MailboxWriteError::open_database(database_path, source))?;
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
        return Err(MailboxWriteError::RowCountMismatch {
            operation: "set_attachment_vault_state",
            expected: 1,
            actual: rows_updated,
        });
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn record_attachment_export(
    database_path: &Path,
    busy_timeout_ms: u64,
    event: &AttachmentExportEventInput,
) -> Result<(), MailboxWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| MailboxWriteError::open_database(database_path, source))?;
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
