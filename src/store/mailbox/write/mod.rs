use super::read::{
    count_indexed_messages, count_labels, count_messages, read_full_sync_checkpoint,
    read_sync_pacing_state, read_sync_state,
};
use super::run_history_policy::{
    SYNC_RUN_HISTORY_RETENTION_PER_ACCOUNT, comparability_for_outcome,
};
use super::run_history_summary::recompute_sync_run_summary_in_transaction;
use super::{
    AttachmentExportEventInput, AttachmentVaultStateUpdate, FullSyncCheckpointRecord,
    FullSyncCheckpointUpdate, FullSyncStagePageInput, GmailMessageUpsertInput, MailboxWriteError,
    SyncMode, SyncPacingStateRecord, SyncPacingStateUpdate, SyncRunComparability,
    SyncRunComparabilityKind, SyncRunHistoryRecord, SyncRunOutcomeInput, SyncRunSummaryRecord,
    SyncStateRecord, SyncStateUpdate, unique_sorted_strings,
};
use crate::gmail::GmailLabel;
use crate::store::connection;
use anyhow::{Result, anyhow, ensure};
use rusqlite::{Connection, ToSql, TransactionBehavior, params, params_from_iter};
#[cfg(test)]
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

mod attachments;
mod materialize;
mod staging;
mod sync_history;

use self::attachments::*;
pub(crate) use self::attachments::{record_attachment_export, set_attachment_vault_state};
use self::materialize::*;
use self::staging::*;
use self::sync_history::*;

const DELETE_BATCH_SIZE: usize = 400;
const FULL_SYNC_STAGE_PAGE_STATUS_PARTIAL: &str = "partial";
const FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE: &str = "complete";

fn mailbox_write_invariant(
    operation: &'static str,
    error: impl std::fmt::Display,
) -> MailboxWriteError {
    MailboxWriteError::InvariantViolation {
        operation,
        detail: error.to_string(),
    }
}

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
    ensure!(
        messages.is_empty() || !account_id.is_empty(),
        "mailbox upsert requires a non-empty account_id when messages are provided"
    );
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

pub(crate) fn persist_successful_sync_outcome(
    database_path: &Path,
    busy_timeout_ms: u64,
    sync_state: &SyncStateRecord,
    outcome: &SyncRunOutcomeInput,
) -> std::result::Result<
    (SyncStateRecord, SyncRunHistoryRecord, SyncRunSummaryRecord),
    MailboxWriteError,
> {
    ensure_sync_outcome_matches_account(&sync_state.account_id, &outcome.account_id)?;
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| MailboxWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction().map_err(MailboxWriteError::from)?;
    let sync_state = upsert_sync_state_record_in_transaction(&transaction, sync_state)
        .map_err(|error| mailbox_write_invariant("persist_successful_sync_outcome", error))?;
    let history = insert_sync_run_history_in_transaction(&transaction, outcome)
        .map_err(|error| mailbox_write_invariant("persist_successful_sync_outcome", error))?;
    delete_all_sync_run_summaries_for_account_in_transaction(&transaction, &outcome.account_id)
        .map_err(|error| mailbox_write_invariant("persist_successful_sync_outcome", error))?;
    prune_sync_run_history_in_transaction(&transaction, &outcome.account_id)
        .map_err(|error| mailbox_write_invariant("persist_successful_sync_outcome", error))?;
    let summary = reconcile_sync_run_summaries_for_account_in_transaction(
        &transaction,
        &outcome.account_id,
        outcome.finished_at_epoch_s,
        outcome.sync_mode,
        &comparability_for_outcome(outcome),
    )?;
    transaction.commit().map_err(MailboxWriteError::from)?;
    Ok((sync_state, history, summary))
}

pub(crate) fn persist_failed_sync_outcome(
    database_path: &Path,
    busy_timeout_ms: u64,
    sync_state_update: &SyncStateUpdate,
    outcome: &SyncRunOutcomeInput,
) -> std::result::Result<
    (SyncStateRecord, SyncRunHistoryRecord, SyncRunSummaryRecord),
    MailboxWriteError,
> {
    ensure_sync_outcome_matches_account(&sync_state_update.account_id, &outcome.account_id)?;
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| MailboxWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction().map_err(MailboxWriteError::from)?;
    let sync_state = upsert_sync_state_in_transaction(&transaction, sync_state_update)
        .map_err(|error| mailbox_write_invariant("persist_failed_sync_outcome", error))?;
    let history = insert_sync_run_history_in_transaction(&transaction, outcome)
        .map_err(|error| mailbox_write_invariant("persist_failed_sync_outcome", error))?;
    delete_all_sync_run_summaries_for_account_in_transaction(&transaction, &outcome.account_id)
        .map_err(|error| mailbox_write_invariant("persist_failed_sync_outcome", error))?;
    prune_sync_run_history_in_transaction(&transaction, &outcome.account_id)
        .map_err(|error| mailbox_write_invariant("persist_failed_sync_outcome", error))?;
    let summary = reconcile_sync_run_summaries_for_account_in_transaction(
        &transaction,
        &outcome.account_id,
        outcome.finished_at_epoch_s,
        outcome.sync_mode,
        &comparability_for_outcome(outcome),
    )?;
    transaction.commit().map_err(MailboxWriteError::from)?;
    Ok((sync_state, history, summary))
}

fn ensure_sync_outcome_matches_account(
    expected_account_id: &str,
    outcome_account_id: &str,
) -> std::result::Result<(), MailboxWriteError> {
    if expected_account_id != outcome_account_id {
        return Err(MailboxWriteError::AccountMismatch {
            expected_account_id: expected_account_id.to_owned(),
            outcome_account_id: outcome_account_id.to_owned(),
        });
    }
    Ok(())
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
