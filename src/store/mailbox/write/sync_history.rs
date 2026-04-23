use super::*;
pub(super) fn stage_incremental_sync_batch_with_connection(
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

pub(super) fn stage_incremental_sync_delete_ids_in_transaction(
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

pub(super) fn stage_incremental_sync_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    stage_messages_in_transaction(
        transaction,
        account_id,
        messages,
        SyncStageTables {
            message_table: "gmail_incremental_sync_stage_messages",
            label_table: "gmail_incremental_sync_stage_message_labels",
            attachment_table: "gmail_incremental_sync_stage_attachments",
            page_message_table: None,
        },
        None,
    )
}

pub(super) fn finalize_full_sync_from_stage_with_connection(
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

pub(super) fn finalize_incremental_from_stage_with_connection(
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

pub(super) fn validate_checkpoint_fields(
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

pub(super) fn upsert_full_sync_checkpoint_in_transaction(
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

pub(super) fn count_stage_rows(
    connection: &Connection,
    sql: &str,
    account_id: &str,
) -> Result<i64> {
    Ok(connection.query_row(sql, [account_id], |row| row.get(0))?)
}

pub(super) fn replace_labels_in_transaction(
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

pub(super) fn label_names_changed(
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

/// Merge partial sync-state updates while preserving prior success timestamps when absent.
pub(super) fn upsert_sync_state_in_transaction(
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

/// Restore a fully materialized sync-state record without preserving prior field values.
pub(super) fn upsert_sync_state_record_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    record: &SyncStateRecord,
) -> Result<SyncStateRecord> {
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
             last_full_sync_success_epoch_s = excluded.last_full_sync_success_epoch_s,
             last_incremental_sync_success_epoch_s = excluded.last_incremental_sync_success_epoch_s,
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
            &record.account_id,
            &record.cursor_history_id,
            &record.bootstrap_query,
            record.last_sync_mode.as_str(),
            record.last_sync_status.as_str(),
            &record.last_error,
            record.last_sync_epoch_s,
            record.last_full_sync_success_epoch_s,
            record.last_incremental_sync_success_epoch_s,
            if record.pipeline_enabled { 1_i64 } else { 0_i64 },
            record.pipeline_list_queue_high_water,
            record.pipeline_write_queue_high_water,
            record.pipeline_write_batch_count,
            record.pipeline_writer_wait_ms,
            record.pipeline_fetch_batch_count,
            record.pipeline_fetch_batch_avg_ms,
            record.pipeline_fetch_batch_max_ms,
            record.pipeline_writer_tx_count,
            record.pipeline_writer_tx_avg_ms,
            record.pipeline_writer_tx_max_ms,
            record.pipeline_reorder_buffer_high_water,
            record.pipeline_staged_message_count,
            record.pipeline_staged_delete_count,
            record.pipeline_staged_attachment_count,
            record.message_count,
            record.label_count,
            record.indexed_message_count,
        ],
    )?;

    read_sync_state(transaction, &record.account_id)?
        .ok_or_else(|| anyhow!("failed to re-read sync state for {}", record.account_id))
}

pub(super) fn insert_sync_run_history_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    outcome: &SyncRunOutcomeInput,
) -> Result<SyncRunHistoryRecord> {
    let mut statement = transaction.prepare_cached(
        "INSERT INTO gmail_sync_run_history (
             account_id,
             sync_mode,
             status,
             comparability_kind,
             comparability_key,
             startup_seed_run_id,
             started_at_epoch_s,
             finished_at_epoch_s,
             bootstrap_query,
             cursor_history_id,
             fallback_from_history,
             resumed_from_checkpoint,
             pages_fetched,
             messages_listed,
             messages_upserted,
             messages_deleted,
             labels_synced,
             checkpoint_reused_pages,
             checkpoint_reused_messages_upserted,
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
             adaptive_pacing_enabled,
             quota_units_budget_per_minute,
             message_fetch_concurrency,
             quota_units_cap_per_minute,
             message_fetch_concurrency_cap,
             starting_quota_units_per_minute,
             starting_message_fetch_concurrency,
             effective_quota_units_per_minute,
             effective_message_fetch_concurrency,
             adaptive_downshift_count,
             estimated_quota_units_reserved,
             http_attempt_count,
             retry_count,
             quota_pressure_retry_count,
             concurrency_pressure_retry_count,
             backend_retry_count,
             throttle_wait_count,
             throttle_wait_ms,
             retry_after_wait_ms,
             duration_ms,
             pages_per_second,
             messages_per_second,
             error_message
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40, ?41, ?42, ?43, ?44, ?45, ?46, ?47, ?48, ?49, ?50, ?51, ?52, ?53, ?54, ?55, ?56, ?57)",
    )?;
    statement.execute(params![
        &outcome.account_id,
        outcome.sync_mode.as_str(),
        outcome.status.as_str(),
        outcome.comparability_kind.as_str(),
        &outcome.comparability_key,
        outcome.startup_seed_run_id,
        outcome.started_at_epoch_s,
        outcome.finished_at_epoch_s,
        &outcome.bootstrap_query,
        &outcome.cursor_history_id,
        if outcome.fallback_from_history {
            1_i64
        } else {
            0_i64
        },
        if outcome.resumed_from_checkpoint {
            1_i64
        } else {
            0_i64
        },
        outcome.pages_fetched,
        outcome.messages_listed,
        outcome.messages_upserted,
        outcome.messages_deleted,
        outcome.labels_synced,
        outcome.checkpoint_reused_pages,
        outcome.checkpoint_reused_messages_upserted,
        if outcome.pipeline_enabled {
            1_i64
        } else {
            0_i64
        },
        outcome.pipeline_list_queue_high_water,
        outcome.pipeline_write_queue_high_water,
        outcome.pipeline_write_batch_count,
        outcome.pipeline_writer_wait_ms,
        outcome.pipeline_fetch_batch_count,
        outcome.pipeline_fetch_batch_avg_ms,
        outcome.pipeline_fetch_batch_max_ms,
        outcome.pipeline_writer_tx_count,
        outcome.pipeline_writer_tx_avg_ms,
        outcome.pipeline_writer_tx_max_ms,
        outcome.pipeline_reorder_buffer_high_water,
        outcome.pipeline_staged_message_count,
        outcome.pipeline_staged_delete_count,
        outcome.pipeline_staged_attachment_count,
        if outcome.adaptive_pacing_enabled {
            1_i64
        } else {
            0_i64
        },
        outcome.quota_units_budget_per_minute,
        outcome.message_fetch_concurrency,
        outcome.quota_units_cap_per_minute,
        outcome.message_fetch_concurrency_cap,
        outcome.starting_quota_units_per_minute,
        outcome.starting_message_fetch_concurrency,
        outcome.effective_quota_units_per_minute,
        outcome.effective_message_fetch_concurrency,
        outcome.adaptive_downshift_count,
        outcome.estimated_quota_units_reserved,
        outcome.http_attempt_count,
        outcome.retry_count,
        outcome.quota_pressure_retry_count,
        outcome.concurrency_pressure_retry_count,
        outcome.backend_retry_count,
        outcome.throttle_wait_count,
        outcome.throttle_wait_ms,
        outcome.retry_after_wait_ms,
        outcome.duration_ms,
        outcome.pages_per_second,
        outcome.messages_per_second,
        &outcome.error_message,
    ])?;

    let run_id = transaction.last_insert_rowid();
    crate::store::mailbox::read::read_sync_run_history_record(transaction, run_id)?
        .ok_or_else(|| anyhow!("sync run history row {run_id} disappeared after insert"))
}

pub(super) fn prune_sync_run_history_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    let mut statement = transaction.prepare_cached(
        "DELETE FROM gmail_sync_run_history
         WHERE run_id IN (
             SELECT run_id
             FROM (
                 SELECT
                     run_id,
                     ROW_NUMBER() OVER (
                         PARTITION BY account_id
                         ORDER BY finished_at_epoch_s DESC, run_id DESC
                     ) AS row_number
                 FROM gmail_sync_run_history
                 WHERE account_id = ?1
             )
             WHERE row_number > ?2
         )",
    )?;
    statement.execute(params![account_id, SYNC_RUN_HISTORY_RETENTION_PER_ACCOUNT])?;
    Ok(())
}
