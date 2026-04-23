use super::*;
pub(super) fn reconcile_sync_run_summaries_for_account_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    updated_at_epoch_s: i64,
    requested_sync_mode: SyncMode,
    requested_comparability: &SyncRunComparability,
) -> Result<SyncRunSummaryRecord, MailboxWriteError> {
    let history_buckets = list_sync_run_history_buckets_in_transaction(transaction, account_id)?;

    let mut requested_summary = None;
    for bucket in history_buckets {
        let summary = recompute_sync_run_summary_in_transaction(
            transaction,
            account_id,
            bucket.sync_mode,
            &bucket.comparability,
            updated_at_epoch_s,
        )?;
        if bucket.sync_mode == requested_sync_mode
            && bucket.comparability.kind == requested_comparability.kind
            && bucket.comparability.key == requested_comparability.key
        {
            requested_summary = Some(summary);
        }
    }

    requested_summary.ok_or_else(|| MailboxWriteError::InvariantViolation {
        operation: "reconcile_sync_run_summaries",
        detail: format!(
            "requested summary bucket {}:{}:{} disappeared after pruning",
            requested_sync_mode.as_str(),
            requested_comparability.kind.as_str(),
            requested_comparability.key
        ),
    })
}

pub(super) fn delete_all_sync_run_summaries_for_account_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<(), MailboxWriteError> {
    transaction.execute(
        "DELETE FROM gmail_sync_run_summary
         WHERE account_id = ?1",
        params![account_id],
    )?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SyncRunSummaryBucket {
    sync_mode: SyncMode,
    comparability: SyncRunComparability,
}

pub(super) fn list_sync_run_history_buckets_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<Vec<SyncRunSummaryBucket>, MailboxWriteError> {
    let mut statement = transaction.prepare_cached(
        "SELECT DISTINCT sync_mode, comparability_kind, comparability_key
         FROM gmail_sync_run_history
         WHERE account_id = ?1",
    )?;
    let rows = statement.query_map([account_id], |row| {
        let sync_mode = SyncMode::from_str(row.get_ref(0)?.as_str()?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        let comparability_kind = SyncRunComparabilityKind::from_str(row.get_ref(1)?.as_str()?)
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
        let comparability_key = row.get::<_, String>(2)?;
        Ok(SyncRunSummaryBucket {
            sync_mode,
            comparability: SyncRunComparability {
                kind: comparability_kind,
                label: String::new(),
                key: comparability_key,
            },
        })
    })?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(super) fn delete_account_messages(
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

pub(super) fn create_preserved_attachment_vault_stage(
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

pub(super) fn create_preserved_incremental_attachment_vault_stage(
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

pub(super) fn replace_labels_from_stage_in_transaction(
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

pub(super) fn insert_live_messages_from_stage_in_transaction(
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

pub(super) fn insert_live_message_labels_from_stage_in_transaction(
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

pub(super) fn insert_live_attachments_from_stage_in_transaction(
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

pub(super) fn insert_live_search_from_stage_in_transaction(
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
             COALESCE((
                 SELECT group_concat(name, ' ')
                 FROM (
                     SELECT gl.name AS name
                     FROM gmail_message_labels gml
                     INNER JOIN gmail_labels gl
                       ON gl.account_id = gm.account_id
                      AND gl.label_id = gml.label_id
                     WHERE gml.message_rowid = gm.message_rowid
                     ORDER BY gl.name ASC
                 )
             ), '')
         FROM gmail_full_sync_stage_messages stage
         INNER JOIN gmail_messages gm
           ON gm.account_id = stage.account_id
          AND gm.message_id = stage.message_id
         WHERE stage.account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

pub(super) fn delete_existing_incremental_stage_messages_in_transaction(
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

pub(super) fn delete_incremental_stage_delete_ids_from_live_in_transaction(
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

pub(super) fn insert_live_messages_from_incremental_stage_in_transaction(
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

pub(super) fn insert_live_message_labels_from_incremental_stage_in_transaction(
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

pub(super) fn insert_live_attachments_from_incremental_stage_in_transaction(
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

pub(super) fn insert_live_search_from_incremental_stage_in_transaction(
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
pub(super) struct PreservedAttachmentVaultState {
    pub(super) content_hash: String,
    pub(super) relative_path: String,
    pub(super) size_bytes: i64,
    pub(super) fetched_at_epoch_s: i64,
}

#[cfg(test)]
pub(super) fn load_attachment_vault_state_for_message(
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
