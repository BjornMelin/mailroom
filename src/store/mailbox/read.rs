use super::{
    AttachmentDetailRecord, AttachmentListItem, AttachmentListQuery, FullSyncCheckpointRecord,
    FullSyncCheckpointStatus, LabelUsageRecord, MailboxCoverageReport, MailboxDoctorReport,
    MailboxReadError, SyncMode, SyncPacingPressureKind, SyncPacingStateRecord,
    SyncRunHistoryRecord, SyncRunRegressionKind, SyncRunSummaryRecord, SyncStateRecord, SyncStatus,
    ThreadMessageSnapshot, is_missing_mailbox_table_error,
};
use crate::store::connection;
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::str::FromStr;

pub(crate) fn get_sync_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Option<SyncStateRecord>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    read_sync_state(&connection, account_id)
}

pub(crate) fn get_full_sync_checkpoint(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Option<FullSyncCheckpointRecord>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    read_full_sync_checkpoint(&connection, account_id)
}

pub(crate) fn get_sync_pacing_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Option<SyncPacingStateRecord>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    read_sync_pacing_state(&connection, account_id)
}

pub(crate) fn get_sync_run_summary(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    sync_mode: SyncMode,
) -> Result<Option<SyncRunSummaryRecord>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    read_sync_run_summary(&connection, account_id, sync_mode)
}

pub(crate) fn list_sync_run_history(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    limit: usize,
) -> Result<Vec<SyncRunHistoryRecord>, MailboxReadError> {
    if !database_path.try_exists()? || limit == 0 {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    read_sync_run_history(&connection, account_id, limit)
}

pub(crate) fn inspect_mailbox(
    database_path: &Path,
    busy_timeout_ms: u64,
) -> Result<Option<MailboxDoctorReport>, MailboxReadError> {
    inspect_mailbox_with_scope(database_path, busy_timeout_ms, None)
}

pub(crate) fn inspect_mailbox_account(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Option<MailboxDoctorReport>, MailboxReadError> {
    inspect_mailbox_with_scope(database_path, busy_timeout_ms, Some(account_id))
}

fn inspect_mailbox_with_scope(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: Option<&str>,
) -> Result<Option<MailboxDoctorReport>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let sync_state = match account_id {
        Some(account_id) => read_sync_state(&connection, account_id)?,
        None => latest_sync_state(&connection)?,
    };
    let full_sync_checkpoint = match account_id {
        Some(account_id) => read_full_sync_checkpoint(&connection, account_id)?,
        None => match sync_state.as_ref() {
            Some(sync_state) => read_full_sync_checkpoint(&connection, &sync_state.account_id)?,
            None => latest_full_sync_checkpoint(&connection)?,
        },
    };
    let sync_pacing_state = match account_id {
        Some(account_id) => read_sync_pacing_state(&connection, account_id)?,
        None => match sync_state.as_ref() {
            Some(sync_state) => read_sync_pacing_state(&connection, &sync_state.account_id)?,
            None => latest_sync_pacing_state(&connection)?,
        },
    };
    let sync_run_summary = match account_id {
        Some(account_id) => match sync_state.as_ref() {
            Some(sync_state) => {
                read_sync_run_summary(&connection, account_id, sync_state.last_sync_mode)?
            }
            None => latest_sync_run_summary_for_account(&connection, account_id)?,
        },
        None => match sync_state.as_ref() {
            Some(sync_state) => read_sync_run_summary(
                &connection,
                &sync_state.account_id,
                sync_state.last_sync_mode,
            )?,
            None => latest_sync_run_summary(&connection)?,
        },
    };
    let message_count = count_messages(&connection, account_id)?;
    let label_count = count_labels(&connection, account_id)?;
    let indexed_message_count = count_indexed_messages(&connection, account_id)?;
    let (attachment_count, vaulted_attachment_count, attachment_export_count) =
        attachment_counts(&connection, account_id)?;

    Ok(Some(MailboxDoctorReport {
        sync_state,
        full_sync_checkpoint,
        sync_pacing_state,
        sync_run_summary,
        message_count,
        label_count,
        indexed_message_count,
        attachment_count,
        vaulted_attachment_count,
        attachment_export_count,
    }))
}

fn attachment_counts(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<(i64, i64, i64), MailboxReadError> {
    Ok((
        count_attachments(connection, account_id)?,
        count_vaulted_attachments(connection, account_id)?,
        count_attachment_export_events(connection, account_id)?,
    ))
}

pub(crate) fn list_attachments(
    database_path: &Path,
    busy_timeout_ms: u64,
    query: &AttachmentListQuery,
) -> Result<Vec<AttachmentListItem>, MailboxReadError> {
    if !database_path.try_exists()? || query.limit == 0 {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let limit = i64::try_from(query.limit).unwrap_or(i64::MAX);
    let mut statement = match connection.prepare(
        "SELECT
             gma.attachment_key,
             gm.message_id,
             gm.thread_id,
             gma.part_id,
             gma.filename,
             gma.mime_type,
             gma.size_bytes,
             gma.content_disposition,
             gma.content_id,
             gma.is_inline,
             gm.internal_date_epoch_ms,
             gm.subject,
             gm.from_header,
             gma.vault_content_hash,
             gma.vault_relative_path,
             gma.vault_size_bytes,
             gma.vault_fetched_at_epoch_s,
             COALESCE(export_stats.export_count, 0)
         FROM gmail_message_attachments gma
         INNER JOIN gmail_messages gm
           ON gm.message_rowid = gma.message_rowid
         LEFT JOIN (
             SELECT attachment_key, COUNT(*) AS export_count
             FROM attachment_export_events
             WHERE account_id = ?1
             GROUP BY attachment_key
         ) AS export_stats
           ON export_stats.attachment_key = gma.attachment_key
         WHERE gma.account_id = ?1
           AND (?2 IS NULL OR gm.thread_id = ?2)
           AND (?3 IS NULL OR gm.message_id = ?3)
           AND (?4 IS NULL OR instr(lower(gma.filename), lower(?4)) > 0)
           AND (?5 IS NULL OR lower(gma.mime_type) = lower(?5))
           AND (?6 = 0 OR gma.vault_relative_path IS NOT NULL)
         ORDER BY gm.internal_date_epoch_ms DESC, gma.filename ASC, gma.attachment_rowid ASC
         LIMIT ?7",
    ) {
        Ok(statement) => statement,
        Err(error) if is_missing_mailbox_table_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let rows = statement
        .query_map(
            rusqlite::params![
                &query.account_id,
                &query.thread_id,
                &query.message_id,
                &query.filename,
                &query.mime_type,
                if query.fetched_only { 1_i64 } else { 0_i64 },
                limit,
            ],
            |row| {
                Ok(AttachmentListItem {
                    attachment_key: row.get(0)?,
                    message_id: row.get(1)?,
                    thread_id: row.get(2)?,
                    part_id: row.get(3)?,
                    filename: row.get(4)?,
                    mime_type: row.get(5)?,
                    size_bytes: row.get(6)?,
                    content_disposition: row.get(7)?,
                    content_id: row.get(8)?,
                    is_inline: row.get::<_, i64>(9)? != 0,
                    internal_date_epoch_ms: row.get(10)?,
                    subject: row.get(11)?,
                    from_header: row.get(12)?,
                    vault_content_hash: row.get(13)?,
                    vault_relative_path: row.get(14)?,
                    vault_size_bytes: row.get(15)?,
                    vault_fetched_at_epoch_s: row.get(16)?,
                    export_count: row.get(17)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub(crate) fn get_attachment_detail(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    attachment_key: &str,
) -> Result<Option<AttachmentDetailRecord>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let detail = connection
        .query_row(
            "SELECT
                 gma.attachment_key,
                 gm.message_id,
                 gm.thread_id,
                 gma.part_id,
                 gma.gmail_attachment_id,
                 gma.filename,
                 gma.mime_type,
                 gma.size_bytes,
                 gma.content_disposition,
                 gma.content_id,
                 gma.is_inline,
                 gm.internal_date_epoch_ms,
                 gm.subject,
                 gm.from_header,
                 gma.vault_content_hash,
                 gma.vault_relative_path,
                 gma.vault_size_bytes,
                 gma.vault_fetched_at_epoch_s,
                 COALESCE((
                     SELECT COUNT(*)
                     FROM attachment_export_events event
                     WHERE event.account_id = ?1
                       AND event.attachment_key = gma.attachment_key
                 ), 0)
             FROM gmail_message_attachments gma
             INNER JOIN gmail_messages gm
               ON gm.message_rowid = gma.message_rowid
             WHERE gma.account_id = ?1
               AND gma.attachment_key = ?2
             LIMIT 1",
            [account_id, attachment_key],
            |row| {
                Ok(AttachmentDetailRecord {
                    attachment_key: row.get(0)?,
                    message_id: row.get(1)?,
                    thread_id: row.get(2)?,
                    part_id: row.get(3)?,
                    gmail_attachment_id: row.get(4)?,
                    filename: row.get(5)?,
                    mime_type: row.get(6)?,
                    size_bytes: row.get(7)?,
                    content_disposition: row.get(8)?,
                    content_id: row.get(9)?,
                    is_inline: row.get::<_, i64>(10)? != 0,
                    internal_date_epoch_ms: row.get(11)?,
                    subject: row.get(12)?,
                    from_header: row.get(13)?,
                    vault_content_hash: row.get(14)?,
                    vault_relative_path: row.get(15)?,
                    vault_size_bytes: row.get(16)?,
                    vault_fetched_at_epoch_s: row.get(17)?,
                    export_count: row.get(18)?,
                })
            },
        )
        .optional();

    match detail {
        Ok(detail) => Ok(detail),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn get_latest_thread_message(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    thread_id: &str,
) -> Result<Option<ThreadMessageSnapshot>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let snapshot = connection
        .query_row(
            "SELECT
                 account_id,
                 message_id,
                 thread_id,
                 internal_date_epoch_ms,
                 subject,
                 from_header,
                 snippet
             FROM gmail_messages
             WHERE account_id = ?1
               AND thread_id = ?2
             ORDER BY internal_date_epoch_ms DESC, message_rowid DESC
             LIMIT 1",
            [account_id, thread_id],
            |row| {
                Ok(ThreadMessageSnapshot {
                    account_id: row.get(0)?,
                    message_id: row.get(1)?,
                    thread_id: row.get(2)?,
                    internal_date_epoch_ms: row.get(3)?,
                    subject: row.get(4)?,
                    from_header: row.get(5)?,
                    snippet: row.get(6)?,
                })
            },
        )
        .optional();

    match snapshot {
        Ok(snapshot) => Ok(snapshot),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn resolve_label_ids_by_names(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    names: &[String],
) -> Result<Vec<(String, String)>, MailboxReadError> {
    if !database_path.try_exists()? || names.is_empty() {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let mut statement = match connection.prepare(
        "SELECT label_id, name
         FROM gmail_labels
         WHERE account_id = ?1
           AND lower(name) = lower(?2)
         ORDER BY name ASC
         LIMIT 1",
    ) {
        Ok(statement) => statement,
        Err(error) if is_missing_mailbox_table_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let mut resolved = Vec::new();
    for name in names {
        let row = statement
            .query_row([account_id, name], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?;
        if let Some(row) = row {
            resolved.push(row);
        }
    }

    Ok(resolved)
}

pub(crate) fn list_label_usage(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Vec<LabelUsageRecord>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let mut statement = match connection.prepare(
        "SELECT
             gl.label_id,
             gl.name,
             gl.label_type,
             gl.messages_total,
             gl.threads_total,
             COUNT(DISTINCT gm.message_rowid) AS local_message_count,
             COUNT(DISTINCT gm.thread_id) AS local_thread_count
         FROM gmail_labels gl
         LEFT JOIN gmail_message_labels gml
           ON gml.label_id = gl.label_id
         LEFT JOIN gmail_messages gm
           ON gm.message_rowid = gml.message_rowid
          AND gm.account_id = gl.account_id
         WHERE gl.account_id = ?1
         GROUP BY
             gl.label_id,
             gl.name,
             gl.label_type,
             gl.messages_total,
             gl.threads_total
         ORDER BY lower(gl.name) ASC, gl.label_id ASC",
    ) {
        Ok(statement) => statement,
        Err(error) if is_missing_mailbox_table_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let rows = statement
        .query_map([account_id], |row| {
            Ok(LabelUsageRecord {
                label_id: row.get(0)?,
                name: row.get(1)?,
                label_type: row.get(2)?,
                messages_total: row.get(3)?,
                threads_total: row.get(4)?,
                local_message_count: row.get(5)?,
                local_thread_count: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub(crate) fn get_mailbox_coverage(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Option<MailboxCoverageReport>, MailboxReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| MailboxReadError::open_database(database_path, source))?;
    let report = connection
        .query_row(
            "SELECT
                 COUNT(*) AS message_count,
                 COUNT(DISTINCT thread_id) AS thread_count,
                 COALESCE((
                     SELECT COUNT(DISTINCT message_rowid)
                     FROM gmail_message_attachments
                     WHERE account_id = ?1
                 ), 0) AS messages_with_attachments,
                 COALESCE(SUM(CASE WHEN list_unsubscribe_header IS NOT NULL THEN 1 ELSE 0 END), 0)
                     AS messages_with_list_unsubscribe,
                 COALESCE(SUM(CASE WHEN list_id_header IS NOT NULL THEN 1 ELSE 0 END), 0)
                     AS messages_with_list_id,
                 COALESCE(SUM(CASE WHEN precedence_header IS NOT NULL THEN 1 ELSE 0 END), 0)
                     AS messages_with_precedence,
                 COALESCE(SUM(CASE WHEN auto_submitted_header IS NOT NULL THEN 1 ELSE 0 END), 0)
                     AS messages_with_auto_submitted
             FROM gmail_messages
             WHERE account_id = ?1",
            [account_id],
            |row| {
                Ok(MailboxCoverageReport {
                    account_id: account_id.to_owned(),
                    message_count: row.get(0)?,
                    thread_count: row.get(1)?,
                    messages_with_attachments: row.get(2)?,
                    messages_with_list_unsubscribe: row.get(3)?,
                    messages_with_list_id: row.get(4)?,
                    messages_with_precedence: row.get(5)?,
                    messages_with_auto_submitted: row.get(6)?,
                })
            },
        )
        .optional();

    match report {
        Ok(report) => Ok(report),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn read_sync_state(
    connection: &Connection,
    account_id: &str,
) -> Result<Option<SyncStateRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
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
             FROM gmail_sync_state
             WHERE account_id = ?1",
            [account_id],
            row_to_sync_state,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_sync_pipeline_column_error(&error) => {
            read_sync_state_without_pipeline_columns(connection, account_id)
        }
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn latest_sync_state(connection: &Connection) -> Result<Option<SyncStateRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
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
             FROM gmail_sync_state
             ORDER BY last_sync_epoch_s DESC
             LIMIT 1",
            [],
            row_to_sync_state,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_sync_pipeline_column_error(&error) => {
            latest_sync_state_without_pipeline_columns(connection)
        }
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn read_full_sync_checkpoint(
    connection: &Connection,
    account_id: &str,
) -> Result<Option<FullSyncCheckpointRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
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
             FROM gmail_full_sync_checkpoint
             WHERE account_id = ?1",
            [account_id],
            row_to_full_sync_checkpoint,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_sync_state_without_pipeline_columns(
    connection: &Connection,
    account_id: &str,
) -> Result<Option<SyncStateRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 cursor_history_id,
                 bootstrap_query,
                 last_sync_mode,
                 last_sync_status,
                 last_error,
                 last_sync_epoch_s,
                 last_full_sync_success_epoch_s,
                 last_incremental_sync_success_epoch_s,
                 0 AS pipeline_enabled,
                 0 AS pipeline_list_queue_high_water,
                 0 AS pipeline_write_queue_high_water,
                 0 AS pipeline_write_batch_count,
                 0 AS pipeline_writer_wait_ms,
                 0 AS pipeline_fetch_batch_count,
                 0 AS pipeline_fetch_batch_avg_ms,
                 0 AS pipeline_fetch_batch_max_ms,
                 0 AS pipeline_writer_tx_count,
                 0 AS pipeline_writer_tx_avg_ms,
                 0 AS pipeline_writer_tx_max_ms,
                 0 AS pipeline_reorder_buffer_high_water,
                 0 AS pipeline_staged_message_count,
                 0 AS pipeline_staged_delete_count,
                 0 AS pipeline_staged_attachment_count,
                 message_count,
                 label_count,
                 indexed_message_count
            FROM gmail_sync_state
             WHERE account_id = ?1",
            [account_id],
            row_to_sync_state,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn latest_sync_state_without_pipeline_columns(
    connection: &Connection,
) -> Result<Option<SyncStateRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 cursor_history_id,
                 bootstrap_query,
                 last_sync_mode,
                 last_sync_status,
                 last_error,
                 last_sync_epoch_s,
                 last_full_sync_success_epoch_s,
                 last_incremental_sync_success_epoch_s,
                 0 AS pipeline_enabled,
                 0 AS pipeline_list_queue_high_water,
                 0 AS pipeline_write_queue_high_water,
                 0 AS pipeline_write_batch_count,
                 0 AS pipeline_writer_wait_ms,
                 0 AS pipeline_fetch_batch_count,
                 0 AS pipeline_fetch_batch_avg_ms,
                 0 AS pipeline_fetch_batch_max_ms,
                 0 AS pipeline_writer_tx_count,
                 0 AS pipeline_writer_tx_avg_ms,
                 0 AS pipeline_writer_tx_max_ms,
                 0 AS pipeline_reorder_buffer_high_water,
                 0 AS pipeline_staged_message_count,
                 0 AS pipeline_staged_delete_count,
                 0 AS pipeline_staged_attachment_count,
                 message_count,
                 label_count,
                 indexed_message_count
             FROM gmail_sync_state
             ORDER BY last_sync_epoch_s DESC
             LIMIT 1",
            [],
            row_to_sync_state,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn is_missing_sync_pipeline_column_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqlInputError { msg, .. } if msg.contains("no such column: pipeline_")
    ) || matches!(
        error,
        rusqlite::Error::SqliteFailure(_, Some(message)) if message.contains("no such column: pipeline_")
    )
}

pub(super) fn read_sync_pacing_state(
    connection: &Connection,
    account_id: &str,
) -> Result<Option<SyncPacingStateRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 learned_quota_units_per_minute,
                 learned_message_fetch_concurrency,
                 clean_run_streak,
                 last_pressure_kind,
                 updated_at_epoch_s
             FROM gmail_sync_pacing_state
             WHERE account_id = ?1",
            [account_id],
            row_to_sync_pacing_state,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn latest_full_sync_checkpoint(
    connection: &Connection,
) -> Result<Option<FullSyncCheckpointRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
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
             FROM gmail_full_sync_checkpoint
             ORDER BY updated_at_epoch_s DESC
             LIMIT 1",
            [],
            row_to_full_sync_checkpoint,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn latest_sync_pacing_state(
    connection: &Connection,
) -> Result<Option<SyncPacingStateRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 learned_quota_units_per_minute,
                 learned_message_fetch_concurrency,
                 clean_run_streak,
                 last_pressure_kind,
                 updated_at_epoch_s
             FROM gmail_sync_pacing_state
             ORDER BY updated_at_epoch_s DESC
             LIMIT 1",
            [],
            row_to_sync_pacing_state,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn read_sync_run_summary(
    connection: &Connection,
    account_id: &str,
    sync_mode: SyncMode,
) -> Result<Option<SyncRunSummaryRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 sync_mode,
                 latest_run_id,
                 latest_status,
                 latest_finished_at_epoch_s,
                 best_clean_run_id,
                 best_clean_quota_units_per_minute,
                 best_clean_message_fetch_concurrency,
                 best_clean_messages_per_second,
                 best_clean_duration_ms,
                 recent_success_count,
                 recent_failure_count,
                 recent_failure_streak,
                 recent_clean_success_streak,
                 regression_detected,
                 regression_kind,
                 regression_run_id,
                 regression_message,
                 updated_at_epoch_s
             FROM gmail_sync_run_summary
             WHERE account_id = ?1
               AND sync_mode = ?2",
            params![account_id, sync_mode.as_str()],
            row_to_sync_run_summary,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn latest_sync_run_summary(
    connection: &Connection,
) -> Result<Option<SyncRunSummaryRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 sync_mode,
                 latest_run_id,
                 latest_status,
                 latest_finished_at_epoch_s,
                 best_clean_run_id,
                 best_clean_quota_units_per_minute,
                 best_clean_message_fetch_concurrency,
                 best_clean_messages_per_second,
                 best_clean_duration_ms,
                 recent_success_count,
                 recent_failure_count,
                 recent_failure_streak,
                 recent_clean_success_streak,
                 regression_detected,
                 regression_kind,
                 regression_run_id,
                 regression_message,
                 updated_at_epoch_s
             FROM gmail_sync_run_summary
             ORDER BY updated_at_epoch_s DESC
             LIMIT 1",
            [],
            row_to_sync_run_summary,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn latest_sync_run_summary_for_account(
    connection: &Connection,
    account_id: &str,
) -> Result<Option<SyncRunSummaryRecord>, MailboxReadError> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 sync_mode,
                 latest_run_id,
                 latest_status,
                 latest_finished_at_epoch_s,
                 best_clean_run_id,
                 best_clean_quota_units_per_minute,
                 best_clean_message_fetch_concurrency,
                 best_clean_messages_per_second,
                 best_clean_duration_ms,
                 recent_success_count,
                 recent_failure_count,
                 recent_failure_streak,
                 recent_clean_success_streak,
                 regression_detected,
                 regression_kind,
                 regression_run_id,
                 regression_message,
                 updated_at_epoch_s
             FROM gmail_sync_run_summary
             WHERE account_id = ?1
             ORDER BY updated_at_epoch_s DESC
             LIMIT 1",
            [account_id],
            row_to_sync_run_summary,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_sync_run_history(
    connection: &Connection,
    account_id: &str,
    limit: usize,
) -> Result<Vec<SyncRunHistoryRecord>, MailboxReadError> {
    let mut statement = match connection.prepare(
        "SELECT
             run_id,
             account_id,
             sync_mode,
             status,
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
         FROM gmail_sync_run_history
         WHERE account_id = ?1
         ORDER BY finished_at_epoch_s DESC, run_id DESC
         LIMIT ?2",
    ) {
        Ok(statement) => statement,
        Err(error) if is_missing_mailbox_table_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = statement
        .query_map(params![account_id, limit], row_to_sync_run_history)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn row_to_sync_state(row: &rusqlite::Row<'_>) -> rusqlite::Result<SyncStateRecord> {
    let last_sync_mode = decode_sync_mode(row.get(3)?, 3)?;
    let last_sync_status = decode_sync_status(row.get(4)?, 4)?;

    Ok(SyncStateRecord {
        account_id: row.get(0)?,
        cursor_history_id: row.get(1)?,
        bootstrap_query: row.get(2)?,
        last_sync_mode,
        last_sync_status,
        last_error: row.get(5)?,
        last_sync_epoch_s: row.get(6)?,
        last_full_sync_success_epoch_s: row.get(7)?,
        last_incremental_sync_success_epoch_s: row.get(8)?,
        pipeline_enabled: row.get::<_, i64>(9)? != 0,
        pipeline_list_queue_high_water: row.get(10)?,
        pipeline_write_queue_high_water: row.get(11)?,
        pipeline_write_batch_count: row.get(12)?,
        pipeline_writer_wait_ms: row.get(13)?,
        pipeline_fetch_batch_count: row.get(14)?,
        pipeline_fetch_batch_avg_ms: row.get(15)?,
        pipeline_fetch_batch_max_ms: row.get(16)?,
        pipeline_writer_tx_count: row.get(17)?,
        pipeline_writer_tx_avg_ms: row.get(18)?,
        pipeline_writer_tx_max_ms: row.get(19)?,
        pipeline_reorder_buffer_high_water: row.get(20)?,
        pipeline_staged_message_count: row.get(21)?,
        pipeline_staged_delete_count: row.get(22)?,
        pipeline_staged_attachment_count: row.get(23)?,
        message_count: row.get(24)?,
        label_count: row.get(25)?,
        indexed_message_count: row.get(26)?,
    })
}

fn row_to_full_sync_checkpoint(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<FullSyncCheckpointRecord> {
    let status =
        FullSyncCheckpointStatus::from_str(&row.get::<_, String>(2)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(error))
        })?;

    Ok(FullSyncCheckpointRecord {
        account_id: row.get(0)?,
        bootstrap_query: row.get(1)?,
        status,
        next_page_token: row.get(3)?,
        cursor_history_id: row.get(4)?,
        pages_fetched: row.get(5)?,
        messages_listed: row.get(6)?,
        messages_upserted: row.get(7)?,
        labels_synced: row.get(8)?,
        staged_label_count: row.get(9)?,
        staged_message_count: row.get(10)?,
        staged_message_label_count: row.get(11)?,
        staged_attachment_count: row.get(12)?,
        started_at_epoch_s: row.get(13)?,
        updated_at_epoch_s: row.get(14)?,
    })
}

fn row_to_sync_pacing_state(row: &rusqlite::Row<'_>) -> rusqlite::Result<SyncPacingStateRecord> {
    let last_pressure_kind = row
        .get::<_, Option<String>>(4)?
        .map(|value| {
            SyncPacingPressureKind::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(error))
            })
        })
        .transpose()?;

    Ok(SyncPacingStateRecord {
        account_id: row.get(0)?,
        learned_quota_units_per_minute: row.get(1)?,
        learned_message_fetch_concurrency: row.get(2)?,
        clean_run_streak: row.get(3)?,
        last_pressure_kind,
        updated_at_epoch_s: row.get(5)?,
    })
}

fn row_to_sync_run_history(row: &rusqlite::Row<'_>) -> rusqlite::Result<SyncRunHistoryRecord> {
    Ok(SyncRunHistoryRecord {
        run_id: row.get(0)?,
        account_id: row.get(1)?,
        sync_mode: decode_sync_mode(row.get(2)?, 2)?,
        status: decode_sync_status(row.get(3)?, 3)?,
        started_at_epoch_s: row.get(4)?,
        finished_at_epoch_s: row.get(5)?,
        bootstrap_query: row.get(6)?,
        cursor_history_id: row.get(7)?,
        fallback_from_history: row.get::<_, i64>(8)? != 0,
        resumed_from_checkpoint: row.get::<_, i64>(9)? != 0,
        pages_fetched: row.get(10)?,
        messages_listed: row.get(11)?,
        messages_upserted: row.get(12)?,
        messages_deleted: row.get(13)?,
        labels_synced: row.get(14)?,
        checkpoint_reused_pages: row.get(15)?,
        checkpoint_reused_messages_upserted: row.get(16)?,
        pipeline_enabled: row.get::<_, i64>(17)? != 0,
        pipeline_list_queue_high_water: row.get(18)?,
        pipeline_write_queue_high_water: row.get(19)?,
        pipeline_write_batch_count: row.get(20)?,
        pipeline_writer_wait_ms: row.get(21)?,
        pipeline_fetch_batch_count: row.get(22)?,
        pipeline_fetch_batch_avg_ms: row.get(23)?,
        pipeline_fetch_batch_max_ms: row.get(24)?,
        pipeline_writer_tx_count: row.get(25)?,
        pipeline_writer_tx_avg_ms: row.get(26)?,
        pipeline_writer_tx_max_ms: row.get(27)?,
        pipeline_reorder_buffer_high_water: row.get(28)?,
        pipeline_staged_message_count: row.get(29)?,
        pipeline_staged_delete_count: row.get(30)?,
        pipeline_staged_attachment_count: row.get(31)?,
        adaptive_pacing_enabled: row.get::<_, i64>(32)? != 0,
        quota_units_budget_per_minute: row.get(33)?,
        message_fetch_concurrency: row.get(34)?,
        quota_units_cap_per_minute: row.get(35)?,
        message_fetch_concurrency_cap: row.get(36)?,
        starting_quota_units_per_minute: row.get(37)?,
        starting_message_fetch_concurrency: row.get(38)?,
        effective_quota_units_per_minute: row.get(39)?,
        effective_message_fetch_concurrency: row.get(40)?,
        adaptive_downshift_count: row.get(41)?,
        estimated_quota_units_reserved: row.get(42)?,
        http_attempt_count: row.get(43)?,
        retry_count: row.get(44)?,
        quota_pressure_retry_count: row.get(45)?,
        concurrency_pressure_retry_count: row.get(46)?,
        backend_retry_count: row.get(47)?,
        throttle_wait_count: row.get(48)?,
        throttle_wait_ms: row.get(49)?,
        retry_after_wait_ms: row.get(50)?,
        duration_ms: row.get(51)?,
        pages_per_second: row.get(52)?,
        messages_per_second: row.get(53)?,
        error_message: row.get(54)?,
    })
}

fn row_to_sync_run_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<SyncRunSummaryRecord> {
    let regression_kind = row
        .get::<_, Option<String>>(15)?
        .map(|value| {
            SyncRunRegressionKind::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(15, Type::Text, Box::new(error))
            })
        })
        .transpose()?;

    Ok(SyncRunSummaryRecord {
        account_id: row.get(0)?,
        sync_mode: decode_sync_mode(row.get(1)?, 1)?,
        latest_run_id: row.get(2)?,
        latest_status: decode_sync_status(row.get(3)?, 3)?,
        latest_finished_at_epoch_s: row.get(4)?,
        best_clean_run_id: row.get(5)?,
        best_clean_quota_units_per_minute: row.get(6)?,
        best_clean_message_fetch_concurrency: row.get(7)?,
        best_clean_messages_per_second: row.get(8)?,
        best_clean_duration_ms: row.get(9)?,
        recent_success_count: row.get(10)?,
        recent_failure_count: row.get(11)?,
        recent_failure_streak: row.get(12)?,
        recent_clean_success_streak: row.get(13)?,
        regression_detected: row.get::<_, i64>(14)? != 0,
        regression_kind,
        regression_run_id: row.get(16)?,
        regression_message: row.get(17)?,
        updated_at_epoch_s: row.get(18)?,
    })
}

fn decode_sync_mode(value: String, column_index: usize) -> rusqlite::Result<SyncMode> {
    SyncMode::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
    })
}

fn decode_sync_status(value: String, column_index: usize) -> rusqlite::Result<SyncStatus> {
    SyncStatus::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
    })
}

pub(super) fn count_messages(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM gmail_messages",
        "SELECT COUNT(*) FROM gmail_messages WHERE account_id = ?1",
        account_id,
    )
}

pub(super) fn count_labels(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM gmail_labels",
        "SELECT COUNT(*) FROM gmail_labels WHERE account_id = ?1",
        account_id,
    )
}

pub(super) fn count_indexed_messages(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM gmail_message_search",
        "SELECT COUNT(*)
         FROM gmail_message_search
         WHERE rowid IN (
             SELECT message_rowid
             FROM gmail_messages
             WHERE account_id = ?1
         )",
        account_id,
    )
}

pub(super) fn count_attachments(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM gmail_message_attachments",
        "SELECT COUNT(*) FROM gmail_message_attachments WHERE account_id = ?1",
        account_id,
    )
}

pub(super) fn count_vaulted_attachments(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM gmail_message_attachments WHERE vault_relative_path IS NOT NULL",
        "SELECT COUNT(*)
         FROM gmail_message_attachments
         WHERE vault_relative_path IS NOT NULL
           AND account_id = ?1",
        account_id,
    )
}

pub(super) fn count_attachment_export_events(
    connection: &Connection,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM attachment_export_events",
        "SELECT COUNT(*) FROM attachment_export_events WHERE account_id = ?1",
        account_id,
    )
}

fn count_with_optional_account(
    connection: &Connection,
    count_all_sql: &str,
    count_account_sql: &str,
    account_id: Option<&str>,
) -> Result<i64, MailboxReadError> {
    let count = match account_id {
        Some(account_id) => connection.query_row(count_account_sql, [account_id], |row| row.get(0)),
        None => connection.query_row(count_all_sql, [], |row| row.get(0)),
    };
    match count {
        Ok(count) => Ok(count),
        Err(error) if is_missing_mailbox_table_error(&error) => Ok(0),
        Err(error) => Err(error.into()),
    }
}
