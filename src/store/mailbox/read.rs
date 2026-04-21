use super::{
    AttachmentDetailRecord, AttachmentListItem, AttachmentListQuery, FullSyncCheckpointRecord,
    FullSyncCheckpointStatus, LabelUsageRecord, MailboxCoverageReport, MailboxDoctorReport,
    MailboxReadError, SyncMode, SyncStateRecord, SyncStatus, ThreadMessageSnapshot,
    is_missing_mailbox_table_error,
};
use crate::store::connection;
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension};
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
    let message_count = count_messages(&connection, account_id)?;
    let label_count = count_labels(&connection, account_id)?;
    let indexed_message_count = count_indexed_messages(&connection, account_id)?;
    let (attachment_count, vaulted_attachment_count, attachment_export_count) =
        attachment_counts(&connection, account_id)?;

    Ok(Some(MailboxDoctorReport {
        sync_state,
        full_sync_checkpoint,
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
        message_count: row.get(9)?,
        label_count: row.get(10)?,
        indexed_message_count: row.get(11)?,
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
