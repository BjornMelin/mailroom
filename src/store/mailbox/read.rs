use super::{
    MailboxDoctorReport, SyncMode, SyncStateRecord, SyncStatus, is_missing_mailbox_table_error,
};
use crate::store::connection;
use anyhow::Result;
use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension};
use std::path::Path;
use std::str::FromStr;

pub(crate) fn get_sync_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Option<SyncStateRecord>> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)?;
    read_sync_state(&connection, account_id)
}

pub(crate) fn inspect_mailbox(
    database_path: &Path,
    busy_timeout_ms: u64,
) -> Result<Option<MailboxDoctorReport>> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)?;
    let sync_state = latest_sync_state(&connection)?;
    let message_count = count_messages(&connection, None)?;
    let label_count = count_labels(&connection, None)?;
    let indexed_message_count = count_indexed_messages(&connection, None)?;

    Ok(Some(MailboxDoctorReport {
        sync_state,
        message_count,
        label_count,
        indexed_message_count,
    }))
}

pub(super) fn read_sync_state(
    connection: &Connection,
    account_id: &str,
) -> Result<Option<SyncStateRecord>> {
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
        Err(error) => Err(anyhow::Error::from(error)),
    }
}

fn latest_sync_state(connection: &Connection) -> Result<Option<SyncStateRecord>> {
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
        Err(error) => Err(anyhow::Error::from(error)),
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

pub(super) fn count_messages(connection: &Connection, account_id: Option<&str>) -> Result<i64> {
    count_with_optional_account(
        connection,
        "SELECT COUNT(*) FROM gmail_messages",
        "SELECT COUNT(*) FROM gmail_messages WHERE account_id = ?1",
        account_id,
    )
}

pub(super) fn count_labels(connection: &Connection, account_id: Option<&str>) -> Result<i64> {
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
) -> Result<i64> {
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

fn count_with_optional_account(
    connection: &Connection,
    count_all_sql: &str,
    count_account_sql: &str,
    account_id: Option<&str>,
) -> Result<i64> {
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
