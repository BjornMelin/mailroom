use super::SQLITE_APPLICATION_ID;
use anyhow::{Result, bail};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum DatabaseOpenError {
    #[error(
        "store.busy_timeout_ms must be greater than zero to avoid immediate SQLITE_BUSY failures"
    )]
    InvalidBusyTimeout { busy_timeout_ms: u64 },
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub fn open_or_create(
    path: &Path,
    busy_timeout_ms: u64,
) -> std::result::Result<Connection, DatabaseOpenError> {
    let connection = Connection::open_with_flags(path, create_flags())?;
    configure_busy_timeout(&connection, busy_timeout_ms)?;
    configure_connection_pragmas(&connection)?;
    Ok(connection)
}

pub fn open_read_only_for_diagnostics(
    path: &Path,
    busy_timeout_ms: u64,
) -> std::result::Result<Connection, DatabaseOpenError> {
    let connection = Connection::open_with_flags(path, read_only_flags())?;
    configure_busy_timeout(&connection, busy_timeout_ms)?;
    configure_read_only_connection_pragmas(&connection)?;
    Ok(connection)
}

pub fn open_existing(
    path: &Path,
    busy_timeout_ms: u64,
) -> std::result::Result<Connection, DatabaseOpenError> {
    let connection = Connection::open_with_flags(path, existing_flags())?;
    configure_busy_timeout(&connection, busy_timeout_ms)?;
    configure_connection_pragmas(&connection)?;
    Ok(connection)
}

fn configure_busy_timeout(
    connection: &Connection,
    busy_timeout_ms: u64,
) -> std::result::Result<(), DatabaseOpenError> {
    if busy_timeout_ms == 0 {
        return Err(DatabaseOpenError::InvalidBusyTimeout { busy_timeout_ms });
    }
    connection.busy_timeout(Duration::from_millis(busy_timeout_ms))?;
    Ok(())
}

fn configure_connection_pragmas(
    connection: &Connection,
) -> std::result::Result<(), DatabaseOpenError> {
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.pragma_update(None, "trusted_schema", false)?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

fn configure_read_only_connection_pragmas(
    connection: &Connection,
) -> std::result::Result<(), DatabaseOpenError> {
    connection.pragma_update(None, "foreign_keys", true)?;
    connection.pragma_update(None, "trusted_schema", false)?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

pub fn configure_hardening_pragmas(connection: &Connection) -> Result<()> {
    let journal_mode = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        bail!("failed to enforce journal_mode=WAL; sqlite reported journal_mode={journal_mode}");
    }
    connection.pragma_update(None, "application_id", SQLITE_APPLICATION_ID)?;
    Ok(())
}

fn create_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
}

fn read_only_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
}

fn existing_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX
}

#[cfg(test)]
mod tests {
    use super::configure_busy_timeout;
    use rusqlite::Connection;

    #[test]
    fn configure_busy_timeout_rejects_zero() {
        let connection = Connection::open_in_memory().unwrap();
        assert!(configure_busy_timeout(&connection, 0).is_err());
    }
}
