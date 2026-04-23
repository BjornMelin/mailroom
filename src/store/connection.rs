use super::SQLITE_APPLICATION_ID;
use anyhow::{Result, bail};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use std::io::{self, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone, Serialize)]
pub struct StoreInitReport {
    pub database_path: PathBuf,
    pub database_previously_existed: bool,
    pub schema_version: i64,
    pub known_migrations: usize,
    pub pending_migrations: usize,
    pub pragmas: StorePragmas,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorePragmas {
    pub application_id: i64,
    pub user_version: i64,
    pub foreign_keys: bool,
    pub trusted_schema: bool,
    pub journal_mode: String,
    pub synchronous: i64,
    pub busy_timeout_ms: i64,
}

impl StoreInitReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("database_path={}", self.database_path.display());
            println!(
                "database_previously_existed={}",
                self.database_previously_existed
            );
            println!("schema_version={}", self.schema_version);
            println!("known_migrations={}", self.known_migrations);
            println!("pending_migrations={}", self.pending_migrations);
            print_pragmas(&self.pragmas)?;
        }

        Ok(())
    }
}

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

pub(super) fn configure_busy_timeout(
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

pub(super) fn read_pragmas(connection: &Connection) -> Result<StorePragmas> {
    let application_id = connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
    let user_version = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let foreign_keys =
        connection.pragma_query_value::<i64, _>(None, "foreign_keys", |row| row.get(0))? != 0;
    let trusted_schema =
        connection.pragma_query_value::<i64, _>(None, "trusted_schema", |row| row.get(0))? != 0;
    let journal_mode = connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let synchronous = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    let busy_timeout_ms = connection.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;

    Ok(StorePragmas {
        application_id,
        user_version,
        foreign_keys,
        trusted_schema,
        journal_mode,
        synchronous,
        busy_timeout_ms,
    })
}

pub(crate) fn print_pragmas(pragmas: &StorePragmas) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_pragmas(&mut stdout, pragmas)
}

pub(crate) fn write_pragmas<W: Write>(writer: &mut W, pragmas: &StorePragmas) -> Result<()> {
    writeln!(writer, "application_id={}", pragmas.application_id)?;
    writeln!(writer, "user_version={}", pragmas.user_version)?;
    writeln!(writer, "foreign_keys={}", pragmas.foreign_keys)?;
    writeln!(writer, "trusted_schema={}", pragmas.trusted_schema)?;
    writeln!(writer, "journal_mode={}", pragmas.journal_mode)?;
    writeln!(writer, "synchronous={}", pragmas.synchronous)?;
    writeln!(writer, "busy_timeout_ms={}", pragmas.busy_timeout_ms)?;
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
