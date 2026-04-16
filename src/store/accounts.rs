use super::connection;
use anyhow::Result;
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};
use serde::Serialize;
use std::path::Path;

const GMAIL_PROVIDER: &str = "gmail";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AccountRecord {
    pub account_id: String,
    pub provider: String,
    pub email_address: String,
    pub history_id: String,
    pub messages_total: i64,
    pub threads_total: i64,
    pub access_scope: String,
    pub is_active: bool,
    pub created_at_epoch_s: i64,
    pub updated_at_epoch_s: i64,
    pub last_profile_refresh_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub struct UpsertAccountInput {
    pub email_address: String,
    pub history_id: String,
    pub messages_total: i64,
    pub threads_total: i64,
    pub access_scope: String,
    pub refreshed_at_epoch_s: i64,
}

impl UpsertAccountInput {
    pub fn gmail_account_id(&self) -> String {
        format!("gmail:{}", self.email_address.to_ascii_lowercase())
    }
}

pub fn upsert_active(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &UpsertAccountInput,
) -> Result<AccountRecord> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)?;
    let transaction = connection.transaction()?;

    transaction.execute(
        "UPDATE accounts
         SET is_active = 0,
             updated_at_epoch_s = ?1
         WHERE is_active = 1
           AND account_id <> ?2",
        params![input.refreshed_at_epoch_s, input.gmail_account_id()],
    )?;

    let existing_created_at: Option<i64> = transaction
        .query_row(
            "SELECT created_at_epoch_s FROM accounts WHERE account_id = ?1",
            [input.gmail_account_id()],
            |row| row.get(0),
        )
        .optional()?;
    let created_at_epoch_s = existing_created_at.unwrap_or(input.refreshed_at_epoch_s);

    transaction.execute(
        "INSERT INTO accounts (
             account_id,
             provider,
             email_address,
             history_id,
             messages_total,
             threads_total,
             access_scope,
             is_active,
             created_at_epoch_s,
             updated_at_epoch_s,
             last_profile_refresh_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?10)
         ON CONFLICT(account_id) DO UPDATE SET
             provider = excluded.provider,
             email_address = excluded.email_address,
             history_id = excluded.history_id,
             messages_total = excluded.messages_total,
             threads_total = excluded.threads_total,
             access_scope = excluded.access_scope,
             is_active = 1,
             updated_at_epoch_s = excluded.updated_at_epoch_s,
             last_profile_refresh_epoch_s = excluded.last_profile_refresh_epoch_s",
        params![
            input.gmail_account_id(),
            GMAIL_PROVIDER,
            &input.email_address,
            &input.history_id,
            input.messages_total,
            input.threads_total,
            &input.access_scope,
            created_at_epoch_s,
            input.refreshed_at_epoch_s,
            input.refreshed_at_epoch_s,
        ],
    )?;

    let record = read_active_account(&transaction)?
        .expect("upsert_active should always leave an active account record");
    transaction.commit()?;
    Ok(record)
}

pub fn get_active(database_path: &Path, busy_timeout_ms: u64) -> Result<Option<AccountRecord>> {
    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)?;
    read_active_account(&connection)
}

pub fn deactivate_all(
    database_path: &Path,
    busy_timeout_ms: u64,
    updated_at_epoch_s: i64,
) -> Result<usize> {
    let connection = match connection::open_existing(database_path, busy_timeout_ms) {
        Ok(connection) => connection,
        Err(error) if is_missing_database_error(&error) => return Ok(0),
        Err(error) => return Err(error),
    };
    let changed = match connection.execute(
        "UPDATE accounts
         SET is_active = 0,
             updated_at_epoch_s = ?1
         WHERE is_active = 1",
        [updated_at_epoch_s],
    ) {
        Ok(changed) => changed,
        Err(error) if is_missing_accounts_table_error(&error) => 0,
        Err(error) => return Err(error.into()),
    };
    Ok(changed)
}

fn read_active_account(connection: &Connection) -> Result<Option<AccountRecord>> {
    let record = connection
        .query_row(
            "SELECT
                 account_id,
                 provider,
                 email_address,
                 history_id,
                 messages_total,
                 threads_total,
                 access_scope,
                 is_active,
                 created_at_epoch_s,
                 updated_at_epoch_s,
                 last_profile_refresh_epoch_s
             FROM accounts
             WHERE is_active = 1
             ORDER BY updated_at_epoch_s DESC
             LIMIT 1",
            [],
            |row| {
                Ok(AccountRecord {
                    account_id: row.get(0)?,
                    provider: row.get(1)?,
                    email_address: row.get(2)?,
                    history_id: row.get(3)?,
                    messages_total: row.get(4)?,
                    threads_total: row.get(5)?,
                    access_scope: row.get(6)?,
                    is_active: row.get::<_, i64>(7)? != 0,
                    created_at_epoch_s: row.get(8)?,
                    updated_at_epoch_s: row.get(9)?,
                    last_profile_refresh_epoch_s: row.get(10)?,
                })
            },
        )
        .optional();
    let record = match record {
        Ok(record) => record,
        Err(error) if is_missing_accounts_table_error(&error) => None,
        Err(error) => return Err(error.into()),
    };
    Ok(record)
}

fn is_missing_accounts_table_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(_, Some(message)) if message.contains("no such table: accounts")
    )
}

fn is_missing_database_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<rusqlite::Error>()
        .is_some_and(|error| {
            matches!(
                error,
                rusqlite::Error::SqliteFailure(code, _)
                    if code.code == ErrorCode::CannotOpen
            )
        })
}

#[cfg(test)]
mod tests {
    use super::{GMAIL_PROVIDER, UpsertAccountInput, deactivate_all, get_active, upsert_active};
    use crate::config::resolve;
    use crate::store::init;
    use crate::workspace::WorkspacePaths;
    use rusqlite::Connection;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn upsert_active_persists_gmail_account_rows() {
        let repo_root = unique_temp_dir("mailroom-accounts-upsert");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        let record = upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("12345"),
                messages_total: 10,
                threads_total: 7,
                access_scope: String::from("scope:a scope:b"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();

        assert_eq!(record.account_id, "gmail:operator@example.com");
        assert_eq!(record.provider, GMAIL_PROVIDER);
        assert!(record.is_active);

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn deactivate_all_clears_active_account_flag() {
        let repo_root = unique_temp_dir("mailroom-accounts-deactivate");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("12345"),
                messages_total: 10,
                threads_total: 7,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();

        let changed = deactivate_all(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            200,
        )
        .unwrap();

        assert_eq!(changed, 1);
        assert!(
            get_active(
                &config_report.config.store.database_path,
                config_report.config.store.busy_timeout_ms,
            )
            .unwrap()
            .is_none()
        );

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn deactivate_all_tolerates_pre_accounts_schema() {
        let repo_root = unique_temp_dir("mailroom-accounts-missing-table");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        let connection = Connection::open(&config_report.config.store.database_path).unwrap();
        connection
            .execute_batch(
                "PRAGMA user_version = 1;
                 CREATE TABLE app_metadata (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 ) STRICT;",
            )
            .unwrap();

        let changed = deactivate_all(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            200,
        )
        .unwrap();

        assert_eq!(changed, 0);

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn deactivate_all_does_not_create_store_for_missing_database() {
        let repo_root = unique_temp_dir("mailroom-accounts-missing-db");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();

        let changed = deactivate_all(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            200,
        )
        .unwrap();

        assert_eq!(changed, 0);
        assert!(!config_report.config.store.database_path.exists());

        fs::remove_dir_all(repo_root).unwrap();
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
