pub mod accounts;
mod connection;
pub mod mailbox;
mod migrations;
pub mod workflows;

use crate::config::ConfigReport;
use anyhow::{Result, anyhow};
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

pub const SQLITE_APPLICATION_ID: i64 = 0x4D41_494C;

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
pub struct StoreDoctorReport {
    pub config: ConfigReport,
    pub database_exists: bool,
    pub database_path: PathBuf,
    pub known_migrations: usize,
    pub schema_version: Option<i64>,
    pub pending_migrations: Option<usize>,
    pub pragmas: Option<StorePragmas>,
    pub mailbox: Option<mailbox::MailboxDoctorReport>,
    pub workflows: Option<workflows::WorkflowDoctorReport>,
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
            print_pragmas(&self.pragmas);
        }

        Ok(())
    }
}

impl StoreDoctorReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("database_path={}", self.database_path.display());
            println!("database_exists={}", self.database_exists);
            println!("known_migrations={}", self.known_migrations);
            println!(
                "user_config={}",
                self.config
                    .locations
                    .user_config_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| String::from("<unavailable>"))
            );
            println!(
                "user_config_exists={}",
                self.config.locations.user_config_exists
            );
            println!(
                "repo_config={}",
                self.config.locations.repo_config_path.display()
            );
            println!(
                "repo_config_exists={}",
                self.config.locations.repo_config_exists
            );
            match self.schema_version {
                Some(version) => println!("schema_version={version}"),
                None => println!("schema_version=<uninitialized>"),
            }
            match self.pending_migrations {
                Some(pending) => println!("pending_migrations={pending}"),
                None => println!("pending_migrations=<uninitialized>"),
            }
            if let Some(pragmas) = &self.pragmas {
                print_pragmas(pragmas);
            }
            if let Some(mailbox) = &self.mailbox {
                println!("mailbox_message_count={}", mailbox.message_count);
                println!("mailbox_label_count={}", mailbox.label_count);
                println!(
                    "mailbox_indexed_message_count={}",
                    mailbox.indexed_message_count
                );
                match &mailbox.sync_state {
                    Some(sync_state) => {
                        println!("mailbox_sync_status={}", sync_state.last_sync_status);
                        println!("mailbox_sync_mode={}", sync_state.last_sync_mode);
                        println!("mailbox_sync_epoch_s={}", sync_state.last_sync_epoch_s);
                        match sync_state.last_full_sync_success_epoch_s {
                            Some(epoch) => {
                                println!("mailbox_last_full_sync_success_epoch_s={epoch}")
                            }
                            None => println!("mailbox_last_full_sync_success_epoch_s=<none>"),
                        }
                        match sync_state.last_incremental_sync_success_epoch_s {
                            Some(epoch) => {
                                println!("mailbox_last_incremental_sync_success_epoch_s={epoch}")
                            }
                            None => {
                                println!("mailbox_last_incremental_sync_success_epoch_s=<none>")
                            }
                        }
                        match &sync_state.cursor_history_id {
                            Some(history_id) => println!("mailbox_cursor_history_id={history_id}"),
                            None => println!("mailbox_cursor_history_id=<none>"),
                        }
                    }
                    None => println!("mailbox_sync_status=<never-run>"),
                }
            }
            if let Some(workflows) = &self.workflows {
                println!("workflow_count={}", workflows.workflow_count);
                println!("workflow_open_count={}", workflows.open_workflow_count);
                println!("workflow_draft_count={}", workflows.draft_workflow_count);
                println!("workflow_event_count={}", workflows.event_count);
                println!(
                    "workflow_draft_revision_count={}",
                    workflows.draft_revision_count
                );
            }
        }

        Ok(())
    }
}

pub fn init(config_report: &ConfigReport) -> Result<StoreInitReport> {
    let database_path = config_report.config.store.database_path.clone();
    let database_previously_existed = database_path.exists();

    ensure_database_parent_exists(&database_path)?;
    let mut connection =
        connection::open_or_create(&database_path, config_report.config.store.busy_timeout_ms)?;

    let initial_pragmas = read_pragmas(&connection)?;
    validate_application_id(&database_path, initial_pragmas.application_id)?;
    connection::configure_hardening_pragmas(&connection)?;
    harden_database_permissions(&database_path)?;

    migrations::apply(&mut connection)?;
    harden_database_permissions(&database_path)?;

    let pragmas = read_pragmas(&connection)?;
    let known_migrations = migrations::known_migration_count();
    let pending_migrations = pending_migrations(known_migrations, pragmas.user_version)?;

    Ok(StoreInitReport {
        database_path,
        database_previously_existed,
        schema_version: pragmas.user_version,
        known_migrations,
        pending_migrations,
        pragmas,
    })
}

pub fn inspect(config_report: ConfigReport) -> Result<StoreDoctorReport> {
    let database_path = config_report.config.store.database_path.clone();
    let known_migrations = migrations::known_migration_count();

    if !database_path.exists() {
        return Ok(StoreDoctorReport {
            config: config_report,
            database_exists: false,
            database_path,
            known_migrations,
            schema_version: None,
            pending_migrations: None,
            pragmas: None,
            mailbox: None,
            workflows: None,
        });
    }

    let connection = connection::open_read_only_for_diagnostics(
        &database_path,
        config_report.config.store.busy_timeout_ms,
    )?;
    let pragmas = read_pragmas(&connection)?;
    let pending_migrations = pending_migrations(known_migrations, pragmas.user_version)?;
    let mailbox =
        mailbox::inspect_mailbox(&database_path, config_report.config.store.busy_timeout_ms)?;
    let workflows =
        workflows::inspect_workflows(&database_path, config_report.config.store.busy_timeout_ms)?;

    Ok(StoreDoctorReport {
        config: config_report,
        database_exists: true,
        database_path,
        known_migrations,
        schema_version: Some(pragmas.user_version),
        pending_migrations: Some(pending_migrations),
        pragmas: Some(pragmas),
        mailbox,
        workflows,
    })
}

fn ensure_database_parent_exists(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("database path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)?;
    Ok(())
}

fn pending_migrations(known_migrations: usize, user_version: i64) -> Result<usize> {
    if user_version < 0 {
        return Ok(known_migrations);
    }

    let user_version = user_version as usize;
    if user_version > known_migrations {
        return Err(anyhow!(
            "database schema version {user_version} is newer than embedded migrations ({known_migrations})"
        ));
    }

    Ok(known_migrations - user_version)
}

fn validate_application_id(database_path: &Path, application_id: i64) -> Result<()> {
    if application_id != 0 && application_id != SQLITE_APPLICATION_ID {
        return Err(anyhow!(
            "database {} has application_id {}, expected 0 or {}",
            database_path.display(),
            application_id,
            SQLITE_APPLICATION_ID
        ));
    }

    Ok(())
}

fn read_pragmas(connection: &Connection) -> Result<StorePragmas> {
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

fn print_pragmas(pragmas: &StorePragmas) {
    println!("application_id={}", pragmas.application_id);
    println!("user_version={}", pragmas.user_version);
    println!("foreign_keys={}", pragmas.foreign_keys);
    println!("trusted_schema={}", pragmas.trusted_schema);
    println!("journal_mode={}", pragmas.journal_mode);
    println!("synchronous={}", pragmas.synchronous);
    println!("busy_timeout_ms={}", pragmas.busy_timeout_ms);
}

#[cfg(unix)]
fn harden_database_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut candidate_paths = Vec::with_capacity(3);
    candidate_paths.push(path.to_path_buf());
    candidate_paths.push(PathBuf::from(format!("{}-wal", path.display())));
    candidate_paths.push(PathBuf::from(format!("{}-shm", path.display())));

    for candidate in candidate_paths {
        if candidate.exists() {
            fs::set_permissions(candidate, fs::Permissions::from_mode(0o600))?;
        }
    }

    Ok(())
}

#[cfg(not(unix))]
fn harden_database_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SQLITE_APPLICATION_ID, harden_database_permissions, init, inspect, migrations};
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use rusqlite::Connection;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn migrations_validate_successfully() {
        migrations::validate_migrations().unwrap();
    }

    #[test]
    fn store_init_creates_and_migrates_database() {
        let repo_root = unique_temp_dir("mailroom-store-init");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();

        let report = init(&config_report).unwrap();

        assert!(report.database_path.exists());
        assert_eq!(report.schema_version, 4);
        assert_eq!(report.pragmas.application_id, SQLITE_APPLICATION_ID);

        let connection = Connection::open(&report.database_path).unwrap();
        let substrate_tables: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table'
                   AND name IN (
                       'app_metadata',
                       'accounts',
                       'gmail_labels',
                       'gmail_messages',
                       'gmail_message_labels',
                       'gmail_sync_state'
                   )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(substrate_tables, 6);

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn store_doctor_reports_absent_database_without_creating_it() {
        let repo_root = unique_temp_dir("mailroom-store-doctor");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();

        let report = inspect(resolve(&paths).unwrap()).unwrap();

        assert!(!report.database_exists);
        assert!(report.pragmas.is_none());
        assert!(report.schema_version.is_none());

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn store_doctor_reports_persisted_drift_without_rewriting_it() {
        let repo_root = unique_temp_dir("mailroom-store-doctor-drift");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();

        let mut config_report = resolve(&paths).unwrap();
        let init_report = init(&config_report).unwrap();

        {
            let connection = Connection::open(&init_report.database_path).unwrap();
            connection
                .pragma_update(None, "application_id", 7_i64)
                .unwrap();
            connection
                .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                    row.get::<_, String>(0)
                })
                .unwrap();
            connection
                .pragma_update(None, "synchronous", "FULL")
                .unwrap();
        }

        config_report.config.store.database_path = init_report.database_path.clone();
        let report = inspect(config_report).unwrap();

        let pragmas = report.pragmas.unwrap();
        assert_eq!(pragmas.application_id, 7);
        assert_eq!(pragmas.journal_mode, "delete");
        assert!(pragmas.foreign_keys);
        assert!(!pragmas.trusted_schema);
        assert_eq!(pragmas.synchronous, 1);

        let connection = Connection::open(&init_report.database_path).unwrap();
        let application_id: i64 = connection
            .pragma_query_value(None, "application_id", |row| row.get(0))
            .unwrap();
        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let synchronous: i64 = connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();

        assert_eq!(application_id, 7);
        assert_eq!(journal_mode, "delete");
        assert_eq!(synchronous, 2);

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn store_init_rejects_foreign_database_before_mutating_it() {
        let repo_root = unique_temp_dir("mailroom-store-init-foreign");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();

        let config_report = resolve(&paths).unwrap();
        {
            let connection = Connection::open(&config_report.config.store.database_path).unwrap();
            connection
                .pragma_update(None, "application_id", 7_i64)
                .unwrap();
            connection
                .pragma_update(None, "user_version", 0_i64)
                .unwrap();
        }

        let error = init(&config_report).unwrap_err();
        let error_message = error.to_string();
        assert!(error_message.contains("application_id 7"));
        assert!(error_message.contains("expected 0 or"));

        let connection = Connection::open(&config_report.config.store.database_path).unwrap();
        let application_id: i64 = connection
            .pragma_query_value(None, "application_id", |row| row.get(0))
            .unwrap();
        let user_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();

        assert_eq!(application_id, 7);
        assert_eq!(user_version, 0);

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn pending_migrations_errors_when_database_is_ahead() {
        let error = super::pending_migrations(1, 4).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("database schema version 4 is newer than embedded migrations (1)")
        );
    }

    #[cfg(unix)]
    #[test]
    fn store_doctor_can_inspect_read_only_database_copy() {
        let repo_root = unique_temp_dir("mailroom-store-doctor-readonly");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();

        let mut config_report = resolve(&paths).unwrap();
        let init_report = init(&config_report).unwrap();
        let read_only_db = repo_root.join("readonly.sqlite3");

        fs::copy(&init_report.database_path, &read_only_db).unwrap();
        fs::set_permissions(&read_only_db, fs::Permissions::from_mode(0o400)).unwrap();

        config_report.config.store.database_path = read_only_db.clone();
        let report = inspect(config_report).unwrap();

        assert!(report.database_exists);
        assert_eq!(report.database_path, read_only_db);
        let pragmas = report.pragmas.unwrap();
        assert_eq!(pragmas.application_id, SQLITE_APPLICATION_ID);
        assert!(pragmas.foreign_keys);
        assert!(!pragmas.trusted_schema);
        assert_eq!(pragmas.synchronous, 1);

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn harden_database_permissions_updates_sqlite_sidecars() {
        let repo_root = unique_temp_dir("mailroom-store-permissions");
        fs::create_dir_all(&repo_root).unwrap();

        let database_path = repo_root.join("store.sqlite3");
        let wal_path = repo_root.join("store.sqlite3-wal");
        let shm_path = repo_root.join("store.sqlite3-shm");

        fs::write(&database_path, b"").unwrap();
        fs::write(&wal_path, b"").unwrap();
        fs::write(&shm_path, b"").unwrap();

        fs::set_permissions(&database_path, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&wal_path, fs::Permissions::from_mode(0o644)).unwrap();
        fs::set_permissions(&shm_path, fs::Permissions::from_mode(0o644)).unwrap();

        harden_database_permissions(&database_path).unwrap();

        let database_mode = fs::metadata(&database_path).unwrap().permissions().mode() & 0o777;
        let wal_mode = fs::metadata(&wal_path).unwrap().permissions().mode() & 0o777;
        let shm_mode = fs::metadata(&shm_path).unwrap().permissions().mode() & 0o777;

        assert_eq!(database_mode, 0o600);
        assert_eq!(wal_mode, 0o600);
        assert_eq!(shm_mode, 0o600);

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
