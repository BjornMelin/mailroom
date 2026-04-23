pub mod accounts;
pub mod automation;
mod connection;
pub mod doctor;
pub mod mailbox;
mod migrations;
pub mod workflows;

use crate::config::ConfigReport;
use anyhow::{Result, anyhow};
use std::fs;
use std::path::Path;

pub use connection::StoreInitReport;
pub use doctor::{StoreDoctorReport, inspect};

pub const SQLITE_APPLICATION_ID: i64 = 0x4D41_494C;

pub fn init(config_report: &ConfigReport) -> Result<StoreInitReport> {
    let database_path = config_report.config.store.database_path.clone();
    let database_previously_existed = database_path.exists();

    ensure_database_parent_exists(&database_path)?;
    let mut connection =
        connection::open_or_create(&database_path, config_report.config.store.busy_timeout_ms)?;

    let initial_pragmas = connection::read_pragmas(&connection)?;
    validate_application_id(&database_path, initial_pragmas.application_id)?;
    connection::configure_hardening_pragmas(&connection)?;
    harden_database_permissions(&database_path)?;

    migrations::apply(&mut connection)?;
    harden_database_permissions(&database_path)?;

    let pragmas = connection::read_pragmas(&connection)?;
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

#[cfg(unix)]
fn harden_database_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut candidate_paths = Vec::with_capacity(3);
    candidate_paths.push(path.to_path_buf());
    candidate_paths.push(std::path::PathBuf::from(format!("{}-wal", path.display())));
    candidate_paths.push(std::path::PathBuf::from(format!("{}-shm", path.display())));

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
mod tests;
