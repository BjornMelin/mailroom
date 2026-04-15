use anyhow::Result;
use include_dir::{Dir, include_dir};
use rusqlite::Connection;
use rusqlite_migration::Migrations;
use std::sync::LazyLock;

static MIGRATIONS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/migrations");
static MIGRATIONS: LazyLock<Migrations<'static>> =
    LazyLock::new(|| Migrations::from_directory(&MIGRATIONS_DIR).unwrap());

pub fn apply(connection: &mut Connection) -> Result<()> {
    MIGRATIONS.to_latest(connection)?;
    Ok(())
}

pub fn known_migration_count() -> usize {
    MIGRATIONS_DIR.dirs().count()
}

#[cfg(test)]
pub fn validate_migrations() -> Result<()> {
    MIGRATIONS.validate()?;
    Ok(())
}
