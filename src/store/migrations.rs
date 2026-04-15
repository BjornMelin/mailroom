use anyhow::{Result, anyhow};
use include_dir::{Dir, include_dir};
use rusqlite::Connection;
use rusqlite_migration::Migrations;
use std::sync::LazyLock;

static MIGRATIONS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/migrations");
static MIGRATIONS: LazyLock<Result<Migrations<'static>, String>> = LazyLock::new(|| {
    Migrations::from_directory(&MIGRATIONS_DIR).map_err(|error| error.to_string())
});

fn embedded_migrations() -> Result<&'static Migrations<'static>> {
    MIGRATIONS.as_ref().map_err(|error| {
        anyhow!(
            "failed to load embedded migrations from {}: {error}",
            MIGRATIONS_DIR.path().display()
        )
    })
}

pub fn apply(connection: &mut Connection) -> Result<()> {
    embedded_migrations()?.to_latest(connection)?;
    Ok(())
}

pub fn known_migration_count() -> usize {
    MIGRATIONS_DIR.dirs().count()
}

#[cfg(test)]
pub fn validate_migrations() -> Result<()> {
    embedded_migrations()?.validate()?;
    Ok(())
}
