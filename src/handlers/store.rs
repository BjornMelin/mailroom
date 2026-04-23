use crate::cli::StoreCommand;
use crate::{config, configured_paths, store, workspace};
use anyhow::Result;

pub(crate) fn handle_store_command(
    paths: &workspace::WorkspacePaths,
    command: StoreCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        StoreCommand::Init { json } => {
            let configured_paths = configured_paths(&config_report)?;
            configured_paths.ensure_runtime_dirs()?;
            store::init(&config_report)?.print(json)?;
        }
        StoreCommand::Doctor { json } => store::inspect(config_report)?.print(json)?,
    }

    Ok(())
}
