use crate::{config, configured_paths, workspace};
use anyhow::Result;

pub(crate) fn handle_workspace_command(
    paths: &workspace::WorkspacePaths,
    command: crate::cli::WorkspaceCommand,
) -> Result<()> {
    match command {
        crate::cli::WorkspaceCommand::Init => {
            let config_report = config::resolve(paths)?;
            let configured_paths = configured_paths(&config_report)?;
            let created = configured_paths.ensure_runtime_dirs()?;
            println!(
                "initialized {} new runtime paths under {}",
                created.len(),
                configured_paths.runtime_root.display()
            );
            for path in created {
                println!("{}", path.display());
            }
        }
    }

    Ok(())
}
