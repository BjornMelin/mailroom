use crate::cli::TuiArgs;
use crate::{config, configured_paths, tui, workspace};
use anyhow::Result;
use tokio::task::spawn_blocking;

pub(crate) async fn handle_tui_command(
    paths: &workspace::WorkspacePaths,
    args: TuiArgs,
) -> Result<()> {
    let resolve_paths = paths.clone();
    let config_report = spawn_blocking(move || config::resolve(&resolve_paths)).await??;
    let paths = configured_paths(&config_report)?;
    tui::run(&paths, config_report, args.search).await
}
