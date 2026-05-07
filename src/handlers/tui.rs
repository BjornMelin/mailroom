use crate::cli::TuiArgs;
use crate::{config, tui, workspace};
use anyhow::Result;

pub(crate) async fn handle_tui_command(
    paths: &workspace::WorkspacePaths,
    args: TuiArgs,
) -> Result<()> {
    let config_report = config::resolve(paths)?;
    tui::run(paths, config_report, args.search).await
}
