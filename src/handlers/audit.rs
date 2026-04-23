use crate::audit;
use crate::cli::AuditCommand;
use crate::{config, workspace};
use anyhow::Result;

pub(crate) fn handle_audit_command(
    paths: &workspace::WorkspacePaths,
    command: AuditCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AuditCommand::Labels { json } => audit::labels(&config_report)?.print(json)?,
        AuditCommand::Verification { json } => audit::verification(&config_report)?.print(json)?,
    }

    Ok(())
}
