use crate::automation;
use crate::cli::{AutomationCommand, AutomationRulesCommand};
use crate::{config, workspace};
use anyhow::Result;

pub(crate) async fn handle_automation_command(
    paths: &workspace::WorkspacePaths,
    command: AutomationCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AutomationCommand::Rules {
            command: AutomationRulesCommand::Validate { json },
        } => automation::validate_rules(&config_report)
            .await?
            .print(json)?,
        AutomationCommand::Run {
            rule_ids,
            limit,
            json,
        } => automation::run_preview(
            &config_report,
            automation::AutomationRunRequest { rule_ids, limit },
        )
        .await?
        .print(json)?,
        AutomationCommand::Show { run_id, json } => automation::show_run(&config_report, run_id)
            .await?
            .print(json)?,
        AutomationCommand::Apply {
            run_id,
            execute,
            json,
        } => automation::apply_run(&config_report, run_id, execute)
            .await?
            .print(json)?,
    }

    Ok(())
}
