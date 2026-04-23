use crate::cli::{GmailCommand, GmailLabelsCommand};
use crate::{config, gmail, gmail_client_for_config, workspace};
use anyhow::Result;
use serde::Serialize;

pub(crate) async fn handle_gmail_command(
    paths: &workspace::WorkspacePaths,
    command: GmailCommand,
) -> Result<()> {
    let paths = paths.clone();
    let config_report = tokio::task::spawn_blocking(move || config::resolve(&paths)).await??;

    match command {
        GmailCommand::Labels {
            command: GmailLabelsCommand::List { json },
        } => GmailLabelsReport {
            labels: gmail_client_for_config(&config_report)?
                .list_labels()
                .await?,
        }
        .print(json)?,
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct GmailLabelsReport {
    labels: Vec<gmail::GmailLabel>,
}

impl GmailLabelsReport {
    fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            for label in &self.labels {
                println!("{}\t{}\t{}", label.id, label.name, label.label_type);
            }
        }
        Ok(())
    }
}
