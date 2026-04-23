use crate::auth;
use crate::cli::AuthCommand;
use crate::{config, workspace};
use anyhow::Result;

pub(crate) async fn handle_auth_command(
    paths: &workspace::WorkspacePaths,
    command: AuthCommand,
) -> Result<()> {
    let paths = paths.clone();
    let config_report = tokio::task::spawn_blocking(move || config::resolve(&paths)).await??;

    match command {
        AuthCommand::Setup {
            credentials_file,
            json,
            no_browser,
        } => auth::setup(&config_report, credentials_file, no_browser, json)
            .await?
            .print(json)?,
        AuthCommand::Login { json, no_browser } => auth::login(&config_report, no_browser, json)
            .await?
            .print(json)?,
        AuthCommand::Status { json } => {
            let config_report = config_report.clone();
            tokio::task::spawn_blocking(move || auth::status(&config_report))
                .await??
                .print(json)?;
        }
        AuthCommand::Logout { json } => {
            let config_report = config_report.clone();
            tokio::task::spawn_blocking(move || auth::logout(&config_report))
                .await??
                .print(json)?;
        }
    }

    Ok(())
}
