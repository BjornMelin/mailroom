use crate::auth;
use crate::cli::AuthCommand;
use crate::{config, workspace};
use anyhow::Result;

pub(crate) async fn handle_auth_command(
    paths: &workspace::WorkspacePaths,
    command: AuthCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

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
        AuthCommand::Status { json } => auth::status(&config_report)?.print(json)?,
        AuthCommand::Logout { json } => auth::logout(&config_report)?.print(json)?,
    }

    Ok(())
}
