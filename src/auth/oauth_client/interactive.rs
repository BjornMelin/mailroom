use super::import::{
    PreparedOAuthClientImport, PreparedSetup, normalize_optional_string,
    prepare_google_desktop_client, prepare_google_desktop_client_from_adc,
    prepare_google_desktop_client_from_values,
};
use crate::config::{GmailConfig, WorkspaceConfig};
use anyhow::{Result, anyhow};
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
use std::io::{IsTerminal, stdin, stdout};
use std::path::PathBuf;

pub(super) fn prepare_interactive_setup(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    candidates: Vec<PathBuf>,
    detected_adc: Option<PathBuf>,
) -> Result<PreparedSetup> {
    let theme = ColorfulTheme::default();

    if candidates.is_empty() && detected_adc.is_none() {
        return prompt_manual_oauth_client(config, workspace, &theme)
            .map(PreparedSetup::ImportClient);
    }

    let mut choices = Vec::new();
    let mut labels = Vec::new();

    for candidate in candidates {
        labels.push(format!(
            "Use downloaded Desktop app JSON: {}",
            candidate.display()
        ));
        choices.push(InteractiveSetupChoice::DownloadedJson(candidate));
    }

    labels.push(String::from(
        "Paste Client ID and optional Client Secret into the CLI",
    ));
    choices.push(InteractiveSetupChoice::ManualPaste);

    if let Some(adc_path) = detected_adc {
        labels.push(format!(
            "Import existing gcloud ADC auth: {}",
            adc_path.display()
        ));
        choices.push(InteractiveSetupChoice::GcloudAdc(adc_path));
    }

    let selection = Select::with_theme(&theme)
        .with_prompt("Choose how Mailroom should configure Gmail OAuth")
        .default(0)
        .items(&labels)
        .interact()?;

    match choices.into_iter().nth(selection) {
        Some(InteractiveSetupChoice::DownloadedJson(path)) => {
            prepare_google_desktop_client(config, workspace, Some(path))
                .map(PreparedSetup::ImportClient)
        }
        Some(InteractiveSetupChoice::ManualPaste) => {
            prompt_manual_oauth_client(config, workspace, &theme).map(PreparedSetup::ImportClient)
        }
        Some(InteractiveSetupChoice::GcloudAdc(path)) => {
            prepare_google_desktop_client_from_adc(config, workspace, path)
                .map(PreparedSetup::ImportAdc)
        }
        None => Err(anyhow!("interactive setup selection was out of bounds")),
    }
}

pub(super) fn prompt_manual_oauth_client(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    theme: &ColorfulTheme,
) -> Result<PreparedOAuthClientImport> {
    if !is_interactive_terminal() {
        return Err(super::resolve::OAuthClientError::PromptUnavailable.into());
    }

    let client_id: String = Input::with_theme(theme)
        .with_prompt("Google OAuth Client ID")
        .validate_with(|value: &String| -> Result<(), &str> {
            if value.trim().is_empty() {
                Err("Client ID cannot be empty")
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    let client_secret = Password::with_theme(theme)
        .with_prompt("Google OAuth Client Secret (optional, press enter to skip)")
        .allow_empty_password(true)
        .interact()?;

    prepare_google_desktop_client_from_values(
        config,
        workspace,
        client_id.trim().to_owned(),
        normalize_optional_string(Some(client_secret)),
    )
}

pub(super) fn should_use_interactive_setup(json: bool, interactive_terminal: bool) -> bool {
    !json && interactive_terminal
}

pub(super) fn is_interactive_terminal() -> bool {
    stdin().is_terminal() && stdout().is_terminal()
}

#[derive(Debug)]
enum InteractiveSetupChoice {
    DownloadedJson(PathBuf),
    ManualPaste,
    GcloudAdc(PathBuf),
}
