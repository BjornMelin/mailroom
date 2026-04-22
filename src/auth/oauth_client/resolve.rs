use super::storage::load_imported_client;
use crate::config::{GmailConfig, WorkspaceConfig};
use anyhow::Result;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedOAuthClient {
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthClientSource {
    WorkspaceFile,
    InlineConfig,
    Unconfigured,
}

impl OAuthClientSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceFile => "workspace_file",
            Self::InlineConfig => "config",
            Self::Unconfigured => "unconfigured",
        }
    }
}

#[derive(Debug, Error)]
pub enum OAuthClientError {
    #[error(
        "gmail OAuth client is not configured; run `mailroom auth setup` or set gmail.client_id"
    )]
    MissingClientConfiguration,
    #[error("Google desktop-app credentials JSON was not found at {path}")]
    MissingImportFile { path: std::path::PathBuf },
    #[error(
        "Mailroom could not auto-discover a Google desktop-app credentials JSON. Pass `--credentials-file PATH` or run `mailroom auth setup` in an interactive terminal."
    )]
    MissingImportCandidate,
    #[error(
        "Mailroom found multiple candidate Google desktop-app credentials JSON files. Pass `--credentials-file PATH` to pick one."
    )]
    AmbiguousImportCandidate,
    #[error(
        "the Google credentials JSON is not a Desktop app client; create a Desktop app OAuth client and use its credentials"
    )]
    UnsupportedClientType,
    #[error("the credentials JSON is missing required field `{0}`")]
    MissingField(&'static str),
    #[error("the imported OAuth client file is missing required field `{0}`")]
    MissingStoredField(&'static str),
    #[error(
        "no interactive terminal is available; pass `--credentials-file PATH` or set gmail.client_id"
    )]
    PromptUnavailable,
    #[error("the gcloud ADC file uses unsupported credential type `{0}`")]
    UnsupportedAdcType(String),
    #[error("the gcloud ADC file is missing required field `{0}`")]
    MissingAdcField(&'static str),
}

pub fn resolve(config: &GmailConfig, workspace: &WorkspaceConfig) -> Result<ResolvedOAuthClient> {
    let oauth_client_path = config.oauth_client_path(workspace);
    if let Some(stored) = load_imported_client(&oauth_client_path)? {
        return Ok(ResolvedOAuthClient {
            client_id: stored.installed.client_id,
            client_secret: normalize_optional_string(Some(stored.installed.client_secret)),
        });
    }

    if let Some(client_id) = normalize_optional_string(config.client_id.clone()) {
        return Ok(ResolvedOAuthClient {
            client_id,
            client_secret: normalize_optional_string(config.client_secret.clone()),
        });
    }

    Err(OAuthClientError::MissingClientConfiguration.into())
}

pub fn oauth_client_source(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
) -> Result<OAuthClientSource> {
    let oauth_client_path = config.oauth_client_path(workspace);
    if load_imported_client(&oauth_client_path)?.is_some() {
        return Ok(OAuthClientSource::WorkspaceFile);
    }

    if normalize_optional_string(config.client_id.clone()).is_some() {
        return Ok(OAuthClientSource::InlineConfig);
    }

    Ok(OAuthClientSource::Unconfigured)
}

#[cfg(test)]
pub(crate) fn oauth_client_exists(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
) -> Result<bool> {
    let oauth_client_path = config.oauth_client_path(workspace);
    if !oauth_client_path.is_file() {
        return Ok(false);
    }

    let _ = load_imported_client(&oauth_client_path)?;
    Ok(true)
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim().to_owned();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}
