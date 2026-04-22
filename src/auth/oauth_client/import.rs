use super::interactive::{
    is_interactive_terminal, prepare_interactive_setup, should_use_interactive_setup,
};
use super::resolve::{
    OAuthClientError, OAuthClientSource, ResolvedOAuthClient, oauth_client_source,
};
use super::storage::{
    default_import_candidates, detect_adc_path, discover_import_path, parse_authorized_user_adc,
    save_imported_client,
};
use super::types::{
    DownloadedGoogleCredentials, StoredInstalledOAuthClient, StoredOAuthClientFile,
};
use super::{
    DEFAULT_REDIRECT_URI, GOOGLE_AUTH_CERTS_URL, GOOGLE_AUTH_CLIENTS_URL, GOOGLE_AUTH_OVERVIEW_URL,
    GOOGLE_GMAIL_API_URL,
};
use crate::config::{GmailConfig, WorkspaceConfig};
use anyhow::{Context, Result, anyhow};
use secrecy::SecretString;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImportedOAuthClientSourceKind {
    DownloadedJson,
    ManualPaste,
    GcloudAdc,
}

impl ImportedOAuthClientSourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DownloadedJson => "downloaded_json",
            Self::ManualPaste => "manual_paste",
            Self::GcloudAdc => "gcloud_adc",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ImportedOAuthClient {
    pub source_kind: ImportedOAuthClientSourceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    pub oauth_client_path: PathBuf,
    pub auto_discovered: bool,
    pub client_id: String,
    pub client_secret_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedOAuthClientImport {
    imported_client: ImportedOAuthClient,
    resolved_client: ResolvedOAuthClient,
    stored_client: StoredOAuthClientFile,
}

impl PreparedOAuthClientImport {
    pub(crate) fn imported_client(&self) -> &ImportedOAuthClient {
        &self.imported_client
    }

    pub(crate) fn resolved_client(&self) -> &ResolvedOAuthClient {
        &self.resolved_client
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedAdcOAuthClientImport {
    client_import: PreparedOAuthClientImport,
    refresh_token: SecretString,
}

impl PreparedAdcOAuthClientImport {
    pub(crate) fn imported_client(&self) -> &ImportedOAuthClient {
        self.client_import.imported_client()
    }

    pub(crate) fn resolved_client(&self) -> &ResolvedOAuthClient {
        self.client_import.resolved_client()
    }

    pub(crate) fn refresh_token(&self) -> &SecretString {
        &self.refresh_token
    }

    pub(crate) fn client_import(&self) -> &PreparedOAuthClientImport {
        &self.client_import
    }
}

pub(crate) enum PreparedSetup {
    UseExisting,
    ImportClient(PreparedOAuthClientImport),
    ImportAdc(PreparedAdcOAuthClientImport),
}

#[cfg(test)]
pub(crate) fn import_google_desktop_client(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    credentials_file: Option<PathBuf>,
) -> Result<ImportedOAuthClient> {
    let prepared = prepare_google_desktop_client(config, workspace, credentials_file)?;
    persist_prepared_google_desktop_client(&prepared)?;
    Ok(prepared.imported_client)
}

pub(crate) fn prepare_setup(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    credentials_file: Option<PathBuf>,
    json: bool,
) -> Result<PreparedSetup> {
    if credentials_file.is_some() {
        return prepare_google_desktop_client(config, workspace, credentials_file)
            .map(PreparedSetup::ImportClient);
    }

    match oauth_client_source(config, workspace)? {
        OAuthClientSource::WorkspaceFile | OAuthClientSource::InlineConfig => {
            return Ok(PreparedSetup::UseExisting);
        }
        OAuthClientSource::Unconfigured => {}
    }

    let candidates = super::storage::normalize_candidate_paths(default_import_candidates()?);
    let detected_adc = detect_adc_path();
    if should_use_interactive_setup(json, is_interactive_terminal()) {
        prepare_interactive_setup(config, workspace, candidates, detected_adc)
    } else {
        prepare_noninteractive_setup(config, workspace, candidates, detected_adc)
    }
}

pub(crate) fn persist_prepared_google_desktop_client(
    prepared: &PreparedOAuthClientImport,
) -> Result<()> {
    save_imported_client(
        &prepared.imported_client.oauth_client_path,
        &prepared.stored_client,
    )
}

pub(crate) fn setup_guidance() -> String {
    format!(
        "Google-side setup checklist:\n\
         1. Open {GOOGLE_AUTH_OVERVIEW_URL}\n\
         2. Enable Gmail API at {GOOGLE_GMAIL_API_URL}\n\
         3. Create a Desktop app OAuth client at {GOOGLE_AUTH_CLIENTS_URL}\n\
         4. Either download the credentials JSON and run `mailroom auth setup --credentials-file /path/to/client_secret.json`, or run `mailroom auth setup` in an interactive terminal to paste the Client ID and optional Client Secret\n\
         5. Advanced: if you already ran `gcloud auth application-default login` with Gmail scopes, `mailroom auth setup` can also import that existing ADC session"
    )
}

pub(super) fn prepare_google_desktop_client(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    credentials_file: Option<PathBuf>,
) -> Result<PreparedOAuthClientImport> {
    let discovery = discover_import_path(credentials_file)?;
    let client = parse_google_desktop_client(&discovery.path, config)?;
    Ok(prepare_client_import(
        config,
        workspace,
        client,
        ImportedOAuthClientSourceKind::DownloadedJson,
        Some(discovery.path),
        discovery.auto_discovered,
    ))
}

pub(super) fn prepare_google_desktop_client_from_values(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    client_id: String,
    client_secret: Option<String>,
) -> Result<PreparedOAuthClientImport> {
    let client = GoogleDesktopClient {
        client_id: normalize_required_input_string(client_id, "client_id")?,
        client_secret: normalize_optional_string(client_secret),
        project_id: None,
        auth_uri: config.auth_url.clone(),
        token_uri: config.token_url.clone(),
        auth_provider_x509_cert_url: GOOGLE_AUTH_CERTS_URL.to_owned(),
        redirect_uris: vec![DEFAULT_REDIRECT_URI.to_owned()],
    };

    Ok(prepare_client_import(
        config,
        workspace,
        client,
        ImportedOAuthClientSourceKind::ManualPaste,
        None,
        false,
    ))
}

pub(super) fn prepare_google_desktop_client_from_adc(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    adc_path: PathBuf,
) -> Result<PreparedAdcOAuthClientImport> {
    let adc = parse_authorized_user_adc(&adc_path)?;
    let client = GoogleDesktopClient {
        client_id: adc.client_id.clone(),
        client_secret: adc.client_secret.clone(),
        project_id: adc.quota_project_id.clone(),
        auth_uri: config.auth_url.clone(),
        token_uri: config.token_url.clone(),
        auth_provider_x509_cert_url: GOOGLE_AUTH_CERTS_URL.to_owned(),
        redirect_uris: vec![DEFAULT_REDIRECT_URI.to_owned()],
    };
    let client_import = prepare_client_import(
        config,
        workspace,
        client,
        ImportedOAuthClientSourceKind::GcloudAdc,
        Some(adc_path),
        false,
    );

    Ok(PreparedAdcOAuthClientImport {
        client_import,
        refresh_token: SecretString::from(adc.refresh_token),
    })
}

pub(super) fn prepare_noninteractive_setup(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    candidates: Vec<PathBuf>,
    detected_adc: Option<PathBuf>,
) -> Result<PreparedSetup> {
    match candidates.as_slice() {
        [path] => prepare_google_desktop_client(config, workspace, Some(path.clone()))
            .map(PreparedSetup::ImportClient),
        [] => match detected_adc {
            Some(adc_path) => prepare_google_desktop_client_from_adc(config, workspace, adc_path)
                .map(PreparedSetup::ImportAdc),
            None => Err(anyhow!(
                "{}\n\n{}",
                OAuthClientError::MissingImportCandidate,
                setup_guidance()
            )),
        },
        _ => {
            let listed = candidates
                .iter()
                .map(|path| format!("- {}", path.display()))
                .collect::<Vec<_>>()
                .join("\n");
            Err(anyhow!(
                "{}\n{}\n\n{}",
                OAuthClientError::AmbiguousImportCandidate,
                listed,
                setup_guidance()
            ))
        }
    }
}

fn prepare_client_import(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    client: GoogleDesktopClient,
    source_kind: ImportedOAuthClientSourceKind,
    source_path: Option<PathBuf>,
    auto_discovered: bool,
) -> PreparedOAuthClientImport {
    let oauth_client_path = config.oauth_client_path(workspace);
    let stored_client = StoredOAuthClientFile {
        installed: StoredInstalledOAuthClient {
            client_id: client.client_id.clone(),
            client_secret: client.client_secret.clone().unwrap_or_default(),
            project_id: client.project_id.clone(),
            auth_uri: client.auth_uri.clone(),
            token_uri: client.token_uri.clone(),
            auth_provider_x509_cert_url: client.auth_provider_x509_cert_url.clone(),
            redirect_uris: client.redirect_uris.clone(),
        },
    };

    PreparedOAuthClientImport {
        imported_client: ImportedOAuthClient {
            source_kind,
            source_path,
            oauth_client_path,
            auto_discovered,
            client_id: client.client_id.clone(),
            client_secret_present: client.client_secret.is_some(),
            project_id: client.project_id,
        },
        resolved_client: ResolvedOAuthClient {
            client_id: client.client_id,
            client_secret: client.client_secret,
        },
        stored_client,
    }
}

fn parse_google_desktop_client(path: &Path, config: &GmailConfig) -> Result<GoogleDesktopClient> {
    let raw = fs::read_to_string(path).with_context(|| {
        format!(
            "failed to read Google credentials JSON from {}",
            path.display()
        )
    })?;
    let parsed: DownloadedGoogleCredentials = serde_json::from_str(&raw).with_context(|| {
        format!(
            "failed to parse Google credentials JSON from {}",
            path.display()
        )
    })?;

    let installed = parsed
        .installed
        .ok_or(OAuthClientError::UnsupportedClientType)?;

    Ok(GoogleDesktopClient {
        client_id: normalize_required_option_string(installed.client_id, "installed.client_id")?,
        client_secret: normalize_optional_string(installed.client_secret),
        project_id: normalize_optional_string(installed.project_id),
        auth_uri: normalize_optional_string(installed.auth_uri)
            .unwrap_or_else(|| config.auth_url.clone()),
        token_uri: normalize_optional_string(installed.token_uri)
            .unwrap_or_else(|| config.token_url.clone()),
        auth_provider_x509_cert_url: normalize_optional_string(
            installed.auth_provider_x509_cert_url,
        )
        .unwrap_or_else(|| GOOGLE_AUTH_CERTS_URL.to_owned()),
        redirect_uris: normalize_redirect_uris(installed.redirect_uris),
    })
}

pub(super) fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim().to_owned();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

pub(super) fn normalize_required_option_string(
    value: Option<String>,
    field: &'static str,
) -> Result<String> {
    normalize_optional_string(value).ok_or_else(|| OAuthClientError::MissingField(field).into())
}

pub(super) fn normalize_required_adc_field(
    value: Option<String>,
    field: &'static str,
) -> Result<String> {
    normalize_optional_string(value).ok_or_else(|| OAuthClientError::MissingAdcField(field).into())
}

pub(super) fn normalize_required_input_string(
    value: String,
    field: &'static str,
) -> Result<String> {
    let trimmed = value.trim().to_owned();
    if trimmed.is_empty() {
        return Err(OAuthClientError::MissingField(field).into());
    }
    Ok(trimmed)
}

fn normalize_redirect_uris(redirect_uris: Option<Vec<String>>) -> Vec<String> {
    let cleaned = redirect_uris
        .unwrap_or_default()
        .into_iter()
        .filter_map(|uri| {
            let trimmed = uri.trim().to_owned();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        })
        .collect::<Vec<_>>();

    if cleaned.is_empty() {
        vec![DEFAULT_REDIRECT_URI.to_owned()]
    } else {
        cleaned
    }
}

#[derive(Debug, Clone)]
struct GoogleDesktopClient {
    client_id: String,
    client_secret: Option<String>,
    project_id: Option<String>,
    auth_uri: String,
    token_uri: String,
    auth_provider_x509_cert_url: String,
    redirect_uris: Vec<String>,
}
