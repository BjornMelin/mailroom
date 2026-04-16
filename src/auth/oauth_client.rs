use crate::config::{GmailConfig, WorkspaceConfig};
use anyhow::{Context, Result, anyhow};
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{IsTerminal, stdin, stdout};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const GOOGLE_AUTH_OVERVIEW_URL: &str = "https://console.cloud.google.com/auth/overview";
pub const GOOGLE_AUTH_CLIENTS_URL: &str = "https://console.cloud.google.com/auth/clients";
pub const GOOGLE_GMAIL_API_URL: &str =
    "https://console.cloud.google.com/apis/library/gmail.googleapis.com";
const GOOGLE_AUTH_CERTS_URL: &str = "https://www.googleapis.com/oauth2/v1/certs";
const DEFAULT_REDIRECT_URI: &str = "http://localhost";
const DEFAULT_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const DEFAULT_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedOAuthClient {
    pub client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
}

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

#[derive(Debug, Error)]
pub enum OAuthClientError {
    #[error(
        "gmail OAuth client is not configured; run `mailroom auth setup` or set gmail.client_id"
    )]
    MissingClientConfiguration,
    #[error("Google desktop-app credentials JSON was not found at {path}")]
    MissingImportFile { path: PathBuf },
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
pub fn oauth_client_exists(config: &GmailConfig, workspace: &WorkspaceConfig) -> Result<bool> {
    let oauth_client_path = config.oauth_client_path(workspace);
    if !oauth_client_path.is_file() {
        return Ok(false);
    }

    let _ = load_imported_client(&oauth_client_path)?;
    Ok(true)
}

#[cfg(test)]
pub fn import_google_desktop_client(
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

    let candidates = normalize_candidate_paths(default_import_candidates()?);
    let detected_adc = detect_adc_path();
    if should_use_interactive_setup(json, is_interactive_terminal()) {
        prepare_interactive_setup(config, workspace, candidates, detected_adc)
    } else {
        prepare_noninteractive_setup(config, workspace, candidates, detected_adc)
    }
}

fn should_use_interactive_setup(json: bool, interactive_terminal: bool) -> bool {
    !json && interactive_terminal
}

pub(crate) fn persist_prepared_google_desktop_client(
    prepared: &PreparedOAuthClientImport,
) -> Result<()> {
    save_imported_client(
        &prepared.imported_client.oauth_client_path,
        &prepared.stored_client,
    )
}

pub fn setup_guidance() -> String {
    format!(
        "Google-side setup checklist:\n\
         1. Open {GOOGLE_AUTH_OVERVIEW_URL}\n\
         2. Enable Gmail API at {GOOGLE_GMAIL_API_URL}\n\
         3. Create a Desktop app OAuth client at {GOOGLE_AUTH_CLIENTS_URL}\n\
         4. Either download the credentials JSON and run `mailroom auth setup --credentials-file /path/to/client_secret.json`, or run `mailroom auth setup` in an interactive terminal to paste the Client ID and optional Client Secret\n\
         5. Advanced: if you already ran `gcloud auth application-default login` with Gmail scopes, `mailroom auth setup` can also import that existing ADC session"
    )
}

pub(crate) fn prepare_google_desktop_client(
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

pub(crate) fn prepare_google_desktop_client_from_values(
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

pub(crate) fn prepare_google_desktop_client_from_adc(
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

fn prepare_interactive_setup(
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

fn prepare_noninteractive_setup(
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

fn prompt_manual_oauth_client(
    config: &GmailConfig,
    workspace: &WorkspaceConfig,
    theme: &ColorfulTheme,
) -> Result<PreparedOAuthClientImport> {
    if !is_interactive_terminal() {
        return Err(OAuthClientError::PromptUnavailable.into());
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

fn load_imported_client(path: &Path) -> Result<Option<StoredOAuthClientFile>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read OAuth client from {}", path.display()));
        }
    };

    if let Ok(stored) = serde_json::from_str::<StoredOAuthClientFile>(&raw) {
        validate_stored_oauth_client(&stored)?;
        return Ok(Some(stored));
    }

    let legacy: LegacyStoredOAuthClient = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse OAuth client from {}", path.display()))?;
    let stored = StoredOAuthClientFile {
        installed: StoredInstalledOAuthClient {
            client_id: normalize_required_input_string(legacy.client_id, "client_id")?,
            client_secret: legacy.client_secret.unwrap_or_default(),
            project_id: None,
            auth_uri: DEFAULT_AUTH_URL.to_owned(),
            token_uri: DEFAULT_TOKEN_URL.to_owned(),
            auth_provider_x509_cert_url: GOOGLE_AUTH_CERTS_URL.to_owned(),
            redirect_uris: vec![DEFAULT_REDIRECT_URI.to_owned()],
        },
    };
    validate_stored_oauth_client(&stored)?;
    Ok(Some(stored))
}

fn validate_stored_oauth_client(client: &StoredOAuthClientFile) -> Result<()> {
    if client.installed.client_id.trim().is_empty() {
        return Err(OAuthClientError::MissingStoredField("installed.client_id").into());
    }
    if client.installed.auth_uri.trim().is_empty() {
        return Err(OAuthClientError::MissingStoredField("installed.auth_uri").into());
    }
    if client.installed.token_uri.trim().is_empty() {
        return Err(OAuthClientError::MissingStoredField("installed.token_uri").into());
    }

    Ok(())
}

fn save_imported_client(path: &Path, client: &StoredOAuthClientFile) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("OAuth client path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)?;
    set_owner_only_dir_permissions(parent)?;

    let payload = serde_json::to_vec_pretty(client)?;
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, payload)?;
    set_owner_only_file_permissions(&tmp_path)?;
    persist_tmp_file(&tmp_path, path)?;
    Ok(())
}

fn discover_import_path(credentials_file: Option<PathBuf>) -> Result<ImportDiscovery> {
    if let Some(path) = credentials_file {
        if !path.exists() {
            return Err(OAuthClientError::MissingImportFile { path }.into());
        }
        return Ok(ImportDiscovery {
            path,
            auto_discovered: false,
        });
    }

    let candidates = normalize_candidate_paths(default_import_candidates()?);

    match candidates.as_slice() {
        [] => Err(anyhow!(
            "{}\n\n{}",
            OAuthClientError::MissingImportCandidate,
            setup_guidance()
        )),
        [path] => Ok(ImportDiscovery {
            path: path.clone(),
            auto_discovered: true,
        }),
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

fn default_import_candidates() -> Result<Vec<PathBuf>> {
    default_import_candidates_from_env(
        &std::env::current_dir()?,
        std::env::var_os("HOME").map(PathBuf::from),
        std::env::var_os("USERPROFILE").map(PathBuf::from),
    )
}

fn default_import_candidates_from_env(
    current_dir: &Path,
    home_dir: Option<PathBuf>,
    user_profile_dir: Option<PathBuf>,
) -> Result<Vec<PathBuf>> {
    let mut candidates = collect_candidate_files(current_dir)?;

    for downloads_dir in default_download_dirs(home_dir, user_profile_dir) {
        candidates.extend(collect_candidate_files(&downloads_dir)?);
    }

    Ok(candidates)
}

fn default_download_dirs(
    home_dir: Option<PathBuf>,
    user_profile_dir: Option<PathBuf>,
) -> Vec<PathBuf> {
    let mut downloads_dirs = Vec::new();

    if let Some(home_dir) = home_dir {
        downloads_dirs.push(home_dir.join("Downloads"));
        downloads_dirs.push(home_dir.join("downloads"));
    }

    if let Some(user_profile_dir) = user_profile_dir {
        let downloads_dir = user_profile_dir.join("Downloads");
        if !downloads_dirs.contains(&downloads_dir) {
            downloads_dirs.push(downloads_dir);
        }
    }

    downloads_dirs
}

fn normalize_candidate_paths(mut candidates: Vec<PathBuf>) -> Vec<PathBuf> {
    candidates.sort();
    candidates.dedup();
    candidates
}

fn collect_candidate_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect candidate credentials directory {}",
                    dir.display()
                )
            });
        }
    };

    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with("client_secret_") && file_name.ends_with(".json") {
            candidates.push(entry.path());
        }
    }

    Ok(candidates)
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
        auth_uri: installed
            .auth_uri
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| config.auth_url.clone()),
        token_uri: installed
            .token_uri
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| config.token_url.clone()),
        auth_provider_x509_cert_url: installed
            .auth_provider_x509_cert_url
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| GOOGLE_AUTH_CERTS_URL.to_owned()),
        redirect_uris: normalize_redirect_uris(installed.redirect_uris),
    })
}

fn parse_authorized_user_adc(path: &Path) -> Result<AuthorizedUserAdc> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read gcloud ADC file from {}", path.display()))?;
    let parsed: AuthorizedUserAdcFile = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse gcloud ADC file from {}", path.display()))?;

    let credential_type = parsed
        .credential_type
        .unwrap_or_else(|| String::from("unknown"));
    if credential_type != "authorized_user" {
        return Err(OAuthClientError::UnsupportedAdcType(credential_type).into());
    }

    Ok(AuthorizedUserAdc {
        client_id: normalize_required_adc_field(parsed.client_id, "client_id")?,
        client_secret: normalize_optional_string(parsed.client_secret),
        refresh_token: normalize_required_adc_field(parsed.refresh_token, "refresh_token")?,
        quota_project_id: normalize_optional_string(parsed.quota_project_id),
    })
}

fn detect_adc_path() -> Option<PathBuf> {
    detect_adc_path_from_env(
        std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

fn detect_adc_path_from_env(
    adc_env_path: Option<PathBuf>,
    home_dir: Option<PathBuf>,
) -> Option<PathBuf> {
    if let Some(adc_path) = adc_env_path
        && adc_path.exists()
    {
        return Some(adc_path);
    }

    let home_dir = home_dir?;
    let well_known = home_dir.join(".config/gcloud/application_default_credentials.json");
    if well_known.exists() {
        return Some(well_known);
    }

    None
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

fn normalize_required_option_string(value: Option<String>, field: &'static str) -> Result<String> {
    normalize_optional_string(value).ok_or_else(|| OAuthClientError::MissingField(field).into())
}

fn normalize_required_adc_field(value: Option<String>, field: &'static str) -> Result<String> {
    normalize_optional_string(value).ok_or_else(|| OAuthClientError::MissingAdcField(field).into())
}

fn normalize_required_input_string(value: String, field: &'static str) -> Result<String> {
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

fn is_interactive_terminal() -> bool {
    stdin().is_terminal() && stdout().is_terminal()
}

#[cfg(unix)]
fn set_owner_only_dir_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_dir_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_file_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn persist_tmp_file(tmp_path: &Path, destination: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        if destination.exists() {
            fs::remove_file(destination)?;
        }
    }

    fs::rename(tmp_path, destination)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct ImportDiscovery {
    path: PathBuf,
    auto_discovered: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredOAuthClientFile {
    installed: StoredInstalledOAuthClient,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredInstalledOAuthClient {
    client_id: String,
    #[serde(default)]
    client_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    project_id: Option<String>,
    auth_uri: String,
    token_uri: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    auth_provider_x509_cert_url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    redirect_uris: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct DownloadedGoogleCredentials {
    installed: Option<DownloadedInstalledClient>,
}

#[derive(Debug, Deserialize)]
struct DownloadedInstalledClient {
    client_id: Option<String>,
    client_secret: Option<String>,
    project_id: Option<String>,
    auth_uri: Option<String>,
    token_uri: Option<String>,
    auth_provider_x509_cert_url: Option<String>,
    redirect_uris: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct LegacyStoredOAuthClient {
    client_id: String,
    client_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthorizedUserAdcFile {
    #[serde(rename = "type")]
    credential_type: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
    quota_project_id: Option<String>,
}

#[derive(Debug)]
struct AuthorizedUserAdc {
    client_id: String,
    client_secret: Option<String>,
    refresh_token: String,
    quota_project_id: Option<String>,
}

#[derive(Debug)]
enum InteractiveSetupChoice {
    DownloadedJson(PathBuf),
    ManualPaste,
    GcloudAdc(PathBuf),
}

#[cfg(test)]
mod tests {
    use super::{
        GOOGLE_AUTH_CLIENTS_URL, GOOGLE_AUTH_OVERVIEW_URL, ImportedOAuthClient,
        ImportedOAuthClientSourceKind, OAuthClientError, OAuthClientSource, PreparedSetup,
        default_download_dirs, detect_adc_path_from_env, import_google_desktop_client,
        normalize_candidate_paths, oauth_client_exists, oauth_client_source,
        prepare_google_desktop_client_from_adc, prepare_google_desktop_client_from_values,
        prepare_noninteractive_setup, resolve, setup_guidance, should_use_interactive_setup,
    };
    use crate::config::{GmailConfig, WorkspaceConfig};
    use secrecy::ExposeSecret;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn workspace_for(temp_dir: &TempDir) -> WorkspaceConfig {
        let root = temp_dir.path().join(".mailroom");
        WorkspaceConfig {
            runtime_root: root.clone(),
            auth_dir: root.join("auth"),
            cache_dir: root.join("cache"),
            state_dir: root.join("state"),
            vault_dir: root.join("vault"),
            exports_dir: root.join("exports"),
            logs_dir: root.join("logs"),
        }
    }

    fn gmail_config() -> GmailConfig {
        GmailConfig {
            client_id: None,
            client_secret: None,
            auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
            token_url: String::from("https://oauth2.googleapis.com/token"),
            api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: true,
            request_timeout_secs: 30,
            scopes: vec![String::from("scope:a")],
        }
    }

    #[test]
    fn imported_client_becomes_the_resolved_oauth_source() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        let config = gmail_config();
        let credentials_path = temp_dir.path().join("client_secret_test.json");
        fs::write(
            &credentials_path,
            r#"{
  "installed": {
    "client_id": "desktop-client.apps.googleusercontent.com",
    "client_secret": "desktop-secret",
    "project_id": "mailroom-dev",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "redirect_uris": ["http://localhost"]
  }
}"#,
        )
        .unwrap();

        let imported =
            import_google_desktop_client(&config, &workspace, Some(credentials_path)).unwrap();
        let resolved = resolve(&config, &workspace).unwrap();

        assert_eq!(
            imported,
            ImportedOAuthClient {
                source_kind: ImportedOAuthClientSourceKind::DownloadedJson,
                source_path: Some(temp_dir.path().join("client_secret_test.json")),
                oauth_client_path: workspace.auth_dir.join("gmail-oauth-client.json"),
                auto_discovered: false,
                client_id: String::from("desktop-client.apps.googleusercontent.com"),
                client_secret_present: true,
                project_id: Some(String::from("mailroom-dev")),
            }
        );
        assert_eq!(
            resolved.client_id,
            "desktop-client.apps.googleusercontent.com"
        );
        assert_eq!(resolved.client_secret.as_deref(), Some("desktop-secret"));
        assert!(oauth_client_exists(&config, &workspace).unwrap());

        let saved = fs::read_to_string(workspace.auth_dir.join("gmail-oauth-client.json")).unwrap();
        assert!(saved.contains("\"installed\""));
        assert!(saved.contains("\"project_id\": \"mailroom-dev\""));
    }

    #[test]
    fn manual_paste_preparation_builds_standard_google_client_file() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        let config = gmail_config();

        let prepared = prepare_google_desktop_client_from_values(
            &config,
            &workspace,
            String::from("manual-client.apps.googleusercontent.com"),
            Some(String::from("manual-secret")),
        )
        .unwrap();

        assert_eq!(
            prepared.imported_client().source_kind,
            ImportedOAuthClientSourceKind::ManualPaste
        );
        assert_eq!(
            prepared.resolved_client().client_id,
            "manual-client.apps.googleusercontent.com"
        );
        assert_eq!(
            prepared.resolved_client().client_secret.as_deref(),
            Some("manual-secret")
        );
    }

    #[test]
    fn adc_import_extracts_client_and_refresh_token() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        let config = gmail_config();
        let adc_path = temp_dir.path().join("application_default_credentials.json");
        fs::write(
            &adc_path,
            r#"{
  "type": "authorized_user",
  "client_id": "adc-client.apps.googleusercontent.com",
  "client_secret": "adc-secret",
  "refresh_token": "adc-refresh-token",
  "quota_project_id": "adc-project"
}"#,
        )
        .unwrap();

        let prepared =
            prepare_google_desktop_client_from_adc(&config, &workspace, adc_path.clone()).unwrap();

        assert_eq!(
            prepared.imported_client().source_kind,
            ImportedOAuthClientSourceKind::GcloudAdc
        );
        assert_eq!(
            prepared.imported_client().source_path.as_ref(),
            Some(&adc_path)
        );
        assert_eq!(
            prepared.resolved_client().client_id,
            "adc-client.apps.googleusercontent.com"
        );
        assert_eq!(
            prepared.refresh_token().expose_secret(),
            "adc-refresh-token"
        );
    }

    #[test]
    fn detect_adc_path_ignores_missing_env_path_and_falls_back_to_well_known_adc() {
        let temp_dir = TempDir::new().unwrap();
        let missing_env_path = temp_dir.path().join("missing-adc.json");
        let well_known_adc = temp_dir
            .path()
            .join(".config/gcloud/application_default_credentials.json");
        fs::create_dir_all(well_known_adc.parent().unwrap()).unwrap();
        fs::write(&well_known_adc, "{}").unwrap();

        let detected =
            detect_adc_path_from_env(Some(missing_env_path), Some(temp_dir.path().into()));

        assert_eq!(detected, Some(well_known_adc));
    }

    #[test]
    fn noninteractive_setup_uses_adc_when_no_downloaded_json_is_available() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        let config = gmail_config();
        let adc_path = temp_dir.path().join("application_default_credentials.json");
        fs::write(
            &adc_path,
            r#"{
  "type": "authorized_user",
  "client_id": "adc-client.apps.googleusercontent.com",
  "client_secret": "adc-secret",
  "refresh_token": "adc-refresh-token"
}"#,
        )
        .unwrap();

        let prepared =
            prepare_noninteractive_setup(&config, &workspace, Vec::new(), Some(adc_path)).unwrap();

        match prepared {
            PreparedSetup::ImportAdc(prepared) => {
                assert_eq!(
                    prepared.imported_client().source_kind,
                    ImportedOAuthClientSourceKind::GcloudAdc
                );
                assert_eq!(
                    prepared.resolved_client().client_id,
                    "adc-client.apps.googleusercontent.com"
                );
                assert_eq!(
                    prepared.refresh_token().expose_secret(),
                    "adc-refresh-token"
                );
            }
            PreparedSetup::UseExisting | PreparedSetup::ImportClient(_) => {
                panic!("expected non-interactive setup to import the ADC path")
            }
        }
    }

    #[test]
    fn source_falls_back_to_unconfigured_when_workspace_file_disappears() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);

        assert_eq!(
            oauth_client_source(&gmail_config(), &workspace).unwrap(),
            OAuthClientSource::Unconfigured
        );
    }

    #[test]
    fn prepare_setup_deduplicates_discovered_candidate_paths() {
        let candidate = PathBuf::from("/tmp/client_secret_duplicate.json");
        let normalized = normalize_candidate_paths(vec![candidate.clone(), candidate.clone()]);

        assert_eq!(normalized, vec![candidate]);
    }

    #[test]
    fn default_download_dirs_only_include_download_locations() {
        let home_dir = PathBuf::from("/home/tester");
        let user_profile_dir = PathBuf::from("C:/Users/tester");
        let downloads_dirs = default_download_dirs(Some(home_dir), Some(user_profile_dir));

        assert_eq!(
            downloads_dirs,
            vec![
                PathBuf::from("/home/tester/Downloads"),
                PathBuf::from("/home/tester/downloads"),
                PathBuf::from("C:/Users/tester/Downloads"),
            ]
        );
    }

    #[test]
    fn json_setup_never_uses_the_interactive_wizard() {
        assert!(!should_use_interactive_setup(true, true));
        assert!(!should_use_interactive_setup(true, false));
        assert!(should_use_interactive_setup(false, true));
        assert!(!should_use_interactive_setup(false, false));
    }

    #[test]
    fn legacy_stored_client_file_is_still_resolved() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        fs::create_dir_all(&workspace.auth_dir).unwrap();
        fs::write(
            workspace.auth_dir.join("gmail-oauth-client.json"),
            r#"{
  "client_id": "legacy-client.apps.googleusercontent.com",
  "client_secret": "legacy-secret"
}"#,
        )
        .unwrap();

        let resolved = resolve(&gmail_config(), &workspace).unwrap();

        assert_eq!(
            resolved.client_id,
            "legacy-client.apps.googleusercontent.com"
        );
        assert_eq!(resolved.client_secret.as_deref(), Some("legacy-secret"));
    }

    #[test]
    fn imported_client_takes_precedence_over_inline_config() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        fs::create_dir_all(&workspace.auth_dir).unwrap();
        fs::write(
            workspace.auth_dir.join("gmail-oauth-client.json"),
            r#"{
  "installed": {
    "client_id": "imported-client.apps.googleusercontent.com",
    "client_secret": "imported-secret",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}"#,
        )
        .unwrap();

        let resolved = resolve(
            &GmailConfig {
                client_id: Some(String::from("inline-client.apps.googleusercontent.com")),
                client_secret: Some(String::from("inline-secret")),
                ..gmail_config()
            },
            &workspace,
        )
        .unwrap();

        assert_eq!(
            resolved.client_id,
            "imported-client.apps.googleusercontent.com"
        );
        assert_eq!(resolved.client_secret.as_deref(), Some("imported-secret"));
    }

    #[test]
    fn inline_config_reports_config_source_without_workspace_file() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        let config = GmailConfig {
            client_id: Some(String::from("inline-client.apps.googleusercontent.com")),
            client_secret: Some(String::from("inline-secret")),
            ..gmail_config()
        };

        let source = oauth_client_source(&config, &workspace).unwrap();

        assert_eq!(source, OAuthClientSource::InlineConfig);
        assert!(!oauth_client_exists(&config, &workspace).unwrap());
    }

    #[test]
    fn source_reports_workspace_file_and_validates_it() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        fs::create_dir_all(&workspace.auth_dir).unwrap();
        fs::write(
            workspace.auth_dir.join("gmail-oauth-client.json"),
            r#"{
  "installed": {
    "client_id": "saved-client.apps.googleusercontent.com",
    "client_secret": "",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}"#,
        )
        .unwrap();

        assert_eq!(
            oauth_client_source(&gmail_config(), &workspace).unwrap(),
            OAuthClientSource::WorkspaceFile
        );
    }

    #[test]
    fn malformed_workspace_file_returns_a_validation_error() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        fs::create_dir_all(&workspace.auth_dir).unwrap();
        fs::write(
            workspace.auth_dir.join("gmail-oauth-client.json"),
            r#"{
  "installed": {
    "client_id": "",
    "client_secret": "",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}"#,
        )
        .unwrap();

        let error = oauth_client_source(&gmail_config(), &workspace).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<OAuthClientError>(),
            Some(OAuthClientError::MissingStoredField("installed.client_id"))
        ));
    }

    #[test]
    fn unsupported_adc_type_is_rejected() {
        let temp_dir = TempDir::new().unwrap();
        let workspace = workspace_for(&temp_dir);
        let config = gmail_config();
        let adc_path = temp_dir.path().join("application_default_credentials.json");
        fs::write(
            &adc_path,
            r#"{
  "type": "service_account",
  "client_id": "adc-client.apps.googleusercontent.com",
  "client_secret": "adc-secret",
  "refresh_token": "adc-refresh-token"
}"#,
        )
        .unwrap();

        let error =
            prepare_google_desktop_client_from_adc(&config, &workspace, adc_path).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<OAuthClientError>(),
            Some(OAuthClientError::UnsupportedAdcType(kind)) if kind == "service_account"
        ));
    }

    #[test]
    fn setup_guidance_points_to_console_urls_and_interactive_setup() {
        let guidance = setup_guidance();

        assert!(guidance.contains(GOOGLE_AUTH_OVERVIEW_URL));
        assert!(guidance.contains(GOOGLE_AUTH_CLIENTS_URL));
        assert!(guidance.contains("mailroom auth setup"));
        assert!(guidance.contains("interactive terminal"));
    }
}
