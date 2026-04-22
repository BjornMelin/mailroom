use super::resolve::OAuthClientError;
use super::types::{
    AuthorizedUserAdc, AuthorizedUserAdcFile, LegacyStoredOAuthClient, StoredInstalledOAuthClient,
    StoredOAuthClientFile,
};
use super::{DEFAULT_AUTH_URL, DEFAULT_REDIRECT_URI, DEFAULT_TOKEN_URL, GOOGLE_AUTH_CERTS_URL};
use anyhow::{Context, Result, anyhow};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

pub(super) fn load_imported_client(path: &Path) -> Result<Option<StoredOAuthClientFile>> {
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

pub(super) fn validate_stored_oauth_client(client: &StoredOAuthClientFile) -> Result<()> {
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

pub(super) fn save_imported_client(path: &Path, client: &StoredOAuthClientFile) -> Result<()> {
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

pub(super) fn discover_import_path(credentials_file: Option<PathBuf>) -> Result<ImportDiscovery> {
    if let Some(path) = credentials_file {
        if !path.is_file() {
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
            super::import::setup_guidance()
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
                super::import::setup_guidance()
            ))
        }
    }
}

pub(super) fn default_import_candidates() -> Result<Vec<PathBuf>> {
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

pub(super) fn default_download_dirs(
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

pub(super) fn normalize_candidate_paths(mut candidates: Vec<PathBuf>) -> Vec<PathBuf> {
    candidates.sort();
    candidates.dedup();
    candidates
}

pub(super) fn collect_candidate_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(Vec::new());
        }
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
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => continue,
            Err(error) => return Err(error.into()),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => continue,
            Err(error) => return Err(error.into()),
        };
        if !file_type.is_file() {
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

pub(super) fn detect_adc_path() -> Option<PathBuf> {
    detect_adc_path_from_env(
        std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
        std::env::var_os("APPDATA").map(PathBuf::from),
    )
}

pub(super) fn detect_adc_path_from_env(
    adc_env_path: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    appdata_dir: Option<PathBuf>,
) -> Option<PathBuf> {
    if let Some(adc_path) = adc_env_path
        && adc_path.exists()
    {
        return Some(adc_path);
    }

    if let Some(home_dir) = home_dir {
        let well_known = home_dir.join(".config/gcloud/application_default_credentials.json");
        if well_known.exists() {
            return Some(well_known);
        }
    }

    if let Some(appdata_dir) = appdata_dir {
        let well_known = appdata_dir.join("gcloud/application_default_credentials.json");
        if well_known.exists() {
            return Some(well_known);
        }
    }

    None
}

pub(super) fn parse_authorized_user_adc(path: &Path) -> Result<AuthorizedUserAdc> {
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
pub(super) struct ImportDiscovery {
    pub(super) path: PathBuf,
    pub(super) auto_discovered: bool,
}
