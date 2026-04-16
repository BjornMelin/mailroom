use anyhow::{Context, Result};
use oauth2::TokenResponse;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub trait CredentialStore {
    fn path(&self) -> &Path;
    fn load(&self) -> Result<Option<StoredCredentials>>;
    fn save(&self, credentials: &StoredCredentials) -> Result<()>;
    fn clear(&self) -> Result<bool>;
}

#[derive(Debug, Clone)]
pub struct StoredCredentials {
    pub account_id: String,
    pub access_token: SecretString,
    pub refresh_token: Option<SecretString>,
    pub expires_at_epoch_s: Option<u64>,
    pub scopes: Vec<String>,
}

impl StoredCredentials {
    pub fn from_token_response<T>(account_id: String, token: &T, fallback_scopes: &[String]) -> Self
    where
        T: TokenResponse,
    {
        let expires_at_epoch_s = token.expires_in().and_then(|duration| {
            SystemTime::now()
                .checked_add(duration)
                .and_then(|deadline| deadline.duration_since(UNIX_EPOCH).ok())
                .map(|deadline| deadline.as_secs())
        });
        let scopes = token
            .scopes()
            .map(|scopes| scopes.iter().map(|scope| scope.to_string()).collect())
            .unwrap_or_else(|| fallback_scopes.to_vec());

        Self {
            account_id,
            access_token: SecretString::from(token.access_token().secret().to_owned()),
            refresh_token: token
                .refresh_token()
                .map(|refresh_token| SecretString::from(refresh_token.secret().to_owned())),
            expires_at_epoch_s,
            scopes,
        }
    }

    pub fn should_refresh(&self, leeway_secs: u64, now_epoch_s: u64) -> bool {
        self.expires_at_epoch_s
            .is_some_and(|expires_at| expires_at <= now_epoch_s.saturating_add(leeway_secs))
    }
}

#[derive(Debug, Clone)]
pub struct FileCredentialStore {
    path: PathBuf,
}

impl FileCredentialStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl CredentialStore for FileCredentialStore {
    fn path(&self) -> &Path {
        &self.path
    }

    fn load(&self) -> Result<Option<StoredCredentials>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let raw = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read credentials from {}", self.path.display()))?;
        let disk: DiskCredentials = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse credentials from {}", self.path.display()))?;

        Ok(Some(StoredCredentials {
            account_id: disk.account_id,
            access_token: SecretString::from(disk.access_token),
            refresh_token: disk.refresh_token.map(SecretString::from),
            expires_at_epoch_s: disk.expires_at_epoch_s,
            scopes: disk.scopes,
        }))
    }

    fn save(&self, credentials: &StoredCredentials) -> Result<()> {
        let parent = self
            .path
            .parent()
            .with_context(|| format!("credential path {} has no parent", self.path.display()))?;
        fs::create_dir_all(parent)?;
        set_owner_only_dir_permissions(parent)?;

        let disk = DiskCredentials {
            account_id: credentials.account_id.clone(),
            access_token: credentials.access_token.expose_secret().to_owned(),
            refresh_token: credentials
                .refresh_token
                .as_ref()
                .map(|token| token.expose_secret().to_owned()),
            expires_at_epoch_s: credentials.expires_at_epoch_s,
            scopes: credentials.scopes.clone(),
        };
        let payload = serde_json::to_vec_pretty(&disk)?;
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, payload)?;
        set_owner_only_file_permissions(&tmp_path)?;
        fs::rename(&tmp_path, &self.path)?;
        set_owner_only_file_permissions(&self.path)?;
        Ok(())
    }

    fn clear(&self) -> Result<bool> {
        if !self.path.exists() {
            return Ok(false);
        }
        fs::remove_file(&self.path)?;
        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskCredentials {
    account_id: String,
    access_token: String,
    refresh_token: Option<String>,
    expires_at_epoch_s: Option<u64>,
    scopes: Vec<String>,
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

#[cfg(test)]
mod tests {
    use super::{CredentialStore, FileCredentialStore, StoredCredentials};
    use secrecy::SecretString;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    #[test]
    fn file_store_round_trips_credentials() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileCredentialStore::new(temp_dir.path().join("gmail-credentials.json"));

        store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("access-token")),
                refresh_token: Some(SecretString::from(String::from("refresh-token"))),
                expires_at_epoch_s: Some(123),
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.account_id, "gmail:operator@example.com");
        assert_eq!(loaded.expires_at_epoch_s, Some(123));
        assert_eq!(loaded.scopes, vec![String::from("scope:a")]);
    }

    #[cfg(unix)]
    #[test]
    fn file_store_hardens_credentials_permissions() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileCredentialStore::new(temp_dir.path().join("gmail-credentials.json"));

        store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("access-token")),
                refresh_token: None,
                expires_at_epoch_s: None,
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();

        let mode = fs::metadata(store.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn clear_removes_credentials_file() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileCredentialStore::new(temp_dir.path().join("gmail-credentials.json"));

        store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("access-token")),
                refresh_token: None,
                expires_at_epoch_s: None,
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();

        assert!(store.clear().unwrap());
        assert!(store.load().unwrap().is_none());
    }
}
