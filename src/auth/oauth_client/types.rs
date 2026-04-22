use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredOAuthClientFile {
    pub(super) installed: StoredInstalledOAuthClient,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredInstalledOAuthClient {
    pub(super) client_id: String,
    #[serde(default)]
    pub(super) client_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) project_id: Option<String>,
    pub(super) auth_uri: String,
    pub(super) token_uri: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(super) auth_provider_x509_cert_url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) redirect_uris: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DownloadedGoogleCredentials {
    pub(super) installed: Option<DownloadedInstalledClient>,
}

#[derive(Debug, Deserialize)]
pub(super) struct DownloadedInstalledClient {
    pub(super) client_id: Option<String>,
    pub(super) client_secret: Option<String>,
    pub(super) project_id: Option<String>,
    pub(super) auth_uri: Option<String>,
    pub(super) token_uri: Option<String>,
    pub(super) auth_provider_x509_cert_url: Option<String>,
    pub(super) redirect_uris: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub(super) struct LegacyStoredOAuthClient {
    pub(super) client_id: String,
    pub(super) client_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct AuthorizedUserAdcFile {
    #[serde(rename = "type")]
    pub(super) credential_type: Option<String>,
    pub(super) client_id: Option<String>,
    pub(super) client_secret: Option<String>,
    pub(super) refresh_token: Option<String>,
    pub(super) quota_project_id: Option<String>,
}

#[derive(Debug)]
pub(super) struct AuthorizedUserAdc {
    pub(super) client_id: String,
    pub(super) client_secret: Option<String>,
    pub(super) refresh_token: String,
    pub(super) quota_project_id: Option<String>,
}
