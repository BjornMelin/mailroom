use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::config::GmailConfig;
use anyhow::{Context, Result};
use oauth2::{AuthUrl, ClientId, ClientSecret, RefreshToken, TokenUrl, basic::BasicClient};
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;

const TOKEN_REFRESH_LEEWAY_SECS: u64 = 60;

#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct GmailProfile {
    #[serde(rename = "emailAddress")]
    pub email_address: String,
    #[serde(rename = "messagesTotal")]
    pub messages_total: i64,
    #[serde(rename = "threadsTotal")]
    pub threads_total: i64,
    #[serde(rename = "historyId")]
    pub history_id: String,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct GmailLabel {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub label_type: String,
    #[serde(rename = "messageListVisibility")]
    pub message_list_visibility: Option<String>,
    #[serde(rename = "labelListVisibility")]
    pub label_list_visibility: Option<String>,
    #[serde(rename = "messagesTotal")]
    pub messages_total: Option<i64>,
    #[serde(rename = "messagesUnread")]
    pub messages_unread: Option<i64>,
    #[serde(rename = "threadsTotal")]
    pub threads_total: Option<i64>,
    #[serde(rename = "threadsUnread")]
    pub threads_unread: Option<i64>,
}

#[derive(Debug, Error)]
pub enum GmailClientError {
    #[error("mailroom is not authenticated; run `mailroom auth login` first")]
    MissingCredentials,
    #[error("stored Gmail credentials do not include a refresh token")]
    MissingRefreshToken,
    #[error("gmail API request to {path} failed with status {status}: {body}")]
    Api {
        path: String,
        status: StatusCode,
        body: String,
    },
}

#[derive(Debug, Deserialize)]
struct GmailLabelsResponse {
    labels: Vec<GmailLabel>,
}

#[derive(Debug, Clone)]
pub struct GmailClient {
    config: GmailConfig,
    http: reqwest::Client,
    credential_store: FileCredentialStore,
}

impl GmailClient {
    pub fn new(config: GmailConfig, credential_store: FileCredentialStore) -> Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .context("failed to build reqwest Gmail client")?;

        Ok(Self {
            config,
            http,
            credential_store,
        })
    }

    pub async fn get_profile(&self) -> Result<GmailProfile> {
        self.get_json(
            "users/me/profile",
            &[(
                "fields",
                "emailAddress,messagesTotal,threadsTotal,historyId",
            )],
        )
        .await
    }

    pub async fn list_labels(&self) -> Result<Vec<GmailLabel>> {
        let response: GmailLabelsResponse = self
            .get_json(
                "users/me/labels",
                &[(
                    "fields",
                    "labels(id,name,type,messageListVisibility,labelListVisibility,messagesTotal,messagesUnread,threadsTotal,threadsUnread)",
                )],
            )
            .await?;
        Ok(response.labels)
    }

    pub async fn fetch_profile_with_access_token(
        config: &GmailConfig,
        access_token: &str,
    ) -> Result<GmailProfile> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(config.request_timeout_secs))
            .build()
            .context("failed to build reqwest Gmail client")?;

        let url = format!(
            "{}/users/me/profile",
            config.api_base_url.trim_end_matches('/')
        );
        let response = http
            .get(url)
            .bearer_auth(access_token)
            .query(&[(
                "fields",
                "emailAddress,messagesTotal,threadsTotal,historyId",
            )])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(GmailClientError::Api {
                path: String::from("users/me/profile"),
                status,
                body,
            }
            .into());
        }

        Ok(response.json().await?)
    }

    async fn get_json<T>(&self, path: &str, query: &[(&str, &str)]) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let credentials = self.active_credentials().await?;
        match self
            .request_json::<T>(path, query, credentials.access_token.expose_secret())
            .await
        {
            Ok(payload) => Ok(payload),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                self.request_json::<T>(path, query, refreshed.access_token.expose_secret())
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn active_credentials(&self) -> Result<StoredCredentials> {
        let credentials = self
            .credential_store
            .load()?
            .ok_or(GmailClientError::MissingCredentials)?;

        let now_epoch_s = current_epoch_seconds();
        if credentials.should_refresh(TOKEN_REFRESH_LEEWAY_SECS, now_epoch_s) {
            return self.refresh_credentials(&credentials).await;
        }

        Ok(credentials)
    }

    async fn refresh_credentials(
        &self,
        credentials: &StoredCredentials,
    ) -> Result<StoredCredentials> {
        let refresh_token = credentials
            .refresh_token
            .as_ref()
            .ok_or(GmailClientError::MissingRefreshToken)?;
        let mut oauth_client = BasicClient::new(ClientId::new(
            self.config
                .client_id
                .clone()
                .ok_or_else(|| anyhow::anyhow!("gmail.client_id is not configured"))?,
        ))
        .set_auth_uri(AuthUrl::new(self.config.auth_url.clone())?)
        .set_token_uri(TokenUrl::new(self.config.token_url.clone())?);
        if let Some(secret) = &self.config.client_secret
            && !secret.is_empty()
        {
            oauth_client = oauth_client.set_client_secret(ClientSecret::new(secret.clone()));
        }
        let token = oauth_client
            .exchange_refresh_token(&RefreshToken::new(refresh_token.expose_secret().to_owned()))
            .request_async(&self.http)
            .await
            .context("failed to refresh Gmail access token")?;

        let mut refreshed = StoredCredentials::from_token_response(
            credentials.account_id.clone(),
            &token,
            &credentials.scopes,
        );
        if refreshed.refresh_token.is_none() {
            refreshed.refresh_token = credentials.refresh_token.clone();
        }
        self.credential_store.save(&refreshed)?;
        Ok(refreshed)
    }

    async fn request_json<T>(
        &self,
        path: &str,
        query: &[(&str, &str)],
        access_token: &str,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = format!(
            "{}/{}",
            self.config.api_base_url.trim_end_matches('/'),
            path
        );
        let response = self
            .http
            .get(url)
            .bearer_auth(access_token)
            .query(query)
            .send()
            .await
            .with_context(|| format!("failed to call Gmail API path {path}"))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(GmailClientError::Api {
                path: path.to_owned(),
                status,
                body,
            }
            .into());
        }

        Ok(response.json().await?)
    }
}

fn matches_unauthorized(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<GmailClientError>()
        .is_some_and(|error| matches!(error, GmailClientError::Api { status, .. } if *status == StatusCode::UNAUTHORIZED))
}

fn current_epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{GmailClient, GmailProfile};
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::GmailConfig;
    use secrecy::{ExposeSecret, SecretString};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn get_profile_uses_stored_credentials() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/profile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "emailAddress": "operator@example.com",
                "messagesTotal": 10,
                "threadsTotal": 7,
                "historyId": "12345"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let store = FileCredentialStore::new(temp_dir.path().join("gmail-credentials.json"));
        store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("access-token")),
                refresh_token: Some(SecretString::from(String::from("refresh-token"))),
                expires_at_epoch_s: Some(u64::MAX),
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();

        let client = GmailClient::new(
            GmailConfig {
                client_id: Some(String::from("client-id")),
                client_secret: None,
                auth_url: format!("{}/oauth2/auth", mock_server.uri()),
                token_url: format!("{}/oauth2/token", mock_server.uri()),
                api_base_url: format!("{}/gmail/v1", mock_server.uri()),
                listen_host: String::from("127.0.0.1"),
                listen_port: 0,
                open_browser: false,
                request_timeout_secs: 30,
                scopes: vec![String::from("scope:a")],
            },
            store,
        )
        .unwrap();

        let profile = client.get_profile().await.unwrap();
        assert_eq!(
            profile,
            GmailProfile {
                email_address: String::from("operator@example.com"),
                messages_total: 10,
                threads_total: 7,
                history_id: String::from("12345"),
            }
        );
    }

    #[tokio::test]
    async fn refresh_credentials_persists_rotated_refresh_tokens() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth2/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "fresh-access-token",
                "refresh_token": "rotated-refresh-token",
                "expires_in": 3600,
                "scope": "scope:a",
                "token_type": "Bearer"
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/profile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "emailAddress": "operator@example.com",
                "messagesTotal": 10,
                "threadsTotal": 7,
                "historyId": "12345"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let store = FileCredentialStore::new(temp_dir.path().join("gmail-credentials.json"));
        store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("stale-access-token")),
                refresh_token: Some(SecretString::from(String::from("stale-refresh-token"))),
                expires_at_epoch_s: Some(0),
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();

        let client = GmailClient::new(
            GmailConfig {
                client_id: Some(String::from("client-id")),
                client_secret: None,
                auth_url: format!("{}/oauth2/auth", mock_server.uri()),
                token_url: format!("{}/oauth2/token", mock_server.uri()),
                api_base_url: format!("{}/gmail/v1", mock_server.uri()),
                listen_host: String::from("127.0.0.1"),
                listen_port: 0,
                open_browser: false,
                request_timeout_secs: 30,
                scopes: vec![String::from("scope:a")],
            },
            store.clone(),
        )
        .unwrap();

        let profile = client.get_profile().await.unwrap();
        let refreshed = store.load().unwrap().unwrap();

        assert_eq!(profile.email_address, "operator@example.com");
        assert_eq!(
            refreshed.refresh_token.as_ref().unwrap().expose_secret(),
            "rotated-refresh-token"
        );
    }
}
