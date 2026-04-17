use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::auth::oauth_client::resolve as resolve_oauth_client;
use crate::config::GmailConfig;
use anyhow::{Context, Result};
use oauth2::{AuthUrl, ClientId, ClientSecret, RefreshToken, TokenUrl, basic::BasicClient};
use reqwest::StatusCode;
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

const TOKEN_REFRESH_LEEWAY_SECS: u64 = 60;
const GMAIL_MAX_RETRY_ATTEMPTS: usize = 4;
const GMAIL_INITIAL_RETRY_DELAY_MS: u64 = 500;

#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq, Eq)]
pub(crate) struct GmailProfile {
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
pub(crate) struct GmailLabel {
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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(crate) struct GmailMessageMetadata {
    pub id: String,
    pub thread_id: String,
    pub label_ids: Vec<String>,
    pub snippet: String,
    pub history_id: String,
    pub internal_date_epoch_ms: i64,
    pub size_estimate: i64,
    pub subject: String,
    pub from_header: String,
    pub from_address: Option<String>,
    pub to_header: String,
    pub cc_header: String,
    pub bcc_header: String,
    pub reply_to_header: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailMessageListPage {
    pub messages: Vec<GmailMessageListItem>,
    pub next_page_token: Option<String>,
    pub result_size_estimate: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GmailMessageListItem {
    pub id: String,
    #[serde(rename = "threadId")]
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailHistoryPage {
    pub changed_message_ids: Vec<String>,
    pub deleted_message_ids: Vec<String>,
    pub next_page_token: Option<String>,
    pub history_id: String,
}

#[derive(Debug, Error)]
pub(crate) enum GmailClientError {
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

#[derive(Debug, Deserialize)]
struct GmailMessagesListResponse {
    messages: Option<Vec<GmailMessageListItem>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "resultSizeEstimate")]
    result_size_estimate: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct GmailMessageMetadataResponse {
    id: String,
    #[serde(rename = "threadId")]
    thread_id: String,
    #[serde(rename = "labelIds", default)]
    label_ids: Vec<String>,
    snippet: Option<String>,
    #[serde(rename = "historyId")]
    history_id: String,
    #[serde(rename = "internalDate")]
    internal_date: String,
    #[serde(rename = "sizeEstimate")]
    size_estimate: i64,
    payload: Option<GmailMessagePayload>,
}

#[derive(Debug, Deserialize)]
struct GmailMessagePayload {
    #[serde(default)]
    headers: Vec<GmailHeader>,
}

#[derive(Debug, Deserialize)]
struct GmailHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryResponse {
    history: Option<Vec<GmailHistoryRecord>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "historyId")]
    history_id: String,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryRecord {
    #[serde(rename = "messagesAdded", default)]
    messages_added: Vec<GmailHistoryMessageRef>,
    #[serde(rename = "messagesDeleted", default)]
    messages_deleted: Vec<GmailHistoryMessageRef>,
    #[serde(rename = "labelsAdded", default)]
    labels_added: Vec<GmailHistoryLabelRef>,
    #[serde(rename = "labelsRemoved", default)]
    labels_removed: Vec<GmailHistoryLabelRef>,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryMessageRef {
    message: GmailHistoryMessage,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryLabelRef {
    message: GmailHistoryMessage,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryMessage {
    id: String,
}

#[derive(Debug, Clone)]
pub(crate) struct GmailClient {
    config: GmailConfig,
    workspace: crate::config::WorkspaceConfig,
    http: reqwest::Client,
    credential_store: FileCredentialStore,
}

impl GmailClient {
    pub(crate) fn new(
        config: GmailConfig,
        workspace: crate::config::WorkspaceConfig,
        credential_store: FileCredentialStore,
    ) -> Result<Self> {
        let http = build_gmail_http_client(&config)?;

        Ok(Self {
            config,
            workspace,
            http,
            credential_store,
        })
    }

    pub(crate) async fn get_profile_with_access_scope(&self) -> Result<(GmailProfile, String)> {
        let credentials = self.active_credentials().await?;
        let access_scope = credentials.scopes.join(" ");
        let query = [(
            "fields",
            String::from("emailAddress,messagesTotal,threadsTotal,historyId"),
        )];
        match self
            .request_json::<GmailProfile>(
                "users/me/profile",
                &query,
                credentials.access_token.expose_secret(),
            )
            .await
        {
            Ok(profile) => Ok((profile, access_scope)),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                let access_scope = refreshed.scopes.join(" ");
                let profile = self
                    .request_json::<GmailProfile>(
                        "users/me/profile",
                        &query,
                        refreshed.access_token.expose_secret(),
                    )
                    .await?;
                Ok((profile, access_scope))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn list_labels(&self) -> Result<Vec<GmailLabel>> {
        let query = [(
            "fields",
            String::from(
                "labels(id,name,type,messageListVisibility,labelListVisibility,messagesTotal,messagesUnread,threadsTotal,threadsUnread)",
            ),
        )];
        let response: GmailLabelsResponse = self.get_json("users/me/labels", &query).await?;
        Ok(response.labels)
    }

    pub(crate) async fn list_message_ids(
        &self,
        query: Option<&str>,
        page_token: Option<&str>,
        max_results: u32,
    ) -> Result<GmailMessageListPage> {
        let mut params = vec![
            (
                "fields",
                String::from("messages(id,threadId),nextPageToken,resultSizeEstimate"),
            ),
            ("maxResults", max_results.to_string()),
        ];
        if let Some(query) = query {
            params.push(("q", query.to_owned()));
        }
        if let Some(page_token) = page_token {
            params.push(("pageToken", page_token.to_owned()));
        }

        let response: GmailMessagesListResponse =
            self.get_json("users/me/messages", &params).await?;
        Ok(GmailMessageListPage {
            messages: response.messages.unwrap_or_default(),
            next_page_token: response.next_page_token,
            result_size_estimate: response.result_size_estimate,
        })
    }

    pub(crate) async fn get_message_metadata(
        &self,
        message_id: &str,
    ) -> Result<GmailMessageMetadata> {
        let response: GmailMessageMetadataResponse = self
            .get_json(
                &format!("users/me/messages/{message_id}"),
                &[
                    ("format", String::from("metadata")),
                    (
                        "fields",
                        String::from(
                            "id,threadId,labelIds,snippet,historyId,internalDate,sizeEstimate,payload/headers",
                        ),
                    ),
                    ("metadataHeaders[]", String::from("Subject")),
                    ("metadataHeaders[]", String::from("From")),
                    ("metadataHeaders[]", String::from("To")),
                    ("metadataHeaders[]", String::from("Cc")),
                    ("metadataHeaders[]", String::from("Bcc")),
                    ("metadataHeaders[]", String::from("Reply-To")),
                ],
            )
            .await?;
        response.into_message_metadata()
    }

    pub(crate) async fn get_message_metadata_if_present(
        &self,
        message_id: &str,
    ) -> Result<Option<GmailMessageMetadata>> {
        match self.get_message_metadata(message_id).await {
            Ok(metadata) => Ok(Some(metadata)),
            Err(error) if matches_missing_message_error(&error, message_id) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn list_history(
        &self,
        start_history_id: &str,
        page_token: Option<&str>,
    ) -> Result<GmailHistoryPage> {
        let mut params = vec![
            ("startHistoryId", start_history_id.to_owned()),
            (
                "fields",
                String::from(
                    "history(messagesAdded/message/id,messagesDeleted/message/id,labelsAdded/message/id,labelsRemoved/message/id),nextPageToken,historyId",
                ),
            ),
            ("historyTypes[]", String::from("messageAdded")),
            ("historyTypes[]", String::from("messageDeleted")),
            ("historyTypes[]", String::from("labelAdded")),
            ("historyTypes[]", String::from("labelRemoved")),
            ("maxResults", String::from("500")),
        ];
        if let Some(page_token) = page_token {
            params.push(("pageToken", page_token.to_owned()));
        }

        let response: GmailHistoryResponse = self.get_json("users/me/history", &params).await?;
        Ok(response.into_history_page())
    }

    pub(crate) async fn fetch_profile_with_access_token(
        config: &GmailConfig,
        access_token: &str,
    ) -> Result<GmailProfile> {
        let http = build_gmail_http_client(config)?;

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

    async fn get_json<T>(&self, path: &str, query: &[(&str, String)]) -> Result<T>
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

        let now_epoch_s = u64::try_from(crate::time::current_epoch_seconds()?)?;
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
        let resolved_client = resolve_oauth_client(&self.config, &self.workspace)?;
        let mut oauth_client = BasicClient::new(ClientId::new(resolved_client.client_id))
            .set_auth_uri(AuthUrl::new(self.config.auth_url.clone())?)
            .set_token_uri(TokenUrl::new(self.config.token_url.clone())?);
        if let Some(secret) = resolved_client.client_secret
            && !secret.is_empty()
        {
            oauth_client = oauth_client.set_client_secret(ClientSecret::new(secret));
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
        query: &[(&str, String)],
        access_token: &str,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self.send_get_request(path, query, access_token).await?;
        Ok(response.json().await?)
    }

    async fn send_get_request(
        &self,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
    ) -> Result<reqwest::Response> {
        let url = format!(
            "{}/{}",
            self.config.api_base_url.trim_end_matches('/'),
            path
        );
        let mut retry_delay_ms = GMAIL_INITIAL_RETRY_DELAY_MS;

        for attempt in 0..GMAIL_MAX_RETRY_ATTEMPTS {
            let response = self
                .http
                .get(&url)
                .bearer_auth(access_token)
                .query(query)
                .send()
                .await;

            match response {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    if is_retryable_status(status) && attempt + 1 < GMAIL_MAX_RETRY_ATTEMPTS {
                        sleep(Duration::from_millis(retry_delay_ms)).await;
                        retry_delay_ms *= 2;
                        continue;
                    }

                    return Err(GmailClientError::Api {
                        path: path.to_owned(),
                        status,
                        body,
                    }
                    .into());
                }
                Err(error) => {
                    if is_retryable_transport_error(&error)
                        && attempt + 1 < GMAIL_MAX_RETRY_ATTEMPTS
                    {
                        sleep(Duration::from_millis(retry_delay_ms)).await;
                        retry_delay_ms *= 2;
                        continue;
                    }

                    return Err(error)
                        .with_context(|| format!("failed to call Gmail API path {path}"));
                }
            }
        }

        Err(anyhow::anyhow!(
            "exhausted retry attempts for Gmail API path {path}"
        ))
    }
}

impl GmailMessageMetadataResponse {
    fn into_message_metadata(self) -> Result<GmailMessageMetadata> {
        let headers = self
            .payload
            .map(|payload| payload.headers)
            .unwrap_or_default();
        Ok(GmailMessageMetadata {
            id: self.id,
            thread_id: self.thread_id,
            label_ids: self.label_ids,
            snippet: self.snippet.unwrap_or_default(),
            history_id: self.history_id,
            internal_date_epoch_ms: self
                .internal_date
                .parse::<i64>()
                .context("gmail internalDate was not a valid integer")?,
            size_estimate: self.size_estimate,
            subject: header_value(&headers, "Subject").unwrap_or_default(),
            from_header: header_value(&headers, "From").unwrap_or_default(),
            from_address: header_value(&headers, "From")
                .and_then(|value| extract_email_address(&value)),
            to_header: header_value(&headers, "To").unwrap_or_default(),
            cc_header: header_value(&headers, "Cc").unwrap_or_default(),
            bcc_header: header_value(&headers, "Bcc").unwrap_or_default(),
            reply_to_header: header_value(&headers, "Reply-To").unwrap_or_default(),
        })
    }
}

impl GmailHistoryResponse {
    fn into_history_page(self) -> GmailHistoryPage {
        let mut changed_message_ids = std::collections::BTreeSet::new();
        let mut deleted_message_ids = std::collections::BTreeSet::new();

        for record in self.history.unwrap_or_default() {
            for entry in record.messages_added {
                changed_message_ids.insert(entry.message.id);
            }
            for entry in record.labels_added {
                changed_message_ids.insert(entry.message.id);
            }
            for entry in record.labels_removed {
                changed_message_ids.insert(entry.message.id);
            }
            for entry in record.messages_deleted {
                deleted_message_ids.insert(entry.message.id);
            }
        }

        for deleted_id in &deleted_message_ids {
            changed_message_ids.remove(deleted_id);
        }

        GmailHistoryPage {
            changed_message_ids: changed_message_ids.into_iter().collect(),
            deleted_message_ids: deleted_message_ids.into_iter().collect(),
            next_page_token: self.next_page_token,
            history_id: self.history_id,
        }
    }
}

fn matches_unauthorized(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<GmailClientError>()
        .is_some_and(|error| matches!(error, GmailClientError::Api { status, .. } if *status == StatusCode::UNAUTHORIZED))
}

fn matches_missing_message_error(error: &anyhow::Error, message_id: &str) -> bool {
    let expected_path = format!("users/me/messages/{message_id}");
    error
        .downcast_ref::<GmailClientError>()
        .is_some_and(|error| {
            matches!(
                error,
                GmailClientError::Api { path, status, .. }
                    if *status == StatusCode::NOT_FOUND && path == &expected_path
            )
        })
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout()
}

fn header_value(headers: &[GmailHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .map(|header| header.value.trim().to_owned())
}

fn extract_email_address(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.contains(',') || value.contains('\n') {
        return None;
    }
    if value.contains(':') && value.ends_with(';') {
        return None;
    }

    if let Some((display_name, remainder)) = value.rsplit_once('<') {
        let (candidate, suffix) = remainder.split_once('>')?;
        if display_name.trim().is_empty() || !suffix.trim().is_empty() {
            return None;
        }
        return normalize_email_candidate(candidate);
    }

    if value.contains('<') || value.contains('>') {
        return None;
    }

    normalize_email_candidate(value)
}

fn normalize_email_candidate(candidate: &str) -> Option<String> {
    let candidate = candidate
        .trim()
        .trim_matches(|character: char| matches!(character, '"' | '\''));
    if candidate.is_empty()
        || candidate.contains(char::is_whitespace)
        || candidate.matches('@').count() != 1
    {
        return None;
    }

    let (local_part, domain_part) = candidate.split_once('@')?;
    if !is_valid_email_local_part(local_part) || !is_valid_email_domain(domain_part) {
        return None;
    }

    Some(candidate.to_ascii_lowercase())
}

fn is_valid_email_local_part(local_part: &str) -> bool {
    !local_part.is_empty()
        && local_part.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '.' | '_' | '%' | '+' | '-' | '\'')
        })
}

fn is_valid_email_domain(domain_part: &str) -> bool {
    if domain_part.is_empty() || !domain_part.contains('.') {
        return false;
    }

    domain_part.split('.').all(|label| {
        !label.is_empty()
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '-')
    })
}

fn build_gmail_http_client(config: &GmailConfig) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .user_agent(format!("mailroom/{} (gzip)", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build reqwest Gmail client")
}

#[cfg(test)]
mod tests {
    use super::{GmailClient, GmailProfile, extract_email_address};
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::{GmailConfig, WorkspaceConfig};
    use secrecy::{ExposeSecret, SecretString};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
        let workspace = workspace_for(&temp_dir);
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
            workspace,
            store,
        )
        .unwrap();

        let (profile, access_scope) = client.get_profile_with_access_scope().await.unwrap();
        assert_eq!(
            profile,
            GmailProfile {
                email_address: String::from("operator@example.com"),
                messages_total: 10,
                threads_total: 7,
                history_id: String::from("12345"),
            }
        );
        assert_eq!(access_scope, "scope:a");
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
        let workspace = workspace_for(&temp_dir);
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
            workspace,
            store.clone(),
        )
        .unwrap();

        let (profile, access_scope) = client.get_profile_with_access_scope().await.unwrap();
        let refreshed = store.load().unwrap().unwrap();

        assert_eq!(profile.email_address, "operator@example.com");
        assert_eq!(access_scope, "scope:a");
        assert_eq!(
            refreshed.refresh_token.as_ref().unwrap().expose_secret(),
            "rotated-refresh-token"
        );
    }

    #[test]
    fn extract_email_address_accepts_supported_header_shapes() {
        assert_eq!(
            extract_email_address("\"Alice Example\" <Alice.Example+ops@example.com>"),
            Some(String::from("alice.example+ops@example.com"))
        );
        assert_eq!(
            extract_email_address("O'Hara <o'hara@example.com>"),
            Some(String::from("o'hara@example.com"))
        );
        assert_eq!(
            extract_email_address("bob@example.com"),
            Some(String::from("bob@example.com"))
        );
    }

    #[test]
    fn extract_email_address_rejects_ambiguous_or_unsupported_headers() {
        assert_eq!(
            extract_email_address("alice@example.com, bob@example.com"),
            None
        );
        assert_eq!(extract_email_address("Team: alice@example.com;"), None);
        assert_eq!(
            extract_email_address("Display Name <broken@example.com> trailing"),
            None
        );
    }
}
