use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::auth::oauth_client::resolve as resolve_oauth_client;
use crate::config::GmailConfig;
use oauth2::{AuthUrl, ClientId, ClientSecret, RefreshToken, TokenUrl, basic::BasicClient};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::json;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tokio::time::sleep;

const TOKEN_REFRESH_LEEWAY_SECS: u64 = 60;
const GMAIL_MAX_RETRY_ATTEMPTS: usize = 4;
const GMAIL_INITIAL_RETRY_DELAY_MS: u64 = 500;

type GmailResult<T> = std::result::Result<T, GmailClientError>;

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
pub(crate) struct GmailMessageListItem {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailThreadContext {
    pub id: String,
    pub history_id: String,
    pub messages: Vec<GmailThreadMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailThreadMessage {
    pub id: String,
    pub thread_id: String,
    pub history_id: String,
    pub internal_date_epoch_ms: i64,
    pub snippet: String,
    pub subject: String,
    pub from_header: String,
    pub from_address: Option<String>,
    pub to_header: String,
    pub cc_header: String,
    pub bcc_header: String,
    pub reply_to_header: String,
    pub message_id_header: Option<String>,
    pub references_header: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailDraftRef {
    pub id: String,
    pub message_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailSentMessageRef {
    pub message_id: String,
    pub thread_id: String,
    pub history_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GmailThreadMutationRef {
    pub thread_id: String,
    pub history_id: Option<String>,
}

#[derive(Debug, Error)]
pub(crate) enum GmailClientError {
    #[error("mailroom is not authenticated; run `mailroom auth login` first")]
    MissingCredentials,
    #[error("stored Gmail credentials do not include a refresh token")]
    MissingRefreshToken,
    #[error("failed to read stored Gmail credentials")]
    CredentialLoad {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to persist refreshed Gmail credentials")]
    CredentialSave {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to resolve Gmail OAuth client configuration")]
    OAuthClient {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to refresh Gmail access token")]
    TokenRefresh {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to compute Gmail credential freshness")]
    Clock {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to build reqwest Gmail client")]
    HttpClientBuild {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to call Gmail API path {path}")]
    Transport {
        path: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("gmail response for {path} could not be decoded")]
    ResponseDecode {
        path: String,
        #[source]
        source: anyhow::Error,
    },
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
struct GmailThreadResponse {
    id: String,
    #[serde(rename = "historyId")]
    history_id: String,
    #[serde(default)]
    messages: Vec<GmailThreadMessageResponse>,
}

#[derive(Debug, Deserialize)]
struct GmailThreadMessageResponse {
    id: String,
    #[serde(rename = "threadId")]
    thread_id: String,
    #[serde(rename = "historyId")]
    history_id: String,
    #[serde(rename = "internalDate")]
    internal_date: String,
    snippet: Option<String>,
    payload: Option<GmailMessagePayload>,
}

#[derive(Debug, Deserialize)]
struct GmailDraftResponse {
    id: String,
    message: GmailDraftMessageResponse,
}

#[derive(Debug, Deserialize)]
struct GmailDraftMessageResponse {
    id: String,
    #[serde(rename = "threadId")]
    thread_id: String,
    #[serde(rename = "historyId")]
    history_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GmailThreadMutationResponse {
    id: String,
    #[serde(rename = "historyId")]
    history_id: Option<String>,
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
    ) -> GmailResult<Self> {
        let http = build_gmail_http_client(&config)?;

        Ok(Self {
            config,
            workspace,
            http,
            credential_store,
        })
    }

    pub(crate) async fn get_profile_with_access_scope(
        &self,
    ) -> GmailResult<(GmailProfile, String)> {
        let credentials = self.active_credentials().await?;
        let access_scope = credentials.scopes.join(" ");
        let query = [(
            "fields",
            String::from("emailAddress,messagesTotal,threadsTotal,historyId"),
        )];
        match self
            .request_json::<GmailProfile>(
                Method::GET,
                "users/me/profile",
                &query,
                credentials.access_token.expose_secret(),
                None,
            )
            .await
        {
            Ok(profile) => Ok((profile, access_scope)),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                let access_scope = refreshed.scopes.join(" ");
                let profile = self
                    .request_json::<GmailProfile>(
                        Method::GET,
                        "users/me/profile",
                        &query,
                        refreshed.access_token.expose_secret(),
                        None,
                    )
                    .await?;
                Ok((profile, access_scope))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn list_labels(&self) -> GmailResult<Vec<GmailLabel>> {
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
    ) -> GmailResult<GmailMessageListPage> {
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
    ) -> GmailResult<GmailMessageMetadata> {
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
    ) -> GmailResult<Option<GmailMessageMetadata>> {
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
    ) -> GmailResult<GmailHistoryPage> {
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

    pub(crate) async fn get_thread_context(
        &self,
        thread_id: &str,
    ) -> GmailResult<GmailThreadContext> {
        let response: GmailThreadResponse = self
            .get_json(
                &format!("users/me/threads/{thread_id}"),
                &[
                    ("format", String::from("metadata")),
                    (
                        "fields",
                        String::from(
                            "id,historyId,messages(id,threadId,historyId,internalDate,snippet,payload/headers)",
                        ),
                    ),
                    ("metadataHeaders[]", String::from("Subject")),
                    ("metadataHeaders[]", String::from("From")),
                    ("metadataHeaders[]", String::from("To")),
                    ("metadataHeaders[]", String::from("Cc")),
                    ("metadataHeaders[]", String::from("Bcc")),
                    ("metadataHeaders[]", String::from("Reply-To")),
                    ("metadataHeaders[]", String::from("Message-ID")),
                    ("metadataHeaders[]", String::from("References")),
                ],
            )
            .await?;
        response.into_thread_context()
    }

    pub(crate) async fn create_draft(
        &self,
        raw_message: &str,
        thread_id: Option<&str>,
    ) -> GmailResult<GmailDraftRef> {
        let body = draft_request_body(raw_message, thread_id);
        let response: GmailDraftResponse = self.post_json("users/me/drafts", &[], body).await?;
        Ok(response.into_draft_ref())
    }

    pub(crate) async fn update_draft(
        &self,
        draft_id: &str,
        raw_message: &str,
        thread_id: Option<&str>,
    ) -> GmailResult<GmailDraftRef> {
        let body = draft_request_body(raw_message, thread_id);
        let response: GmailDraftResponse = self
            .put_json(&format!("users/me/drafts/{draft_id}"), &[], body)
            .await?;
        Ok(response.into_draft_ref())
    }

    pub(crate) async fn send_draft(&self, draft_id: &str) -> GmailResult<GmailSentMessageRef> {
        let response: GmailDraftMessageResponse = self
            .post_json(
                "users/me/drafts/send",
                &[],
                json!({
                    "id": draft_id,
                }),
            )
            .await?;
        Ok(GmailSentMessageRef {
            message_id: response.id,
            thread_id: response.thread_id,
            history_id: response.history_id,
        })
    }

    pub(crate) async fn delete_draft(&self, draft_id: &str) -> GmailResult<()> {
        self.execute_empty(Method::DELETE, &format!("users/me/drafts/{draft_id}"), &[])
            .await
    }

    pub(crate) async fn modify_thread_labels(
        &self,
        thread_id: &str,
        add_label_ids: &[String],
        remove_label_ids: &[String],
    ) -> GmailResult<GmailThreadMutationRef> {
        let response: GmailThreadMutationResponse = self
            .post_json(
                &format!("users/me/threads/{thread_id}/modify"),
                &[],
                json!({
                    "addLabelIds": add_label_ids,
                    "removeLabelIds": remove_label_ids,
                }),
            )
            .await?;
        Ok(response.into_mutation_ref())
    }

    pub(crate) async fn trash_thread(
        &self,
        thread_id: &str,
    ) -> GmailResult<GmailThreadMutationRef> {
        let response: GmailThreadMutationResponse = self
            .post_json(
                &format!("users/me/threads/{thread_id}/trash"),
                &[],
                json!({}),
            )
            .await?;
        Ok(response.into_mutation_ref())
    }

    pub(crate) async fn fetch_profile_with_access_token(
        config: &GmailConfig,
        access_token: &str,
    ) -> GmailResult<GmailProfile> {
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
            .await
            .map_err(|source| GmailClientError::Transport {
                path: String::from("users/me/profile"),
                source,
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(GmailClientError::Api {
                path: String::from("users/me/profile"),
                status,
                body,
            });
        }

        response
            .json()
            .await
            .map_err(|source| GmailClientError::ResponseDecode {
                path: String::from("users/me/profile"),
                source: source.into(),
            })
    }

    async fn get_json<T>(&self, path: &str, query: &[(&str, String)]) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::GET, path, query, None).await
    }

    async fn post_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: serde_json::Value,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::POST, path, query, Some(body))
            .await
    }

    async fn put_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: serde_json::Value,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::PUT, path, query, Some(body))
            .await
    }

    async fn execute_json<T>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<serde_json::Value>,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let credentials = self.active_credentials().await?;
        match self
            .request_json::<T>(
                method.clone(),
                path,
                query,
                credentials.access_token.expose_secret(),
                body.as_ref(),
            )
            .await
        {
            Ok(payload) => Ok(payload),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                self.request_json::<T>(
                    method,
                    path,
                    query,
                    refreshed.access_token.expose_secret(),
                    body.as_ref(),
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    async fn execute_empty(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
    ) -> GmailResult<()> {
        let credentials = self.active_credentials().await?;
        match self
            .request_empty(
                method.clone(),
                path,
                query,
                credentials.access_token.expose_secret(),
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                self.request_empty(method, path, query, refreshed.access_token.expose_secret())
                    .await
            }
            Err(error) => Err(error),
        }
    }

    async fn active_credentials(&self) -> GmailResult<StoredCredentials> {
        let credentials = self
            .credential_store
            .load()
            .map_err(|source| GmailClientError::CredentialLoad { source })?
            .ok_or(GmailClientError::MissingCredentials)?;

        let now_epoch_s = u64::try_from(
            crate::time::current_epoch_seconds()
                .map_err(|source| GmailClientError::Clock { source })?,
        )
        .map_err(|source| GmailClientError::ResponseDecode {
            path: String::from("credentials.expiry"),
            source: source.into(),
        })?;
        if credentials.should_refresh(TOKEN_REFRESH_LEEWAY_SECS, now_epoch_s) {
            return self.refresh_credentials(&credentials).await;
        }

        Ok(credentials)
    }

    async fn refresh_credentials(
        &self,
        credentials: &StoredCredentials,
    ) -> GmailResult<StoredCredentials> {
        let refresh_token = credentials
            .refresh_token
            .as_ref()
            .ok_or(GmailClientError::MissingRefreshToken)?;
        let resolved_client = resolve_oauth_client(&self.config, &self.workspace)
            .map_err(|source| GmailClientError::OAuthClient { source })?;
        let mut oauth_client = BasicClient::new(ClientId::new(resolved_client.client_id))
            .set_auth_uri(
                AuthUrl::new(self.config.auth_url.clone()).map_err(|source| {
                    GmailClientError::OAuthClient {
                        source: source.into(),
                    }
                })?,
            )
            .set_token_uri(
                TokenUrl::new(self.config.token_url.clone()).map_err(|source| {
                    GmailClientError::OAuthClient {
                        source: source.into(),
                    }
                })?,
            );
        if let Some(secret) = resolved_client.client_secret
            && !secret.is_empty()
        {
            oauth_client = oauth_client.set_client_secret(ClientSecret::new(secret));
        }
        let token = oauth_client
            .exchange_refresh_token(&RefreshToken::new(refresh_token.expose_secret().to_owned()))
            .request_async(&self.http)
            .await
            .map_err(|source| GmailClientError::TokenRefresh {
                source: source.into(),
            })?;

        let mut refreshed = StoredCredentials::from_token_response(
            credentials.account_id.clone(),
            &token,
            &credentials.scopes,
        );
        if refreshed.refresh_token.is_none() {
            refreshed.refresh_token = credentials.refresh_token.clone();
        }
        self.credential_store
            .save(&refreshed)
            .map_err(|source| GmailClientError::CredentialSave { source })?;
        Ok(refreshed)
    }

    async fn request_json<T>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
        body: Option<&serde_json::Value>,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self
            .send_request(method, path, query, access_token, body)
            .await?;
        response
            .json()
            .await
            .map_err(|source| GmailClientError::ResponseDecode {
                path: path.to_owned(),
                source: source.into(),
            })
    }

    async fn request_empty(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
    ) -> GmailResult<()> {
        self.send_request(method, path, query, access_token, None)
            .await?;
        Ok(())
    }

    async fn send_request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
        body: Option<&serde_json::Value>,
    ) -> GmailResult<reqwest::Response> {
        let url = format!(
            "{}/{}",
            self.config.api_base_url.trim_end_matches('/'),
            path
        );
        let retryable_request = request_supports_automatic_retry(&method);
        let mut retry_delay_ms = GMAIL_INITIAL_RETRY_DELAY_MS;
        let mut attempt = 0usize;

        loop {
            attempt += 1;
            let mut request = self
                .http
                .request(method.clone(), &url)
                .bearer_auth(access_token)
                .query(query);
            if let Some(body) = body {
                request = request.json(body);
            }
            let response = request.send().await;

            match response {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    let retry_delay = retry_delay_duration(response.headers(), retry_delay_ms);
                    let body = response.text().await.unwrap_or_default();
                    if retryable_request
                        && is_retryable_status(status)
                        && attempt < GMAIL_MAX_RETRY_ATTEMPTS
                    {
                        sleep(retry_delay).await;
                        retry_delay_ms = duration_to_retry_delay_ms(retry_delay).saturating_mul(2);
                        continue;
                    }

                    return Err(GmailClientError::Api {
                        path: path.to_owned(),
                        status,
                        body,
                    });
                }
                Err(error) => {
                    if retryable_request
                        && is_retryable_transport_error(&error)
                        && attempt < GMAIL_MAX_RETRY_ATTEMPTS
                    {
                        sleep(Duration::from_millis(retry_delay_ms)).await;
                        retry_delay_ms *= 2;
                        continue;
                    }

                    return Err(GmailClientError::Transport {
                        path: path.to_owned(),
                        source: error,
                    });
                }
            }
        }
    }
}

fn request_supports_automatic_retry(method: &Method) -> bool {
    *method == Method::GET
}

impl GmailMessageMetadataResponse {
    fn into_message_metadata(self) -> GmailResult<GmailMessageMetadata> {
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
            internal_date_epoch_ms: self.internal_date.parse::<i64>().map_err(|source| {
                GmailClientError::ResponseDecode {
                    path: String::from("gmail.message.internal_date"),
                    source: source.into(),
                }
            })?,
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

impl GmailThreadResponse {
    fn into_thread_context(self) -> GmailResult<GmailThreadContext> {
        let mut messages = self
            .messages
            .into_iter()
            .map(GmailThreadMessageResponse::into_thread_message)
            .collect::<GmailResult<Vec<_>>>()?;
        messages.sort_by_key(|message| (message.internal_date_epoch_ms, message.id.clone()));
        Ok(GmailThreadContext {
            id: self.id,
            history_id: self.history_id,
            messages,
        })
    }
}

impl GmailThreadMessageResponse {
    fn into_thread_message(self) -> GmailResult<GmailThreadMessage> {
        let headers = self
            .payload
            .map(|payload| payload.headers)
            .unwrap_or_default();
        Ok(GmailThreadMessage {
            id: self.id,
            thread_id: self.thread_id,
            history_id: self.history_id,
            internal_date_epoch_ms: self.internal_date.parse::<i64>().map_err(|source| {
                GmailClientError::ResponseDecode {
                    path: String::from("gmail.thread_message.internal_date"),
                    source: source.into(),
                }
            })?,
            snippet: self.snippet.unwrap_or_default(),
            subject: header_value(&headers, "Subject").unwrap_or_default(),
            from_header: header_value(&headers, "From").unwrap_or_default(),
            from_address: header_value(&headers, "From")
                .and_then(|value| extract_email_address(&value)),
            to_header: header_value(&headers, "To").unwrap_or_default(),
            cc_header: header_value(&headers, "Cc").unwrap_or_default(),
            bcc_header: header_value(&headers, "Bcc").unwrap_or_default(),
            reply_to_header: header_value(&headers, "Reply-To").unwrap_or_default(),
            message_id_header: header_value(&headers, "Message-ID"),
            references_header: header_value(&headers, "References").unwrap_or_default(),
        })
    }
}

impl GmailDraftResponse {
    fn into_draft_ref(self) -> GmailDraftRef {
        GmailDraftRef {
            id: self.id,
            message_id: self.message.id,
            thread_id: self.message.thread_id,
        }
    }
}

impl GmailThreadMutationResponse {
    fn into_mutation_ref(self) -> GmailThreadMutationRef {
        GmailThreadMutationRef {
            thread_id: self.id,
            history_id: self.history_id,
        }
    }
}

fn draft_request_body(raw_message: &str, thread_id: Option<&str>) -> serde_json::Value {
    let mut message = json!({
        "raw": raw_message,
    });
    if let Some(thread_id) = thread_id {
        message["threadId"] = serde_json::Value::String(thread_id.to_owned());
    }
    json!({ "message": message })
}

fn matches_unauthorized(error: &GmailClientError) -> bool {
    matches!(
        error,
        GmailClientError::Api { status, .. } if *status == StatusCode::UNAUTHORIZED
    )
}

fn matches_missing_message_error(error: &GmailClientError, message_id: &str) -> bool {
    let expected_path = format!("users/me/messages/{message_id}");
    matches!(
        error,
        GmailClientError::Api { path, status, .. }
            if *status == StatusCode::NOT_FOUND && path == &expected_path
    )
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

fn retry_delay_duration(headers: &HeaderMap, default_delay_ms: u64) -> Duration {
    retry_after_delay(headers).unwrap_or_else(|| Duration::from_millis(default_delay_ms))
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let value = value.trim();

    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    let retry_at = httpdate::parse_http_date(value).ok()?;
    match retry_at.duration_since(SystemTime::now()) {
        Ok(duration) => Some(duration),
        Err(_) => Some(Duration::ZERO),
    }
}

fn duration_to_retry_delay_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn header_value(headers: &[GmailHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .map(|header| header.value.trim().to_owned())
}

fn extract_email_address(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.contains('\n') || value.contains('\r') {
        return None;
    }

    let mut in_quotes = false;
    let mut escaped = false;
    let mut angle_start = None;
    let mut angle_end = None;

    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }

        match character {
            '\\' if in_quotes => {
                escaped = true;
            }
            '"' => {
                in_quotes = !in_quotes;
            }
            '<' if !in_quotes && angle_start.replace(index).is_some() => {
                return None;
            }
            '>' if !in_quotes && (angle_start.is_none() || angle_end.replace(index).is_some()) => {
                return None;
            }
            ',' if !in_quotes => {
                return None;
            }
            _ => {}
        }
    }

    if in_quotes || escaped {
        return None;
    }

    if let Some(open_index) = angle_start {
        let close_index = angle_end?;
        if open_index >= close_index {
            return None;
        }

        let display_name = value[..open_index].trim();
        let candidate = value[open_index + 1..close_index].trim();
        let suffix = value[close_index + 1..].trim();
        if !suffix.is_empty() {
            return None;
        }

        if display_name.is_empty() && matches!(candidate.chars().next(), Some('"')) {
            return None;
        }

        return normalize_email_candidate(candidate);
    }

    if value.contains('<') || value.contains('>') {
        return None;
    }

    if value.contains(':') && value.ends_with(';') {
        return None;
    }

    normalize_email_candidate(value)
}

fn normalize_email_candidate(candidate: &str) -> Option<String> {
    let candidate = candidate.trim().trim_matches('"');
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

fn build_gmail_http_client(config: &GmailConfig) -> GmailResult<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .user_agent(format!("mailroom/{} (gzip)", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|source| GmailClientError::HttpClientBuild {
            source: source.into(),
        })
}

#[cfg(test)]
mod tests {
    use super::{
        GmailClient, GmailProfile, duration_to_retry_delay_ms, extract_email_address,
        request_supports_automatic_retry, retry_after_delay,
    };
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::{GmailConfig, WorkspaceConfig};
    use reqwest::Method;
    use reqwest::header::HeaderMap;
    use secrecy::{ExposeSecret, SecretString};
    use serde_json::json;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;
    use wiremock::matchers::{body_json, method, path};
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

    fn test_store(temp_dir: &TempDir) -> FileCredentialStore {
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
        store
    }

    fn test_client(mock_server: &MockServer, temp_dir: &TempDir) -> GmailClient {
        GmailClient::new(
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
            workspace_for(temp_dir),
            test_store(temp_dir),
        )
        .unwrap()
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
        let client = test_client(&mock_server, &temp_dir);

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
        let store = test_store(&temp_dir);
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
            workspace_for(&temp_dir),
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
    fn retry_after_delay_reads_delta_seconds_header_values() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "7".parse().unwrap());

        assert_eq!(retry_after_delay(&headers), Some(Duration::from_secs(7)));
    }

    #[test]
    fn retry_after_delay_reads_http_date_header_values() {
        let mut headers = HeaderMap::new();
        let retry_at = SystemTime::now() + Duration::from_secs(2);
        headers.insert(
            reqwest::header::RETRY_AFTER,
            httpdate::fmt_http_date(retry_at).parse().unwrap(),
        );

        let delay = retry_after_delay(&headers).unwrap();
        assert!(delay <= Duration::from_secs(2));
        assert!(delay >= Duration::from_secs(1));
    }

    #[test]
    fn duration_to_retry_delay_ms_scales_seconds_to_millis() {
        assert_eq!(duration_to_retry_delay_ms(Duration::from_secs(7)), 7_000);
    }

    #[test]
    fn automatic_retries_are_limited_to_get_requests() {
        assert!(request_supports_automatic_retry(&Method::GET));
        assert!(!request_supports_automatic_retry(&Method::POST));
        assert!(!request_supports_automatic_retry(&Method::PUT));
    }

    #[tokio::test]
    async fn list_labels_waits_for_retry_after_before_retrying_throttled_requests() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/labels"))
            .respond_with(
                ResponseTemplate::new(429)
                    .append_header("Retry-After", "2")
                    .set_body_string("slow down"),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/labels"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "labels": []
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let result = tokio::time::timeout(Duration::from_millis(800), client.list_labels()).await;

        assert!(result.is_err(), "client retried before Retry-After elapsed");
    }

    #[tokio::test]
    async fn send_draft_does_not_retry_throttled_write_requests() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts/send"))
            .and(body_json(json!({
                "id": "draft-1"
            })))
            .respond_with(
                ResponseTemplate::new(429)
                    .append_header("Retry-After", "1")
                    .set_body_string("slow down"),
            )
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let error = client.send_draft("draft-1").await.unwrap_err();
        assert!(
            error.to_string().contains("users/me/drafts/send"),
            "unexpected error: {error:#}"
        );

        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
    }

    #[test]
    fn extract_email_address_accepts_supported_header_shapes() {
        assert_eq!(
            extract_email_address("\"Alice Example\" <Alice.Example+ops@example.com>"),
            Some(String::from("alice.example+ops@example.com"))
        );
        assert_eq!(
            extract_email_address("\"Alice, Example\" <alice@example.com>"),
            Some(String::from("alice@example.com"))
        );
        assert_eq!(
            extract_email_address("O'Hara <o'hara@example.com>"),
            Some(String::from("o'hara@example.com"))
        );
        assert_eq!(
            extract_email_address("bob@example.com"),
            Some(String::from("bob@example.com"))
        );
        assert_eq!(
            extract_email_address("<alerts@example.com>"),
            Some(String::from("alerts@example.com"))
        );
        assert_eq!(
            extract_email_address("\"o'hara@example.com\""),
            Some(String::from("o'hara@example.com"))
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

    #[tokio::test]
    async fn get_thread_context_parses_and_sorts_thread_messages() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/threads/thread-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "thread-1",
                "historyId": "500",
                "messages": [
                    {
                        "id": "m-2",
                        "threadId": "thread-1",
                        "historyId": "500",
                        "internalDate": "200",
                        "snippet": "Later message",
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Re: Project"},
                                {"name": "From", "value": "Operator <operator@example.com>"},
                                {"name": "To", "value": "alice@example.com"},
                                {"name": "Message-ID", "value": "<m-2@example.com>"},
                                {"name": "References", "value": "<m-1@example.com>"}
                            ]
                        }
                    },
                    {
                        "id": "m-1",
                        "threadId": "thread-1",
                        "historyId": "400",
                        "internalDate": "100",
                        "snippet": "Earlier message",
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Project"},
                                {"name": "From", "value": "\"Alice Example\" <alice@example.com>"},
                                {"name": "To", "value": "operator@example.com"},
                                {"name": "Reply-To", "value": "replies@example.com"},
                                {"name": "Message-ID", "value": "<m-1@example.com>"}
                            ]
                        }
                    }
                ]
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let thread = client.get_thread_context("thread-1").await.unwrap();

        assert_eq!(thread.id, "thread-1");
        assert_eq!(thread.history_id, "500");
        assert_eq!(thread.messages.len(), 2);
        assert_eq!(thread.messages[0].id, "m-1");
        assert_eq!(thread.messages[1].id, "m-2");
        assert_eq!(
            thread.messages[0].from_address.as_deref(),
            Some("alice@example.com")
        );
        assert_eq!(thread.messages[0].reply_to_header, "replies@example.com");
        assert_eq!(thread.messages[1].references_header, "<m-1@example.com>");
    }

    #[tokio::test]
    async fn draft_and_thread_write_operations_return_expected_refs() {
        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts"))
            .and(body_json(json!({
                "message": {
                    "raw": "raw-1",
                    "threadId": "thread-1"
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "draft-1",
                "message": {
                    "id": "draft-message-1",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .and(body_json(json!({
                "message": {
                    "raw": "raw-2",
                    "threadId": "thread-1"
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "draft-1",
                "message": {
                    "id": "draft-message-2",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts/send"))
            .and(body_json(json!({
                "id": "draft-1"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "sent-1",
                "threadId": "thread-1",
                "historyId": "700"
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .and(body_json(json!({
                "addLabelIds": ["Label_1"],
                "removeLabelIds": ["INBOX"]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "thread-1",
                "historyId": "710"
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/trash"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "thread-1",
                "historyId": "720"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let created = client
            .create_draft("raw-1", Some("thread-1"))
            .await
            .unwrap();
        assert_eq!(created.id, "draft-1");
        assert_eq!(created.message_id, "draft-message-1");
        assert_eq!(created.thread_id, "thread-1");

        let updated = client
            .update_draft("draft-1", "raw-2", Some("thread-1"))
            .await
            .unwrap();
        assert_eq!(updated.message_id, "draft-message-2");

        let sent = client.send_draft("draft-1").await.unwrap();
        assert_eq!(sent.message_id, "sent-1");
        assert_eq!(sent.thread_id, "thread-1");
        assert_eq!(sent.history_id.as_deref(), Some("700"));

        client.delete_draft("draft-1").await.unwrap();

        let modified = client
            .modify_thread_labels(
                "thread-1",
                &[String::from("Label_1")],
                &[String::from("INBOX")],
            )
            .await
            .unwrap();
        assert_eq!(modified.thread_id, "thread-1");
        assert_eq!(modified.history_id.as_deref(), Some("710"));

        let trashed = client.trash_thread("thread-1").await.unwrap();
        assert_eq!(trashed.thread_id, "thread-1");
        assert_eq!(trashed.history_id.as_deref(), Some("720"));
    }
}
