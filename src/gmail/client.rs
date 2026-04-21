use super::quota::{
    GmailQuotaMetricsSnapshot, GmailQuotaPolicy, GmailRequestCost, GmailRetryClassification,
    MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE,
};
use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::auth::oauth_client::resolve as resolve_oauth_client;
use crate::config::GmailConfig;
use base64::Engine as _;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use oauth2::{AuthUrl, ClientId, ClientSecret, RefreshToken, TokenUrl, basic::BasicClient};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::time::sleep;

const TOKEN_REFRESH_LEEWAY_SECS: u64 = 60;
const GMAIL_MAX_RETRY_ATTEMPTS: usize = 4;
const GMAIL_INITIAL_RETRY_DELAY_MS: u64 = 1_000;
const GMAIL_MAX_RETRY_DELAY_MS: u64 = 32_000;
const GMAIL_RETRY_JITTER_MS: u64 = 250;
const MESSAGE_CATALOG_FULL_FIELDS: &str =
    "id,threadId,labelIds,snippet,historyId,internalDate,sizeEstimate,payload";
const MESSAGE_CATALOG_FIELDS: &str = concat!(
    "id,threadId,labelIds,snippet,historyId,internalDate,sizeEstimate,",
    "payload(",
    "headers(name,value),",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(",
    "partId,mimeType,filename,headers(name,value),body(attachmentId,size),",
    "parts(partId,mimeType,filename,headers(name,value),body(attachmentId,size),parts(partId))",
    ")",
    ")",
    ")",
    ")",
    ")"
);

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

#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq, Eq)]
pub(crate) struct GmailAutomationHeaders {
    pub list_id_header: Option<String>,
    pub list_unsubscribe_header: Option<String>,
    pub list_unsubscribe_post_header: Option<String>,
    pub precedence_header: Option<String>,
    pub auto_submitted_header: Option<String>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize, PartialEq, Eq)]
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
    pub automation_headers: GmailAutomationHeaders,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct GmailMessageCatalog {
    pub metadata: GmailMessageMetadata,
    pub attachments: Vec<GmailMessageAttachment>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct GmailMessageAttachment {
    pub attachment_key: String,
    pub part_id: String,
    pub gmail_attachment_id: Option<String>,
    pub filename: String,
    pub mime_type: String,
    pub size_bytes: i64,
    pub content_disposition: Option<String>,
    pub content_id: Option<String>,
    pub is_inline: bool,
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
    #[error(
        "gmail quota budget must be at least {minimum_units_per_minute} units per minute; got {units_per_minute}"
    )]
    InvalidQuotaBudget {
        units_per_minute: u32,
        minimum_units_per_minute: u32,
    },
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
    #[error("gmail message {message_id} did not include attachment part {part_id}")]
    AttachmentPartMissing { message_id: String, part_id: String },
    #[error("gmail message {message_id} did not include attachment bytes for part {part_id}")]
    AttachmentBodyMissing { message_id: String, part_id: String },
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
struct GmailMessagePayloadResponse {
    payload: Option<GmailMessagePayload>,
}

#[derive(Debug, Default, Deserialize)]
struct GmailMessagePayload {
    #[serde(rename = "partId")]
    part_id: Option<String>,
    #[serde(rename = "mimeType")]
    mime_type: Option<String>,
    filename: Option<String>,
    #[serde(default)]
    headers: Vec<GmailHeader>,
    body: Option<GmailMessagePartBody>,
    #[serde(default)]
    parts: Vec<GmailMessagePayload>,
}

#[derive(Debug, Deserialize)]
struct GmailMessagePartBody {
    #[serde(rename = "attachmentId")]
    attachment_id: Option<String>,
    size: Option<i64>,
    data: Option<String>,
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
struct GoogleApiErrorEnvelope {
    error: Option<GoogleApiErrorBody>,
}

#[derive(Debug, Deserialize)]
struct GoogleApiErrorBody {
    message: Option<String>,
    #[serde(default)]
    errors: Vec<GoogleApiErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct GoogleApiErrorDetail {
    message: Option<String>,
    reason: Option<String>,
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
    request_policy: Option<GmailQuotaPolicy>,
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
            request_policy: None,
        })
    }

    pub(crate) fn with_quota_budget(mut self, units_per_minute: u32) -> GmailResult<Self> {
        self.request_policy = Some(GmailQuotaPolicy::new(units_per_minute).ok_or(
            GmailClientError::InvalidQuotaBudget {
                units_per_minute,
                minimum_units_per_minute: MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE,
            },
        )?);
        Ok(self)
    }

    pub(crate) fn request_metrics_snapshot(&self) -> Option<GmailQuotaMetricsSnapshot> {
        self.request_policy.as_ref().map(GmailQuotaPolicy::snapshot)
    }

    pub(crate) fn update_quota_budget(&self, units_per_minute: u32) -> GmailResult<()> {
        let Some(policy) = &self.request_policy else {
            return Ok(());
        };

        policy
            .reconfigure(units_per_minute)
            .ok_or(GmailClientError::InvalidQuotaBudget {
                units_per_minute,
                minimum_units_per_minute: MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE,
            })?;
        Ok(())
    }

    async fn acquire_request_budget(&self, request_cost: GmailRequestCost) -> GmailResult<()> {
        if let Some(policy) = &self.request_policy {
            policy
                .acquire(request_cost)
                .await
                .map_err(|requested_units| GmailClientError::InvalidQuotaBudget {
                    units_per_minute: policy.units_per_minute(),
                    minimum_units_per_minute: requested_units,
                })?;
        }

        Ok(())
    }

    fn record_http_attempt(&self) {
        if let Some(policy) = &self.request_policy {
            policy.record_http_attempt();
        }
    }

    fn record_retry(&self, classification: GmailRetryClassification) {
        if let Some(policy) = &self.request_policy {
            policy.record_retry(classification);
        }
    }

    fn record_retry_after_wait(&self, waited: Duration) {
        if let Some(policy) = &self.request_policy {
            policy.record_retry_after_wait(waited);
        }
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
                GmailRequestCost::ProfileGet,
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
                        GmailRequestCost::ProfileGet,
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
        let response: GmailLabelsResponse = self
            .get_json("users/me/labels", &query, GmailRequestCost::LabelRead)
            .await?;
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

        let response: GmailMessagesListResponse = self
            .get_json("users/me/messages", &params, GmailRequestCost::MessageList)
            .await?;
        Ok(GmailMessageListPage {
            messages: response.messages.unwrap_or_default(),
            next_page_token: response.next_page_token,
            result_size_estimate: response.result_size_estimate,
        })
    }

    pub(crate) async fn get_message_catalog(
        &self,
        message_id: &str,
    ) -> GmailResult<GmailMessageCatalog> {
        let message_path = format!("users/me/messages/{message_id}");
        let response: GmailMessageMetadataResponse = self
            .get_json(
                &message_path,
                &[
                    ("format", String::from("full")),
                    ("fields", String::from(MESSAGE_CATALOG_FIELDS)),
                ],
                GmailRequestCost::MessageGet,
            )
            .await?;
        if response
            .payload
            .as_ref()
            .is_some_and(payload_projection_truncated)
        {
            let full_response: GmailMessageMetadataResponse = self
                .get_json(
                    &message_path,
                    &[
                        ("format", String::from("full")),
                        ("fields", String::from(MESSAGE_CATALOG_FULL_FIELDS)),
                    ],
                    GmailRequestCost::MessageGet,
                )
                .await?;
            return full_response.into_message_catalog();
        }
        response.into_message_catalog()
    }

    pub(crate) async fn get_message_catalog_if_present(
        &self,
        message_id: &str,
    ) -> GmailResult<Option<GmailMessageCatalog>> {
        match self.get_message_catalog(message_id).await {
            Ok(catalog) => Ok(Some(catalog)),
            Err(error) if matches_missing_message_error(&error, message_id) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn get_attachment_bytes(
        &self,
        message_id: &str,
        part_id: &str,
        gmail_attachment_id: Option<&str>,
    ) -> GmailResult<Vec<u8>> {
        if let Some(gmail_attachment_id) = gmail_attachment_id {
            let response: GmailMessagePartBody = self
                .get_json(
                    &format!("users/me/messages/{message_id}/attachments/{gmail_attachment_id}"),
                    &[],
                    GmailRequestCost::AttachmentGet,
                )
                .await?;
            let encoded = response
                .data
                .ok_or_else(|| GmailClientError::AttachmentBodyMissing {
                    message_id: message_id.to_owned(),
                    part_id: part_id.to_owned(),
                })?;
            return decode_base64url(
                &encoded,
                &format!("gmail.attachment.{message_id}.{part_id}.data"),
            );
        }

        let response: GmailMessagePayloadResponse = self
            .get_json(
                &format!("users/me/messages/{message_id}"),
                &[
                    ("format", String::from("full")),
                    ("fields", String::from("id,payload")),
                ],
                GmailRequestCost::MessageGet,
            )
            .await?;
        let payload = response
            .payload
            .ok_or_else(|| GmailClientError::AttachmentPartMissing {
                message_id: message_id.to_owned(),
                part_id: part_id.to_owned(),
            })?;
        let body = find_part_body(&payload, part_id).ok_or_else(|| {
            GmailClientError::AttachmentPartMissing {
                message_id: message_id.to_owned(),
                part_id: part_id.to_owned(),
            }
        })?;
        let encoded =
            body.data
                .as_deref()
                .ok_or_else(|| GmailClientError::AttachmentBodyMissing {
                    message_id: message_id.to_owned(),
                    part_id: part_id.to_owned(),
                })?;
        decode_base64url(
            encoded,
            &format!("gmail.inline_attachment.{message_id}.{part_id}.data"),
        )
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

        let response: GmailHistoryResponse = self
            .get_json("users/me/history", &params, GmailRequestCost::HistoryList)
            .await?;
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
                GmailRequestCost::ThreadGet,
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
        let response: GmailDraftResponse = self
            .post_json("users/me/drafts", &[], body, GmailRequestCost::DraftWrite)
            .await?;
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
            .put_json(
                &format!("users/me/drafts/{draft_id}"),
                &[],
                body,
                GmailRequestCost::DraftWrite,
            )
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
                GmailRequestCost::DraftWrite,
            )
            .await?;
        Ok(GmailSentMessageRef {
            message_id: response.id,
            thread_id: response.thread_id,
            history_id: response.history_id,
        })
    }

    pub(crate) async fn delete_draft(&self, draft_id: &str) -> GmailResult<()> {
        self.execute_empty(
            Method::DELETE,
            &format!("users/me/drafts/{draft_id}"),
            &[],
            GmailRequestCost::DraftDelete,
        )
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
                GmailRequestCost::ThreadModify,
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
                GmailRequestCost::ThreadModify,
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

    async fn get_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::GET, path, query, None, request_cost)
            .await
    }

    async fn post_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: serde_json::Value,
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::POST, path, query, Some(body), request_cost)
            .await
    }

    async fn put_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: serde_json::Value,
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::PUT, path, query, Some(body), request_cost)
            .await
    }

    async fn execute_json<T>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<serde_json::Value>,
        request_cost: GmailRequestCost,
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
                request_cost,
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
                    request_cost,
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
        request_cost: GmailRequestCost,
    ) -> GmailResult<()> {
        let credentials = self.active_credentials().await?;
        match self
            .request_empty(
                method.clone(),
                path,
                query,
                credentials.access_token.expose_secret(),
                request_cost,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                self.request_empty(
                    method,
                    path,
                    query,
                    refreshed.access_token.expose_secret(),
                    request_cost,
                )
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
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self
            .send_request(method, path, query, access_token, body, request_cost)
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
        request_cost: GmailRequestCost,
    ) -> GmailResult<()> {
        self.send_request(method, path, query, access_token, None, request_cost)
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
        request_cost: GmailRequestCost,
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
            self.acquire_request_budget(request_cost).await?;
            self.record_http_attempt();
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
                    let retry_after = retry_after_delay(response.headers());
                    let body = response.text().await.unwrap_or_default();
                    let retry_classification = retryable_request
                        .then(|| classify_retryable_api_response(status, &body))
                        .flatten();
                    if let Some(retry_classification) = retry_classification
                        && attempt < GMAIL_MAX_RETRY_ATTEMPTS
                    {
                        let retry_delay = retry_after
                            .unwrap_or_else(|| jittered_retry_delay(retry_delay_ms, attempt));
                        self.record_retry(retry_classification);
                        if retry_after.is_some() {
                            self.record_retry_after_wait(retry_delay);
                        }
                        sleep(retry_delay).await;
                        retry_delay_ms = next_retry_delay_ms(retry_delay_ms);
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
                        self.record_retry(GmailRetryClassification::Backend);
                        sleep(retry_delay_duration(
                            &HeaderMap::new(),
                            retry_delay_ms,
                            attempt,
                        ))
                        .await;
                        retry_delay_ms = next_retry_delay_ms(retry_delay_ms);
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
    fn into_message_catalog(self) -> GmailResult<GmailMessageCatalog> {
        let GmailMessageMetadataResponse {
            id,
            thread_id,
            label_ids,
            snippet,
            history_id,
            internal_date,
            size_estimate,
            payload,
        } = self;
        let payload = payload.unwrap_or_default();
        let metadata = message_metadata_from_payload(
            GmailMessageMetadataFields {
                id: id.clone(),
                thread_id,
                label_ids,
                snippet,
                history_id,
                internal_date,
                size_estimate,
            },
            &payload,
        )?;
        let mut attachments = Vec::new();
        collect_message_attachments(&id, &payload, &mut attachments);
        Ok(GmailMessageCatalog {
            metadata,
            attachments,
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

fn classify_retryable_api_response(
    status: StatusCode,
    body: &str,
) -> Option<GmailRetryClassification> {
    if is_retryable_status(status) {
        return Some(if matches_concurrent_request_limit(body) {
            GmailRetryClassification::ConcurrencyPressure
        } else if status == StatusCode::TOO_MANY_REQUESTS {
            GmailRetryClassification::QuotaPressure
        } else {
            GmailRetryClassification::Backend
        });
    }

    (status == StatusCode::FORBIDDEN)
        .then(|| classify_forbidden_retry(body))
        .flatten()
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

fn classify_forbidden_retry(body: &str) -> Option<GmailRetryClassification> {
    serde_json::from_str::<GoogleApiErrorEnvelope>(body)
        .ok()
        .and_then(|payload| payload.error)
        .and_then(|error| {
            error
                .errors
                .into_iter()
                .find_map(|detail| match detail.reason.as_deref() {
                    Some("rateLimitExceeded" | "userRateLimitExceeded") => {
                        Some(GmailRetryClassification::QuotaPressure)
                    }
                    _ => None,
                })
        })
}

fn matches_concurrent_request_limit(body: &str) -> bool {
    if let Ok(payload) = serde_json::from_str::<GoogleApiErrorEnvelope>(body)
        && let Some(error) = payload.error
    {
        if error
            .message
            .as_deref()
            .is_some_and(is_concurrent_request_limit_message)
        {
            return true;
        }

        return error.errors.into_iter().any(|detail| {
            detail
                .message
                .as_deref()
                .is_some_and(is_concurrent_request_limit_message)
        });
    }

    is_concurrent_request_limit_message(body)
}

fn is_concurrent_request_limit_message(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("too many concurrent requests for user")
}

fn retry_delay_duration(headers: &HeaderMap, default_delay_ms: u64, attempt: usize) -> Duration {
    retry_after_delay(headers).unwrap_or_else(|| jittered_retry_delay(default_delay_ms, attempt))
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

fn next_retry_delay_ms(current_delay_ms: u64) -> u64 {
    current_delay_ms
        .saturating_mul(2)
        .clamp(GMAIL_INITIAL_RETRY_DELAY_MS, GMAIL_MAX_RETRY_DELAY_MS)
}

fn jittered_retry_delay(default_delay_ms: u64, attempt: usize) -> Duration {
    let jitter_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_millis() as u64)
        .unwrap_or((attempt as u64).saturating_mul(17))
        % GMAIL_RETRY_JITTER_MS.max(1);
    Duration::from_millis(default_delay_ms.saturating_add(jitter_ms))
}

fn header_value(headers: &[GmailHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .map(|header| header.value.trim().to_owned())
}

struct GmailMessageMetadataFields {
    id: String,
    thread_id: String,
    label_ids: Vec<String>,
    snippet: Option<String>,
    history_id: String,
    internal_date: String,
    size_estimate: i64,
}

fn message_metadata_from_payload(
    fields: GmailMessageMetadataFields,
    payload: &GmailMessagePayload,
) -> GmailResult<GmailMessageMetadata> {
    Ok(GmailMessageMetadata {
        id: fields.id,
        thread_id: fields.thread_id,
        label_ids: fields.label_ids,
        snippet: fields.snippet.unwrap_or_default(),
        history_id: fields.history_id,
        internal_date_epoch_ms: fields.internal_date.parse::<i64>().map_err(|source| {
            GmailClientError::ResponseDecode {
                path: String::from("gmail.message.internal_date"),
                source: source.into(),
            }
        })?,
        size_estimate: fields.size_estimate,
        subject: header_value(&payload.headers, "Subject").unwrap_or_default(),
        from_header: header_value(&payload.headers, "From").unwrap_or_default(),
        from_address: header_value(&payload.headers, "From")
            .and_then(|value| extract_email_address(&value)),
        to_header: header_value(&payload.headers, "To").unwrap_or_default(),
        cc_header: header_value(&payload.headers, "Cc").unwrap_or_default(),
        bcc_header: header_value(&payload.headers, "Bcc").unwrap_or_default(),
        reply_to_header: header_value(&payload.headers, "Reply-To").unwrap_or_default(),
        automation_headers: GmailAutomationHeaders {
            list_id_header: header_value(&payload.headers, "List-Id"),
            list_unsubscribe_header: header_value(&payload.headers, "List-Unsubscribe"),
            list_unsubscribe_post_header: header_value(&payload.headers, "List-Unsubscribe-Post"),
            precedence_header: header_value(&payload.headers, "Precedence"),
            auto_submitted_header: header_value(&payload.headers, "Auto-Submitted"),
        },
    })
}

fn collect_message_attachments(
    message_id: &str,
    payload: &GmailMessagePayload,
    attachments: &mut Vec<GmailMessageAttachment>,
) {
    if let Some(attachment) = attachment_from_part(message_id, payload) {
        attachments.push(attachment);
    }
    for part in &payload.parts {
        collect_message_attachments(message_id, part, attachments);
    }
}

fn payload_projection_truncated(payload: &GmailMessagePayload) -> bool {
    payload
        .parts
        .iter()
        .any(|part| projection_depth_marker(part) || payload_projection_truncated(part))
}

fn projection_depth_marker(part: &GmailMessagePayload) -> bool {
    part.part_id
        .as_deref()
        .is_some_and(|part_id| !part_id.trim().is_empty())
        && part.mime_type.is_none()
        && part.filename.is_none()
        && part.headers.is_empty()
        && part.body.is_none()
        && part.parts.is_empty()
}

fn attachment_from_part(
    message_id: &str,
    part: &GmailMessagePayload,
) -> Option<GmailMessageAttachment> {
    let part_id = part.part_id.as_deref()?.trim();
    if part_id.is_empty() {
        return None;
    }

    let filename = part.filename.as_deref().unwrap_or_default().trim();
    let content_disposition = header_value(&part.headers, "Content-Disposition");
    let content_id = header_value(&part.headers, "Content-Id")
        .or_else(|| header_value(&part.headers, "Content-ID"));
    let is_inline = content_disposition.as_deref().is_some_and(|value| {
        value
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("inline")
    }) || content_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let gmail_attachment_id = part
        .body
        .as_ref()
        .and_then(|body| body.attachment_id.clone())
        .filter(|value| !value.trim().is_empty());
    if filename.is_empty() && gmail_attachment_id.is_none() && !is_inline {
        return None;
    }

    Some(GmailMessageAttachment {
        attachment_key: attachment_key(message_id, part_id),
        part_id: part_id.to_owned(),
        gmail_attachment_id,
        filename: filename.to_owned(),
        mime_type: part
            .mime_type
            .clone()
            .unwrap_or_else(|| String::from("application/octet-stream")),
        size_bytes: part.body.as_ref().and_then(|body| body.size).unwrap_or(0),
        content_disposition,
        content_id,
        is_inline,
    })
}

fn attachment_key(message_id: &str, part_id: &str) -> String {
    format!("{message_id}:{part_id}")
}

fn find_part_body<'a>(
    payload: &'a GmailMessagePayload,
    part_id: &str,
) -> Option<&'a GmailMessagePartBody> {
    if payload.part_id.as_deref() == Some(part_id) {
        return payload.body.as_ref();
    }

    payload
        .parts
        .iter()
        .find_map(|part| find_part_body(part, part_id))
}

fn decode_base64url(encoded: &str, path: &str) -> GmailResult<Vec<u8>> {
    let trimmed = encoded.trim();
    URL_SAFE_NO_PAD
        .decode(trimmed.as_bytes())
        .or_else(|_| URL_SAFE.decode(trimmed.as_bytes()))
        .map_err(|source| GmailClientError::ResponseDecode {
            path: path.to_owned(),
            source: source.into(),
        })
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
        GmailClient, GmailClientError, GmailMessagePayload, GmailProfile, MESSAGE_CATALOG_FIELDS,
        MESSAGE_CATALOG_FULL_FIELDS, classify_retryable_api_response, extract_email_address,
        next_retry_delay_ms, payload_projection_truncated, request_supports_automatic_retry,
        retry_after_delay,
    };
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::{GmailConfig, WorkspaceConfig};
    use crate::gmail::quota::GmailRetryClassification;
    use reqwest::header::HeaderMap;
    use reqwest::{Method, StatusCode};
    use secrecy::{ExposeSecret, SecretString};
    use serde_json::json;
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;
    use wiremock::matchers::{body_json, method, path, query_param};
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
    fn next_retry_delay_ms_doubles_and_caps_backoff() {
        assert_eq!(next_retry_delay_ms(1_000), 2_000);
        assert_eq!(next_retry_delay_ms(16_000), 32_000);
        assert_eq!(next_retry_delay_ms(32_000), 32_000);
    }

    #[test]
    fn automatic_retries_are_limited_to_get_requests() {
        assert!(request_supports_automatic_retry(&Method::GET));
        assert!(!request_supports_automatic_retry(&Method::POST));
        assert!(!request_supports_automatic_retry(&Method::PUT));
    }

    #[test]
    fn gmail_usage_limit_forbidden_errors_are_classified_as_quota_pressure() {
        let body = serde_json::to_string(&json!({
            "error": {
                "errors": [
                    {
                        "domain": "usageLimits",
                        "reason": "userRateLimitExceeded",
                        "message": "quota exhausted"
                    }
                ]
            }
        }))
        .unwrap();

        assert_eq!(
            classify_retryable_api_response(StatusCode::FORBIDDEN, &body),
            Some(GmailRetryClassification::QuotaPressure)
        );
    }

    #[test]
    fn gmail_concurrent_request_429_errors_are_classified_as_concurrency_pressure() {
        let body = serde_json::to_string(&json!({
            "error": {
                "message": "Too many concurrent requests for user",
                "errors": [
                    {
                        "domain": "usageLimits",
                        "reason": "rateLimitExceeded",
                        "message": "Too many concurrent requests for user"
                    }
                ]
            }
        }))
        .unwrap();

        assert_eq!(
            classify_retryable_api_response(StatusCode::TOO_MANY_REQUESTS, &body),
            Some(GmailRetryClassification::ConcurrencyPressure)
        );
    }

    #[test]
    fn gmail_backend_5xx_errors_are_classified_as_backend_pressure() {
        assert_eq!(
            classify_retryable_api_response(StatusCode::BAD_GATEWAY, "upstream failure"),
            Some(GmailRetryClassification::Backend)
        );
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
    async fn list_labels_retries_gmail_user_rate_limit_forbidden_errors() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/labels"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {
                    "errors": [
                        {
                            "domain": "usageLimits",
                            "reason": "userRateLimitExceeded",
                            "message": "quota exhausted"
                        }
                    ]
                }
            })))
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
        let client = test_client(&mock_server, &temp_dir)
            .with_quota_budget(12_000)
            .unwrap();

        let labels = client.list_labels().await.unwrap();
        assert!(labels.is_empty());

        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
    }

    #[tokio::test]
    async fn list_labels_does_not_retry_non_quota_forbidden_errors() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/labels"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": {
                    "errors": [
                        {
                            "domain": "global",
                            "reason": "insufficientPermissions",
                            "message": "forbidden"
                        }
                    ]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let error = client.list_labels().await.unwrap_err();
        assert!(matches!(
            error,
            GmailClientError::Api { status, .. } if status == StatusCode::FORBIDDEN
        ));

        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
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
    async fn get_message_catalog_collects_nested_attachments() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-1"))
            .and(query_param("format", "full"))
            .and(query_param("fields", MESSAGE_CATALOG_FIELDS))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "m-1",
                "threadId": "thread-1",
                "labelIds": ["INBOX"],
                "snippet": "Quarterly statement attached",
                "historyId": "500",
                "internalDate": "1700000000000",
                "sizeEstimate": 123,
                "payload": {
                    "headers": [
                        {"name": "Subject", "value": "Statement"},
                        {"name": "From", "value": "Billing <billing@example.com>"},
                        {"name": "To", "value": "operator@example.com"}
                    ],
                    "parts": [
                        {
                            "partId": "1.1",
                            "mimeType": "application/pdf",
                            "filename": "statement.pdf",
                            "headers": [
                                {"name": "Content-Disposition", "value": "attachment; filename=\"statement.pdf\""}
                            ],
                            "body": {
                                "attachmentId": "att-1",
                                "size": 42
                            }
                        },
                        {
                            "partId": "1.2",
                            "mimeType": "image/png",
                            "filename": "inline.png",
                            "headers": [
                                {"name": "Content-Id", "value": "<image-1>"}
                            ],
                            "body": {
                                "size": 5
                            }
                        }
                    ]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let catalog = client.get_message_catalog("m-1").await.unwrap();

        assert_eq!(catalog.metadata.subject, "Statement");
        assert_eq!(catalog.attachments.len(), 2);
        assert_eq!(catalog.attachments[0].attachment_key, "m-1:1.1");
        assert_eq!(
            catalog.attachments[0].gmail_attachment_id.as_deref(),
            Some("att-1")
        );
        assert!(catalog.attachments[1].is_inline);
    }

    #[tokio::test]
    async fn get_message_catalog_collects_cid_only_inline_parts() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-inline-cid"))
            .and(query_param("format", "full"))
            .and(query_param("fields", MESSAGE_CATALOG_FIELDS))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "m-inline-cid",
                "threadId": "thread-inline",
                "labelIds": ["INBOX"],
                "snippet": "Inline image",
                "historyId": "501",
                "internalDate": "1700000000000",
                "sizeEstimate": 5,
                "payload": {
                    "headers": [
                        {"name": "Subject", "value": "Inline image"},
                        {"name": "From", "value": "Billing <billing@example.com>"},
                        {"name": "To", "value": "operator@example.com"}
                    ],
                    "parts": [
                        {
                            "partId": "1.2",
                            "mimeType": "image/png",
                            "filename": "",
                            "headers": [
                                {"name": "Content-Id", "value": "<image-1>"}
                            ],
                            "body": {
                                "data": "aGVsbG8",
                                "size": 5
                            }
                        }
                    ]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let catalog = client.get_message_catalog("m-inline-cid").await.unwrap();

        assert_eq!(catalog.attachments.len(), 1);
        assert_eq!(catalog.attachments[0].attachment_key, "m-inline-cid:1.2");
        assert!(catalog.attachments[0].is_inline);
        assert_eq!(
            catalog.attachments[0].content_id.as_deref(),
            Some("<image-1>")
        );
    }

    #[tokio::test]
    async fn get_message_catalog_refetches_full_payload_when_projection_hits_depth_marker() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-deep"))
            .and(query_param("format", "full"))
            .and(query_param("fields", MESSAGE_CATALOG_FIELDS))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "m-deep",
                "threadId": "thread-deep",
                "labelIds": ["INBOX"],
                "snippet": "deep part marker",
                "historyId": "700",
                "internalDate": "1700000000123",
                "sizeEstimate": 222,
                "payload": {
                    "headers": [
                        {"name": "Subject", "value": "Deep"},
                        {"name": "From", "value": "Nested <nested@example.com>"},
                        {"name": "To", "value": "operator@example.com"}
                    ],
                    "parts": [{"partId": "1"}]
                }
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-deep"))
            .and(query_param("format", "full"))
            .and(query_param("fields", MESSAGE_CATALOG_FULL_FIELDS))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "m-deep",
                "threadId": "thread-deep",
                "labelIds": ["INBOX"],
                "snippet": "deep attachment",
                "historyId": "700",
                "internalDate": "1700000000123",
                "sizeEstimate": 333,
                "payload": {
                    "headers": [
                        {"name": "Subject", "value": "Deep"},
                        {"name": "From", "value": "Nested <nested@example.com>"},
                        {"name": "To", "value": "operator@example.com"}
                    ],
                    "parts": [{
                        "partId": "1.1.1.1.1.1.1",
                        "mimeType": "application/pdf",
                        "filename": "deep.pdf",
                        "headers": [
                            {"name": "Content-Disposition", "value": "attachment; filename=\"deep.pdf\""}
                        ],
                        "body": {
                            "attachmentId": "att-deep",
                            "size": 99
                        }
                    }]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let catalog = client.get_message_catalog("m-deep").await.unwrap();

        assert_eq!(catalog.attachments.len(), 1);
        assert_eq!(
            catalog.attachments[0].attachment_key,
            "m-deep:1.1.1.1.1.1.1"
        );
        assert_eq!(
            catalog.attachments[0].gmail_attachment_id.as_deref(),
            Some("att-deep")
        );
    }

    #[test]
    fn message_catalog_fields_selector_is_parenthesis_balanced() {
        let mut depth = 0_i32;
        for character in MESSAGE_CATALOG_FIELDS.chars() {
            match character {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    assert!(
                        depth >= 0,
                        "MESSAGE_CATALOG_FIELDS has an unmatched closing parenthesis"
                    );
                }
                _ => {}
            }
        }

        assert_eq!(
            depth, 0,
            "MESSAGE_CATALOG_FIELDS must have balanced parentheses"
        );
    }

    #[test]
    fn payload_projection_truncated_detects_nested_depth_markers() {
        let payload: GmailMessagePayload = serde_json::from_value(json!({
            "partId": "0",
            "mimeType": "multipart/mixed",
            "parts": [
                {
                    "partId": "1",
                    "mimeType": "multipart/alternative",
                    "parts": [
                        { "partId": "1.1" }
                    ]
                }
            ]
        }))
        .unwrap();

        assert!(payload_projection_truncated(&payload));
    }

    #[tokio::test]
    async fn get_attachment_bytes_supports_remote_attachment_and_inline_part_bodies() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/gmail/v1/users/me/messages/m-remote/attachments/att-1",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": "cGRmLWJ5dGVz"
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/gmail/v1/users/me/messages/m-remote-padded/attachments/att-2",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": "cGRmLWJ5dGVzPQ=="
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-inline"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "m-inline",
                "threadId": "thread-inline",
                "labelIds": [],
                "historyId": "501",
                "internalDate": "1700000000000",
                "sizeEstimate": 5,
                "payload": {
                    "parts": [
                        {
                            "partId": "1.2",
                            "body": {
                                "data": "aGVsbG8"
                            }
                        }
                    ]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let remote = client
            .get_attachment_bytes("m-remote", "1.1", Some("att-1"))
            .await
            .unwrap();
        let remote_padded = client
            .get_attachment_bytes("m-remote-padded", "1.1", Some("att-2"))
            .await
            .unwrap();
        let inline = client
            .get_attachment_bytes("m-inline", "1.2", None)
            .await
            .unwrap();

        assert_eq!(remote, b"pdf-bytes");
        assert_eq!(remote_padded, b"pdf-bytes=");
        assert_eq!(inline, b"hello");
    }

    #[tokio::test]
    async fn get_attachment_bytes_returns_attachment_part_missing_for_unknown_inline_part() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-inline-missing"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "payload": {
                    "parts": [
                        {
                            "partId": "1.1",
                            "body": {
                                "data": "aGVsbG8"
                            }
                        }
                    ]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let error = client
            .get_attachment_bytes("m-inline-missing", "9.9", None)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            GmailClientError::AttachmentPartMissing { message_id, part_id }
            if message_id == "m-inline-missing" && part_id == "9.9"
        ));
    }

    #[tokio::test]
    async fn get_attachment_bytes_returns_attachment_body_missing_for_empty_inline_part() {
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/messages/m-inline-empty-body"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "payload": {
                    "parts": [
                        {
                            "partId": "1.2",
                            "body": {}
                        }
                    ]
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let client = test_client(&mock_server, &temp_dir);

        let error = client
            .get_attachment_bytes("m-inline-empty-body", "1.2", None)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            GmailClientError::AttachmentBodyMissing { message_id, part_id }
            if message_id == "m-inline-empty-body" && part_id == "1.2"
        ));
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
