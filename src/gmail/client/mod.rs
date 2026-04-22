use super::constants::{MESSAGE_CATALOG_FIELDS, MESSAGE_CATALOG_FULL_FIELDS};
use super::http::{
    build_gmail_http_client, fetch_profile_with_access_token, matches_missing_message_error,
};
use super::quota::{
    GmailQuotaMetricsSnapshot, GmailQuotaPolicy, GmailRequestCost,
    MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE,
};
use super::response::{
    GmailDraftMessageResponse, GmailDraftResponse, GmailHistoryResponse, GmailLabelsResponse,
    GmailMessageMetadataResponse, GmailMessagePartBody, GmailMessagePayloadResponse,
    GmailMessagesListResponse, GmailThreadMutationResponse, GmailThreadResponse, decode_base64url,
    draft_request_body, find_part_body, payload_projection_truncated,
};
use super::types::{
    GmailDraftRef, GmailHistoryPage, GmailLabel, GmailMessageCatalog, GmailMessageListPage,
    GmailProfile, GmailSentMessageRef, GmailThreadContext, GmailThreadMutationRef,
};
use crate::auth::file_store::FileCredentialStore;
use crate::config::{GmailConfig, WorkspaceConfig};
use reqwest::{Method, StatusCode};
use secrecy::ExposeSecret;
use serde_json::json;
use thiserror::Error;

pub(super) type GmailResult<T> = std::result::Result<T, GmailClientError>;

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

#[derive(Debug, Clone)]
pub(crate) struct GmailClient {
    pub(super) config: GmailConfig,
    pub(super) workspace: WorkspaceConfig,
    pub(super) http: reqwest::Client,
    pub(super) credential_store: FileCredentialStore,
    pub(super) request_policy: Option<GmailQuotaPolicy>,
}

impl GmailClient {
    pub(crate) fn new(
        config: GmailConfig,
        workspace: WorkspaceConfig,
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

    pub(crate) async fn get_profile_with_access_scope(
        &self,
    ) -> GmailResult<(GmailProfile, String)> {
        let credentials = self.active_credentials().await?;
        let access_scope = credentials.scopes.join(" ");
        match self
            .request_json::<GmailProfile>(
                Method::GET,
                "users/me/profile",
                &[(
                    "fields",
                    String::from("emailAddress,messagesTotal,threadsTotal,historyId"),
                )],
                credentials.access_token.expose_secret(),
                None,
                GmailRequestCost::ProfileGet,
            )
            .await
        {
            Ok(profile) => Ok((profile, access_scope)),
            Err(error) if super::http::matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                let access_scope = refreshed.scopes.join(" ");
                let profile = self
                    .request_json::<GmailProfile>(
                        Method::GET,
                        "users/me/profile",
                        &[(
                            "fields",
                            String::from("emailAddress,messagesTotal,threadsTotal,historyId"),
                        )],
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
        let response: GmailLabelsResponse = self
            .get_json(
                "users/me/labels",
                &[(
                    "fields",
                    String::from(
                        "labels(id,name,type,messageListVisibility,labelListVisibility,messagesTotal,messagesUnread,threadsTotal,threadsUnread)",
                    ),
                )],
                GmailRequestCost::LabelRead,
            )
            .await?;
        Ok(response.into_labels())
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
        Ok(response.into_message_list_page())
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
        if response.payload().is_some_and(payload_projection_truncated) {
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
        let response: GmailDraftResponse = self
            .post_json(
                "users/me/drafts",
                &[],
                draft_request_body(raw_message, thread_id),
                GmailRequestCost::DraftWrite,
            )
            .await?;
        Ok(response.into_draft_ref())
    }

    pub(crate) async fn update_draft(
        &self,
        draft_id: &str,
        raw_message: &str,
        thread_id: Option<&str>,
    ) -> GmailResult<GmailDraftRef> {
        let response: GmailDraftResponse = self
            .put_json(
                &format!("users/me/drafts/{draft_id}"),
                &[],
                draft_request_body(raw_message, thread_id),
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
        Ok(response.into_sent_message_ref())
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
        fetch_profile_with_access_token(config, access_token).await
    }
}

#[cfg(test)]
mod tests;
