use serde::Deserialize;

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
