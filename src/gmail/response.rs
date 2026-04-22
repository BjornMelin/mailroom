use super::client::{GmailClientError, GmailResult};
use super::types::{
    GmailAutomationHeaders, GmailDraftRef, GmailHistoryPage, GmailLabel, GmailMessageAttachment,
    GmailMessageCatalog, GmailMessageListItem, GmailMessageListPage, GmailMessageMetadata,
    GmailSentMessageRef, GmailThreadContext, GmailThreadMessage, GmailThreadMutationRef,
};
use base64::Engine as _;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub(super) struct GmailLabelsResponse {
    labels: Vec<GmailLabel>,
}

impl GmailLabelsResponse {
    pub(super) fn into_labels(self) -> Vec<GmailLabel> {
        self.labels
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct GmailMessagesListResponse {
    messages: Option<Vec<GmailMessageListItem>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "resultSizeEstimate")]
    result_size_estimate: Option<i64>,
}

impl GmailMessagesListResponse {
    pub(super) fn into_message_list_page(self) -> GmailMessageListPage {
        GmailMessageListPage {
            messages: self.messages.unwrap_or_default(),
            next_page_token: self.next_page_token,
            result_size_estimate: self.result_size_estimate,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct GmailMessageMetadataResponse {
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

impl GmailMessageMetadataResponse {
    pub(super) fn payload(&self) -> Option<&GmailMessagePayload> {
        self.payload.as_ref()
    }

    pub(super) fn into_message_catalog(self) -> GmailResult<GmailMessageCatalog> {
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

#[derive(Debug, Deserialize)]
pub(super) struct GmailMessagePayloadResponse {
    pub(super) payload: Option<GmailMessagePayload>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct GmailMessagePayload {
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
pub(super) struct GmailMessagePartBody {
    #[serde(rename = "attachmentId")]
    pub(super) attachment_id: Option<String>,
    pub(super) size: Option<i64>,
    pub(super) data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GmailHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct GmailHistoryResponse {
    history: Option<Vec<GmailHistoryRecord>>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
    #[serde(rename = "historyId")]
    history_id: String,
}

impl GmailHistoryResponse {
    pub(super) fn into_history_page(self) -> GmailHistoryPage {
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

#[derive(Debug, Deserialize)]
pub(super) struct GmailThreadResponse {
    id: String,
    #[serde(rename = "historyId")]
    history_id: String,
    #[serde(default)]
    messages: Vec<GmailThreadMessageResponse>,
}

impl GmailThreadResponse {
    pub(super) fn into_thread_context(self) -> GmailResult<GmailThreadContext> {
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

#[derive(Debug, Deserialize)]
pub(super) struct GmailDraftResponse {
    id: String,
    message: GmailDraftMessageResponse,
}

impl GmailDraftResponse {
    pub(super) fn into_draft_ref(self) -> GmailDraftRef {
        GmailDraftRef {
            id: self.id,
            message_id: self.message.id,
            thread_id: self.message.thread_id,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct GmailDraftMessageResponse {
    id: String,
    #[serde(rename = "threadId")]
    thread_id: String,
    #[serde(rename = "historyId")]
    history_id: Option<String>,
}

impl GmailDraftMessageResponse {
    pub(super) fn into_sent_message_ref(self) -> GmailSentMessageRef {
        GmailSentMessageRef {
            message_id: self.id,
            thread_id: self.thread_id,
            history_id: self.history_id,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct GoogleApiErrorEnvelope {
    pub(super) error: Option<GoogleApiErrorBody>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GoogleApiErrorBody {
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) errors: Vec<GoogleApiErrorDetail>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GoogleApiErrorDetail {
    pub(super) message: Option<String>,
    pub(super) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GmailThreadMutationResponse {
    id: String,
    #[serde(rename = "historyId")]
    history_id: Option<String>,
}

impl GmailThreadMutationResponse {
    pub(super) fn into_mutation_ref(self) -> GmailThreadMutationRef {
        GmailThreadMutationRef {
            thread_id: self.id,
            history_id: self.history_id,
        }
    }
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

pub(super) fn draft_request_body(raw_message: &str, thread_id: Option<&str>) -> serde_json::Value {
    let mut message = json!({
        "raw": raw_message,
    });
    if let Some(thread_id) = thread_id {
        message["threadId"] = serde_json::Value::String(thread_id.to_owned());
    }
    json!({ "message": message })
}

pub(super) fn payload_projection_truncated(payload: &GmailMessagePayload) -> bool {
    payload
        .parts
        .iter()
        .any(|part| projection_depth_marker(part) || payload_projection_truncated(part))
}

pub(super) fn find_part_body<'a>(
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

pub(super) fn decode_base64url(encoded: &str, path: &str) -> GmailResult<Vec<u8>> {
    let trimmed = encoded.trim();
    URL_SAFE_NO_PAD
        .decode(trimmed.as_bytes())
        .or_else(|_| URL_SAFE.decode(trimmed.as_bytes()))
        .map_err(|source| GmailClientError::ResponseDecode {
            path: path.to_owned(),
            source: source.into(),
        })
}

pub(super) fn extract_email_address(value: &str) -> Option<String> {
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
