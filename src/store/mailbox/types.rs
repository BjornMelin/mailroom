use crate::store::connection::DatabaseOpenError;
use serde::Serialize;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct GmailAttachmentUpsertInput {
    pub(crate) attachment_key: String,
    pub(crate) part_id: String,
    pub(crate) gmail_attachment_id: Option<String>,
    pub(crate) filename: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_id: Option<String>,
    pub(crate) is_inline: bool,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub(crate) struct GmailAutomationHeaders {
    pub(crate) list_id_header: Option<String>,
    pub(crate) list_unsubscribe_header: Option<String>,
    pub(crate) list_unsubscribe_post_header: Option<String>,
    pub(crate) precedence_header: Option<String>,
    pub(crate) auto_submitted_header: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct GmailMessageUpsertInput {
    pub(crate) account_id: String,
    pub(crate) message_id: String,
    pub(crate) thread_id: String,
    pub(crate) history_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) snippet: String,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) from_address: Option<String>,
    pub(crate) recipient_headers: String,
    pub(crate) to_header: String,
    pub(crate) cc_header: String,
    pub(crate) bcc_header: String,
    pub(crate) reply_to_header: String,
    pub(crate) size_estimate: i64,
    pub(crate) automation_headers: GmailAutomationHeaders,
    pub(crate) label_ids: Vec<String>,
    pub(crate) label_names_text: String,
    pub(crate) attachments: Vec<GmailAttachmentUpsertInput>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SyncStateUpdate {
    pub(crate) account_id: String,
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) bootstrap_query: String,
    pub(crate) last_sync_mode: SyncMode,
    pub(crate) last_sync_status: SyncStatus,
    pub(crate) last_error: Option<String>,
    pub(crate) last_sync_epoch_s: i64,
    pub(crate) last_full_sync_success_epoch_s: Option<i64>,
    pub(crate) last_incremental_sync_success_epoch_s: Option<i64>,
    pub(crate) pipeline_enabled: bool,
    pub(crate) pipeline_list_queue_high_water: i64,
    pub(crate) pipeline_write_queue_high_water: i64,
    pub(crate) pipeline_write_batch_count: i64,
    pub(crate) pipeline_writer_wait_ms: i64,
    pub(crate) pipeline_fetch_batch_count: i64,
    pub(crate) pipeline_fetch_batch_avg_ms: i64,
    pub(crate) pipeline_fetch_batch_max_ms: i64,
    pub(crate) pipeline_writer_tx_count: i64,
    pub(crate) pipeline_writer_tx_avg_ms: i64,
    pub(crate) pipeline_writer_tx_max_ms: i64,
    pub(crate) pipeline_reorder_buffer_high_water: i64,
    pub(crate) pipeline_staged_message_count: i64,
    pub(crate) pipeline_staged_delete_count: i64,
    pub(crate) pipeline_staged_attachment_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SyncStateRecord {
    pub(crate) account_id: String,
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) bootstrap_query: String,
    pub(crate) last_sync_mode: SyncMode,
    pub(crate) last_sync_status: SyncStatus,
    pub(crate) last_error: Option<String>,
    pub(crate) last_sync_epoch_s: i64,
    pub(crate) last_full_sync_success_epoch_s: Option<i64>,
    pub(crate) last_incremental_sync_success_epoch_s: Option<i64>,
    pub(crate) pipeline_enabled: bool,
    pub(crate) pipeline_list_queue_high_water: i64,
    pub(crate) pipeline_write_queue_high_water: i64,
    pub(crate) pipeline_write_batch_count: i64,
    pub(crate) pipeline_writer_wait_ms: i64,
    pub(crate) pipeline_fetch_batch_count: i64,
    pub(crate) pipeline_fetch_batch_avg_ms: i64,
    pub(crate) pipeline_fetch_batch_max_ms: i64,
    pub(crate) pipeline_writer_tx_count: i64,
    pub(crate) pipeline_writer_tx_avg_ms: i64,
    pub(crate) pipeline_writer_tx_max_ms: i64,
    pub(crate) pipeline_reorder_buffer_high_water: i64,
    pub(crate) pipeline_staged_message_count: i64,
    pub(crate) pipeline_staged_delete_count: i64,
    pub(crate) pipeline_staged_attachment_count: i64,
    pub(crate) message_count: i64,
    pub(crate) label_count: i64,
    pub(crate) indexed_message_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct FullSyncCheckpointRecord {
    pub(crate) account_id: String,
    pub(crate) bootstrap_query: String,
    pub(crate) status: FullSyncCheckpointStatus,
    pub(crate) next_page_token: Option<String>,
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) pages_fetched: i64,
    pub(crate) messages_listed: i64,
    pub(crate) messages_upserted: i64,
    pub(crate) labels_synced: i64,
    pub(crate) staged_label_count: i64,
    pub(crate) staged_message_count: i64,
    pub(crate) staged_message_label_count: i64,
    pub(crate) staged_attachment_count: i64,
    pub(crate) started_at_epoch_s: i64,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct FullSyncCheckpointUpdate {
    pub(crate) bootstrap_query: String,
    pub(crate) status: FullSyncCheckpointStatus,
    pub(crate) next_page_token: Option<String>,
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) pages_fetched: i64,
    pub(crate) messages_listed: i64,
    pub(crate) messages_upserted: i64,
    pub(crate) labels_synced: i64,
    pub(crate) started_at_epoch_s: i64,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct FullSyncStagePageInput {
    pub(crate) page_seq: i64,
    pub(crate) listed_count: i64,
    pub(crate) next_page_token: Option<String>,
    pub(crate) updated_at_epoch_s: i64,
    pub(crate) page_complete: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SyncPacingStateUpdate {
    pub(crate) account_id: String,
    pub(crate) learned_quota_units_per_minute: i64,
    pub(crate) learned_message_fetch_concurrency: i64,
    pub(crate) clean_run_streak: i64,
    pub(crate) last_pressure_kind: Option<SyncPacingPressureKind>,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SyncPacingStateRecord {
    pub(crate) account_id: String,
    pub(crate) learned_quota_units_per_minute: i64,
    pub(crate) learned_message_fetch_concurrency: i64,
    pub(crate) clean_run_streak: i64,
    pub(crate) last_pressure_kind: Option<SyncPacingPressureKind>,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SearchQuery {
    pub(crate) account_id: String,
    pub(crate) terms: String,
    pub(crate) label: Option<String>,
    pub(crate) from_address: Option<String>,
    pub(crate) after_epoch_ms: Option<i64>,
    pub(crate) before_epoch_ms: Option<i64>,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct SearchResult {
    pub(crate) message_id: String,
    pub(crate) thread_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) from_address: Option<String>,
    pub(crate) recipient_headers: String,
    pub(crate) snippet: String,
    pub(crate) label_names: Vec<String>,
    pub(crate) thread_message_count: i64,
    pub(crate) rank: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AttachmentListQuery {
    pub(crate) account_id: String,
    pub(crate) thread_id: Option<String>,
    pub(crate) message_id: Option<String>,
    pub(crate) filename: Option<String>,
    pub(crate) mime_type: Option<String>,
    pub(crate) fetched_only: bool,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct LabelUsageRecord {
    pub(crate) label_id: String,
    pub(crate) name: String,
    pub(crate) label_type: String,
    pub(crate) messages_total: Option<i64>,
    pub(crate) threads_total: Option<i64>,
    pub(crate) local_message_count: i64,
    pub(crate) local_thread_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MailboxCoverageReport {
    pub(crate) account_id: String,
    pub(crate) message_count: i64,
    pub(crate) thread_count: i64,
    pub(crate) messages_with_attachments: i64,
    pub(crate) messages_with_list_unsubscribe: i64,
    pub(crate) messages_with_list_id: i64,
    pub(crate) messages_with_precedence: i64,
    pub(crate) messages_with_auto_submitted: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AttachmentListItem {
    pub(crate) attachment_key: String,
    pub(crate) message_id: String,
    pub(crate) thread_id: String,
    pub(crate) part_id: String,
    pub(crate) filename: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_id: Option<String>,
    pub(crate) is_inline: bool,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) vault_content_hash: Option<String>,
    pub(crate) vault_relative_path: Option<String>,
    pub(crate) vault_size_bytes: Option<i64>,
    pub(crate) vault_fetched_at_epoch_s: Option<i64>,
    pub(crate) export_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AttachmentDetailRecord {
    pub(crate) attachment_key: String,
    pub(crate) message_id: String,
    pub(crate) thread_id: String,
    pub(crate) part_id: String,
    pub(crate) gmail_attachment_id: Option<String>,
    pub(crate) filename: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
    pub(crate) content_disposition: Option<String>,
    pub(crate) content_id: Option<String>,
    pub(crate) is_inline: bool,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) vault_content_hash: Option<String>,
    pub(crate) vault_relative_path: Option<String>,
    pub(crate) vault_size_bytes: Option<i64>,
    pub(crate) vault_fetched_at_epoch_s: Option<i64>,
    pub(crate) export_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AttachmentVaultStateUpdate {
    pub(crate) account_id: String,
    pub(crate) attachment_key: String,
    pub(crate) content_hash: String,
    pub(crate) relative_path: String,
    pub(crate) size_bytes: i64,
    pub(crate) fetched_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AttachmentExportEventInput {
    pub(crate) account_id: String,
    pub(crate) attachment_key: String,
    pub(crate) message_id: String,
    pub(crate) thread_id: String,
    pub(crate) destination_path: String,
    pub(crate) content_hash: String,
    pub(crate) exported_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ThreadMessageSnapshot {
    pub(crate) account_id: String,
    pub(crate) message_id: String,
    pub(crate) thread_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) snippet: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MailboxDoctorReport {
    pub(crate) sync_state: Option<SyncStateRecord>,
    pub(crate) full_sync_checkpoint: Option<FullSyncCheckpointRecord>,
    pub(crate) sync_pacing_state: Option<SyncPacingStateRecord>,
    pub(crate) message_count: i64,
    pub(crate) label_count: i64,
    pub(crate) indexed_message_count: i64,
    pub(crate) attachment_count: i64,
    pub(crate) vaulted_attachment_count: i64,
    pub(crate) attachment_export_count: i64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FullSyncCheckpointStatus {
    Paging,
    ReadyToFinalize,
}

impl FullSyncCheckpointStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Paging => "paging",
            Self::ReadyToFinalize => "ready_to_finalize",
        }
    }
}

impl Display for FullSyncCheckpointStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for FullSyncCheckpointStatus {
    type Err = SyncStateStatusDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "paging" => Ok(Self::Paging),
            "ready_to_finalize" => Ok(Self::ReadyToFinalize),
            _ => Err(SyncStateStatusDecodeError::CheckpointStatus(
                value.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncPacingPressureKind {
    Quota,
    Concurrency,
    Mixed,
}

impl SyncPacingPressureKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Quota => "quota",
            Self::Concurrency => "concurrency",
            Self::Mixed => "mixed",
        }
    }
}

impl Display for SyncPacingPressureKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SyncPacingPressureKind {
    type Err = SyncStateStatusDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "quota" => Ok(Self::Quota),
            "concurrency" => Ok(Self::Concurrency),
            "mixed" => Ok(Self::Mixed),
            _ => Err(SyncStateStatusDecodeError::PacingPressureKind(
                value.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncMode {
    Full,
    Incremental,
}

impl SyncMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Incremental => "incremental",
        }
    }
}

impl Display for SyncMode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SyncMode {
    type Err = SyncStateStatusDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "full" => Ok(Self::Full),
            "incremental" => Ok(Self::Incremental),
            _ => Err(SyncStateStatusDecodeError::Mode(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncStatus {
    Ok,
    Failed,
}

impl SyncStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

impl Display for SyncStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SyncStatus {
    type Err = SyncStateStatusDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ok" => Ok(Self::Ok),
            "failed" => Ok(Self::Failed),
            _ => Err(SyncStateStatusDecodeError::Status(value.to_owned())),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum SyncStateStatusDecodeError {
    #[error("invalid mailbox sync mode `{0}`")]
    Mode(String),
    #[error("invalid mailbox sync status `{0}`")]
    Status(String),
    #[error("invalid full sync checkpoint status `{0}`")]
    CheckpointStatus(String),
    #[error("invalid mailbox sync pacing pressure kind `{0}`")]
    PacingPressureKind(String),
}

#[derive(Debug, Error)]
pub(crate) enum MailboxReadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to open local mailbox store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: DatabaseOpenError,
    },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
}

impl MailboxReadError {
    pub(crate) fn open_database(path: &Path, source: DatabaseOpenError) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum MailboxWriteError {
    #[error(
        "attachment `{attachment_key}` for account `{account_id}` was not found while persisting vault state"
    )]
    AttachmentNotFound {
        account_id: String,
        attachment_key: String,
    },
    #[error("failed to open local mailbox store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: DatabaseOpenError,
    },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
    #[error(
        "mailbox write operation `{operation}` unexpectedly touched {actual} rows (expected {expected})"
    )]
    RowCountMismatch {
        operation: &'static str,
        expected: usize,
        actual: usize,
    },
}

impl MailboxWriteError {
    pub(crate) fn open_database(path: &Path, source: DatabaseOpenError) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}
