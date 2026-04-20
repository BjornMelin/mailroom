use serde::Serialize;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;

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
    pub(crate) label_ids: Vec<String>,
    pub(crate) label_names_text: String,
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
    pub(crate) message_count: i64,
    pub(crate) label_count: i64,
    pub(crate) indexed_message_count: i64,
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
    pub(crate) message_count: i64,
    pub(crate) label_count: i64,
    pub(crate) indexed_message_count: i64,
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
            _ => Err(SyncStateStatusDecodeError::InvalidMode(value.to_owned())),
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
            _ => Err(SyncStateStatusDecodeError::InvalidStatus(value.to_owned())),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum SyncStateStatusDecodeError {
    #[error("invalid mailbox sync mode `{0}`")]
    InvalidMode(String),
    #[error("invalid mailbox sync status `{0}`")]
    InvalidStatus(String),
}

#[derive(Debug, Error)]
pub(crate) enum MailboxReadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to open local mailbox store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
}

impl MailboxReadError {
    pub(crate) fn open_database(path: &Path, source: anyhow::Error) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}
