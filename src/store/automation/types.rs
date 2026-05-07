use crate::store::connection::DatabaseOpenError;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationActionKind {
    Archive,
    Label,
    Trash,
}

impl AutomationActionKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Label => "label",
            Self::Trash => "trash",
        }
    }
}

impl Display for AutomationActionKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AutomationActionKind {
    type Err = AutomationDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "archive" => Ok(Self::Archive),
            "label" => Ok(Self::Label),
            "trash" => Ok(Self::Trash),
            _ => Err(AutomationDecodeError::ActionKind(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationRunStatus {
    Previewed,
    Applying,
    Applied,
    ApplyFailed,
}

impl AutomationRunStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Previewed => "previewed",
            Self::Applying => "applying",
            Self::Applied => "applied",
            Self::ApplyFailed => "apply_failed",
        }
    }
}

impl Display for AutomationRunStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AutomationRunStatus {
    type Err = AutomationDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "previewed" => Ok(Self::Previewed),
            "applying" => Ok(Self::Applying),
            "applied" => Ok(Self::Applied),
            "apply_failed" => Ok(Self::ApplyFailed),
            _ => Err(AutomationDecodeError::RunStatus(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationApplyStatus {
    Succeeded,
    Failed,
}

impl AutomationApplyStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

impl Display for AutomationApplyStatus {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for AutomationApplyStatus {
    type Err = AutomationDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            _ => Err(AutomationDecodeError::ApplyStatus(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationActionSnapshot {
    pub(crate) kind: AutomationActionKind,
    pub(crate) add_label_ids: Vec<String>,
    pub(crate) add_label_names: Vec<String>,
    pub(crate) remove_label_ids: Vec<String>,
    pub(crate) remove_label_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationMatchReason {
    pub(crate) from_address: Option<String>,
    pub(crate) subject_terms: Vec<String>,
    pub(crate) label_names: Vec<String>,
    pub(crate) older_than_days: Option<u32>,
    pub(crate) has_attachments: Option<bool>,
    pub(crate) has_list_unsubscribe: Option<bool>,
    pub(crate) list_id_terms: Vec<String>,
    pub(crate) precedence_values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationThreadCandidate {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) message_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) from_address: Option<String>,
    pub(crate) snippet: String,
    pub(crate) label_names: Vec<String>,
    pub(crate) attachment_count: i64,
    pub(crate) list_id_header: Option<String>,
    pub(crate) list_unsubscribe_header: Option<String>,
    pub(crate) list_unsubscribe_post_header: Option<String>,
    pub(crate) precedence_header: Option<String>,
    pub(crate) auto_submitted_header: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NewAutomationRunCandidate {
    pub(crate) rule_id: String,
    pub(crate) thread_id: String,
    pub(crate) message_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) from_address: Option<String>,
    pub(crate) snippet: String,
    pub(crate) label_names: Vec<String>,
    pub(crate) attachment_count: i64,
    pub(crate) has_list_unsubscribe: bool,
    pub(crate) list_id_header: Option<String>,
    pub(crate) list_unsubscribe_header: Option<String>,
    pub(crate) list_unsubscribe_post_header: Option<String>,
    pub(crate) precedence_header: Option<String>,
    pub(crate) auto_submitted_header: Option<String>,
    pub(crate) action: AutomationActionSnapshot,
    pub(crate) reason: AutomationMatchReason,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CreateAutomationRunInput {
    pub(crate) account_id: String,
    pub(crate) rule_file_path: String,
    pub(crate) rule_file_hash: String,
    pub(crate) selected_rule_ids: Vec<String>,
    pub(crate) created_at_epoch_s: i64,
    pub(crate) candidates: Vec<NewAutomationRunCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationRunRecord {
    pub(crate) run_id: i64,
    pub(crate) account_id: String,
    pub(crate) rule_file_path: String,
    pub(crate) rule_file_hash: String,
    pub(crate) selected_rule_ids: Vec<String>,
    pub(crate) status: AutomationRunStatus,
    pub(crate) candidate_count: i64,
    pub(crate) created_at_epoch_s: i64,
    pub(crate) applied_at_epoch_s: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationRunCandidateRecord {
    pub(crate) candidate_id: i64,
    pub(crate) run_id: i64,
    pub(crate) account_id: String,
    pub(crate) rule_id: String,
    pub(crate) thread_id: String,
    pub(crate) message_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) from_address: Option<String>,
    pub(crate) snippet: String,
    pub(crate) label_names: Vec<String>,
    pub(crate) attachment_count: i64,
    pub(crate) has_list_unsubscribe: bool,
    pub(crate) list_id_header: Option<String>,
    pub(crate) list_unsubscribe_header: Option<String>,
    pub(crate) list_unsubscribe_post_header: Option<String>,
    pub(crate) precedence_header: Option<String>,
    pub(crate) auto_submitted_header: Option<String>,
    pub(crate) action: AutomationActionSnapshot,
    pub(crate) reason: AutomationMatchReason,
    pub(crate) apply_status: Option<AutomationApplyStatus>,
    pub(crate) applied_at_epoch_s: Option<i64>,
    pub(crate) apply_error: Option<String>,
    pub(crate) created_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationRunEventRecord {
    pub(crate) event_id: i64,
    pub(crate) run_id: i64,
    pub(crate) account_id: String,
    pub(crate) event_kind: String,
    pub(crate) payload_json: String,
    pub(crate) created_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationRunDetail {
    pub(crate) run: AutomationRunRecord,
    pub(crate) candidates: Vec<AutomationRunCandidateRecord>,
    pub(crate) events: Vec<AutomationRunEventRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AppendAutomationRunEventInput {
    pub(crate) run_id: i64,
    pub(crate) account_id: String,
    pub(crate) event_kind: String,
    pub(crate) payload_json: String,
    pub(crate) created_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CandidateApplyResultInput {
    pub(crate) run_id: i64,
    pub(crate) candidate_id: i64,
    pub(crate) status: AutomationApplyStatus,
    pub(crate) applied_at_epoch_s: i64,
    pub(crate) apply_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FinalizeAutomationRunInput {
    pub(crate) run_id: i64,
    pub(crate) status: AutomationRunStatus,
    pub(crate) applied_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PruneAutomationRunsInput {
    pub(crate) account_id: String,
    pub(crate) cutoff_epoch_s: i64,
    pub(crate) statuses: Vec<AutomationRunStatus>,
    pub(crate) execute: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationPruneStoreReport {
    pub(crate) matched_run_count: i64,
    pub(crate) matched_candidate_count: i64,
    pub(crate) matched_event_count: i64,
    pub(crate) deleted_run_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AutomationDoctorReport {
    pub(crate) run_count: i64,
    pub(crate) previewed_run_count: i64,
    pub(crate) applied_run_count: i64,
    pub(crate) apply_failed_run_count: i64,
    pub(crate) candidate_count: i64,
}

#[derive(Debug, Error)]
pub(crate) enum AutomationDecodeError {
    #[error("invalid automation action kind `{0}`")]
    ActionKind(String),
    #[error("invalid automation run status `{0}`")]
    RunStatus(String),
    #[error("invalid automation candidate apply status `{0}`")]
    ApplyStatus(String),
}

#[derive(Debug, Error)]
pub(crate) enum AutomationStoreReadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to open local automation store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: DatabaseOpenError,
    },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Decode(#[from] AutomationDecodeError),
}

impl AutomationStoreReadError {
    pub(crate) fn open_database(path: &Path, source: DatabaseOpenError) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum AutomationStoreWriteError {
    #[error("automation run {run_id} was not found")]
    MissingRun { run_id: i64 },
    #[error(
        "automation run {run_id} belongs to {expected_account_id} but the event was written for {actual_account_id}"
    )]
    RunAccountMismatch {
        run_id: i64,
        expected_account_id: String,
        actual_account_id: String,
    },
    #[error("automation run candidate {candidate_id} was not found for run {run_id}")]
    MissingCandidate { run_id: i64, candidate_id: i64 },
    #[error("failed to open local automation store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: DatabaseOpenError,
    },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Read(#[from] AutomationStoreReadError),
    #[error(
        "automation write operation `{operation}` unexpectedly touched {actual} rows (expected {expected})"
    )]
    RowCountMismatch {
        operation: &'static str,
        expected: usize,
        actual: usize,
    },
}

impl AutomationStoreWriteError {
    pub(crate) fn open_database(path: &Path, source: DatabaseOpenError) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}
