use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkflowStage {
    Triage,
    FollowUp,
    Drafting,
    ReadyToSend,
    Sent,
    Closed,
}

impl WorkflowStage {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Triage => "triage",
            Self::FollowUp => "follow_up",
            Self::Drafting => "drafting",
            Self::ReadyToSend => "ready_to_send",
            Self::Sent => "sent",
            Self::Closed => "closed",
        }
    }
}

impl Display for WorkflowStage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for WorkflowStage {
    type Err = WorkflowDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "triage" => Ok(Self::Triage),
            "follow_up" => Ok(Self::FollowUp),
            "drafting" => Ok(Self::Drafting),
            "ready_to_send" => Ok(Self::ReadyToSend),
            "sent" => Ok(Self::Sent),
            "closed" => Ok(Self::Closed),
            _ => Err(WorkflowDecodeError::Stage(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TriageBucket {
    Urgent,
    NeedsReplySoon,
    Waiting,
    Fyi,
}

impl TriageBucket {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Urgent => "urgent",
            Self::NeedsReplySoon => "needs_reply_soon",
            Self::Waiting => "waiting",
            Self::Fyi => "fyi",
        }
    }
}

impl Display for TriageBucket {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TriageBucket {
    type Err = WorkflowDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "urgent" => Ok(Self::Urgent),
            "needs_reply_soon" => Ok(Self::NeedsReplySoon),
            "waiting" => Ok(Self::Waiting),
            "fyi" => Ok(Self::Fyi),
            _ => Err(WorkflowDecodeError::Bucket(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplyMode {
    Reply,
    ReplyAll,
}

impl ReplyMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Reply => "reply",
            Self::ReplyAll => "reply_all",
        }
    }
}

impl Display for ReplyMode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ReplyMode {
    type Err = WorkflowDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "reply" => Ok(Self::Reply),
            "reply_all" => Ok(Self::ReplyAll),
            _ => Err(WorkflowDecodeError::ReplyMode(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CleanupAction {
    Archive,
    Label,
    Trash,
}

impl CleanupAction {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Label => "label",
            Self::Trash => "trash",
        }
    }
}

impl Display for CleanupAction {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CleanupAction {
    type Err = WorkflowDecodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "archive" => Ok(Self::Archive),
            "label" => Ok(Self::Label),
            "trash" => Ok(Self::Trash),
            _ => Err(WorkflowDecodeError::CleanupAction(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkflowMessageSnapshot {
    pub(crate) message_id: String,
    pub(crate) internal_date_epoch_ms: i64,
    pub(crate) subject: String,
    pub(crate) from_header: String,
    pub(crate) snippet: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkflowRecord {
    pub(crate) workflow_id: i64,
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) current_stage: WorkflowStage,
    pub(crate) triage_bucket: Option<TriageBucket>,
    pub(crate) note: String,
    pub(crate) snoozed_until_epoch_s: Option<i64>,
    pub(crate) follow_up_due_epoch_s: Option<i64>,
    pub(crate) latest_message_id: Option<String>,
    pub(crate) latest_message_internal_date_epoch_ms: Option<i64>,
    pub(crate) latest_message_subject: String,
    pub(crate) latest_message_from_header: String,
    pub(crate) latest_message_snippet: String,
    pub(crate) current_draft_revision_id: Option<i64>,
    pub(crate) gmail_draft_id: Option<String>,
    pub(crate) gmail_draft_message_id: Option<String>,
    pub(crate) gmail_draft_thread_id: Option<String>,
    pub(crate) last_remote_sync_epoch_s: Option<i64>,
    pub(crate) last_sent_message_id: Option<String>,
    pub(crate) last_cleanup_action: Option<CleanupAction>,
    pub(crate) created_at_epoch_s: i64,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkflowEventRecord {
    pub(crate) event_id: i64,
    pub(crate) workflow_id: i64,
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) event_kind: String,
    pub(crate) from_stage: Option<WorkflowStage>,
    pub(crate) to_stage: Option<WorkflowStage>,
    pub(crate) triage_bucket: Option<TriageBucket>,
    pub(crate) note: Option<String>,
    pub(crate) payload_json: String,
    pub(crate) created_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DraftRevisionRecord {
    pub(crate) draft_revision_id: i64,
    pub(crate) workflow_id: i64,
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) source_message_id: String,
    pub(crate) reply_mode: ReplyMode,
    pub(crate) subject: String,
    pub(crate) to_addresses: Vec<String>,
    pub(crate) cc_addresses: Vec<String>,
    pub(crate) bcc_addresses: Vec<String>,
    pub(crate) body_text: String,
    pub(crate) created_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DraftAttachmentRecord {
    pub(crate) attachment_id: i64,
    pub(crate) draft_revision_id: i64,
    pub(crate) path: String,
    pub(crate) file_name: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
    pub(crate) created_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DraftRevisionDetail {
    pub(crate) revision: DraftRevisionRecord,
    pub(crate) attachments: Vec<DraftAttachmentRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkflowDetail {
    pub(crate) workflow: WorkflowRecord,
    pub(crate) current_draft: Option<DraftRevisionDetail>,
    pub(crate) events: Vec<WorkflowEventRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkflowListFilter {
    pub(crate) account_id: String,
    pub(crate) stage: Option<WorkflowStage>,
    pub(crate) triage_bucket: Option<TriageBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkflowDoctorReport {
    pub(crate) workflow_count: i64,
    pub(crate) open_workflow_count: i64,
    pub(crate) draft_workflow_count: i64,
    pub(crate) event_count: i64,
    pub(crate) draft_revision_count: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct SetTriageStateInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) triage_bucket: TriageBucket,
    pub(crate) note: Option<String>,
    pub(crate) snapshot: WorkflowMessageSnapshot,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct PromoteWorkflowInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) to_stage: WorkflowStage,
    pub(crate) snapshot: WorkflowMessageSnapshot,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct SnoozeWorkflowInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) snoozed_until_epoch_s: Option<i64>,
    pub(crate) snapshot: WorkflowMessageSnapshot,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct UpsertDraftRevisionInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) reply_mode: ReplyMode,
    pub(crate) source_message_id: String,
    pub(crate) subject: String,
    pub(crate) to_addresses: Vec<String>,
    pub(crate) cc_addresses: Vec<String>,
    pub(crate) bcc_addresses: Vec<String>,
    pub(crate) body_text: String,
    pub(crate) attachments: Vec<AttachmentInput>,
    pub(crate) snapshot: WorkflowMessageSnapshot,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteDraftStateInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) gmail_draft_id: Option<String>,
    pub(crate) gmail_draft_message_id: Option<String>,
    pub(crate) gmail_draft_thread_id: Option<String>,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct MarkSentInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) sent_message_id: String,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ApplyCleanupInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) cleanup_action: CleanupAction,
    pub(crate) payload_json: String,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct RetireDraftStateInput {
    pub(crate) account_id: String,
    pub(crate) thread_id: String,
    pub(crate) updated_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AttachmentInput {
    pub(crate) path: String,
    pub(crate) file_name: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: i64,
}

#[derive(Debug, Error)]
pub(crate) enum WorkflowDecodeError {
    #[error("invalid workflow stage `{0}`")]
    Stage(String),
    #[error("invalid triage bucket `{0}`")]
    Bucket(String),
    #[error("invalid reply mode `{0}`")]
    ReplyMode(String),
    #[error("invalid cleanup action `{0}`")]
    CleanupAction(String),
}

#[derive(Debug, Error)]
pub(crate) enum WorkflowStoreReadError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("failed to open workflow store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
}

impl WorkflowStoreReadError {
    pub(crate) fn open_database(path: &Path, source: anyhow::Error) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum WorkflowStoreWriteError {
    #[error(transparent)]
    Read(#[from] WorkflowStoreReadError),
    #[error("failed to open workflow store at {path}")]
    OpenDatabase {
        path: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("no workflow found for thread {thread_id}")]
    MissingWorkflow { thread_id: String },
    #[error("ready_to_send requires a current draft revision and synced Gmail draft")]
    ReadyToSendRequiresSendableDraft,
    #[error("failed to reload workflow for thread {thread_id}")]
    ReloadWorkflow { thread_id: String },
    #[error("failed to reload draft revision {draft_revision_id}")]
    ReloadDraftRevision { draft_revision_id: String },
    #[error(transparent)]
    Query(#[from] rusqlite::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

impl WorkflowStoreWriteError {
    pub(crate) fn open_database(path: &Path, source: anyhow::Error) -> Self {
        Self::OpenDatabase {
            path: path.display().to_string(),
            source,
        }
    }
}
