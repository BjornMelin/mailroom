use crate::gmail::GmailClientError;
use crate::store;
use thiserror::Error;
use tokio::task::JoinError;

pub(super) type WorkflowResult<T> = std::result::Result<T, WorkflowServiceError>;

#[derive(Debug, Error)]
pub(crate) enum WorkflowServiceError {
    #[error(transparent)]
    Gmail(#[from] GmailClientError),
    #[error(transparent)]
    WorkflowStoreRead(#[from] store::workflows::WorkflowStoreReadError),
    #[error(transparent)]
    WorkflowStoreWrite(#[from] store::workflows::WorkflowStoreWriteError),
    #[error(transparent)]
    MailboxRead(#[from] store::mailbox::MailboxReadError),
    #[error("failed to initialize the local store")]
    StoreInit {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to refresh the active Gmail account")]
    ActiveAccountRefresh {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to access local account state")]
    AccountState {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to initialize the Gmail client")]
    GmailClientInit {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to resolve the configured repo root")]
    RepoRoot {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to read the current system time")]
    Time {
        #[source]
        source: anyhow::Error,
    },
    #[error("workflow blocking task `{operation}` failed")]
    BlockingTask {
        operation: &'static str,
        #[source]
        source: JoinError,
    },
    #[error("no workflow found for thread {thread_id}")]
    WorkflowNotFound { thread_id: String },
    #[error("no current draft found for thread {thread_id}")]
    CurrentDraftNotFound { thread_id: String },
    #[error("no remote Gmail draft is associated with thread {thread_id}")]
    RemoteDraftNotFound { thread_id: String },
    #[error(
        "stored Gmail draft {draft_id} for thread {thread_id} no longer exists; refusing to recreate it during send because the previous send may have already succeeded; run `mailroom sync run` and inspect the thread before retrying"
    )]
    RemoteDraftMissingBeforeSend { thread_id: String, draft_id: String },
    #[error("no active Gmail account found; run `mailroom auth login` first")]
    NoActiveAccount,
    #[error(
        "thread {thread_id} belongs to {expected_account_id}, but the authenticated Gmail account is {actual_account_id}; switch accounts before mutating this workflow"
    )]
    AuthenticatedAccountMismatch {
        thread_id: String,
        expected_account_id: String,
        actual_account_id: String,
    },
    #[error(
        "no locally synced message found for thread {thread_id}; run `mailroom sync run` first"
    )]
    LocalSnapshotMissing { thread_id: String },
    #[error("thread {thread_id} has no messages")]
    ThreadHasNoMessages { thread_id: String },
    #[error("thread {thread_id} does not contain source message {message_id}")]
    SourceMessageMissing {
        thread_id: String,
        message_id: String,
    },
    #[error("could not determine reply recipient from thread headers")]
    ReplyRecipientUndetermined,
    #[error("reply draft has no recipients")]
    ReplyDraftWithoutRecipients,
    #[error("draft must have at least one To recipient")]
    DraftWithoutToRecipients,
    #[error("at least one label must be added or removed")]
    CleanupLabelsRequired,
    #[error("one or more add-label names were not found locally; run `mailroom sync run` first")]
    AddLabelsNotFoundLocally,
    #[error("one or more remove-label names were not found locally; run `mailroom sync run` first")]
    RemoveLabelsNotFoundLocally,
    #[error("label cleanup executed without resolved label ids")]
    LabelCleanupInvariant,
    #[error("no draft attachment matched `{path_or_name}`")]
    DraftAttachmentNotFound { path_or_name: String },
    #[error(
        "attachment name `{file_name}` matches multiple draft attachments; use the stored attachment path instead"
    )]
    DraftAttachmentNameAmbiguous { file_name: String },
    #[error("failed to read attachment metadata for {path}")]
    AttachmentMetadata {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not a file")]
    AttachmentNotFile { path: String },
    #[error("failed to normalize attachment path {path}")]
    AttachmentNormalize {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("attachment path {path} has no valid file name")]
    AttachmentFileName { path: String },
    #[error("failed to read attachment {path}")]
    AttachmentRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("date `{value}` must be in YYYY-MM-DD format")]
    InvalidDateFormat { value: String },
    #[error("date `{value}` has an invalid month")]
    InvalidDateMonth { value: String },
    #[error("date `{value}` has an invalid day")]
    InvalidDateDay { value: String },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error("failed to build Gmail message")]
    MessageBuild {
        #[source]
        source: anyhow::Error,
    },
    #[error(
        "created Gmail draft {draft_id} for thread {thread_id} but could not persist or roll it back locally"
    )]
    RemoteDraftRollback {
        thread_id: String,
        draft_id: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(
        "Gmail sent the draft for thread {thread_id} as message {sent_message_id}, but mailroom could not record the sent state locally; inspect the thread before retrying send"
    )]
    RemoteSendStateReconcile {
        thread_id: String,
        sent_message_id: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(
        "Gmail updated draft {draft_id} for thread {thread_id}, but mailroom could not record draft state locally; inspect the thread before retrying"
    )]
    RemoteDraftStateReconcile {
        thread_id: String,
        draft_id: String,
        #[source]
        source: anyhow::Error,
    },
}
