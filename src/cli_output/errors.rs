use crate::CliInputError;
use crate::attachments::AttachmentServiceError;
use crate::auth::{self, oauth_client::OAuthClientError};
use crate::automation::AutomationServiceError;
use crate::gmail::GmailClientError;
use crate::store::{
    automation::{AutomationStoreReadError, AutomationStoreWriteError},
    mailbox::{MailboxReadError, MailboxWriteError},
    workflows::{WorkflowStoreReadError, WorkflowStoreWriteError},
};
use crate::workflows::WorkflowServiceError;
use anyhow::Error as AnyhowError;
use serde::Serialize;
use std::process::ExitCode;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ErrorCode {
    ValidationFailed,
    AuthRequired,
    NotFound,
    Conflict,
    Timeout,
    RateLimited,
    RemoteFailure,
    StorageFailure,
    InternalFailure,
}

impl ErrorCode {
    fn exit_code(self) -> u8 {
        match self {
            Self::ValidationFailed => 2,
            Self::AuthRequired => 3,
            Self::NotFound => 4,
            Self::Conflict => 5,
            Self::Timeout | Self::RateLimited | Self::RemoteFailure => 6,
            Self::StorageFailure => 7,
            Self::InternalFailure => 10,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct JsonErrorBody {
    code: ErrorCode,
    message: String,
    kind: String,
    operation: String,
    causes: Vec<String>,
}

pub(crate) fn describe_error(error: &AnyhowError, operation: &str) -> JsonErrorBody {
    let (code, kind) = classify_error(error);
    let message = error.to_string();
    let causes = error
        .chain()
        .skip(1)
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>();

    JsonErrorBody {
        code,
        message,
        kind: kind.to_owned(),
        operation: operation.to_owned(),
        causes,
    }
}

pub(crate) fn exit_code(error: &JsonErrorBody) -> ExitCode {
    ExitCode::from(error.code.exit_code())
}

fn classify_error(error: &AnyhowError) -> (ErrorCode, &'static str) {
    if let Some(attachment_error) = find_cause::<AttachmentServiceError>(error) {
        return match attachment_error {
            AttachmentServiceError::NoActiveAccount => {
                (ErrorCode::AuthRequired, "attachment.account.required")
            }
            AttachmentServiceError::AttachmentNotFound { .. } => {
                (ErrorCode::NotFound, "attachment.not_found")
            }
            AttachmentServiceError::InvalidLimit
            | AttachmentServiceError::InvalidVaultPath { .. } => {
                (ErrorCode::ValidationFailed, "attachment.validation")
            }
            AttachmentServiceError::DestinationConflict { .. } => {
                (ErrorCode::Conflict, "attachment.destination_conflict")
            }
            AttachmentServiceError::BlockingTask { .. } => {
                (ErrorCode::InternalFailure, "attachment.blocking_join")
            }
            AttachmentServiceError::CreateDirectory { .. }
            | AttachmentServiceError::WriteFile { .. }
            | AttachmentServiceError::ReadFile { .. }
            | AttachmentServiceError::CopyFile { .. }
            | AttachmentServiceError::StoreWrite { .. }
            | AttachmentServiceError::StoreRead { .. } => {
                (ErrorCode::StorageFailure, "attachment.storage")
            }
        };
    }

    if let Some(automation_error) = find_cause::<AutomationServiceError>(error)
        && !matches!(automation_error, AutomationServiceError::Gmail { .. })
    {
        return match automation_error {
            AutomationServiceError::NoActiveAccount => {
                (ErrorCode::AuthRequired, "automation.account.required")
            }
            AutomationServiceError::RunAccountMismatch { .. } => {
                (ErrorCode::AuthRequired, "automation.account.mismatch")
            }
            AutomationServiceError::ApplyAlreadyInProgress { .. } => {
                (ErrorCode::Conflict, "automation.apply.in_progress")
            }
            AutomationServiceError::InvalidLimit
            | AutomationServiceError::ExecuteRequired
            | AutomationServiceError::RuleFileMissing { .. }
            | AutomationServiceError::RuleFileRead { .. }
            | AutomationServiceError::RuleFileParse { .. }
            | AutomationServiceError::RuleValidation { .. } => {
                (ErrorCode::ValidationFailed, "automation.validation")
            }
            AutomationServiceError::RunNotFound { .. } => {
                (ErrorCode::NotFound, "automation.not_found")
            }
            AutomationServiceError::ApplyLock { .. } => {
                (ErrorCode::StorageFailure, "automation.apply_lock")
            }
            AutomationServiceError::TaskPanic { .. } => {
                (ErrorCode::InternalFailure, "automation.task_panic")
            }
            AutomationServiceError::StoreInit { .. }
            | AutomationServiceError::MailboxRead { .. }
            | AutomationServiceError::AutomationRead { .. }
            | AutomationServiceError::AutomationWrite { .. } => {
                (ErrorCode::StorageFailure, "automation.storage")
            }
            AutomationServiceError::Gmail { .. } => unreachable!(),
        };
    }

    if find_cause::<AutomationStoreWriteError>(error).is_some() {
        return (ErrorCode::StorageFailure, "store.automation.write");
    }

    if find_cause::<AutomationStoreReadError>(error).is_some() {
        return (ErrorCode::StorageFailure, "store.automation.read");
    }

    if let Some(workflow_error) = find_cause::<WorkflowServiceError>(error) {
        match workflow_error {
            WorkflowServiceError::WorkflowNotFound { .. }
            | WorkflowServiceError::CurrentDraftNotFound { .. }
            | WorkflowServiceError::RemoteDraftNotFound { .. }
            | WorkflowServiceError::LocalSnapshotMissing { .. }
            | WorkflowServiceError::ThreadHasNoMessages { .. }
            | WorkflowServiceError::SourceMessageMissing { .. }
            | WorkflowServiceError::DraftAttachmentNotFound { .. } => {
                return (ErrorCode::NotFound, "workflow.not_found");
            }
            WorkflowServiceError::NoActiveAccount => {
                return (ErrorCode::AuthRequired, "workflow.account.required");
            }
            WorkflowServiceError::AuthenticatedAccountMismatch { .. } => {
                return (ErrorCode::AuthRequired, "workflow.account.mismatch");
            }
            WorkflowServiceError::ReplyRecipientUndetermined
            | WorkflowServiceError::ReplyDraftWithoutRecipients
            | WorkflowServiceError::DraftWithoutToRecipients
            | WorkflowServiceError::CleanupLabelsRequired
            | WorkflowServiceError::AddLabelsNotFoundLocally
            | WorkflowServiceError::RemoveLabelsNotFoundLocally
            | WorkflowServiceError::DraftAttachmentNameAmbiguous { .. }
            | WorkflowServiceError::AttachmentNotFile { .. }
            | WorkflowServiceError::AttachmentFileName { .. }
            | WorkflowServiceError::InvalidDateFormat { .. }
            | WorkflowServiceError::InvalidDateMonth { .. }
            | WorkflowServiceError::InvalidDateDay { .. } => {
                return (ErrorCode::ValidationFailed, "workflow.validation");
            }
            WorkflowServiceError::RemoteDraftMissingBeforeSend { .. } => {
                return (ErrorCode::Conflict, "workflow.remote_draft.send_guard");
            }
            WorkflowServiceError::BlockingTask { .. } => {
                return (ErrorCode::InternalFailure, "workflow.blocking_join");
            }
            WorkflowServiceError::Time { .. } => {
                return (ErrorCode::InternalFailure, "workflow.time");
            }
            WorkflowServiceError::LabelCleanupInvariant => {
                return (ErrorCode::InternalFailure, "workflow.invariant");
            }
            WorkflowServiceError::RemoteDraftRollback { .. } => {
                return (ErrorCode::InternalFailure, "workflow.remote_draft.rollback");
            }
            WorkflowServiceError::RemoteSendStateReconcile { .. } => {
                return (ErrorCode::StorageFailure, "workflow.send.reconcile");
            }
            WorkflowServiceError::RemoteDraftStateReconcile { .. } => {
                return (ErrorCode::StorageFailure, "workflow.draft.reconcile");
            }
            WorkflowServiceError::AttachmentMetadata { .. }
            | WorkflowServiceError::AttachmentNormalize { .. }
            | WorkflowServiceError::AttachmentRead { .. } => {
                return (ErrorCode::ValidationFailed, "workflow.validation");
            }
            WorkflowServiceError::StoreInit { .. } => {
                return (ErrorCode::StorageFailure, "workflow.store_init");
            }
            WorkflowServiceError::AccountState { .. } => {
                return (ErrorCode::StorageFailure, "workflow.account_state");
            }
            WorkflowServiceError::GmailClientInit { .. } => {
                return (ErrorCode::InternalFailure, "workflow.gmail_client");
            }
            WorkflowServiceError::RepoRoot { .. } => {
                return (ErrorCode::InternalFailure, "workflow.repo_root");
            }
            WorkflowServiceError::MessageBuild { .. } => {
                return (ErrorCode::InternalFailure, "workflow.message_build");
            }
            WorkflowServiceError::Gmail(_)
            | WorkflowServiceError::WorkflowStoreRead(_)
            | WorkflowServiceError::WorkflowStoreWrite(_)
            | WorkflowServiceError::MailboxRead(_)
            | WorkflowServiceError::ActiveAccountRefresh { .. }
            | WorkflowServiceError::Json(_)
            | WorkflowServiceError::IntConversion(_) => {}
        }
    }

    if let Some(workflow_write_error) = find_cause::<WorkflowStoreWriteError>(error) {
        return match workflow_write_error {
            WorkflowStoreWriteError::MissingWorkflow { .. } => {
                (ErrorCode::NotFound, "store.workflow.write.missing_workflow")
            }
            WorkflowStoreWriteError::Conflict { .. } => {
                (ErrorCode::Conflict, "store.workflow.write.conflict")
            }
            WorkflowStoreWriteError::ReadyToSendRequiresSendableDraft => {
                (ErrorCode::Conflict, "store.workflow.write.ready_to_send")
            }
            WorkflowStoreWriteError::Read(_) => (ErrorCode::StorageFailure, "store.workflow.read"),
            WorkflowStoreWriteError::OpenDatabase { .. }
            | WorkflowStoreWriteError::ReloadWorkflow { .. }
            | WorkflowStoreWriteError::ReloadDraftRevision { .. }
            | WorkflowStoreWriteError::Query(_)
            | WorkflowStoreWriteError::Serialization(_) => {
                (ErrorCode::StorageFailure, "store.workflow.write")
            }
        };
    }

    if find_cause::<WorkflowStoreReadError>(error).is_some() {
        return (ErrorCode::StorageFailure, "store.workflow.read");
    }

    if find_cause::<MailboxReadError>(error).is_some() {
        return (ErrorCode::StorageFailure, "store.mailbox.read");
    }

    if let Some(mailbox_write_error) = find_cause::<MailboxWriteError>(error) {
        return match mailbox_write_error {
            MailboxWriteError::OpenDatabase { .. } | MailboxWriteError::Query(_) => {
                (ErrorCode::StorageFailure, "store.mailbox.write")
            }
            MailboxWriteError::AccountMismatch { .. } => (
                ErrorCode::AuthRequired,
                "store.mailbox.write.account_mismatch",
            ),
            MailboxWriteError::AttachmentNotFound { .. } => (
                ErrorCode::NotFound,
                "store.mailbox.write.attachment_not_found",
            ),
            MailboxWriteError::InvariantViolation { .. }
            | MailboxWriteError::RowCountMismatch { .. } => {
                (ErrorCode::InternalFailure, "store.mailbox.write")
            }
        };
    }

    if let Some(gmail_error) = find_cause::<GmailClientError>(error) {
        return match gmail_error {
            GmailClientError::InvalidQuotaBudget { .. } => {
                (ErrorCode::ValidationFailed, "gmail.quota_budget")
            }
            GmailClientError::QuotaExhausted { .. } => {
                (ErrorCode::ValidationFailed, "gmail.quota_exhausted")
            }
            GmailClientError::MissingCredentials | GmailClientError::MissingRefreshToken => {
                (ErrorCode::AuthRequired, "gmail.credentials")
            }
            GmailClientError::CredentialLoad { .. } | GmailClientError::CredentialSave { .. } => {
                (ErrorCode::StorageFailure, "gmail.credentials.store")
            }
            GmailClientError::OAuthClient { .. } => {
                (ErrorCode::ValidationFailed, "gmail.oauth_client")
            }
            GmailClientError::TokenRefresh { .. } => {
                (ErrorCode::AuthRequired, "gmail.token_refresh")
            }
            GmailClientError::Clock { .. } | GmailClientError::HttpClientBuild { .. } => {
                (ErrorCode::InternalFailure, "gmail.client")
            }
            GmailClientError::Transport { source, .. } if source.is_timeout() => {
                (ErrorCode::Timeout, "gmail.transport")
            }
            GmailClientError::Transport { .. } => (ErrorCode::RemoteFailure, "gmail.transport"),
            GmailClientError::ResponseDecode { .. } => {
                (ErrorCode::RemoteFailure, "gmail.response_decode")
            }
            GmailClientError::AttachmentPartMissing { .. }
            | GmailClientError::AttachmentBodyMissing { .. } => {
                (ErrorCode::RemoteFailure, "gmail.attachment")
            }
            GmailClientError::Api { status, .. }
                if *status == reqwest::StatusCode::UNAUTHORIZED =>
            {
                (ErrorCode::AuthRequired, "gmail.api_status")
            }
            GmailClientError::Api { status, .. } if *status == reqwest::StatusCode::NOT_FOUND => {
                (ErrorCode::NotFound, "gmail.api_status")
            }
            GmailClientError::Api { status, .. }
                if *status == reqwest::StatusCode::TOO_MANY_REQUESTS =>
            {
                (ErrorCode::RateLimited, "gmail.api_status")
            }
            GmailClientError::Api { status, .. }
                if *status == reqwest::StatusCode::REQUEST_TIMEOUT =>
            {
                (ErrorCode::Timeout, "gmail.api_status")
            }
            GmailClientError::Api { status, .. } if status.is_server_error() => {
                (ErrorCode::RemoteFailure, "gmail.api_status")
            }
            GmailClientError::Api { .. } => (ErrorCode::RemoteFailure, "gmail.api_status"),
        };
    }

    if let Some(auth_error) = find_cause::<auth::AuthError>(error) {
        return match auth_error {
            auth::AuthError::CallbackTimedOut => (ErrorCode::Timeout, "auth.callback"),
            auth::AuthError::MalformedCallbackRequest
            | auth::AuthError::MissingAuthorizationCode
            | auth::AuthError::OAuthCallback(_)
            | auth::AuthError::StateMismatch
            | auth::AuthError::InvalidRedirectUrl
            | auth::AuthError::BrowserOpen(_) => (ErrorCode::ValidationFailed, "auth.callback"),
            auth::AuthError::CallbackIo(_) => (ErrorCode::InternalFailure, "auth.callback"),
        };
    }

    if find_cause::<OAuthClientError>(error).is_some() {
        return (ErrorCode::ValidationFailed, "auth.oauth_client");
    }

    if find_cause::<CliInputError>(error).is_some() {
        return (ErrorCode::ValidationFailed, "cli.validation");
    }

    if find_cause::<rusqlite::Error>(error).is_some() {
        return (ErrorCode::StorageFailure, "store.sqlite");
    }

    if let Some(reqwest_error) = find_cause::<reqwest::Error>(error) {
        if reqwest_error.is_timeout() {
            return (ErrorCode::Timeout, "http.transport");
        }
        return (ErrorCode::RemoteFailure, "http.transport");
    }

    (ErrorCode::InternalFailure, "internal.unclassified")
}

fn find_cause<T>(error: &AnyhowError) -> Option<&T>
where
    T: std::error::Error + Send + Sync + 'static,
{
    error.chain().find_map(|cause| cause.downcast_ref::<T>())
}
