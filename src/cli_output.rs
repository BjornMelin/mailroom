use crate::CliInputError;
use crate::auth::{self, oauth_client::OAuthClientError};
use crate::gmail::GmailClientError;
use crate::store::{
    mailbox::MailboxReadError,
    workflows::{WorkflowStoreReadError, WorkflowStoreWriteError},
};
use crate::workflows::WorkflowServiceError;
use anyhow::Error as AnyhowError;
use serde::Serialize;
use std::io::Write;
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

#[derive(Debug, Serialize)]
struct JsonSuccessEnvelope<'a, T> {
    success: bool,
    data: &'a T,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct JsonErrorBody {
    code: ErrorCode,
    message: String,
    kind: String,
    operation: String,
    causes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct JsonFailureEnvelope<'a> {
    success: bool,
    error: &'a JsonErrorBody,
}

pub(crate) fn print_json_success<T: Serialize>(data: &T) -> anyhow::Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    write_json_success(&mut stdout, data)
}

pub(crate) fn print_json_failure(error: &JsonErrorBody) -> anyhow::Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    write_json_failure(&mut stdout, error)
}

pub(crate) fn write_json_success<W: Write, T: Serialize>(
    writer: &mut W,
    data: &T,
) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(&mut *writer, &json_success_value(data))?;
    writeln!(writer)?;
    Ok(())
}

pub(crate) fn write_json_failure<W: Write>(
    writer: &mut W,
    error: &JsonErrorBody,
) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(&mut *writer, &json_failure_value(error))?;
    writeln!(writer)?;
    Ok(())
}

pub(crate) fn describe_error(error: &AnyhowError, operation: &str) -> JsonErrorBody {
    let (code, kind) = classify_error(error);
    let message = error.to_string();
    let causes = error
        .chain()
        .skip(1)
        .map(|cause| cause.to_string())
        .filter(|cause| !cause.is_empty() && cause != &message)
        .fold(Vec::<String>::new(), |mut acc, cause| {
            if !acc.iter().any(|existing| existing == &cause) {
                acc.push(cause);
            }
            acc
        });

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

fn json_success_value<T: Serialize>(data: &T) -> JsonSuccessEnvelope<'_, T> {
    JsonSuccessEnvelope {
        success: true,
        data,
    }
}

fn json_failure_value(error: &JsonErrorBody) -> JsonFailureEnvelope<'_> {
    JsonFailureEnvelope {
        success: false,
        error,
    }
}

fn classify_error(error: &AnyhowError) -> (ErrorCode, &'static str) {
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
            WorkflowServiceError::LabelCleanupInvariant => {
                return (ErrorCode::InternalFailure, "workflow.invariant");
            }
            WorkflowServiceError::RemoteDraftRollback { .. } => {
                return (ErrorCode::InternalFailure, "workflow.remote_draft.rollback");
            }
            WorkflowServiceError::RemoteSendStateReconcile { .. } => {
                return (ErrorCode::StorageFailure, "workflow.send.reconcile");
            }
            WorkflowServiceError::AttachmentMetadata { .. }
            | WorkflowServiceError::AttachmentNormalize { .. }
            | WorkflowServiceError::AttachmentRead { .. } => {
                return (ErrorCode::ValidationFailed, "workflow.validation");
            }
            WorkflowServiceError::MessageBuild { .. } => {
                return (ErrorCode::InternalFailure, "workflow.message_build");
            }
            WorkflowServiceError::Gmail(_)
            | WorkflowServiceError::WorkflowStoreRead(_)
            | WorkflowServiceError::WorkflowStoreWrite(_)
            | WorkflowServiceError::MailboxRead(_)
            | WorkflowServiceError::StoreInit { .. }
            | WorkflowServiceError::ActiveAccountRefresh { .. }
            | WorkflowServiceError::AccountState { .. }
            | WorkflowServiceError::Json(_)
            | WorkflowServiceError::IntConversion(_)
            | WorkflowServiceError::Unexpected(_) => {}
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
            WorkflowStoreWriteError::Read(_)
            | WorkflowStoreWriteError::OpenDatabase { .. }
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

    if let Some(gmail_error) = find_cause::<GmailClientError>(error) {
        return match gmail_error {
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

#[cfg(test)]
mod tests {
    use super::{describe_error, exit_code, json_failure_value, json_success_value};
    use crate::CliInputError;
    use crate::gmail::GmailClientError;
    use crate::workflows::WorkflowServiceError;
    use anyhow::anyhow;
    use reqwest::StatusCode;
    use serde_json::{json, to_value};
    use std::io::ErrorKind;

    #[test]
    fn json_success_envelope_wraps_payload_in_success_and_data() {
        let value = to_value(json_success_value(&json!({ "thread_id": "thread-1" }))).unwrap();

        assert_eq!(
            value,
            json!({
                "success": true,
                "data": {
                    "thread_id": "thread-1"
                }
            })
        );
    }

    #[test]
    fn workflow_not_found_uses_not_found_code_and_exit_bucket() {
        let error = anyhow!(WorkflowServiceError::WorkflowNotFound {
            thread_id: String::from("thread-1"),
        });

        let report = describe_error(&error, "workflow.show");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["success"], json!(false));
        assert_eq!(value["error"]["code"], json!("not_found"));
        assert_eq!(value["error"]["kind"], json!("workflow.not_found"));
        assert_eq!(value["error"]["operation"], json!("workflow.show"));
        assert_eq!(exit_code(&report), std::process::ExitCode::from(4));
    }

    #[test]
    fn gmail_rate_limit_maps_to_rate_limited_code() {
        let error = anyhow!(GmailClientError::Api {
            path: String::from("users/me/labels"),
            status: StatusCode::TOO_MANY_REQUESTS,
            body: String::from("slow down"),
        });

        let report = describe_error(&error, "gmail.labels.list");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("rate_limited"));
        assert_eq!(value["error"]["kind"], json!("gmail.api_status"));
        assert_eq!(exit_code(&report), std::process::ExitCode::from(6));
    }

    #[test]
    fn remote_draft_send_guard_maps_to_conflict_code() {
        let error = anyhow!(WorkflowServiceError::RemoteDraftMissingBeforeSend {
            thread_id: String::from("thread-1"),
            draft_id: String::from("draft-1"),
        });

        let report = describe_error(&error, "workflow.draft.send");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("conflict"));
        assert_eq!(
            value["error"]["kind"],
            json!("workflow.remote_draft.send_guard")
        );
        assert_eq!(exit_code(&report), std::process::ExitCode::from(5));
    }

    #[test]
    fn remote_draft_rollback_maps_to_internal_failure_code() {
        let error = anyhow!(WorkflowServiceError::RemoteDraftRollback {
            thread_id: String::from("thread-1"),
            draft_id: String::from("draft-1"),
            source: anyhow!("rollback failed"),
        });

        let report = describe_error(&error, "workflow.draft.start");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("internal_failure"));
        assert_eq!(
            value["error"]["kind"],
            json!("workflow.remote_draft.rollback")
        );
        assert_eq!(exit_code(&report), std::process::ExitCode::from(10));
    }

    #[test]
    fn local_cli_input_errors_map_to_validation_failed_code() {
        let error = anyhow!(CliInputError::DraftBodyInputSourceConflict);

        let report = describe_error(&error, "draft.body.set");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("validation_failed"));
        assert_eq!(value["error"]["kind"], json!("cli.validation"));
        assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
    }

    #[test]
    fn attachment_file_errors_map_to_validation_failed_code() {
        let error = anyhow!(WorkflowServiceError::AttachmentRead {
            path: String::from("/tmp/report.pdf"),
            source: std::io::Error::new(ErrorKind::NotFound, "missing attachment"),
        });

        let report = describe_error(&error, "draft.send");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("validation_failed"));
        assert_eq!(value["error"]["kind"], json!("workflow.validation"));
        assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
    }

    #[test]
    fn remote_send_state_reconcile_maps_to_storage_failure_code() {
        let error = anyhow!(WorkflowServiceError::RemoteSendStateReconcile {
            thread_id: String::from("thread-1"),
            sent_message_id: String::from("sent-message-1"),
            source: anyhow!("database is locked"),
        });

        let report = describe_error(&error, "draft.send");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("storage_failure"));
        assert_eq!(value["error"]["kind"], json!("workflow.send.reconcile"));
        assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
    }

    #[test]
    fn workflow_store_write_conflict_maps_to_conflict_code() {
        let error = anyhow!(crate::store::workflows::WorkflowStoreWriteError::Conflict {
            thread_id: String::from("thread-1"),
        });

        let report = describe_error(&error, "workflow.promote");
        let value = to_value(json_failure_value(&report)).unwrap();

        assert_eq!(value["error"]["code"], json!("conflict"));
        assert_eq!(
            value["error"]["kind"],
            json!("store.workflow.write.conflict")
        );
        assert_eq!(value["error"]["operation"], json!("workflow.promote"));
        assert_eq!(exit_code(&report), std::process::ExitCode::from(5));
    }

    #[test]
    fn cli_entrypoint_contract_round_trips_json_and_human_failures() {
        use std::process::Command;
        use tempfile::TempDir;

        let cargo = std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
        let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
        let repo_root = TempDir::new().unwrap();
        std::fs::create_dir(repo_root.path().join(".git")).unwrap();
        let home_dir = TempDir::new().unwrap();
        let xdg_config_home = home_dir.path().join(".config");

        let report = describe_error(
            &anyhow!(WorkflowServiceError::NoActiveAccount),
            "workflow.show",
        );
        let expected_json = to_value(json_failure_value(&report)).unwrap();

        let json_output = Command::new(&cargo)
            .args([
                "run",
                "--quiet",
                "--manifest-path",
                &manifest_path,
                "--",
                "workflow",
                "show",
                "thread-1",
                "--json",
            ])
            .env("XDG_CONFIG_HOME", &xdg_config_home)
            .current_dir(repo_root.path())
            .output()
            .unwrap();
        assert_eq!(json_output.status.code(), Some(3));
        assert!(json_output.stderr.is_empty());

        let json_stdout = String::from_utf8(json_output.stdout).unwrap();
        let json_value: serde_json::Value = serde_json::from_str(&json_stdout).unwrap();
        assert_eq!(json_value, expected_json);
        assert_eq!(exit_code(&report), std::process::ExitCode::from(3));

        let human_output = Command::new(&cargo)
            .args([
                "run",
                "--quiet",
                "--manifest-path",
                &manifest_path,
                "--",
                "workflow",
                "show",
                "thread-1",
            ])
            .env("XDG_CONFIG_HOME", &xdg_config_home)
            .current_dir(repo_root.path())
            .output()
            .unwrap();
        assert_eq!(human_output.status.code(), Some(3));
        assert!(human_output.stdout.is_empty());
        let human_stderr = String::from_utf8(human_output.stderr).unwrap();
        let human_stderr_lower = human_stderr.to_lowercase();
        assert!(human_stderr_lower.contains("no active gmail account found"));
        assert!(human_stderr_lower.contains("mailroom auth login"));
    }
}
