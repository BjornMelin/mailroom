use super::json::{json_failure_value, json_success_value};
use super::{describe_error, exit_code};
use crate::CliInputError;
use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::automation::AutomationServiceError;
use crate::config::resolve;
use crate::gmail::GmailClientError;
use crate::store;
use crate::store::accounts;
use crate::store::automation::{
    AutomationActionKind, AutomationActionSnapshot, AutomationMatchReason,
    CreateAutomationRunInput, NewAutomationRunCandidate, create_automation_run,
};
use crate::store::mailbox::{
    GmailAttachmentUpsertInput, GmailMessageUpsertInput, MailboxWriteError,
};
use crate::store::workflows::{WorkflowStoreReadError, WorkflowStoreWriteError};
use crate::workflows::WorkflowServiceError;
use crate::workspace::WorkspacePaths;
use anyhow::anyhow;
use reqwest::StatusCode;
use secrecy::SecretString;
use serde_json::{json, to_value};
use std::fs;
use std::io::ErrorKind;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
fn sync_cli_zero_value_errors_map_to_validation_failed_code() {
    let error = anyhow!(CliInputError::RecentDaysZero);

    let report = describe_error(&error, "sync.run");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("validation_failed"));
    assert_eq!(value["error"]["kind"], json!("cli.validation"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
}

#[test]
fn automation_run_account_mismatch_maps_to_auth_required_code() {
    let error = anyhow!(AutomationServiceError::RunAccountMismatch {
        run_id: 42,
        expected_account_id: String::from("gmail:operator@example.com"),
        actual_account_id: String::from("gmail:other@example.com"),
    });

    let report = describe_error(&error, "automation.apply");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("auth_required"));
    assert_eq!(value["error"]["kind"], json!("automation.account.mismatch"));
    assert_eq!(value["error"]["operation"], json!("automation.apply"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(3));
}

#[test]
fn automation_apply_in_progress_maps_to_conflict_code() {
    let error = anyhow!(AutomationServiceError::ApplyAlreadyInProgress { run_id: 42 });

    let report = describe_error(&error, "automation.apply");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("conflict"));
    assert_eq!(
        value["error"]["kind"],
        json!("automation.apply.in_progress")
    );
    assert_eq!(exit_code(&report), std::process::ExitCode::from(5));
}

#[test]
fn automation_prune_validation_maps_to_validation_failed_code() {
    let error = anyhow!(AutomationServiceError::InvalidPruneWindow);

    let report = describe_error(&error, "automation.prune");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("validation_failed"));
    assert_eq!(value["error"]["kind"], json!("automation.validation"));
    assert_eq!(value["error"]["operation"], json!("automation.prune"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
}

#[test]
fn automation_rules_suggest_validation_maps_to_validation_failed_code() {
    let error = anyhow!(AutomationServiceError::InvalidSuggestionLimit);

    let report = describe_error(&error, "automation.rules.suggest");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("validation_failed"));
    assert_eq!(value["error"]["kind"], json!("automation.validation"));
    assert_eq!(
        value["error"]["operation"],
        json!("automation.rules.suggest")
    );
    assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
}

#[test]
fn automation_rules_suggest_zero_limit_fails_in_json_and_human_modes() {
    use std::process::Command;
    use tempfile::TempDir;

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let repo_root = TempDir::new().unwrap();
    std::fs::create_dir(repo_root.path().join(".git")).unwrap();
    let config_dir = repo_root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();

    let json_output = Command::new(&cargo)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            &manifest_path,
            "--",
            "automation",
            "rules",
            "suggest",
            "--limit",
            "0",
            "--json",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env_remove("HOME")
        .current_dir(repo_root.path())
        .output()
        .unwrap();
    assert_eq!(json_output.status.code(), Some(2));
    assert!(json_output.stderr.is_empty());

    let json_stdout = String::from_utf8(json_output.stdout).unwrap();
    let json_value: serde_json::Value = serde_json::from_str(&json_stdout).unwrap();
    assert_eq!(json_value["success"], json!(false));
    assert_eq!(json_value["error"]["code"], json!("validation_failed"));
    assert_eq!(
        json_value["error"]["operation"],
        json!("automation.rules.suggest")
    );
    assert!(
        json_value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--limit")
    );

    let human_output = Command::new(&cargo)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            &manifest_path,
            "--",
            "automation",
            "rules",
            "suggest",
            "--limit",
            "0",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env_remove("HOME")
        .current_dir(repo_root.path())
        .output()
        .unwrap();
    assert_eq!(human_output.status.code(), Some(2));
    assert!(human_output.stdout.is_empty());
    let human_stderr = String::from_utf8(human_output.stderr).unwrap();
    assert!(human_stderr.contains("automation rules suggest --limit"));
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
fn invalid_quota_budget_maps_to_gmail_quota_budget_validation_error() {
    let error = anyhow!(GmailClientError::InvalidQuotaBudget {
        units_per_minute: 0,
        minimum_units_per_minute: 5,
    });

    let report = describe_error(&error, "sync.run");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("validation_failed"));
    assert_eq!(value["error"]["kind"], json!("gmail.quota_budget"));
    assert_eq!(value["error"]["operation"], json!("sync.run"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
}

#[test]
fn quota_exhausted_maps_to_gmail_quota_exhausted_validation_error() {
    let error = anyhow!(GmailClientError::QuotaExhausted {
        requested_units: 10,
        available_units_per_minute: 5,
    });

    let report = describe_error(&error, "thread.show");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("validation_failed"));
    assert_eq!(value["error"]["kind"], json!("gmail.quota_exhausted"));
    assert_eq!(value["error"]["operation"], json!("thread.show"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(2));
}

#[test]
fn attachment_store_write_errors_map_to_storage_failure_code() {
    let error = anyhow!(crate::attachments::AttachmentServiceError::StoreWrite {
        source: anyhow!("database is locked"),
    });

    let report = describe_error(&error, "attachment.fetch");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("attachment.storage"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn attachment_store_read_errors_map_to_storage_failure_code() {
    let error = anyhow!(crate::attachments::AttachmentServiceError::StoreRead {
        source: crate::store::mailbox::MailboxReadError::Query(rusqlite::Error::InvalidQuery),
    });

    let report = describe_error(&error, "attachment.list");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("attachment.storage"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn mailbox_write_query_errors_map_to_storage_failure_code() {
    let error = anyhow!(MailboxWriteError::Query(rusqlite::Error::InvalidQuery));

    let report = describe_error(&error, "sync.run");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("store.mailbox.write"));
    assert_eq!(value["error"]["operation"], json!("sync.run"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn mailbox_write_account_mismatch_maps_to_auth_required_code() {
    let error = anyhow!(MailboxWriteError::AccountMismatch {
        expected_account_id: String::from("gmail:expected@example.com"),
        outcome_account_id: String::from("gmail:actual@example.com"),
    });

    let report = describe_error(&error, "sync.run");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("auth_required"));
    assert_eq!(
        value["error"]["kind"],
        json!("store.mailbox.write.account_mismatch")
    );
    assert_eq!(value["error"]["operation"], json!("sync.run"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(3));
}

#[test]
fn mailbox_write_attachment_not_found_maps_to_not_found_code() {
    let error = anyhow!(MailboxWriteError::AttachmentNotFound {
        account_id: String::from("gmail:operator@example.com"),
        attachment_key: String::from("m-1:1.2"),
    });

    let report = describe_error(&error, "attachment.fetch");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("not_found"));
    assert_eq!(
        value["error"]["kind"],
        json!("store.mailbox.write.attachment_not_found")
    );
    assert_eq!(value["error"]["operation"], json!("attachment.fetch"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(4));
}

#[test]
fn mailbox_write_invariant_violation_maps_to_internal_failure_code() {
    let error = anyhow!(MailboxWriteError::InvariantViolation {
        operation: "persist_successful_sync_outcome",
        detail: String::from("summary disappeared"),
    });

    let report = describe_error(&error, "sync.run");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("internal_failure"));
    assert_eq!(value["error"]["kind"], json!("store.mailbox.write"));
    assert_eq!(value["error"]["operation"], json!("sync.run"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(10));
}

#[test]
fn mailbox_write_row_count_mismatch_maps_to_internal_failure_code() {
    let error = anyhow!(MailboxWriteError::RowCountMismatch {
        operation: "delete_messages",
        expected: 1,
        actual: 0,
    });

    let report = describe_error(&error, "sync.run");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("internal_failure"));
    assert_eq!(value["error"]["kind"], json!("store.mailbox.write"));
    assert_eq!(value["error"]["operation"], json!("sync.run"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(10));
}

#[test]
fn attachment_show_not_found_maps_to_not_found_exit_code() {
    let error = anyhow!(
        crate::attachments::AttachmentServiceError::AttachmentNotFound {
            attachment_key: String::from("m-1:1.2"),
        }
    );

    let report = describe_error(&error, "attachment.show");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("not_found"));
    assert_eq!(value["error"]["kind"], json!("attachment.not_found"));
    assert_eq!(value["error"]["operation"], json!("attachment.show"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(4));
}

#[test]
fn attachment_not_found_maps_to_not_found_exit_code() {
    let error = anyhow!(
        crate::attachments::AttachmentServiceError::AttachmentNotFound {
            attachment_key: String::from("m-1:1.2"),
        }
    );

    let report = describe_error(&error, "attachment.fetch");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("not_found"));
    assert_eq!(value["error"]["kind"], json!("attachment.not_found"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(4));
}

#[test]
fn attachment_export_store_write_errors_map_to_storage_failure_code() {
    let error = anyhow!(crate::attachments::AttachmentServiceError::StoreWrite {
        source: anyhow!("database is locked"),
    });

    let report = describe_error(&error, "attachment.export");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("attachment.storage"));
    assert_eq!(value["error"]["operation"], json!("attachment.export"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn attachment_export_conflicts_map_to_conflict_exit_code() {
    let error = anyhow!(
        crate::attachments::AttachmentServiceError::DestinationConflict {
            path: std::path::PathBuf::from("/tmp/export.bin"),
        }
    );

    let report = describe_error(&error, "attachment.export");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("conflict"));
    assert_eq!(
        value["error"]["kind"],
        json!("attachment.destination_conflict")
    );
    assert_eq!(value["error"]["operation"], json!("attachment.export"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(5));
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
fn remote_draft_state_reconcile_maps_to_storage_failure_code() {
    let error = anyhow!(WorkflowServiceError::RemoteDraftStateReconcile {
        thread_id: String::from("thread-1"),
        draft_id: String::from("draft-1"),
        source: anyhow!("database is locked"),
    });

    let report = describe_error(&error, "workflow.draft.body");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("workflow.draft.reconcile"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn workflow_account_mismatch_maps_to_auth_required_code() {
    let error = anyhow!(WorkflowServiceError::AuthenticatedAccountMismatch {
        thread_id: String::from("thread-1"),
        expected_account_id: String::from("gmail:other@example.com"),
        actual_account_id: String::from("gmail:operator@example.com"),
    });

    let report = describe_error(&error, "workflow.cleanup");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("auth_required"));
    assert_eq!(value["error"]["kind"], json!("workflow.account.mismatch"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(3));
}

#[test]
fn workflow_time_error_maps_to_internal_failure_code() {
    let error = anyhow!(WorkflowServiceError::Time {
        source: anyhow!("system time before unix epoch"),
    });

    let report = describe_error(&error, "workflow.snooze");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("internal_failure"));
    assert_eq!(value["error"]["kind"], json!("workflow.time"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(10));
}

#[test]
fn workflow_gmail_client_error_maps_to_internal_failure_code() {
    let error = anyhow!(WorkflowServiceError::GmailClientInit {
        source: anyhow!("gmail client init failed"),
    });

    let report = describe_error(&error, "workflow.cleanup");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("internal_failure"));
    assert_eq!(value["error"]["kind"], json!("workflow.gmail_client"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(10));
}

#[test]
fn workflow_repo_root_error_maps_to_internal_failure_code() {
    let error = anyhow!(WorkflowServiceError::RepoRoot {
        source: anyhow!("repo root lookup failed"),
    });

    let report = describe_error(&error, "workflow.draft.attach_remove");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("internal_failure"));
    assert_eq!(value["error"]["kind"], json!("workflow.repo_root"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(10));
}

#[test]
fn workflow_store_write_conflict_maps_to_conflict_code() {
    let error = anyhow!(WorkflowStoreWriteError::Conflict {
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
fn workflow_store_write_read_passthrough_maps_to_read_kind() {
    let error = anyhow!(WorkflowStoreWriteError::Read(WorkflowStoreReadError::Io(
        std::io::Error::new(ErrorKind::NotFound, "missing db"),
    )));

    let report = describe_error(&error, "workflow.promote");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("store.workflow.read"));
    assert_eq!(value["error"]["operation"], json!("workflow.promote"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn store_init_maps_to_workflow_storage_kind() {
    let error = anyhow!(WorkflowServiceError::StoreInit {
        source: anyhow!("disk offline"),
    });

    let report = describe_error(&error, "workflow.show");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["code"], json!("storage_failure"));
    assert_eq!(value["error"]["kind"], json!("workflow.store_init"));
    assert_eq!(value["error"]["operation"], json!("workflow.show"));
    assert_eq!(exit_code(&report), std::process::ExitCode::from(7));
}

#[test]
fn describe_error_preserves_ordered_cause_chain_with_duplicates() {
    let nested = anyhow!("leaf");
    let wrapped = nested.context("leaf").context("top");

    let report = describe_error(&wrapped, "workflow.show");
    let value = to_value(json_failure_value(&report)).unwrap();

    assert_eq!(value["error"]["message"], json!("top"));
    assert_eq!(value["error"]["causes"], json!(["leaf", "leaf"]));
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

#[test]
fn automation_apply_auth_failure_uses_auth_exit_code_in_json_and_human_modes() {
    use std::process::Command;
    use tempfile::TempDir;

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let repo_root = TempDir::new().unwrap();
    std::fs::create_dir(repo_root.path().join(".git")).unwrap();
    let config_dir = repo_root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();

    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    store::init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    let detail = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id,
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("hash"),
            selected_rule_ids: vec![String::from("archive-digest")],
            created_at_epoch_s: 100,
            candidates: vec![NewAutomationRunCandidate {
                rule_id: String::from("archive-digest"),
                thread_id: String::from("thread-1"),
                message_id: String::from("message-1"),
                internal_date_epoch_ms: 1_700_000_000_000,
                subject: String::from("Daily digest"),
                from_header: String::from("Digest <digest@example.com>"),
                from_address: Some(String::from("digest@example.com")),
                snippet: String::from("Digest snippet"),
                label_names: vec![String::from("INBOX")],
                attachment_count: 0,
                has_list_unsubscribe: true,
                list_id_header: Some(String::from("<digest.example.com>")),
                list_unsubscribe_header: Some(String::from("<mailto:unsubscribe@example.com>")),
                list_unsubscribe_post_header: None,
                precedence_header: Some(String::from("bulk")),
                auto_submitted_header: None,
                action: AutomationActionSnapshot {
                    kind: AutomationActionKind::Archive,
                    add_label_ids: Vec::new(),
                    add_label_names: Vec::new(),
                    remove_label_ids: vec![String::from("INBOX")],
                    remove_label_names: vec![String::from("INBOX")],
                },
                reason: AutomationMatchReason {
                    from_address: Some(String::from("digest@example.com")),
                    subject_terms: vec![String::from("digest")],
                    label_names: vec![String::from("INBOX")],
                    older_than_days: Some(7),
                    has_attachments: Some(false),
                    has_list_unsubscribe: Some(true),
                    list_id_terms: vec![String::from("digest")],
                    precedence_values: vec![String::from("bulk")],
                },
            }],
        },
    )
    .unwrap();

    let report = describe_error(
        &anyhow!(GmailClientError::MissingCredentials),
        "automation.apply",
    );
    let expected_json = to_value(json_failure_value(&report)).unwrap();

    let json_output = Command::new(&cargo)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            &manifest_path,
            "--",
            "automation",
            "apply",
            &detail.run.run_id.to_string(),
            "--execute",
            "--json",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env_remove("HOME")
        .current_dir(repo_root.path())
        .output()
        .unwrap();
    assert_eq!(json_output.status.code(), Some(3));
    assert!(json_output.stderr.is_empty());

    let json_stdout = String::from_utf8(json_output.stdout).unwrap();
    let json_value: serde_json::Value = serde_json::from_str(&json_stdout).unwrap();
    assert_eq!(json_value, expected_json);

    let human_output = Command::new(&cargo)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            &manifest_path,
            "--",
            "automation",
            "apply",
            &detail.run.run_id.to_string(),
            "--execute",
        ])
        .env("XDG_CONFIG_HOME", &config_dir)
        .env_remove("HOME")
        .current_dir(repo_root.path())
        .output()
        .unwrap();
    assert_eq!(human_output.status.code(), Some(3));
    assert!(human_output.stdout.is_empty());
    let human_stderr = String::from_utf8(human_output.stderr).unwrap();
    let human_stderr_lower = human_stderr.to_lowercase();
    assert!(human_stderr_lower.contains("mailroom is not authenticated"));
    assert!(human_stderr_lower.contains("mailroom auth login"));
}

#[tokio::test]
async fn attachment_fetch_cli_contract_maps_zero_row_vault_update_to_not_found() {
    use std::process::Command;
    use tempfile::TempDir;

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
    let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
    let repo_root = TempDir::new().unwrap();
    fs::create_dir(repo_root.path().join(".git")).unwrap();
    let home_dir = TempDir::new().unwrap();
    let xdg_config_home = home_dir.path().join(".config");

    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    store::init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    store::mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Attachment fixture"),
            subject: String::from("Fixture"),
            from_header: String::from("Fixture <fixture@example.com>"),
            from_address: Some(String::from("fixture@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 256,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("m-1:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("fixture.bin"),
                mime_type: String::from("application/octet-stream"),
                size_bytes: 5,
                content_disposition: Some(String::from("attachment")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    let credentials_path = config_report
        .config
        .gmail
        .credential_path(&config_report.config.workspace);
    FileCredentialStore::new(credentials_path)
        .save(&StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("fixture-access-token")),
            refresh_token: None,
            expires_at_epoch_s: Some(u64::MAX),
            scopes: vec![String::from("https://www.googleapis.com/auth/gmail.modify")],
        })
        .unwrap();

    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER test_ignore_attachment_vault_update
             BEFORE UPDATE OF
                 vault_content_hash,
                 vault_relative_path,
                 vault_size_bytes,
                 vault_fetched_at_epoch_s
             ON gmail_message_attachments
             FOR EACH ROW
             BEGIN
                 SELECT RAISE(IGNORE);
             END;",
        )
        .unwrap();

    let gmail_api = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-1/attachments/att-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": "aGVsbG8",
            "size": 5
        })))
        .mount(&gmail_api)
        .await;

    let output = Command::new(&cargo)
        .args([
            "run",
            "--quiet",
            "--manifest-path",
            &manifest_path,
            "--",
            "attachment",
            "fetch",
            "m-1:1.2",
            "--json",
        ])
        .env("XDG_CONFIG_HOME", &xdg_config_home)
        .env(
            "MAILROOM_GMAIL__API_BASE_URL",
            format!("{}/gmail/v1", gmail_api.uri()),
        )
        .current_dir(repo_root.path())
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(4));
    assert!(output.stderr.is_empty());

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["success"], json!(false));
    assert_eq!(value["error"]["code"], json!("not_found"));
    assert_eq!(value["error"]["kind"], json!("attachment.not_found"));
    assert_eq!(value["error"]["operation"], json!("attachment.fetch"));
}
