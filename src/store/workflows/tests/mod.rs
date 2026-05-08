use super::{
    ApplyCleanupInput, AttachmentInput, CleanupAction, ClearWorkflowSnoozeInput, MarkSentInput,
    PromoteWorkflowInput, RemoteDraftStateInput, ReplyMode, RestoreDraftStateInput,
    RetireDraftStateInput, SetTriageStateInput, SnoozeWorkflowInput, TriageBucket,
    UpsertDraftRevisionInput, WorkflowListFilter, WorkflowMessageSnapshot, WorkflowRecord,
    WorkflowStage, WorkflowStoreWriteError, apply_cleanup, clear_workflow_snooze,
    get_workflow_detail, inspect_workflows, list_workflows, mark_sent,
    restore_draft_state_with_expected_version, retire_draft_state, set_remote_draft_state,
    set_remote_draft_state_with_expected_version, set_triage_state, snooze_workflow,
    upsert_draft_revision, upsert_stage, write,
};
use crate::config::resolve;
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
use tempfile::{Builder, TempDir};

mod cleanup;
mod concurrency;
mod draft;
mod send;
mod triage;

fn seed_account(config_report: &crate::config::ConfigReport) -> accounts::AccountRecord {
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 10,
            threads_total: 8,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap()
}

fn bootstrap_test_env(
    prefix: &str,
) -> (
    TempDir,
    crate::config::ConfigReport,
    accounts::AccountRecord,
) {
    let repo_root = unique_temp_dir(prefix);
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);
    (repo_root, config_report, account)
}

fn seed_drafting_workflow(
    config_report: &crate::config::ConfigReport,
    account_id: &str,
    thread_id: &str,
) {
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account_id.to_owned(),
            thread_id: thread_id.to_owned(),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: snapshot("message-1", "Project status"),
            updated_at_epoch_s: 100,
        },
    )
    .unwrap();

    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: account_id.to_owned(),
            thread_id: thread_id.to_owned(),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("message-1"),
            subject: String::from("Re: Project status"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: vec![],
            bcc_addresses: vec![],
            body_text: String::from("Draft body"),
            attachments: vec![],
            snapshot: snapshot("message-2", "Re: Project status"),
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();
}

fn snapshot(message_id: &str, subject: &str) -> WorkflowMessageSnapshot {
    WorkflowMessageSnapshot {
        message_id: message_id.to_owned(),
        internal_date_epoch_ms: 1_700_000_000_000,
        subject: subject.to_owned(),
        from_header: String::from("Alice <alice@example.com>"),
        snippet: format!("Snippet for {subject}"),
    }
}

fn unique_temp_dir(prefix: &str) -> TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("failed to create temp dir")
}
