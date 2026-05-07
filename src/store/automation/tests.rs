use super::{
    AppendAutomationRunEventInput, AutomationActionKind, AutomationActionSnapshot,
    AutomationApplyStatus, AutomationMatchReason, AutomationRunStatus, AutomationStoreWriteError,
    CandidateApplyResultInput, CreateAutomationRunInput, FinalizeAutomationRunInput,
    NewAutomationRunCandidate, PruneAutomationRunsInput, append_automation_run_event,
    claim_automation_run_for_apply, create_automation_run, finalize_automation_run,
    get_automation_run_detail, inspect_automation, list_latest_thread_candidates,
    prune_automation_runs, record_candidate_apply_result,
};
use crate::config::resolve;
use crate::store::{accounts, init, mailbox};
use crate::workspace::WorkspacePaths;
use rusqlite::Connection;
use tempfile::TempDir;

#[test]
fn create_automation_run_persists_detail_and_doctor_counts() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    let detail = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("abc123"),
            selected_rule_ids: vec![String::from("archive-digest")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();

    assert_eq!(detail.run.status, AutomationRunStatus::Previewed);
    assert_eq!(detail.candidates.len(), 1);
    assert_eq!(detail.events.len(), 1);

    append_automation_run_event(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AppendAutomationRunEventInput {
            run_id: detail.run.run_id,
            account_id: account.account_id.clone(),
            event_kind: String::from("apply_started"),
            payload_json: String::from("{\"candidate_count\":1}"),
            created_at_epoch_s: 101,
        },
    )
    .unwrap();
    record_candidate_apply_result(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CandidateApplyResultInput {
            run_id: detail.run.run_id,
            candidate_id: detail.candidates[0].candidate_id,
            status: AutomationApplyStatus::Succeeded,
            applied_at_epoch_s: 102,
            apply_error: None,
        },
    )
    .unwrap();
    finalize_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &FinalizeAutomationRunInput {
            run_id: detail.run.run_id,
            status: AutomationRunStatus::Applied,
            applied_at_epoch_s: 103,
        },
    )
    .unwrap();

    let detail = get_automation_run_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        detail.run.run_id,
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.run.status, AutomationRunStatus::Applied);
    assert_eq!(
        detail.candidates[0].apply_status,
        Some(AutomationApplyStatus::Succeeded)
    );
    assert_eq!(detail.events.len(), 2);

    let doctor = inspect_automation(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(doctor.run_count, 1);
    assert_eq!(doctor.previewed_run_count, 0);
    assert_eq!(doctor.applied_run_count, 1);
    assert_eq!(doctor.candidate_count, 1);
}

#[test]
fn prune_automation_runs_dry_run_reports_without_deleting() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);
    let old_run = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("old"),
            selected_rule_ids: vec![String::from("archive-old")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();
    create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("new"),
            selected_rule_ids: vec![String::from("archive-new")],
            created_at_epoch_s: 1_000,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();

    let report = prune_automation_runs(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PruneAutomationRunsInput {
            account_id: account.account_id,
            cutoff_epoch_s: 500,
            statuses: vec![AutomationRunStatus::Previewed],
            execute: false,
        },
    )
    .unwrap();

    assert_eq!(report.matched_run_count, 1);
    assert_eq!(report.matched_candidate_count, 1);
    assert_eq!(report.matched_event_count, 1);
    assert_eq!(report.deleted_run_count, 0);
    assert!(
        get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            old_run.run.run_id,
        )
        .unwrap()
        .is_some()
    );
    let doctor = inspect_automation(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(doctor.run_count, 2);
}

#[test]
fn prune_automation_runs_execute_deletes_runs_and_cascades_detail() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);
    let old_run = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("old"),
            selected_rule_ids: vec![String::from("archive-old")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();
    let applying_run = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("applying"),
            selected_rule_ids: vec![String::from("archive-applying")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();
    finalize_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &FinalizeAutomationRunInput {
            run_id: applying_run.run.run_id,
            status: AutomationRunStatus::Applying,
            applied_at_epoch_s: 101,
        },
    )
    .unwrap();

    let report = prune_automation_runs(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PruneAutomationRunsInput {
            account_id: account.account_id,
            cutoff_epoch_s: 500,
            statuses: vec![
                AutomationRunStatus::Previewed,
                AutomationRunStatus::Applied,
                AutomationRunStatus::ApplyFailed,
            ],
            execute: true,
        },
    )
    .unwrap();

    assert_eq!(report.matched_run_count, 1);
    assert_eq!(report.matched_candidate_count, 1);
    assert_eq!(report.matched_event_count, 1);
    assert_eq!(report.deleted_run_count, 1);
    assert!(
        get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            old_run.run.run_id,
        )
        .unwrap()
        .is_none()
    );
    assert!(
        get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            applying_run.run.run_id,
        )
        .unwrap()
        .is_some()
    );
    let doctor = inspect_automation(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(doctor.run_count, 1);
    assert_eq!(doctor.candidate_count, 1);
}

#[test]
fn append_automation_run_event_rejects_account_mismatch() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);
    let detail = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("abc123"),
            selected_rule_ids: vec![String::from("archive-digest")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();

    let other_account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
            history_id: String::from("54321"),
            messages_total: 4,
            threads_total: 2,
            access_scope: String::from("scope:b"),
            refreshed_at_epoch_s: 101,
        },
    )
    .unwrap();

    let error = append_automation_run_event(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AppendAutomationRunEventInput {
            run_id: detail.run.run_id,
            account_id: other_account.account_id.clone(),
            event_kind: String::from("apply_started"),
            payload_json: String::from("{\"candidate_count\":1}"),
            created_at_epoch_s: 102,
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        AutomationStoreWriteError::RunAccountMismatch {
            run_id,
            expected_account_id,
            actual_account_id
        } if run_id == detail.run.run_id
            && expected_account_id == account.account_id
            && actual_account_id == other_account.account_id
    ));

    let detail = get_automation_run_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        detail.run.run_id,
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.events.len(), 1);
}

#[test]
fn claim_automation_run_for_apply_transitions_previewed_runs_once() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);
    let detail = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("hash"),
            selected_rule_ids: vec![String::from("archive-digest")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();

    claim_automation_run_for_apply(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        detail.run.run_id,
        101,
    )
    .unwrap();

    let error = claim_automation_run_for_apply(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        detail.run.run_id,
        102,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        AutomationStoreWriteError::RowCountMismatch {
            operation,
            expected: 1,
            actual: 0,
        } if operation == "claim_automation_run_for_apply"
    ));

    let refreshed = get_automation_run_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        detail.run.run_id,
    )
    .unwrap()
    .unwrap();
    assert_eq!(refreshed.run.status, AutomationRunStatus::Applying);
    assert_eq!(refreshed.run.applied_at_epoch_s, Some(101));
}

#[test]
fn record_candidate_apply_result_rejects_missing_candidate() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);
    let detail = create_automation_run(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CreateAutomationRunInput {
            account_id: account.account_id.clone(),
            rule_file_path: String::from(".mailroom/automation.toml"),
            rule_file_hash: String::from("hash"),
            selected_rule_ids: vec![String::from("archive-digest")],
            created_at_epoch_s: 100,
            candidates: vec![sample_candidate()],
        },
    )
    .unwrap();

    let error = record_candidate_apply_result(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &CandidateApplyResultInput {
            run_id: detail.run.run_id,
            candidate_id: detail.candidates[0].candidate_id + 1,
            status: AutomationApplyStatus::Succeeded,
            applied_at_epoch_s: 101,
            apply_error: None,
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        AutomationStoreWriteError::MissingCandidate {
            run_id,
            candidate_id,
        } if run_id == detail.run.run_id
            && candidate_id == detail.candidates[0].candidate_id + 1
    ));

    let refreshed = get_automation_run_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        detail.run.run_id,
    )
    .unwrap()
    .unwrap();
    assert_eq!(refreshed.candidates[0].apply_status, None);
}

#[test]
fn list_latest_thread_candidates_returns_latest_message_and_headers() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    mailbox::replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        &[crate::gmail::GmailLabel {
            id: String::from("INBOX"),
            name: String::from("INBOX"),
            label_type: String::from("system"),
            message_list_visibility: None,
            label_list_visibility: None,
            messages_total: None,
            messages_unread: None,
            threads_total: None,
            threads_unread: None,
        }],
        100,
    )
    .unwrap();
    mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[
            mailbox_message(
                &account.account_id,
                "thread-1",
                "message-1",
                100,
                None,
                None,
            ),
            mailbox_message(
                &account.account_id,
                "thread-1",
                "message-2",
                200,
                Some(String::from("<list.example.com>")),
                Some(String::from("<mailto:list@example.com>")),
            ),
        ],
        200,
    )
    .unwrap();

    let candidates = list_latest_thread_candidates(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
    )
    .unwrap();

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].message_id, "message-2");
    assert_eq!(
        candidates[0].list_id_header.as_deref(),
        Some("<list.example.com>")
    );
    assert_eq!(candidates[0].label_names, vec![String::from("INBOX")]);
}

#[test]
fn list_latest_thread_candidates_returns_empty_for_pre_migration_schemas() {
    let repo_root = temp_repo_root();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();

    let connection = Connection::open(&config_report.config.store.database_path).unwrap();
    connection
        .execute_batch(
            "
            CREATE TABLE gmail_messages (
                message_rowid INTEGER PRIMARY KEY,
                account_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                internal_date_epoch_ms INTEGER NOT NULL,
                subject TEXT NOT NULL,
                from_header TEXT NOT NULL,
                from_address TEXT,
                snippet TEXT NOT NULL
            );
            CREATE TABLE gmail_message_labels (
                message_rowid INTEGER NOT NULL,
                label_id TEXT NOT NULL
            );
            CREATE TABLE gmail_labels (
                account_id TEXT NOT NULL,
                label_id TEXT NOT NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE gmail_message_attachments (
                account_id TEXT NOT NULL,
                message_rowid INTEGER NOT NULL
            );
            ",
        )
        .unwrap();

    let candidates = list_latest_thread_candidates(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap();

    assert!(candidates.is_empty());
}

fn sample_candidate() -> NewAutomationRunCandidate {
    NewAutomationRunCandidate {
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
    }
}

fn mailbox_message(
    account_id: &str,
    thread_id: &str,
    message_id: &str,
    internal_date_epoch_ms: i64,
    list_id_header: Option<String>,
    list_unsubscribe_header: Option<String>,
) -> mailbox::GmailMessageUpsertInput {
    mailbox::GmailMessageUpsertInput {
        account_id: account_id.to_owned(),
        message_id: message_id.to_owned(),
        thread_id: thread_id.to_owned(),
        history_id: format!("{}", internal_date_epoch_ms / 100),
        internal_date_epoch_ms,
        snippet: format!("Snippet for {message_id}"),
        subject: format!("Subject for {message_id}"),
        from_header: String::from("Sender <sender@example.com>"),
        from_address: Some(String::from("sender@example.com")),
        recipient_headers: String::from("operator@example.com"),
        to_header: String::from("operator@example.com"),
        cc_header: String::new(),
        bcc_header: String::new(),
        reply_to_header: String::new(),
        size_estimate: 128,
        automation_headers: mailbox::GmailAutomationHeaders {
            list_id_header,
            list_unsubscribe_header,
            list_unsubscribe_post_header: None,
            precedence_header: Some(String::from("bulk")),
            auto_submitted_header: None,
        },
        label_ids: vec![String::from("INBOX")],
        label_names_text: String::from("INBOX"),
        attachments: Vec::new(),
    }
}

fn seed_account(config_report: &crate::config::ConfigReport) -> accounts::AccountRecord {
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 10,
            threads_total: 10,
            access_scope: String::from("https://www.googleapis.com/auth/gmail.modify"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap()
}

fn temp_repo_root() -> TempDir {
    TempDir::new().unwrap()
}
