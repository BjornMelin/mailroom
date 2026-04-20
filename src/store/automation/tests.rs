use super::{
    AppendAutomationRunEventInput, AutomationActionKind, AutomationActionSnapshot,
    AutomationApplyStatus, AutomationMatchReason, AutomationRunStatus, CandidateApplyResultInput,
    CreateAutomationRunInput, FinalizeAutomationRunInput, NewAutomationRunCandidate,
    append_automation_run_event, create_automation_run, finalize_automation_run,
    get_automation_run_detail, inspect_automation, list_latest_thread_candidates,
    record_candidate_apply_result,
};
use crate::config::resolve;
use crate::store::{accounts, init, mailbox};
use crate::workspace::WorkspacePaths;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn create_automation_run_persists_detail_and_doctor_counts() {
    let repo_root = unique_temp_dir("mailroom-automation-run-roundtrip");
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
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

    fs::remove_dir_all(repo_root).unwrap();
}

#[test]
fn list_latest_thread_candidates_returns_latest_message_and_headers() {
    let repo_root = unique_temp_dir("mailroom-automation-thread-candidates");
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
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

    fs::remove_dir_all(repo_root).unwrap();
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

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}
