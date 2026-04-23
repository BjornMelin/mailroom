use super::*;

#[test]
fn mark_sent_reports_missing_workflow_for_unknown_thread() {
    let repo_root = unique_temp_dir("mailroom-workflow-mark-sent-missing");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    let error = mark_sent(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &MarkSentInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-missing"),
            sent_message_id: String::from("sent-message-1"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "no workflow found for thread thread-missing"
    );
}

#[test]
fn ready_to_send_requires_current_draft_revision() {
    let repo_root = unique_temp_dir("mailroom-workflow-ready-guard");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-no-draft"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: snapshot("message-1", "Project status"),
            updated_at_epoch_s: 100,
        },
    )
    .unwrap();

    let error = upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-no-draft"),
            to_stage: WorkflowStage::ReadyToSend,
            snapshot: snapshot("message-1", "Project status"),
            updated_at_epoch_s: 200,
        },
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "ready_to_send requires a current draft revision and synced Gmail draft"
    );
}

#[test]
fn ready_to_send_requires_synced_gmail_draft() {
    let repo_root = unique_temp_dir("mailroom-workflow-ready-remote-guard");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(
        &config_report,
        &account.account_id,
        "thread-no-remote-draft",
    );

    let error = upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-no-remote-draft"),
            to_stage: WorkflowStage::ReadyToSend,
            snapshot: snapshot("message-2", "Re: Project status"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "ready_to_send requires a current draft revision and synced Gmail draft"
    );
}

#[test]
fn ready_to_send_allows_existing_synced_draft() {
    let repo_root = unique_temp_dir("mailroom-workflow-ready-allowed");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(&config_report, &account.account_id, "thread-ready");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-ready"),
            gmail_draft_id: Some(String::from("gmail-draft-1")),
            gmail_draft_message_id: Some(String::from("gmail-message-1")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-1")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();

    let workflow = upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-ready"),
            to_stage: WorkflowStage::ReadyToSend,
            snapshot: snapshot("message-2", "Re: Project status"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::ReadyToSend);
    assert!(workflow.current_draft_revision_id.is_some());
    assert!(workflow.gmail_draft_id.is_some());
}

#[test]
fn mark_sent_retires_local_and_remote_draft_state() {
    let repo_root = unique_temp_dir("mailroom-workflow-sent");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(&config_report, &account.account_id, "thread-sent");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-sent"),
            gmail_draft_id: Some(String::from("gmail-draft-1")),
            gmail_draft_message_id: Some(String::from("gmail-message-1")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-1")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();

    let workflow = mark_sent(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &MarkSentInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-sent"),
            sent_message_id: String::from("sent-message-1"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Sent);
    assert_eq!(workflow.current_draft_revision_id, None);
    assert_eq!(workflow.gmail_draft_id, None);
    assert_eq!(workflow.gmail_draft_message_id, None);
    assert_eq!(workflow.gmail_draft_thread_id, None);

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-sent",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.current_stage, WorkflowStage::Sent);
    assert_eq!(detail.current_draft, None);
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.event_kind == "draft_sent")
    );
}
