use super::*;

#[test]
fn apply_cleanup_reports_missing_workflow_for_unknown_thread() {
    let repo_root = unique_temp_dir("mailroom-workflow-apply-cleanup-missing");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    let error = apply_cleanup(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &ApplyCleanupInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-missing"),
            cleanup_action: CleanupAction::Archive,
            payload_json: String::from(r#"{"execute":true}"#),
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
fn workflow_listing_orders_stage_then_bucket_and_cleanup_closes_flow() {
    let repo_root = unique_temp_dir("mailroom-workflow-listing");
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
            thread_id: String::from("thread-urgent"),
            triage_bucket: TriageBucket::Urgent,
            note: None,
            snapshot: snapshot("message-urgent", "Urgent"),
            updated_at_epoch_s: 100,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-fyi"),
            triage_bucket: TriageBucket::Fyi,
            note: None,
            snapshot: snapshot("message-fyi", "FYI"),
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-follow-up"),
            to_stage: WorkflowStage::FollowUp,
            snapshot: snapshot("message-follow-up", "Follow-up"),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let promoted_detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-follow-up",
    )
    .unwrap()
    .unwrap();
    let promoted_event = promoted_detail
        .events
        .iter()
        .find(|event| event.event_kind == "stage_promoted")
        .unwrap();
    assert_eq!(promoted_event.from_stage, None);

    let listed = list_workflows(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &WorkflowListFilter {
            account_id: account.account_id.clone(),
            stage: None,
            triage_bucket: None,
        },
    )
    .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|workflow| workflow.thread_id.as_str())
            .collect::<Vec<_>>(),
        vec!["thread-urgent", "thread-fyi", "thread-follow-up"]
    );

    let cleaned = apply_cleanup(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &ApplyCleanupInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-follow-up"),
            cleanup_action: CleanupAction::Archive,
            payload_json: String::from(r#"{"execute":true}"#),
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();
    assert_eq!(cleaned.current_stage, WorkflowStage::Closed);
    assert_eq!(cleaned.last_cleanup_action, Some(CleanupAction::Archive));

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-follow-up",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.current_stage, WorkflowStage::Closed);
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.event_kind == "cleanup_applied")
    );

    let doctor = inspect_workflows(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(doctor.workflow_count, 3);
    assert_eq!(doctor.open_workflow_count, 2);
    assert_eq!(doctor.draft_workflow_count, 0);
}

#[test]
fn cleanup_marks_closed_while_preserving_draft_state_for_reconciliation() {
    let repo_root = unique_temp_dir("mailroom-workflow-cleanup");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(&config_report, &account.account_id, "thread-cleanup");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-cleanup"),
            gmail_draft_id: Some(String::from("gmail-draft-2")),
            gmail_draft_message_id: Some(String::from("gmail-message-2")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-2")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();

    let workflow = apply_cleanup(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &ApplyCleanupInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-cleanup"),
            cleanup_action: CleanupAction::Trash,
            payload_json: String::from(r#"{"execute":true}"#),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Closed);
    assert!(workflow.current_draft_revision_id.is_some());
    assert_eq!(workflow.gmail_draft_id.as_deref(), Some("gmail-draft-2"));
    assert_eq!(
        workflow.gmail_draft_message_id.as_deref(),
        Some("gmail-message-2")
    );
    assert_eq!(
        workflow.gmail_draft_thread_id.as_deref(),
        Some("gmail-thread-2")
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-cleanup",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.current_stage, WorkflowStage::Closed);
    assert_eq!(
        detail.workflow.last_cleanup_action,
        Some(CleanupAction::Trash)
    );
    assert!(detail.current_draft.is_some());
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.event_kind == "cleanup_applied")
    );
}
