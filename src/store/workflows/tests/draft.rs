use super::*;

#[test]
fn retire_draft_state_reports_missing_workflow_for_unknown_thread() {
    let repo_root = unique_temp_dir("mailroom-workflow-retire-draft-missing");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    let error = retire_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RetireDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-missing"),
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
fn upsert_draft_revision_persists_current_draft_and_attachments() {
    let repo_root = unique_temp_dir("mailroom-workflow-draft");
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
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: snapshot("message-1", "Project status"),
            updated_at_epoch_s: 100,
        },
    )
    .unwrap();

    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("gmail-draft-1")),
            gmail_draft_message_id: Some(String::from("gmail-message-1")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-1")),
            updated_at_epoch_s: 150,
        },
    )
    .unwrap();

    let attachment_path = repo_root.path().join("reply.txt");
    std::fs::write(&attachment_path, "hello from attachment").unwrap();

    let (workflow, revision) = upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::ReplyAll,
            source_message_id: String::from("message-1"),
            subject: String::from("Re: Project status"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: vec![String::from("bob@example.com")],
            bcc_addresses: vec![],
            body_text: String::from("Draft body"),
            attachments: vec![AttachmentInput {
                path: attachment_path.display().to_string(),
                file_name: String::from("reply.txt"),
                mime_type: String::from("text/plain"),
                size_bytes: 21,
            }],
            snapshot: snapshot("message-2", "Re: Project status"),
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Drafting);
    assert_eq!(
        workflow.current_draft_revision_id,
        Some(revision.draft_revision_id)
    );
    assert_eq!(workflow.gmail_draft_id.as_deref(), Some("gmail-draft-1"));
    assert_eq!(workflow.gmail_draft_message_id, None);
    assert_eq!(workflow.gmail_draft_thread_id, None);
    assert_eq!(workflow.last_remote_sync_epoch_s, None);

    let ready_error = upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            to_stage: WorkflowStage::ReadyToSend,
            snapshot: snapshot("message-2", "Re: Project status"),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap_err();
    assert_eq!(
        ready_error.to_string(),
        "ready_to_send requires a current draft revision and synced Gmail draft"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    let draft = detail.current_draft.unwrap();
    assert_ne!(detail.workflow.current_stage, WorkflowStage::ReadyToSend);
    assert_eq!(detail.workflow.gmail_draft_message_id, None);
    assert_eq!(detail.workflow.gmail_draft_thread_id, None);
    assert_eq!(detail.workflow.last_remote_sync_epoch_s, None);

    assert_eq!(draft.revision.reply_mode, ReplyMode::ReplyAll);
    assert_eq!(draft.revision.subject, "Re: Project status");
    assert_eq!(draft.revision.to_addresses, vec!["alice@example.com"]);
    assert_eq!(draft.revision.cc_addresses, vec!["bob@example.com"]);
    assert_eq!(draft.revision.body_text, "Draft body");
    assert_eq!(draft.attachments.len(), 1);
    assert_eq!(draft.attachments[0].file_name, "reply.txt");
    assert_eq!(detail.events.len(), 3);
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.event_kind == "draft_revision_upserted")
    );
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.event_kind == "remote_draft_synced")
    );
}

#[test]
fn closed_stage_promotion_preserves_draft_state_until_remote_retirement() {
    let repo_root = unique_temp_dir("mailroom-workflow-closed-promotion");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(&config_report, &account.account_id, "thread-closed");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-closed"),
            gmail_draft_id: Some(String::from("gmail-draft-3")),
            gmail_draft_message_id: Some(String::from("gmail-message-3")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-3")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();

    let workflow = upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-closed"),
            to_stage: WorkflowStage::Closed,
            snapshot: snapshot("message-2", "Re: Project status"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Closed);
    assert!(workflow.current_draft_revision_id.is_some());
    assert_eq!(workflow.gmail_draft_id.as_deref(), Some("gmail-draft-3"));
    assert_eq!(
        workflow.gmail_draft_message_id.as_deref(),
        Some("gmail-message-3")
    );
    assert_eq!(
        workflow.gmail_draft_thread_id.as_deref(),
        Some("gmail-thread-3")
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-closed",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.current_stage, WorkflowStage::Closed);
    assert!(detail.current_draft.is_some());
    assert!(
        detail
            .events
            .iter()
            .any(|event| event.event_kind == "stage_promoted")
    );
}

#[test]
fn retire_draft_state_clears_local_and_remote_draft_fields() {
    let repo_root = unique_temp_dir("mailroom-workflow-retire-draft-state");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(&config_report, &account.account_id, "thread-retire");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-retire"),
            gmail_draft_id: Some(String::from("gmail-draft-4")),
            gmail_draft_message_id: Some(String::from("gmail-message-4")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-4")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();

    let workflow = retire_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RetireDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-retire"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Drafting);
    assert_eq!(workflow.current_draft_revision_id, None);
    assert_eq!(workflow.gmail_draft_id, None);
    assert_eq!(workflow.gmail_draft_message_id, None);
    assert_eq!(workflow.gmail_draft_thread_id, None);
    assert_eq!(workflow.last_remote_sync_epoch_s, Some(300));
}
