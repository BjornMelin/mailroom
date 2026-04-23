use super::*;

fn workflow_event_payload(
    detail: &crate::store::workflows::WorkflowDetail,
    event_kind: &str,
) -> serde_json::Value {
    let event = detail
        .events
        .iter()
        .find(|event| event.event_kind == event_kind)
        .unwrap();
    serde_json::from_str(&event.payload_json).unwrap()
}

#[test]
fn retire_draft_state_reports_missing_workflow_for_unknown_thread() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-retire-draft-missing");

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
    let (repo_root, config_report, account) = bootstrap_test_env("mailroom-workflow-draft");

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
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-closed-promotion");

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
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-retire-draft-state");

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
    let original_detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-retire",
    )
    .unwrap()
    .unwrap();
    let original_workflow = &original_detail.workflow;

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

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-retire",
    )
    .unwrap()
    .unwrap();
    let payload = workflow_event_payload(&detail, "draft_state_retired");
    assert_eq!(
        payload["previous_draft_revision_id"],
        serde_json::json!(original_workflow.current_draft_revision_id)
    );
    assert_eq!(
        payload["previous_gmail_draft_id"],
        serde_json::json!(original_workflow.gmail_draft_id.as_deref())
    );
    assert_eq!(
        payload["previous_gmail_draft_message_id"],
        serde_json::json!(original_workflow.gmail_draft_message_id.as_deref())
    );
    assert_eq!(
        payload["previous_gmail_draft_thread_id"],
        serde_json::json!(original_workflow.gmail_draft_thread_id.as_deref())
    );
}

#[test]
fn restore_draft_state_with_expected_version_records_restore_event() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-restore-draft-state");

    seed_drafting_workflow(&config_report, &account.account_id, "thread-restore");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-restore"),
            gmail_draft_id: Some(String::from("gmail-draft-5")),
            gmail_draft_message_id: Some(String::from("gmail-message-5")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-5")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();
    let original_detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-restore",
    )
    .unwrap()
    .unwrap();
    let original_draft_revision_id = original_detail.workflow.current_draft_revision_id;

    let retired_workflow = retire_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RetireDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-restore"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    let restored_workflow = restore_draft_state_with_expected_version(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RestoreDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-restore"),
            current_draft_revision_id: original_draft_revision_id,
            gmail_draft_id: Some(String::from("gmail-draft-5")),
            gmail_draft_message_id: Some(String::from("gmail-message-5")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-5")),
            updated_at_epoch_s: 350,
        },
        Some(retired_workflow.workflow_version),
    )
    .unwrap();

    assert_eq!(
        restored_workflow.current_draft_revision_id,
        original_draft_revision_id
    );
    assert_eq!(
        restored_workflow.gmail_draft_id.as_deref(),
        Some("gmail-draft-5")
    );
    assert_eq!(
        restored_workflow.gmail_draft_message_id.as_deref(),
        Some("gmail-message-5")
    );
    assert_eq!(
        restored_workflow.gmail_draft_thread_id.as_deref(),
        Some("gmail-thread-5")
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-restore",
    )
    .unwrap()
    .unwrap();
    let retired_payload = workflow_event_payload(&detail, "draft_state_retired");
    assert_eq!(
        retired_payload["previous_draft_revision_id"],
        serde_json::json!(original_detail.workflow.current_draft_revision_id)
    );
    assert_eq!(
        retired_payload["previous_gmail_draft_id"],
        serde_json::json!(original_detail.workflow.gmail_draft_id.as_deref())
    );
    assert_eq!(
        retired_payload["previous_gmail_draft_message_id"],
        serde_json::json!(original_detail.workflow.gmail_draft_message_id.as_deref())
    );
    assert_eq!(
        retired_payload["previous_gmail_draft_thread_id"],
        serde_json::json!(original_detail.workflow.gmail_draft_thread_id.as_deref())
    );

    let restored_payload = workflow_event_payload(&detail, "draft_state_restored");
    assert_eq!(
        restored_payload["previous_draft_revision_id"],
        serde_json::json!(retired_workflow.current_draft_revision_id)
    );
    assert_eq!(
        restored_payload["previous_gmail_draft_id"],
        serde_json::json!(retired_workflow.gmail_draft_id.as_deref())
    );
    assert_eq!(
        restored_payload["previous_gmail_draft_message_id"],
        serde_json::json!(retired_workflow.gmail_draft_message_id.as_deref())
    );
    assert_eq!(
        restored_payload["previous_gmail_draft_thread_id"],
        serde_json::json!(retired_workflow.gmail_draft_thread_id.as_deref())
    );
    assert_eq!(
        restored_payload["restored_draft_revision_id"],
        serde_json::json!(original_draft_revision_id)
    );
    assert_eq!(
        restored_payload["restored_gmail_draft_id"],
        serde_json::json!(Some("gmail-draft-5"))
    );
    assert_eq!(
        restored_payload["restored_gmail_draft_message_id"],
        serde_json::json!(Some("gmail-message-5"))
    );
    assert_eq!(
        restored_payload["restored_gmail_draft_thread_id"],
        serde_json::json!(Some("gmail-thread-5"))
    );
}
