use super::{
    ApplyCleanupInput, AttachmentInput, CleanupAction, MarkSentInput, PromoteWorkflowInput,
    RemoteDraftStateInput, ReplyMode, RetireDraftStateInput, SetTriageStateInput, TriageBucket,
    UpsertDraftRevisionInput, WorkflowListFilter, WorkflowMessageSnapshot, WorkflowStage,
    apply_cleanup, get_workflow_detail, inspect_workflows, list_workflows, mark_sent,
    retire_draft_state, set_remote_draft_state, set_triage_state, upsert_draft_revision,
    upsert_stage,
};
use crate::config::resolve;
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
use tempfile::{Builder, TempDir};

#[test]
fn set_triage_state_creates_workflow_and_event_log() {
    let repo_root = unique_temp_dir("mailroom-workflow-triage");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    let workflow = set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Urgent,
            note: Some(String::from("Reply before end of day")),
            snapshot: snapshot("message-1", "Urgent follow-up"),
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Triage);
    assert_eq!(workflow.triage_bucket, Some(TriageBucket::Urgent));
    assert_eq!(workflow.note, "Reply before end of day");
    assert_eq!(workflow.latest_message_id.as_deref(), Some("message-1"));

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();

    assert_eq!(detail.workflow.workflow_id, workflow.workflow_id);
    assert_eq!(detail.events.len(), 1);
    assert_eq!(detail.events[0].event_kind, "triage_set");
    assert_eq!(detail.events[0].from_stage, None);
    assert_eq!(detail.events[0].triage_bucket, Some(TriageBucket::Urgent));

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
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].thread_id, "thread-1");

    let doctor = inspect_workflows(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(doctor.workflow_count, 1);
    assert_eq!(doctor.open_workflow_count, 1);
    assert_eq!(doctor.draft_workflow_count, 0);
    assert_eq!(doctor.event_count, 1);
    assert_eq!(doctor.draft_revision_count, 0);
}

#[test]
fn set_triage_state_preserves_existing_stage() {
    let repo_root = unique_temp_dir("mailroom-workflow-triage-preserve-stage");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = seed_account(&config_report);

    seed_drafting_workflow(&config_report, &account.account_id, "thread-1");
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("gmail-draft-1")),
            gmail_draft_message_id: Some(String::from("gmail-message-1")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-1")),
            updated_at_epoch_s: 201,
        },
    )
    .unwrap();
    let drafting_detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    let existing_draft_revision_id = drafting_detail.workflow.current_draft_revision_id;

    let workflow = set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Urgent,
            note: Some(String::from("Escalated after manual review")),
            snapshot: snapshot("message-3", "Updated status"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::Drafting);
    assert_eq!(workflow.triage_bucket, Some(TriageBucket::Urgent));
    assert_eq!(
        workflow.current_draft_revision_id,
        existing_draft_revision_id
    );
    assert_eq!(workflow.gmail_draft_id.as_deref(), Some("gmail-draft-1"));

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.current_stage, WorkflowStage::Drafting);
    assert_eq!(detail.workflow.triage_bucket, Some(TriageBucket::Urgent));
    assert_eq!(
        detail.current_draft.unwrap().revision.draft_revision_id,
        existing_draft_revision_id.unwrap()
    );
    let triage_event = detail
        .events
        .iter()
        .find(|event| event.event_kind == "triage_set" && event.created_at_epoch_s == 300)
        .unwrap();
    assert_eq!(triage_event.to_stage, Some(WorkflowStage::Drafting));
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

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    let draft = detail.current_draft.unwrap();

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
