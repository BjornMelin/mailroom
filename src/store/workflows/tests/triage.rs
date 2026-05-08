use super::*;

#[test]
fn set_triage_state_creates_workflow_and_event_log() {
    let (_repo_root, config_report, account) = bootstrap_test_env("mailroom-workflow-triage");

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
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-triage-preserve-stage");

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
fn set_triage_state_preserves_newer_existing_snapshot_metadata() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-triage-preserve-snapshot");

    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Urgent,
            note: None,
            snapshot: WorkflowMessageSnapshot {
                message_id: String::from("message-new"),
                internal_date_epoch_ms: 200,
                subject: String::from("Current subject"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Snippet for Current subject"),
            },
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();

    let workflow = set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Fyi,
            note: Some(String::from("Re-triaged from cached snapshot")),
            snapshot: WorkflowMessageSnapshot {
                message_id: String::from("message-stale"),
                internal_date_epoch_ms: 100,
                subject: String::from("Stale subject"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Snippet for Stale subject"),
            },
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.triage_bucket, Some(TriageBucket::Fyi));
    assert_eq!(workflow.latest_message_id.as_deref(), Some("message-new"));
    assert_eq!(workflow.latest_message_subject, "Current subject");
    assert_eq!(
        workflow.latest_message_snippet,
        "Snippet for Current subject"
    );
}

#[test]
fn set_triage_state_preserves_existing_snapshot_metadata_on_equal_timestamp() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-triage-equal-snapshot");

    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Urgent,
            note: None,
            snapshot: WorkflowMessageSnapshot {
                message_id: String::from("message-new"),
                internal_date_epoch_ms: 200,
                subject: String::from("Current subject"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Snippet for Current subject"),
            },
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();

    let workflow = set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Fyi,
            note: Some(String::from("Re-triaged from cached snapshot")),
            snapshot: WorkflowMessageSnapshot {
                message_id: String::from("message-stale"),
                internal_date_epoch_ms: 200,
                subject: String::from("Stale subject"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Snippet for Stale subject"),
            },
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    assert_eq!(workflow.triage_bucket, Some(TriageBucket::Fyi));
    assert_eq!(workflow.latest_message_id.as_deref(), Some("message-new"));
    assert_eq!(workflow.latest_message_subject, "Current subject");
    assert_eq!(
        workflow.latest_message_snippet,
        "Snippet for Current subject"
    );
}

#[test]
fn clear_workflow_snooze_preserves_existing_stage() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-clear-snooze-preserve-stage");

    seed_drafting_workflow(&config_report, &account.account_id, "thread-1");
    let workflow = snooze_workflow(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SnoozeWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            snoozed_until_epoch_s: Some(1_800_000_000),
            snapshot: snapshot("message-2", "Follow-up"),
            updated_at_epoch_s: 200,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: workflow.thread_id.clone(),
            gmail_draft_id: Some(String::from("gmail-draft-1")),
            gmail_draft_message_id: Some(String::from("gmail-message-1")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-1")),
            updated_at_epoch_s: 250,
        },
    )
    .unwrap();
    let workflow = upsert_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &PromoteWorkflowInput {
            account_id: account.account_id.clone(),
            thread_id: workflow.thread_id.clone(),
            to_stage: WorkflowStage::ReadyToSend,
            snapshot: snapshot("message-3", "Follow-up"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();
    assert_eq!(workflow.current_stage, WorkflowStage::ReadyToSend);
    assert_eq!(workflow.snoozed_until_epoch_s, Some(1_800_000_000));

    let workflow = clear_workflow_snooze(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &ClearWorkflowSnoozeInput {
            account_id: account.account_id.clone(),
            thread_id: workflow.thread_id.clone(),
            snapshot: snapshot("message-4", "Follow-up"),
            updated_at_epoch_s: 400,
        },
    )
    .unwrap();

    assert_eq!(workflow.current_stage, WorkflowStage::ReadyToSend);
    assert_eq!(workflow.snoozed_until_epoch_s, None);

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.current_stage, WorkflowStage::ReadyToSend);
    assert_eq!(detail.workflow.snoozed_until_epoch_s, None);
    let event = detail
        .events
        .iter()
        .find(|event| event.event_kind == "workflow_snooze_cleared")
        .unwrap();
    assert_eq!(event.from_stage, Some(WorkflowStage::ReadyToSend));
    assert_eq!(event.to_stage, Some(WorkflowStage::ReadyToSend));
}
