use super::*;

#[test]
fn persist_workflow_rejects_stale_updates() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-write-conflict");

    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::Urgent,
            note: None,
            snapshot: snapshot("message-1", "Project status"),
            updated_at_epoch_s: 100,
        },
    )
    .unwrap();

    let stale_workflow = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap()
    .workflow;
    let expected_workflow_version = stale_workflow.workflow_version;

    let mut connection = crate::store::connection::open_or_create(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap();
    let rows_affected = connection
        .execute(
            "UPDATE thread_workflows
             SET workflow_version = workflow_version + 1
             WHERE workflow_id = ?1",
            rusqlite::params![stale_workflow.workflow_id],
        )
        .unwrap();
    assert_eq!(
        rows_affected, 1,
        "expected exactly one workflow row to be bumped"
    );
    let transaction = connection.transaction().unwrap();

    let error = super::write::persist_workflow(
        &transaction,
        super::WorkflowRecord {
            note: String::from("conflicting note"),
            ..stale_workflow
        },
        Some(expected_workflow_version),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        super::WorkflowStoreWriteError::Conflict { .. }
    ));
}

#[test]
fn set_remote_draft_state_rejects_stale_caller_version() {
    let (_repo_root, config_report, account) =
        bootstrap_test_env("mailroom-workflow-remote-draft-version-conflict");

    seed_drafting_workflow(&config_report, &account.account_id, "thread-1");
    let stale_workflow = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap()
    .workflow;

    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: snapshot("message-2", "advanced state"),
            updated_at_epoch_s: 300,
        },
    )
    .unwrap();

    let error = set_remote_draft_state_with_expected_version(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("gmail-draft-1")),
            gmail_draft_message_id: Some(String::from("gmail-message-1")),
            gmail_draft_thread_id: Some(String::from("gmail-thread-1")),
            updated_at_epoch_s: 301,
        },
        Some(stale_workflow.workflow_version),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        super::WorkflowStoreWriteError::Conflict { .. }
    ));
}
