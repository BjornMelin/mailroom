use super::{
    AttachmentRemovalResult, RemoteDraftUpsert, WorkflowServiceError, attachment_input_from_path,
    best_effort_sync_report, build_reply_recipients, cleanup_archive, cleanup_label,
    cleanup_tracked_thread_for_automation, draft_body_set, draft_send, draft_start, list_workflows,
    mark_sent_after_remote_send, persist_remote_draft_state, promote_workflow,
    remove_attachment_by_path_or_name, retire_local_draft_then_delete_remote, show_workflow,
};
use crate::auth;
use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::config::{ConfigReport, resolve};
use crate::gmail::{GmailLabel, GmailThreadMessage};
use crate::mailbox::SyncRunReport;
use crate::store::mailbox::{
    GmailMessageUpsertInput, SyncMode, SyncStateUpdate, SyncStatus, replace_labels,
    upsert_messages, upsert_sync_state,
};
use crate::store::workflows::{
    AttachmentInput, CleanupAction, ReplyMode, TriageBucket, UpsertDraftRevisionInput,
    get_workflow_detail, set_remote_draft_state, set_triage_state, upsert_draft_revision,
};
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
use anyhow::anyhow;
use secrecy::SecretString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::sync_channel;
use std::time::Duration;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn draft_start_reuses_existing_remote_gmail_draft() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    mount_thread(&mock_server).await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "draft-1",
            "message": {
                "id": "draft-message-1",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "draft-1",
            "message": {
                "id": "draft-message-2",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);

    let first = draft_start(&config_report, String::from("thread-1"), ReplyMode::Reply)
        .await
        .unwrap();
    let second = draft_start(&config_report, String::from("thread-1"), ReplyMode::Reply)
        .await
        .unwrap();

    assert_eq!(first.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert_eq!(second.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert_eq!(
        second.workflow.gmail_draft_message_id.as_deref(),
        Some("draft-message-2")
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert_eq!(
        detail.workflow.gmail_draft_message_id.as_deref(),
        Some("draft-message-2")
    );

    let requests = mock_server.received_requests().await.unwrap();
    let create_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/gmail/v1/users/me/drafts"
        })
        .count();
    let update_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "PUT"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        })
        .count();

    assert_eq!(create_count, 1);
    assert_eq!(update_count, 1);
}

#[tokio::test]
async fn draft_start_and_body_set_persist_live_thread_metadata_when_local_snapshot_is_stale() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/threads/thread-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "thread-1",
            "historyId": "500",
            "messages": [
                {
                    "id": "m-2",
                    "threadId": "thread-1",
                    "historyId": "401",
                    "internalDate": "200",
                    "snippet": "Fresh status",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Project updated"},
                            {"name": "From", "value": "\"Alice Example\" <alice@example.com>"},
                            {"name": "To", "value": "operator@example.com"},
                            {"name": "Message-ID", "value": "<m-2@example.com>"}
                        ]
                    }
                }
            ]
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "draft-1",
            "message": {
                "id": "draft-message-1",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "draft-1",
            "message": {
                "id": "draft-message-2",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot_with_message(
        &config_report,
        "m-1",
        100,
        "Project",
        "Alice <alice@example.com>",
        Some("alice@example.com"),
        "Stale status",
    );

    let report = draft_start(&config_report, String::from("thread-1"), ReplyMode::Reply)
        .await
        .unwrap();
    draft_body_set(
        &config_report,
        String::from("thread-1"),
        String::from("Updated body"),
    )
    .await
    .unwrap();

    assert_eq!(report.workflow.latest_message_id.as_deref(), Some("m-2"));
    assert_eq!(
        report.workflow.latest_message_internal_date_epoch_ms,
        Some(200)
    );
    assert_eq!(report.workflow.latest_message_subject, "Project updated");
    assert_eq!(
        report.workflow.latest_message_from_header,
        "\"Alice Example\" <alice@example.com>"
    );
    assert_eq!(report.workflow.latest_message_snippet, "Fresh status");

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.latest_message_id.as_deref(), Some("m-2"));
    assert_eq!(detail.workflow.latest_message_subject, "Project updated");
    assert_eq!(detail.workflow.latest_message_snippet, "Fresh status");
}

#[tokio::test]
async fn draft_body_set_recreates_missing_remote_gmail_draft() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    mount_thread(&mock_server).await;
    Mock::given(method("PUT"))
        .and(path("/gmail/v1/users/me/drafts/draft-stale"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "draft-2",
            "message": {
                "id": "draft-message-2",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-stale")),
            gmail_draft_message_id: Some(String::from("draft-message-stale")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let report = draft_body_set(
        &config_report,
        String::from("thread-1"),
        String::from("Updated body"),
    )
    .await
    .unwrap();

    assert_eq!(report.workflow.gmail_draft_id.as_deref(), Some("draft-2"));
    assert_eq!(
        report.workflow.gmail_draft_message_id.as_deref(),
        Some("draft-message-2")
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-2"));
    assert_eq!(
        detail.workflow.gmail_draft_message_id.as_deref(),
        Some("draft-message-2")
    );

    let requests = mock_server.received_requests().await.unwrap();
    assert!(requests.iter().any(|request| {
        request.method.as_str() == "PUT"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-stale"
    }));
    assert!(requests.iter().any(|request| {
        request.method.as_str() == "POST" && request.url.path() == "/gmail/v1/users/me/drafts"
    }));
}

#[tokio::test]
async fn persist_remote_draft_state_rolls_back_created_remote_draft_when_local_write_fails() {
    let mock_server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-created"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    let (workflow, _) = upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
    let original_database_path = config_report.config.store.database_path.clone();
    let original_busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    config_report.config.store.database_path = temp_dir.path().join("missing").join("db.sqlite");

    let error = persist_remote_draft_state(
        &config_report,
        workflow,
        &RemoteDraftUpsert {
            gmail_draft_id: String::from("draft-created"),
            gmail_draft_message_id: String::from("draft-message-created"),
            gmail_draft_thread_id: String::from("thread-1"),
            created_new: true,
        },
        &gmail_client,
        "draft.test.remote_state",
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("failed to open workflow store"),
        "unexpected error: {error}"
    );

    let detail = get_workflow_detail(
        &original_database_path,
        original_busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id, None);
    assert!(detail.current_draft.is_some());

    let requests = mock_server.received_requests().await.unwrap();
    let create_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/gmail/v1/users/me/drafts"
        })
        .count();
    let delete_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-created"
        })
        .count();
    assert_eq!(create_count, 0);
    assert_eq!(delete_count, 1);
}

#[tokio::test]
async fn persist_remote_draft_state_reports_rollback_failure_when_cleanup_delete_fails() {
    let mock_server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-created"))
        .respond_with(ResponseTemplate::new(500).set_body_string("delete failed"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    let (workflow, _) = upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
    let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(450),
    );
    lock_ready.recv().unwrap();

    let error = persist_remote_draft_state(
        &config_report,
        workflow,
        &RemoteDraftUpsert {
            gmail_draft_id: String::from("draft-created"),
            gmail_draft_message_id: String::from("draft-message-created"),
            gmail_draft_thread_id: String::from("thread-1"),
            created_new: true,
        },
        &gmail_client,
        "draft.test.remote_state",
    )
    .await
    .unwrap_err();
    lock_handle.join().unwrap();

    match error {
        WorkflowServiceError::RemoteDraftRollback {
            thread_id,
            draft_id,
            source,
        } => {
            assert_eq!(thread_id, "thread-1");
            assert_eq!(draft_id, "draft-created");
            assert!(
                source
                    .to_string()
                    .contains("failed to delete Gmail draft draft-created")
            );
        }
        other => panic!("expected RemoteDraftRollback, got {other}"),
    }
}

#[tokio::test]
async fn persist_remote_draft_state_retries_existing_remote_draft_write_after_transient_lock() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    let (workflow, _) = upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
    let (lock_handle, ready_rx) = lock_workflow_store_after_delay(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(0),
        Duration::from_millis(80),
    );
    ready_rx.recv().unwrap();

    let persisted = persist_remote_draft_state(
        &config_report,
        workflow,
        &RemoteDraftUpsert {
            gmail_draft_id: String::from("draft-updated"),
            gmail_draft_message_id: String::from("draft-message-updated"),
            gmail_draft_thread_id: String::from("thread-1"),
            created_new: false,
        },
        &gmail_client,
        "draft.test.remote_state",
    )
    .await
    .unwrap();
    lock_handle.join().unwrap();

    assert_eq!(persisted.gmail_draft_id.as_deref(), Some("draft-updated"));
    assert_eq!(
        persisted.gmail_draft_message_id.as_deref(),
        Some("draft-message-updated")
    );
    assert_eq!(persisted.gmail_draft_thread_id.as_deref(), Some("thread-1"));

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.gmail_draft_id.as_deref(),
        Some("draft-updated")
    );
    assert_eq!(
        detail.workflow.gmail_draft_message_id.as_deref(),
        Some("draft-message-updated")
    );
    assert_eq!(
        detail.workflow.gmail_draft_thread_id.as_deref(),
        Some("thread-1")
    );
}

#[tokio::test]
async fn persist_remote_draft_state_reports_reconcile_failure_after_retry_exhaustion() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    let (workflow, _) = upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
    let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(450),
    );
    lock_ready.recv().unwrap();

    let error = persist_remote_draft_state(
        &config_report,
        workflow,
        &RemoteDraftUpsert {
            gmail_draft_id: String::from("draft-updated"),
            gmail_draft_message_id: String::from("draft-message-updated"),
            gmail_draft_thread_id: String::from("thread-1"),
            created_new: false,
        },
        &gmail_client,
        "draft.test.remote_state",
    )
    .await
    .unwrap_err();
    lock_handle.join().unwrap();

    match error {
        WorkflowServiceError::RemoteDraftStateReconcile {
            thread_id,
            draft_id,
            ..
        } => {
            assert_eq!(thread_id, "thread-1");
            assert_eq!(draft_id, "draft-updated");
        }
        other => panic!("expected RemoteDraftStateReconcile, got {other}"),
    }

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id, None);
}

#[tokio::test]
async fn draft_send_refuses_to_recreate_missing_remote_draft() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    mount_thread(&mock_server).await;
    Mock::given(method("PUT"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "draft-2",
            "message": {
                "id": "draft-message-2",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts/send"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(150))
                .set_body_json(serde_json::json!({
                    "id": "sent-message-1",
                    "threadId": "thread-1",
                    "historyId": "900"
                })),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts/send"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "sent-message-2",
            "threadId": "thread-1",
            "historyId": "901"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let error = draft_send(&config_report, String::from("thread-1"))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "stored Gmail draft draft-1 for thread thread-1 no longer exists; refusing to recreate it during send because the previous send may have already succeeded; run `mailroom sync run` and inspect the thread before retrying"
    );

    let requests = mock_server.received_requests().await.unwrap();
    let update_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "PUT"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        })
        .count();
    let create_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/gmail/v1/users/me/drafts"
        })
        .count();
    let send_count = requests
        .iter()
        .filter(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/drafts/send"
        })
        .count();
    assert_eq!(update_count, 1);
    assert_eq!(create_count, 0);
    assert_eq!(send_count, 0);
}

#[tokio::test]
async fn draft_send_retries_mark_sent_after_transient_local_lock() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let mut config_report = resolve(&paths).unwrap();
    config_report.config.store.busy_timeout_ms = 1;
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    let workflow = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap()
    .workflow;
    let (lock_handle, ready_rx) = lock_workflow_store_after_delay(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(0),
        Duration::from_millis(80),
    );
    ready_rx.recv().unwrap();

    let report = mark_sent_after_remote_send(&config_report, &workflow, "sent-message-1")
        .await
        .unwrap();
    lock_handle.join().unwrap();

    assert_eq!(
        report.current_stage,
        crate::store::workflows::WorkflowStage::Sent
    );
    assert_eq!(report.gmail_draft_id, None);
    assert_eq!(
        report.last_sent_message_id.as_deref(),
        Some("sent-message-1")
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Sent
    );
    assert_eq!(detail.current_draft, None);
}

#[test]
fn remove_attachment_by_path_or_name_removes_matching_filename() {
    let mut attachments = vec![
        sample_attachment("/tmp/one.txt", "one.txt"),
        sample_attachment("/tmp/two.txt", "two.txt"),
    ];

    let removed = remove_attachment_by_path_or_name(&mut attachments, "two.txt", Path::new("/tmp"));

    assert_eq!(removed, AttachmentRemovalResult::Removed);
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].file_name, "one.txt");
}

#[test]
fn remove_attachment_by_path_or_name_reports_when_nothing_matches() {
    let mut attachments = vec![sample_attachment("/tmp/one.txt", "one.txt")];

    let removed =
        remove_attachment_by_path_or_name(&mut attachments, "missing.txt", Path::new("/tmp"));

    assert_eq!(removed, AttachmentRemovalResult::NotFound);
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].file_name, "one.txt");
}

#[test]
fn remove_attachment_by_path_or_name_resolves_relative_path_from_repo_root() {
    let repo_root = TempDir::new().unwrap();
    let relative_path = Path::new("notes").join("note.txt");
    let full_path = repo_root.path().join(&relative_path);
    fs::create_dir_all(full_path.parent().unwrap()).unwrap();
    fs::write(&full_path, "hello").unwrap();

    let mut attachments = vec![sample_attachment(
        full_path.canonicalize().unwrap().to_str().unwrap(),
        "note.txt",
    )];

    let removed = remove_attachment_by_path_or_name(
        &mut attachments,
        relative_path.to_str().unwrap(),
        repo_root.path(),
    );

    assert_eq!(removed, AttachmentRemovalResult::Removed);
    assert!(attachments.is_empty());
}

#[test]
fn remove_attachment_by_path_or_name_matches_relative_path_after_file_is_deleted() {
    let repo_root = TempDir::new().unwrap();
    let relative_path = Path::new("notes").join("note.txt");
    let full_path = repo_root.path().join(&relative_path);
    fs::create_dir_all(full_path.parent().unwrap()).unwrap();
    fs::write(&full_path, "hello").unwrap();

    let mut attachments = vec![sample_attachment(
        full_path.canonicalize().unwrap().to_str().unwrap(),
        "note.txt",
    )];
    fs::remove_file(&full_path).unwrap();

    let removed = remove_attachment_by_path_or_name(
        &mut attachments,
        relative_path.to_str().unwrap(),
        repo_root.path(),
    );

    assert_eq!(removed, AttachmentRemovalResult::Removed);
    assert!(attachments.is_empty());
}

#[test]
fn remove_attachment_by_path_or_name_rejects_ambiguous_filename_matches() {
    let mut attachments = vec![
        sample_attachment("/tmp/a/report.pdf", "report.pdf"),
        sample_attachment("/tmp/b/report.pdf", "report.pdf"),
    ];

    let removed =
        remove_attachment_by_path_or_name(&mut attachments, "report.pdf", Path::new("/tmp"));

    assert_eq!(removed, AttachmentRemovalResult::AmbiguousFileName);
    assert_eq!(attachments.len(), 2);
}

#[test]
fn build_reply_recipients_uses_non_self_participant_when_latest_message_is_from_operator() {
    let recipients = build_reply_recipients(
        "operator@example.com",
        &sample_thread_message(
            "Operator <operator@example.com>",
            Some("operator@example.com"),
            "alice@example.com, operator@example.com",
            "carol@example.com",
            "",
        ),
        ReplyMode::Reply,
    )
    .unwrap();

    assert_eq!(
        recipients.to_addresses,
        vec![String::from("alice@example.com")]
    );
    assert!(recipients.cc_addresses.is_empty());
}

#[test]
fn attachment_input_from_path_persists_a_normalized_absolute_path() {
    let current_dir = std::env::current_dir().unwrap();
    let temp_dir = tempfile::Builder::new()
        .prefix("mailroom-attachment-")
        .tempdir_in(&current_dir)
        .unwrap();
    let relative_dir = temp_dir.path().strip_prefix(&current_dir).unwrap();
    let relative_path = relative_dir.join("note.txt");
    fs::write(current_dir.join(&relative_path), "hello").unwrap();

    let attachment = attachment_input_from_path(&relative_path).unwrap();
    let expected_path = current_dir.join(&relative_path).canonicalize().unwrap();

    assert_eq!(Path::new(&attachment.path), expected_path.as_path());
    assert_eq!(attachment.file_name, "note.txt");
}

#[test]
fn best_effort_sync_report_returns_none_when_sync_fails() {
    assert!(best_effort_sync_report(Err(anyhow!("stale history")), "sync failed").is_none());
}

#[test]
fn best_effort_sync_report_preserves_successful_sync_results() {
    let report = SyncRunReport {
        run_id: 0,
        mode: SyncMode::Incremental,
        comparability_kind:
            crate::store::mailbox::SyncRunComparabilityKind::IncrementalWorkloadTier,
        comparability_key: String::from("tiny"),
        comparability_label: String::from("incremental workload=tiny"),
        startup_seed_run_id: None,
        fallback_from_history: false,
        resumed_from_checkpoint: false,
        bootstrap_query: String::from("newer_than:90d"),
        cursor_history_id: String::from("123"),
        pages_fetched: 1,
        messages_listed: 3,
        messages_upserted: 3,
        messages_deleted: 0,
        labels_synced: 4,
        checkpoint_reused_pages: 0,
        checkpoint_reused_messages_upserted: 0,
        pipeline_enabled: true,
        pipeline_list_queue_high_water: 1,
        pipeline_write_queue_high_water: 1,
        pipeline_write_batch_count: 1,
        pipeline_writer_wait_ms: 0,
        pipeline_fetch_batch_count: 1,
        pipeline_fetch_batch_avg_ms: 10,
        pipeline_fetch_batch_max_ms: 10,
        pipeline_writer_tx_count: 1,
        pipeline_writer_tx_avg_ms: 5,
        pipeline_writer_tx_max_ms: 5,
        pipeline_reorder_buffer_high_water: 1,
        pipeline_staged_message_count: 3,
        pipeline_staged_delete_count: 0,
        pipeline_staged_attachment_count: 0,
        store_message_count: 3,
        store_label_count: 4,
        store_indexed_message_count: 3,
        adaptive_pacing_enabled: true,
        quota_units_budget_per_minute: 12_000,
        message_fetch_concurrency: 4,
        quota_units_cap_per_minute: 12_000,
        message_fetch_concurrency_cap: 4,
        starting_quota_units_per_minute: 12_000,
        starting_message_fetch_concurrency: 4,
        effective_quota_units_per_minute: 12_000,
        effective_message_fetch_concurrency: 4,
        adaptive_downshift_count: 0,
        estimated_quota_units_reserved: 20,
        http_attempt_count: 4,
        retry_count: 0,
        quota_pressure_retry_count: 0,
        concurrency_pressure_retry_count: 0,
        backend_retry_count: 0,
        throttle_wait_count: 0,
        throttle_wait_ms: 0,
        retry_after_wait_ms: 0,
        duration_ms: 100,
        pages_per_second: 10.0,
        messages_per_second: 30.0,
        regression_detected: false,
        regression_kind: None,
    };

    let sync_report = best_effort_sync_report(Ok(report), "sync failed").unwrap();
    assert_eq!(sync_report.mode, SyncMode::Incremental);
    assert_eq!(sync_report.cursor_history_id, "123");
    assert_eq!(sync_report.messages_upserted, 3);
}

#[tokio::test]
async fn list_workflows_uses_persisted_mailbox_account_after_logout() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("100")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 100,
            last_full_sync_success_epoch_s: Some(100),
            last_incremental_sync_success_epoch_s: Some(100),
            pipeline_enabled: false,
            pipeline_list_queue_high_water: 0,
            pipeline_write_queue_high_water: 0,
            pipeline_write_batch_count: 0,
            pipeline_writer_wait_ms: 0,
            pipeline_fetch_batch_count: 0,
            pipeline_fetch_batch_avg_ms: 0,
            pipeline_fetch_batch_max_ms: 0,
            pipeline_writer_tx_count: 0,
            pipeline_writer_tx_avg_ms: 0,
            pipeline_writer_tx_max_ms: 0,
            pipeline_reorder_buffer_high_water: 0,
            pipeline_staged_message_count: 0,
            pipeline_staged_delete_count: 0,
            pipeline_staged_attachment_count: 0,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();

    let logout_report = auth::logout(&config_report).unwrap();
    assert_eq!(logout_report.deactivated_accounts, 1);

    let report = list_workflows(&config_report, None, None).await.unwrap();

    assert_eq!(report.workflows.len(), 1);
    assert_eq!(report.workflows[0].thread_id, "thread-1");
}

#[tokio::test]
async fn workflow_commands_use_persisted_workflow_account_after_logout_without_sync_state() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();

    let logout_report = auth::logout(&config_report).unwrap();
    assert_eq!(logout_report.deactivated_accounts, 1);

    let show = show_workflow(&config_report, String::from("thread-1"))
        .await
        .unwrap();
    assert_eq!(show.detail.workflow.thread_id, "thread-1");

    let report = list_workflows(&config_report, None, None).await.unwrap();
    assert_eq!(report.workflows.len(), 1);
    assert_eq!(report.workflows[0].thread_id, "thread-1");

    let resolved_account_id =
        super::queries::resolve_workflow_account_id(&config_report, Some("thread-1"))
            .await
            .unwrap();
    assert_eq!(resolved_account_id, "gmail:operator@example.com");

    let preview = cleanup_archive(&config_report, String::from("thread-1"), false)
        .await
        .unwrap();
    assert_eq!(preview.workflow.thread_id, "thread-1");
    assert!(!preview.cleanup_preview.as_ref().unwrap().execute);
}

#[tokio::test]
async fn show_workflow_prefers_thread_owned_account_over_active_account() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
            history_id: String::from("99"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 99,
        },
    )
    .unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:other@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();

    let report = show_workflow(&config_report, String::from("thread-1"))
        .await
        .unwrap();

    assert_eq!(report.detail.workflow.account_id, "gmail:other@example.com");
}

#[tokio::test]
async fn cleanup_archive_rejects_cross_account_thread_mutation() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
            history_id: String::from("99"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 99,
        },
    )
    .unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:other@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();

    let error = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "thread thread-1 belongs to gmail:other@example.com, but the authenticated Gmail account is gmail:operator@example.com; switch accounts before mutating this workflow"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:other@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Triage
    );
    assert_eq!(detail.workflow.last_cleanup_action, None);
}

#[tokio::test]
async fn cleanup_archive_deletes_remote_draft_and_treats_sync_as_best_effort() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "thread-1",
            "historyId": "710"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let report = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap();

    assert_eq!(
        report.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert!(report.sync_report.is_none());

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id, None);
    assert_eq!(detail.current_draft, None);

    let requests = mock_server.received_requests().await.unwrap();
    let delete_index = requests
        .iter()
        .position(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        })
        .unwrap();
    let modify_index = requests
        .iter()
        .position(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        })
        .unwrap();
    assert!(modify_index < delete_index);
}

#[tokio::test]
async fn promote_workflow_closed_deletes_remote_draft_after_local_close_persists() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let report = promote_workflow(
        &config_report,
        String::from("thread-1"),
        crate::store::workflows::WorkflowStage::Closed,
    )
    .await
    .unwrap();

    assert_eq!(
        report.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert_eq!(report.workflow.gmail_draft_id, None);

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert_eq!(detail.workflow.gmail_draft_id, None);

    let requests = mock_server.received_requests().await.unwrap();
    assert!(requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
}

#[tokio::test]
async fn promote_workflow_closed_requires_gmail_auth_before_persisting_close() {
    let mock_server = MockServer::start().await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_local_thread_snapshot(&config_report);
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let error = promote_workflow(
        &config_report,
        String::from("thread-1"),
        crate::store::workflows::WorkflowStage::Closed,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "failed to refresh the active Gmail account"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Triage
    );
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
}

#[tokio::test]
async fn promote_workflow_closed_keeps_remote_draft_when_local_close_write_fails() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(150),
    );
    lock_ready.recv().unwrap();

    let _error = promote_workflow(
        &config_report,
        String::from("thread-1"),
        crate::store::workflows::WorkflowStage::Closed,
    )
    .await
    .unwrap_err();
    lock_handle.join().unwrap();

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Triage
    );
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));

    let requests = mock_server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
}

#[tokio::test]
async fn retire_local_draft_then_delete_remote_skips_delete_when_local_retire_fails() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let workflow = set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
    let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(150),
    );
    lock_ready.recv().unwrap();

    let _error = retire_local_draft_then_delete_remote(
        &config_report,
        &gmail_client,
        workflow,
        "draft.retire.test",
    )
    .await
    .unwrap_err();
    lock_handle.join().unwrap();

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert!(detail.current_draft.is_some());

    let requests = mock_server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
}

#[tokio::test]
async fn retire_local_draft_then_delete_remote_restores_draft_state_when_delete_fails() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(500).set_body_string("draft delete failed"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let workflow = set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();

    let error = retire_local_draft_then_delete_remote(
        &config_report,
        &gmail_client,
        workflow,
        "draft.retire.test",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "gmail API request to users/me/drafts/draft-1 failed with status 500 Internal Server Error: draft delete failed"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert!(detail.current_draft.is_some());
}

#[tokio::test]
async fn retire_local_draft_then_delete_remote_reports_reconcile_after_restore_retry_exhaustion() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_delay(Duration::from_millis(120))
                .set_body_string("draft delete failed"),
        )
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let workflow = set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
    let config_for_task = config_report.clone();
    let gmail_client_for_task = gmail_client.clone();
    let workflow_for_task = workflow.clone();
    let operation = tokio::spawn(async move {
        retire_local_draft_then_delete_remote(
            &config_for_task,
            &gmail_client_for_task,
            workflow_for_task,
            "draft.retire.test",
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (lock_handle, ready_rx) = lock_workflow_store_after_delay(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(0),
        Duration::from_millis(450),
    );
    ready_rx.recv().unwrap();

    let error = operation.await.unwrap().unwrap_err();
    lock_handle.join().unwrap();

    match error {
        WorkflowServiceError::RemoteDraftStateReconcile {
            thread_id,
            draft_id,
            source,
        } => {
            assert_eq!(thread_id, "thread-1");
            assert_eq!(draft_id, "draft-1");
            let source_text = source.to_string();
            assert!(source_text.contains("remote draft delete failed"));
            assert!(source_text.contains("draft delete failed"));
        }
        other => panic!("expected RemoteDraftStateReconcile, got {other}"),
    }
}

#[tokio::test]
async fn cleanup_label_validates_before_deleting_remote_draft() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "thread-1",
            "historyId": "710"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        &[GmailLabel {
            id: String::from("Label_1"),
            name: String::from("Project/Alpha"),
            label_type: String::from("user"),
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
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let error = cleanup_label(
        &config_report,
        String::from("thread-1"),
        true,
        vec![String::from("Missing/Label")],
        Vec::new(),
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "one or more add-label names were not found locally; run `mailroom sync run` first"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));

    let requests = mock_server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "POST"
            && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
    }));
}

#[tokio::test]
async fn cleanup_archive_treats_missing_remote_draft_as_already_deleted() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-stale"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "thread-1",
            "historyId": "711"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: account.account_id.clone(),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-stale")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 103,
        },
    )
    .unwrap();

    let report = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap();

    assert_eq!(
        report.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert_eq!(report.workflow.gmail_draft_id, None);

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert_eq!(detail.workflow.gmail_draft_id, None);
    assert_eq!(detail.current_draft, None);

    let requests = mock_server.received_requests().await.unwrap();
    let delete_index = requests
        .iter()
        .position(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-stale"
        })
        .unwrap();
    let modify_index = requests
        .iter()
        .position(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        })
        .unwrap();
    assert!(modify_index < delete_index);
}

#[tokio::test]
async fn cleanup_archive_keeps_local_remote_draft_state_when_cleanup_mutation_fails() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .respond_with(ResponseTemplate::new(500).set_body_string("mailbox mutation failed"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let error = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "gmail API request to users/me/threads/thread-1/modify failed with status 500 Internal Server Error: mailbox mutation failed"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert_eq!(
        detail.workflow.last_cleanup_action,
        Some(CleanupAction::Archive)
    );
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert!(detail.current_draft.is_some());

    let requests = mock_server.received_requests().await.unwrap();
    assert!(requests.iter().any(|request| {
        request.method.as_str() == "POST"
            && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
    }));
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
}

#[tokio::test]
async fn cleanup_archive_keeps_remote_draft_when_local_cleanup_write_fails() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(150))
                .set_body_json(serde_json::json!({
                    "id": "thread-1",
                    "historyId": "710"
                })),
        )
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let mut config_report = config_report_for(&temp_dir, &mock_server);
    config_report.config.store.busy_timeout_ms = 1;
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();
    let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
        config_report.config.store.database_path.clone(),
        Duration::from_millis(150),
    );
    lock_ready.recv().unwrap();

    let _error = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap_err();
    lock_handle.join().unwrap();

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Drafting
    );
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert!(detail.current_draft.is_some());

    let requests = mock_server.received_requests().await.unwrap();
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "POST"
            && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
    }));
    assert!(!requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
}

#[tokio::test]
async fn cleanup_archive_requires_gmail_auth_before_persisting_cleanup() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_local_thread_snapshot(&config_report);
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();

    let error = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "failed to refresh the active Gmail account"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Triage
    );
    assert_eq!(detail.workflow.last_cleanup_action, None);
}

#[tokio::test]
async fn cleanup_tracked_thread_for_automation_requires_gmail_auth_before_persisting_cleanup() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_local_thread_snapshot(&config_report);
    set_triage_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::SetTriageStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            triage_bucket: TriageBucket::NeedsReplySoon,
            note: None,
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-1"),
                internal_date_epoch_ms: 100,
                subject: String::from("Project"),
                from_header: String::from("Alice <alice@example.com>"),
                snippet: String::from("Project status"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();

    let error = cleanup_tracked_thread_for_automation(
        &config_report,
        &gmail_client,
        "gmail:operator@example.com",
        "thread-1",
        CleanupAction::Archive,
        Vec::new(),
        vec![String::from("INBOX")],
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "mailroom is not authenticated; run `mailroom auth login` first"
    );

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Triage
    );
    assert_eq!(detail.workflow.last_cleanup_action, None);
}

#[tokio::test]
async fn cleanup_archive_keeps_draft_state_when_remote_delete_fails() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server).await;
    Mock::given(method("DELETE"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .respond_with(ResponseTemplate::new(500).set_body_string("draft delete failed"))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "thread-1",
            "historyId": "710"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_local_thread_snapshot(&config_report);
    upsert_draft_revision(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &UpsertDraftRevisionInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            reply_mode: ReplyMode::Reply,
            source_message_id: String::from("m-1"),
            subject: String::from("Re: Project"),
            to_addresses: vec![String::from("alice@example.com")],
            cc_addresses: Vec::new(),
            bcc_addresses: Vec::new(),
            body_text: String::from("Draft body"),
            attachments: Vec::new(),
            snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                message_id: String::from("m-2"),
                internal_date_epoch_ms: 101,
                subject: String::from("Re: Project"),
                from_header: String::from("Operator <operator@example.com>"),
                snippet: String::from("Draft body"),
            },
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
    set_remote_draft_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::workflows::RemoteDraftStateInput {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from("thread-1"),
            gmail_draft_id: Some(String::from("draft-1")),
            gmail_draft_message_id: Some(String::from("draft-message-1")),
            gmail_draft_thread_id: Some(String::from("thread-1")),
            updated_at_epoch_s: 102,
        },
    )
    .unwrap();

    let _error = cleanup_archive(&config_report, String::from("thread-1"), true)
        .await
        .unwrap_err();

    let detail = get_workflow_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "thread-1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        detail.workflow.current_stage,
        crate::store::workflows::WorkflowStage::Closed
    );
    assert_eq!(
        detail.workflow.last_cleanup_action,
        Some(CleanupAction::Archive)
    );
    assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
    assert!(detail.current_draft.is_some());

    let requests = mock_server.received_requests().await.unwrap();
    assert!(requests.iter().any(|request| {
        request.method.as_str() == "POST"
            && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
    }));
    assert!(requests.iter().any(|request| {
        request.method.as_str() == "DELETE"
            && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
    }));
}

fn sample_attachment(path: &str, file_name: &str) -> AttachmentInput {
    AttachmentInput {
        path: String::from(path),
        file_name: String::from(file_name),
        mime_type: String::from("text/plain"),
        size_bytes: 1,
    }
}

fn sample_thread_message(
    from_header: &str,
    from_address: Option<&str>,
    to_header: &str,
    cc_header: &str,
    reply_to_header: &str,
) -> GmailThreadMessage {
    GmailThreadMessage {
        id: String::from("m-1"),
        thread_id: String::from("thread-1"),
        history_id: String::from("400"),
        internal_date_epoch_ms: 100,
        snippet: String::from("snippet"),
        subject: String::from("Project"),
        from_header: String::from(from_header),
        from_address: from_address.map(String::from),
        to_header: String::from(to_header),
        cc_header: String::from(cc_header),
        bcc_header: String::new(),
        reply_to_header: String::from(reply_to_header),
        message_id_header: Some(String::from("<m-1@example.com>")),
        references_header: String::new(),
    }
}

fn config_report_for(temp_dir: &TempDir, mock_server: &MockServer) -> ConfigReport {
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let mut config_report = resolve(&paths).unwrap();
    config_report.config.gmail.api_base_url = format!("{}/gmail/v1", mock_server.uri());
    config_report.config.gmail.auth_url = format!("{}/oauth2/auth", mock_server.uri());
    config_report.config.gmail.token_url = format!("{}/oauth2/token", mock_server.uri());
    config_report.config.gmail.open_browser = false;
    config_report.config.gmail.client_id = Some(String::from("client-id"));
    config_report.config.gmail.client_secret = Some(String::from("client-secret"));
    config_report
}

fn seed_credentials(config_report: &ConfigReport) {
    let credential_store = FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    );
    credential_store
        .save(&StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("access-token")),
            refresh_token: Some(SecretString::from(String::from("refresh-token"))),
            expires_at_epoch_s: Some(u64::MAX),
            scopes: vec![String::from("scope:a")],
        })
        .unwrap();
}

fn seed_local_thread_snapshot(config_report: &ConfigReport) {
    seed_local_thread_snapshot_with_message(
        config_report,
        "m-1",
        1_700_000_000_000,
        "Project",
        "Alice <alice@example.com>",
        Some("alice@example.com"),
        "Project status",
    );
}

fn seed_local_thread_snapshot_with_message(
    config_report: &ConfigReport,
    message_id: &str,
    internal_date_epoch_ms: i64,
    subject: &str,
    from_header: &str,
    from_address: Option<&str>,
    snippet: &str,
) {
    init(config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from(message_id),
            thread_id: String::from("thread-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms,
            snippet: String::from(snippet),
            subject: String::from(subject),
            from_header: String::from(from_header),
            from_address: from_address.map(String::from),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: Vec::new(),
        }],
        100,
    )
    .unwrap();
}

async fn mount_profile(mock_server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "emailAddress": "operator@example.com",
            "messagesTotal": 1,
            "threadsTotal": 1,
            "historyId": "12345"
        })))
        .mount(mock_server)
        .await;
}

async fn mount_thread(mock_server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/threads/thread-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "thread-1",
            "historyId": "500",
            "messages": [
                {
                    "id": "m-1",
                    "threadId": "thread-1",
                    "historyId": "400",
                    "internalDate": "100",
                    "snippet": "Project status",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Project"},
                            {"name": "From", "value": "\"Alice Example\" <alice@example.com>"},
                            {"name": "To", "value": "operator@example.com"},
                            {"name": "Message-ID", "value": "<m-1@example.com>"}
                        ]
                    }
                }
            ]
        })))
        .mount(mock_server)
        .await;
}

fn lock_workflow_store_after_delay(
    database_path: PathBuf,
    start_delay: Duration,
    hold_for: Duration,
) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<()>) {
    let (ready_tx, ready_rx) = sync_channel(1);
    let handle = std::thread::spawn(move || {
        std::thread::sleep(start_delay);
        let connection = rusqlite::Connection::open(database_path).unwrap();
        connection.busy_timeout(Duration::from_millis(1)).unwrap();
        loop {
            match connection.execute_batch("BEGIN IMMEDIATE;") {
                Ok(()) => {
                    ready_tx.send(()).unwrap();
                    break;
                }
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if error.code == rusqlite::ErrorCode::DatabaseBusy =>
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("failed to lock workflow store: {error}"),
            }
        }
        std::thread::sleep(hold_for);
        connection.execute_batch("ROLLBACK;").unwrap();
    });
    (handle, ready_rx)
}

fn lock_workflow_store_until_locked(
    database_path: PathBuf,
    hold_for: Duration,
) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<()>) {
    let (ready_tx, ready_rx) = sync_channel(1);
    let handle = std::thread::spawn(move || {
        let connection = rusqlite::Connection::open(database_path).unwrap();
        connection.busy_timeout(Duration::from_millis(1)).unwrap();
        loop {
            match connection.execute_batch("BEGIN IMMEDIATE;") {
                Ok(()) => {
                    ready_tx.send(()).unwrap();
                    break;
                }
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if error.code == rusqlite::ErrorCode::DatabaseBusy =>
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("failed to lock workflow store: {error}"),
            }
        }
        std::thread::sleep(hold_for);
        connection.execute_batch("ROLLBACK;").unwrap();
    });
    (handle, ready_rx)
}
