use super::export::{copy_from_vault, default_export_path, export_filename};
use super::service::{export, list, map_mailbox_write_error, show};
use super::vault::{
    existing_vault_report, hash_file_blake3, resolve_vault_relative_path, write_vault_bytes,
};
use super::{AttachmentListRequest, AttachmentServiceError};
use crate::config::resolve;
use crate::store::mailbox::{AttachmentDetailRecord, GmailAttachmentUpsertInput};
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tempfile::TempDir;

struct ExportTestFixture {
    temp_dir: TempDir,
    config_report: crate::config::ConfigReport,
}

#[test]
fn export_filename_falls_back_when_gmail_filename_is_blank() {
    assert_eq!(export_filename("", "m-1:2"), "attachment-m-12.bin");
}

#[test]
fn export_filename_falls_back_when_sanitized_filename_is_empty() {
    assert_eq!(export_filename("///", "m-1:2"), "attachment-m-12.bin");
}

#[test]
fn default_export_path_uses_thread_and_message_partitions() {
    let repo_root = PathBuf::from("mailroom-test-root");
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let path = default_export_path(&paths, "thread-1", "message-1", "m-1:1.2", "note.pdf");

    assert_eq!(
        path,
        repo_root
            .join(".mailroom")
            .join("exports")
            .join("thread-1")
            .join("message-1--m-11.2--note.pdf")
    );
}

#[test]
fn default_export_path_falls_back_when_partition_ids_sanitize_to_empty() {
    let repo_root = PathBuf::from("mailroom-test-root");
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let path = default_export_path(&paths, "///", "\\\\", "///", "note.pdf");

    assert_eq!(
        path,
        repo_root
            .join(".mailroom")
            .join("exports")
            .join("thread")
            .join("message--attachment--note.pdf")
    );
}

#[test]
fn default_export_path_is_unique_per_attachment_key() {
    let paths = WorkspacePaths::from_repo_root(PathBuf::from("/tmp/mailroom"));
    let first = default_export_path(&paths, "thread-1", "message-1", "m-1:1.1", "note.pdf");
    let second = default_export_path(&paths, "thread-1", "message-1", "m-1:1.2", "note.pdf");

    assert_ne!(first, second);
}

#[test]
fn resolve_vault_relative_path_rejects_parent_traversal() {
    let paths = WorkspacePaths::from_repo_root(PathBuf::from("/tmp/mailroom"));
    let error = resolve_vault_relative_path(&paths, "../escape.bin").unwrap_err();

    assert!(matches!(
        error.downcast_ref::<AttachmentServiceError>(),
        Some(AttachmentServiceError::InvalidVaultPath { relative_path })
            if relative_path == "../escape.bin"
    ));
}

#[test]
fn existing_vault_report_requires_hash_match_before_reuse() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();

    let relative_path = "blake3/ab/abc123";
    let vault_path = paths.vault_dir.join(relative_path);
    fs::create_dir_all(vault_path.parent().unwrap()).unwrap();
    fs::write(&vault_path, b"hello").unwrap();

    let report = existing_vault_report(
        &paths,
        "gmail:operator@example.com",
        &detail_with_vault(relative_path, "invalid-hash", 5),
    )
    .unwrap();

    assert!(report.is_none());
}

#[test]
fn existing_vault_report_reuses_matching_vault_file() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();

    let bytes = b"hello";
    let content_hash = blake3::hash(bytes).to_hex().to_string();
    let relative_path = format!("blake3/{}/{}", &content_hash[..2], content_hash);
    let vault_path = paths.vault_dir.join(&relative_path);
    fs::create_dir_all(vault_path.parent().unwrap()).unwrap();
    fs::write(&vault_path, bytes).unwrap();

    let report = existing_vault_report(
        &paths,
        "gmail:operator@example.com",
        &detail_with_vault(&relative_path, &content_hash, 5),
    )
    .unwrap();

    assert!(report.is_some());
    assert!(!report.unwrap().downloaded);
}

#[test]
fn write_vault_bytes_rewrites_existing_hash_path_blob() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();

    let expected_bytes = b"hello".to_vec();
    let content_hash = blake3::hash(&expected_bytes).to_hex().to_string();
    let relative_path = format!("blake3/{}/{}", &content_hash[..2], &content_hash);
    let vault_path = paths.vault_dir.join(&relative_path);
    fs::create_dir_all(vault_path.parent().unwrap()).unwrap();
    fs::write(&vault_path, b"corrupt").unwrap();

    let write = write_vault_bytes(&paths.vault_dir, expected_bytes.clone()).unwrap();

    assert_eq!(write.path, vault_path);
    assert_eq!(fs::read(&vault_path).unwrap(), expected_bytes);
    assert_eq!(hash_file_blake3(&vault_path).unwrap(), write.content_hash);
    assert_eq!(write.size_bytes, 5);
}

#[test]
fn map_vault_state_write_error_maps_missing_rows_to_attachment_not_found() {
    let mapped = map_mailbox_write_error(
        crate::store::mailbox::MailboxWriteError::AttachmentNotFound {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("m-1:1.2"),
        },
    );
    assert!(matches!(
        mapped,
        AttachmentServiceError::AttachmentNotFound { attachment_key } if attachment_key == "m-1:1.2"
    ));
}

#[test]
fn copy_from_vault_returns_destination_conflict_for_different_existing_bytes() {
    let temp_dir = TempDir::new().unwrap();
    let source_path = temp_dir.path().join("source.bin");
    let destination_path = temp_dir.path().join("exports/export.bin");
    fs::create_dir_all(destination_path.parent().unwrap()).unwrap();
    fs::write(&source_path, b"hello").unwrap();
    fs::write(&destination_path, b"world").unwrap();

    let error = copy_from_vault(
        &source_path,
        &destination_path,
        blake3::hash(b"hello").to_hex().as_ref(),
    )
    .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<AttachmentServiceError>(),
        Some(AttachmentServiceError::DestinationConflict { path })
            if path == &destination_path
    ));
}

#[tokio::test]
async fn list_returns_invalid_limit_when_limit_is_zero() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = list(
        &config_report,
        AttachmentListRequest {
            thread_id: None,
            message_id: None,
            filename: None,
            mime_type: None,
            fetched_only: false,
            limit: 0,
        },
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<AttachmentServiceError>(),
        Some(AttachmentServiceError::InvalidLimit)
    ));
}

#[tokio::test]
async fn show_returns_no_active_account_without_account_state() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = show(&config_report, String::from("m-1:1.2"))
        .await
        .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<AttachmentServiceError>(),
        Some(AttachmentServiceError::NoActiveAccount)
    ));
}

#[tokio::test]
async fn export_returns_destination_conflict_for_existing_different_file() {
    let fixture = setup_export_test_fixture(b"hello".to_vec());
    let destination_path = fixture.temp_dir.path().join("exports/conflict.bin");
    fs::create_dir_all(destination_path.parent().unwrap()).unwrap();
    fs::write(&destination_path, b"world").unwrap();

    let error = export(
        &fixture.config_report,
        String::from("m-1:1.2"),
        Some(destination_path.clone()),
    )
    .await
    .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<AttachmentServiceError>(),
        Some(AttachmentServiceError::DestinationConflict { path })
            if path == &destination_path
    ));
}

#[tokio::test]
async fn export_removes_copied_file_when_event_persistence_fails() {
    let fixture = setup_export_test_fixture(b"hello".to_vec());
    let connection =
        rusqlite::Connection::open(&fixture.config_report.config.store.database_path).unwrap();
    connection
        .execute_batch(
            "
            CREATE TRIGGER fail_attachment_export_event_insert
            BEFORE INSERT ON attachment_export_events
            BEGIN
                SELECT RAISE(FAIL, 'forced export event failure');
            END;
            ",
        )
        .unwrap();

    let destination_path = fixture.temp_dir.path().join("exports/export.bin");
    let error = export(
        &fixture.config_report,
        String::from("m-1:1.2"),
        Some(destination_path.clone()),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            error.downcast_ref::<AttachmentServiceError>(),
            Some(AttachmentServiceError::StoreWrite { .. })
        ),
        "expected store write error, got {error:#}"
    );
    assert!(!destination_path.exists());
}

#[tokio::test]
async fn export_preserves_preexisting_matching_file_when_event_persistence_fails() {
    let vault_bytes = b"hello".to_vec();
    let fixture = setup_export_test_fixture(vault_bytes.clone());
    let connection =
        rusqlite::Connection::open(&fixture.config_report.config.store.database_path).unwrap();
    connection
        .execute_batch(
            "
            CREATE TRIGGER fail_attachment_export_event_insert
            BEFORE INSERT ON attachment_export_events
            BEGIN
                SELECT RAISE(FAIL, 'forced export event failure');
            END;
            ",
        )
        .unwrap();

    let destination_path = fixture.temp_dir.path().join("exports/preexisting.bin");
    fs::create_dir_all(destination_path.parent().unwrap()).unwrap();
    fs::write(&destination_path, &vault_bytes).unwrap();

    let error = export(
        &fixture.config_report,
        String::from("m-1:1.2"),
        Some(destination_path.clone()),
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            error.downcast_ref::<AttachmentServiceError>(),
            Some(AttachmentServiceError::StoreWrite { .. })
        ),
        "expected store write error, got {error:#}"
    );
    assert_eq!(fs::read(&destination_path).unwrap(), vault_bytes);
}

#[test]
#[ignore = "benchmark harness; run manually with: cargo test benchmark_attachment_export_hash_compare_tiers -- --ignored --nocapture"]
fn benchmark_attachment_export_hash_compare_tiers() {
    const COPY_ITERATIONS: usize = 8;
    const HASH_COMPARE_ITERATIONS: usize = 20;
    let tiers = [
        ("small", 64 * 1024_usize),
        ("medium", 1024 * 1024_usize),
        ("large", 8 * 1024 * 1024_usize),
    ];

    for (tier_name, size_bytes) in tiers {
        let temp_dir = TempDir::new().unwrap();
        let source_path = temp_dir.path().join("source.bin");
        let destination_path = temp_dir.path().join("exports/export.bin");
        fs::write(&source_path, vec![0xAC_u8; size_bytes]).unwrap();
        let source_hash = hash_file_blake3(&source_path).unwrap();

        let copy_started_at = Instant::now();
        for _ in 0..COPY_ITERATIONS {
            if destination_path.exists() {
                fs::remove_file(&destination_path).unwrap();
            }
            let copied = copy_from_vault(&source_path, &destination_path, &source_hash).unwrap();
            assert!(copied.copied);
        }
        let copy_elapsed = copy_started_at.elapsed();

        let compare_started_at = Instant::now();
        for _ in 0..HASH_COMPARE_ITERATIONS {
            let copied = copy_from_vault(&source_path, &destination_path, &source_hash).unwrap();
            assert!(!copied.copied);
        }
        let compare_elapsed = compare_started_at.elapsed();

        let copy_avg_ms = copy_elapsed.as_secs_f64() * 1_000.0 / COPY_ITERATIONS as f64;
        let compare_avg_ms =
            compare_elapsed.as_secs_f64() * 1_000.0 / HASH_COMPARE_ITERATIONS as f64;
        println!(
            "{{\"bench\":\"attachment_lane.export\",\"tier\":\"{tier_name}\",\"size_bytes\":{size_bytes},\"copy_avg_ms\":{copy_avg_ms:.3},\"hash_compare_avg_ms\":{compare_avg_ms:.3}}}"
        );
    }
}

fn detail_with_vault(
    relative_path: &str,
    content_hash: &str,
    vault_size_bytes: i64,
) -> AttachmentDetailRecord {
    AttachmentDetailRecord {
        attachment_key: String::from("m-1:1.2"),
        message_id: String::from("m-1"),
        thread_id: String::from("t-1"),
        part_id: String::from("1.2"),
        gmail_attachment_id: Some(String::from("att-1")),
        filename: String::from("statement.pdf"),
        mime_type: String::from("application/pdf"),
        size_bytes: 5,
        content_disposition: None,
        content_id: None,
        is_inline: false,
        internal_date_epoch_ms: 1_700_000_000_000,
        subject: String::from("Statement"),
        from_header: String::from("Billing <billing@example.com>"),
        vault_content_hash: Some(content_hash.to_owned()),
        vault_relative_path: Some(relative_path.to_owned()),
        vault_size_bytes: Some(vault_size_bytes),
        vault_fetched_at_epoch_s: Some(101),
        export_count: 0,
    }
}

fn setup_export_test_fixture(vault_bytes: Vec<u8>) -> ExportTestFixture {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
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
    crate::store::mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[crate::store::mailbox::GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Attachment fixture"),
            subject: String::from("Fixture"),
            from_header: String::from("Fixture <fixture@example.com>"),
            from_address: Some(String::from("fixture@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 256,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("m-1:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("fixture.bin"),
                mime_type: String::from("application/octet-stream"),
                size_bytes: 5,
                content_disposition: Some(String::from("attachment")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();
    let vault_write = write_vault_bytes(&paths.vault_dir, vault_bytes).unwrap();
    crate::store::mailbox::set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &crate::store::mailbox::AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("m-1:1.2"),
            content_hash: vault_write.content_hash,
            relative_path: vault_write.relative_path,
            size_bytes: vault_write.size_bytes,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap();

    ExportTestFixture {
        temp_dir,
        config_report,
    }
}
