use super::{SQLITE_APPLICATION_ID, harden_database_permissions, init, inspect, migrations};
use crate::config::resolve;
use crate::store::{accounts, mailbox};
use crate::workspace::WorkspacePaths;
use rusqlite::Connection;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;

#[test]
fn configure_busy_timeout_rejects_zero() {
    let connection = Connection::open_in_memory().unwrap();
    assert!(super::connection::configure_busy_timeout(&connection, 0).is_err());
}

#[test]
fn migrations_validate_successfully() {
    migrations::validate_migrations().unwrap();
}

#[test]
fn store_init_creates_and_migrates_database() {
    let repo_root = TempDir::with_prefix("mailroom-store-init").unwrap();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();

    let report = init(&config_report).unwrap();

    assert!(report.database_path.exists());
    assert_eq!(report.schema_version, 16);
    assert_eq!(report.pragmas.application_id, SQLITE_APPLICATION_ID);

    let connection = Connection::open(&report.database_path).unwrap();
    let substrate_tables: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table'
               AND name IN (
                   'app_metadata',
                   'accounts',
                   'gmail_labels',
                   'gmail_messages',
                   'gmail_message_labels',
                   'gmail_sync_state',
                   'gmail_full_sync_stage_labels',
                   'gmail_full_sync_stage_messages',
                   'gmail_full_sync_stage_message_labels',
                   'gmail_full_sync_stage_attachments',
                   'gmail_full_sync_checkpoint',
                   'gmail_sync_pacing_state',
                   'gmail_sync_run_history',
                   'gmail_sync_run_summary',
                   'gmail_incremental_sync_stage_delete_ids',
                   'gmail_incremental_sync_stage_messages',
                   'gmail_incremental_sync_stage_message_labels',
                   'gmail_incremental_sync_stage_attachments',
                   'gmail_full_sync_stage_pages',
                   'gmail_full_sync_stage_page_messages'
               )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(substrate_tables, 20);
}

#[test]
fn store_doctor_reports_absent_database_without_creating_it() {
    let repo_root = TempDir::with_prefix("mailroom-store-doctor").unwrap();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();

    let report = inspect(resolve(&paths).unwrap()).unwrap();

    assert!(!report.database_exists);
    assert!(report.pragmas.is_none());
    assert!(report.schema_version.is_none());
}

#[test]
fn store_doctor_reports_persisted_drift_without_rewriting_it() {
    let repo_root = TempDir::with_prefix("mailroom-store-doctor-drift").unwrap();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();

    let mut config_report = resolve(&paths).unwrap();
    let init_report = init(&config_report).unwrap();

    {
        let connection = Connection::open(&init_report.database_path).unwrap();
        connection
            .pragma_update(None, "application_id", 7_i64)
            .unwrap();
        connection
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| {
                row.get::<_, String>(0)
            })
            .unwrap();
        connection
            .pragma_update(None, "synchronous", "FULL")
            .unwrap();
    }

    config_report.config.store.database_path = init_report.database_path.clone();
    let report = inspect(config_report).unwrap();

    let pragmas = report.pragmas.unwrap();
    assert_eq!(pragmas.application_id, 7);
    assert_eq!(pragmas.journal_mode, "delete");
    assert!(pragmas.foreign_keys);
    assert!(!pragmas.trusted_schema);
    // The read-only diagnostics connection used by `inspect` applies its own
    // runtime PRAGMAs, so `report.pragmas.synchronous` reflects that path
    // (synchronous=1). The value persisted in the file from the earlier
    // `pragma_update(..., "synchronous", "FULL")` is still 2, as read below via
    // `Connection::open` and `pragma_query_value` on `init_report.database_path`.
    // The test checks both and asserts the on-disk `synchronous` was not overwritten.
    assert_eq!(pragmas.synchronous, 1);

    let connection = Connection::open(&init_report.database_path).unwrap();
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .unwrap();
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    let synchronous: i64 = connection
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .unwrap();

    assert_eq!(application_id, 7);
    assert_eq!(journal_mode, "delete");
    assert_eq!(synchronous, 2);
}

#[test]
fn store_init_rejects_foreign_database_before_mutating_it() {
    let repo_root = TempDir::with_prefix("mailroom-store-init-foreign").unwrap();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();

    let config_report = resolve(&paths).unwrap();
    {
        let connection = Connection::open(&config_report.config.store.database_path).unwrap();
        connection
            .pragma_update(None, "application_id", 7_i64)
            .unwrap();
        connection
            .pragma_update(None, "user_version", 0_i64)
            .unwrap();
    }

    let error = init(&config_report).unwrap_err();
    let error_message = error.to_string();
    assert!(error_message.contains("application_id 7"));
    assert!(error_message.contains("expected 0 or"));

    let connection = Connection::open(&config_report.config.store.database_path).unwrap();
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .unwrap();
    let user_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();

    assert_eq!(application_id, 7);
    assert_eq!(user_version, 0);
}

#[test]
fn pending_migrations_errors_when_database_is_ahead() {
    let error = super::pending_migrations(12, 13).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("database schema version 13 is newer than embedded migrations (12)")
    );
}

#[cfg(unix)]
#[test]
fn store_doctor_can_inspect_read_only_database_copy() {
    let repo_root = TempDir::with_prefix("mailroom-store-doctor-readonly").unwrap();
    let repo_root_path = repo_root.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root_path.clone());
    paths.ensure_runtime_dirs().unwrap();

    let mut config_report = resolve(&paths).unwrap();
    let init_report = init(&config_report).unwrap();
    let read_only_db = repo_root_path.join("readonly.sqlite3");

    fs::copy(&init_report.database_path, &read_only_db).unwrap();
    fs::set_permissions(&read_only_db, fs::Permissions::from_mode(0o400)).unwrap();

    config_report.config.store.database_path = read_only_db.clone();
    let report = inspect(config_report).unwrap();

    assert!(report.database_exists);
    assert_eq!(report.database_path, read_only_db);
    let pragmas = report.pragmas.unwrap();
    assert_eq!(pragmas.application_id, SQLITE_APPLICATION_ID);
    assert!(pragmas.foreign_keys);
    assert!(!pragmas.trusted_schema);
    assert_eq!(pragmas.synchronous, 1);
}

#[cfg(unix)]
#[test]
fn harden_database_permissions_updates_sqlite_sidecars() {
    let repo_root = TempDir::with_prefix("mailroom-store-permissions").unwrap();
    fs::create_dir_all(repo_root.path()).unwrap();

    let database_path = repo_root.path().join("store.sqlite3");
    let wal_path = repo_root.path().join("store.sqlite3-wal");
    let shm_path = repo_root.path().join("store.sqlite3-shm");

    fs::write(&database_path, b"").unwrap();
    fs::write(&wal_path, b"").unwrap();
    fs::write(&shm_path, b"").unwrap();

    fs::set_permissions(&database_path, fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(&wal_path, fs::Permissions::from_mode(0o644)).unwrap();
    fs::set_permissions(&shm_path, fs::Permissions::from_mode(0o644)).unwrap();

    harden_database_permissions(&database_path).unwrap();

    let database_mode = fs::metadata(&database_path).unwrap().permissions().mode() & 0o777;
    let wal_mode = fs::metadata(&wal_path).unwrap().permissions().mode() & 0o777;
    let shm_mode = fs::metadata(&shm_path).unwrap().permissions().mode() & 0o777;

    assert_eq!(database_mode, 0o600);
    assert_eq!(wal_mode, 0o600);
    assert_eq!(shm_mode, 0o600);
}

#[test]
fn migration_from_v6_backfills_attachment_account_scope_for_realistic_fixture() {
    const MESSAGE_COUNT_PER_ACCOUNT: usize = 160;
    const ATTACHMENTS_PER_MESSAGE: usize = 2;
    let repo_root = TempDir::with_prefix("mailroom-store-migration-v6-backfill").unwrap();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_specs = [
        ("operator@example.com", "gmail:operator@example.com", "op"),
        ("other@example.com", "gmail:other@example.com", "other"),
    ];
    for (email, account_id, prefix) in account_specs {
        accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: email.to_owned(),
                history_id: String::from("100"),
                messages_total: MESSAGE_COUNT_PER_ACCOUNT as i64,
                threads_total: MESSAGE_COUNT_PER_ACCOUNT as i64,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        mailbox::replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
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

        let messages = (0..MESSAGE_COUNT_PER_ACCOUNT)
            .map(|index| mailbox::GmailMessageUpsertInput {
                account_id: account_id.to_owned(),
                message_id: format!("{prefix}-m-{index}"),
                thread_id: format!("{prefix}-t-{index}"),
                history_id: format!("{}", 200 + index),
                internal_date_epoch_ms: 1_700_000_000_000 + i64::try_from(index).unwrap(),
                snippet: format!("Mailbox fixture message {index}"),
                subject: format!("Fixture {index}"),
                from_header: format!("Fixture <{prefix}@example.com>"),
                from_address: Some(format!("{prefix}@example.com")),
                recipient_headers: email.to_owned(),
                to_header: email.to_owned(),
                cc_header: String::new(),
                bcc_header: String::new(),
                reply_to_header: String::new(),
                size_estimate: 2048,
                automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
                attachments: (0..ATTACHMENTS_PER_MESSAGE)
                    .map(|part_index| mailbox::GmailAttachmentUpsertInput {
                        attachment_key: format!("{prefix}-m-{index}:1.{}", part_index + 1),
                        part_id: format!("1.{}", part_index + 1),
                        gmail_attachment_id: Some(format!("att-{prefix}-{index}-{part_index}")),
                        filename: format!("fixture-{index}-{part_index}.bin"),
                        mime_type: String::from("application/octet-stream"),
                        size_bytes: 256,
                        content_disposition: Some(String::from("attachment")),
                        content_id: None,
                        is_inline: false,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        mailbox::upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &messages,
            200,
        )
        .unwrap();
    }

    let expected_attachment_count =
        i64::try_from(account_specs.len() * MESSAGE_COUNT_PER_ACCOUNT * ATTACHMENTS_PER_MESSAGE)
            .unwrap();
    let connection = Connection::open(&config_report.config.store.database_path).unwrap();
    let seeded_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM gmail_message_attachments",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(seeded_count, expected_attachment_count);

    connection
        .execute_batch(include_str!(
            "../../migrations/15-sync-run-history/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/14-sync-pipeline-telemetry-and-page-manifests/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/13-bounded-sync-pipeline/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/12-sync-pacing-state-hardening/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/11-sync-pacing-state/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/10-full-sync-checkpoints/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/09-mailbox-full-sync-staging/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/08-automation-rules-and-bulk-actions/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/07-account-scoped-attachment-keys/down.sql"
        ))
        .unwrap();
    connection
        .pragma_update(None, "user_version", 6_i64)
        .unwrap();
    let account_column_count: i64 = connection
        .query_row(
            "SELECT COUNT(*)
             FROM pragma_table_info('gmail_message_attachments')
             WHERE name = 'account_id'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(account_column_count, 0);
    drop(connection);

    let migration_report = init(&config_report).unwrap();
    assert_eq!(migration_report.schema_version, 16);
    assert_eq!(migration_report.pending_migrations, 0);

    let connection = Connection::open(&config_report.config.store.database_path).unwrap();
    let migrated_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM gmail_message_attachments",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(migrated_count, expected_attachment_count);

    let null_account_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM gmail_message_attachments WHERE account_id IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(null_account_count, 0);

    let mismatched_account_count: i64 = connection
        .query_row(
            "SELECT COUNT(*)
             FROM gmail_message_attachments gma
             INNER JOIN gmail_messages gm
               ON gm.message_rowid = gma.message_rowid
             WHERE gma.account_id != gm.account_id",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mismatched_account_count, 0);

    let shared_key = String::from("post-migration-shared:1.1");
    let operator_message = mailbox::GmailMessageUpsertInput {
        account_id: String::from("gmail:operator@example.com"),
        message_id: String::from("post-op-m-1"),
        thread_id: String::from("post-op-t-1"),
        history_id: String::from("9991"),
        internal_date_epoch_ms: 1_800_000_000_001,
        snippet: String::from("Post migration"),
        subject: String::from("Post migration"),
        from_header: String::from("Fixture <operator@example.com>"),
        from_address: Some(String::from("operator@example.com")),
        recipient_headers: String::from("operator@example.com"),
        to_header: String::from("operator@example.com"),
        cc_header: String::new(),
        bcc_header: String::new(),
        reply_to_header: String::new(),
        size_estimate: 123,
        automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
        label_ids: vec![String::from("INBOX")],
        label_names_text: String::from("INBOX"),
        attachments: vec![mailbox::GmailAttachmentUpsertInput {
            attachment_key: shared_key.clone(),
            part_id: String::from("1.1"),
            gmail_attachment_id: Some(String::from("att-post-op")),
            filename: String::from("post.bin"),
            mime_type: String::from("application/octet-stream"),
            size_bytes: 1,
            content_disposition: Some(String::from("attachment")),
            content_id: None,
            is_inline: false,
        }],
    };
    let other_message = mailbox::GmailMessageUpsertInput {
        account_id: String::from("gmail:other@example.com"),
        message_id: String::from("post-other-m-1"),
        thread_id: String::from("post-other-t-1"),
        history_id: String::from("9992"),
        internal_date_epoch_ms: 1_800_000_000_002,
        snippet: String::from("Post migration"),
        subject: String::from("Post migration"),
        from_header: String::from("Fixture <other@example.com>"),
        from_address: Some(String::from("other@example.com")),
        recipient_headers: String::from("other@example.com"),
        to_header: String::from("other@example.com"),
        cc_header: String::new(),
        bcc_header: String::new(),
        reply_to_header: String::new(),
        size_estimate: 123,
        automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
        label_ids: vec![String::from("INBOX")],
        label_names_text: String::from("INBOX"),
        attachments: vec![mailbox::GmailAttachmentUpsertInput {
            attachment_key: shared_key.clone(),
            part_id: String::from("1.1"),
            gmail_attachment_id: Some(String::from("att-post-other")),
            filename: String::from("post.bin"),
            mime_type: String::from("application/octet-stream"),
            size_bytes: 1,
            content_disposition: Some(String::from("attachment")),
            content_id: None,
            is_inline: false,
        }],
    };
    mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[operator_message],
        300,
    )
    .unwrap();
    mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[other_message],
        300,
    )
    .unwrap();

    let shared_key_count: i64 = Connection::open(&config_report.config.store.database_path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*)
             FROM gmail_message_attachments
             WHERE attachment_key = ?1",
            [&shared_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(shared_key_count, 2);
}

#[test]
fn migration_v16_round_trip_rebuilds_and_preserves_sync_run_summaries() {
    let repo_root = TempDir::with_prefix("mailroom-store-migration-v16-sync-history").unwrap();
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("500"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 500,
        },
    )
    .unwrap();

    let sync_state = mailbox::upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &mailbox::SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("500")),
            bootstrap_query: String::from("in:anywhere -in:spam -in:trash newer_than:30d"),
            last_sync_mode: mailbox::SyncMode::Incremental,
            last_sync_status: mailbox::SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 500,
            last_full_sync_success_epoch_s: Some(490),
            last_incremental_sync_success_epoch_s: Some(500),
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

    let make_outcome = |started_at_epoch_s: i64, finished_at_epoch_s: i64, messages_listed: i64| {
        let comparability = mailbox::comparability_for_incremental_workload(messages_listed, 0);
        mailbox::SyncRunOutcomeInput {
            account_id: account.account_id.clone(),
            sync_mode: mailbox::SyncMode::Incremental,
            status: mailbox::SyncStatus::Ok,
            comparability_kind: comparability.kind,
            comparability_key: comparability.key,
            startup_seed_run_id: None,
            started_at_epoch_s,
            finished_at_epoch_s,
            bootstrap_query: String::from("in:anywhere -in:spam -in:trash newer_than:30d"),
            cursor_history_id: Some(String::from("500")),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            pages_fetched: 1,
            messages_listed,
            messages_upserted: messages_listed,
            messages_deleted: 0,
            labels_synced: 10,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
            pipeline_enabled: true,
            pipeline_list_queue_high_water: 1,
            pipeline_write_queue_high_water: 1,
            pipeline_write_batch_count: 1,
            pipeline_writer_wait_ms: 10,
            pipeline_fetch_batch_count: 1,
            pipeline_fetch_batch_avg_ms: 10,
            pipeline_fetch_batch_max_ms: 10,
            pipeline_writer_tx_count: 1,
            pipeline_writer_tx_avg_ms: 5,
            pipeline_writer_tx_max_ms: 5,
            pipeline_reorder_buffer_high_water: 1,
            pipeline_staged_message_count: messages_listed,
            pipeline_staged_delete_count: 0,
            pipeline_staged_attachment_count: 0,
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
            estimated_quota_units_reserved: messages_listed * 5,
            http_attempt_count: messages_listed,
            retry_count: 0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
            duration_ms: messages_listed * 10,
            pages_per_second: 1.0,
            messages_per_second: messages_listed as f64,
            error_message: None,
        }
    };

    let (_, tiny_history, _) = mailbox::persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &make_outcome(500, 510, 10),
    )
    .unwrap();
    let (_, large_history, _) = mailbox::persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &make_outcome(520, 530, 600),
    )
    .unwrap();

    let connection = Connection::open(&config_report.config.store.database_path).unwrap();
    let pre_down_summary_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM gmail_sync_run_summary WHERE account_id = ?1 AND sync_mode = 'incremental'",
            [&account.account_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pre_down_summary_count, 2);

    connection
        .execute_batch(include_str!(
            "../../migrations/16-sync-history-comparability/down.sql"
        ))
        .unwrap();
    connection
        .pragma_update(None, "user_version", 15_i64)
        .unwrap();

    let post_down_summary_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM gmail_sync_run_summary WHERE account_id = ?1 AND sync_mode = 'incremental'",
            [&account.account_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(post_down_summary_count, 1);
    let post_down_latest_run_id: i64 = connection
        .query_row(
            "SELECT latest_run_id FROM gmail_sync_run_summary WHERE account_id = ?1 AND sync_mode = 'incremental'",
            [&account.account_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(post_down_latest_run_id, large_history.run_id);

    connection
        .execute_batch(include_str!(
            "../../migrations/16-sync-history-comparability/up.sql"
        ))
        .unwrap();
    connection
        .pragma_update(None, "user_version", 16_i64)
        .unwrap();

    let post_up_summary_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM gmail_sync_run_summary WHERE account_id = ?1 AND sync_mode = 'incremental'",
            [&account.account_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(post_up_summary_count, 2);

    let tiny_best_clean_run_id: i64 = connection
        .query_row(
            "SELECT best_clean_run_id
             FROM gmail_sync_run_summary
             WHERE account_id = ?1
               AND sync_mode = 'incremental'
               AND comparability_key = 'tiny'",
            [&account.account_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tiny_best_clean_run_id, tiny_history.run_id);

    let large_best_clean_run_id: i64 = connection
        .query_row(
            "SELECT best_clean_run_id
             FROM gmail_sync_run_summary
             WHERE account_id = ?1
               AND sync_mode = 'incremental'
               AND comparability_key = 'large'",
            [&account.account_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(large_best_clean_run_id, large_history.run_id);

    drop(connection);
}
