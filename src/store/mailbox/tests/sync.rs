use super::*;

#[test]
fn inspect_mailbox_reports_sync_state_and_counts() {
    let repo_root = unique_temp_dir("mailroom-mailbox-inspect");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("200"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 200,
        },
    )
    .unwrap();

    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        200,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("201"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Project alpha launch checklist"),
            subject: String::from("Alpha launch"),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
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
        200,
    )
    .unwrap();

    let sync_state = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(String::from("201")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Full,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 200,
            last_full_sync_success_epoch_s: Some(200),
            last_incremental_sync_success_epoch_s: None,
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
    let pacing_state = upsert_sync_pacing_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncPacingStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 9_500,
            learned_message_fetch_concurrency: 3,
            clean_run_streak: 2,
            last_pressure_kind: Some(SyncPacingPressureKind::Quota),
            updated_at_epoch_s: 205,
        },
    )
    .unwrap();

    let stored_state = get_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(stored_state, sync_state);

    let report = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.message_count, 1);
    assert_eq!(report.label_count, 1);
    assert_eq!(report.indexed_message_count, 1);
    assert_eq!(
        report
            .sync_state
            .as_ref()
            .and_then(|state| state.cursor_history_id.as_deref()),
        Some("201")
    );
    assert_eq!(report.sync_pacing_state.as_ref(), Some(&pacing_state));

    super::delete_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[String::from("m-1")],
    )
    .unwrap();

    let after_delete = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(after_delete.message_count, 0);
    assert_eq!(after_delete.indexed_message_count, 0);
}

#[test]
fn upsert_sync_pacing_state_rejects_invalid_ranges() {
    let repo_root = unique_temp_dir("mailroom-mailbox-sync-pacing-checks");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();

    let result = upsert_sync_pacing_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncPacingStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 4,
            learned_message_fetch_concurrency: 0,
            clean_run_streak: -1,
            last_pressure_kind: None,
            updated_at_epoch_s: 110,
        },
    );

    let error = result.unwrap_err();
    match error.downcast_ref::<rusqlite::Error>() {
        Some(rusqlite::Error::SqliteFailure(_, Some(message))) => {
            assert!(
                message.contains("learned_quota_units_per_minute BETWEEN 5 AND 12000")
                    || message.contains("learned_message_fetch_concurrency BETWEEN 1 AND 4")
                    || message.contains("clean_run_streak >= 0"),
                "expected pacing range constraint failure, got: {message}"
            );
        }
        other => panic!("expected sqlite constraint query error, got: {other:?}"),
    }
}

#[test]
fn get_sync_pacing_state_rejects_invalid_pressure_kind_values() {
    let repo_root = unique_temp_dir("mailroom-mailbox-sync-pacing-decode");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();

    upsert_sync_pacing_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncPacingStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 12_000,
            learned_message_fetch_concurrency: 4,
            clean_run_streak: 1,
            last_pressure_kind: None,
            updated_at_epoch_s: 110,
        },
    )
    .unwrap();

    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE gmail_sync_pacing_state
             SET last_pressure_kind = ?2,
                 updated_at_epoch_s = ?3
             WHERE account_id = ?1",
            rusqlite::params!["gmail:operator@example.com", "bogus", 111_i64],
        )
        .unwrap();
    drop(connection);

    let result = super::get_sync_pacing_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    );

    assert!(matches!(result, Err(MailboxReadError::Query(_))));
}

#[test]
fn inspect_mailbox_tolerates_pre_attachment_schema() {
    let repo_root = unique_temp_dir("mailroom-mailbox-inspect-pre-attachments");
    let database_path = repo_root.path().join("mailroom.sqlite3");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    connection
        .execute_batch(
            "
            CREATE TABLE gmail_sync_state (
                account_id TEXT PRIMARY KEY,
                cursor_history_id TEXT,
                bootstrap_query TEXT NOT NULL,
                last_sync_mode TEXT NOT NULL,
                last_sync_status TEXT NOT NULL,
                last_error TEXT,
                last_sync_epoch_s INTEGER NOT NULL,
                last_full_sync_success_epoch_s INTEGER,
                last_incremental_sync_success_epoch_s INTEGER,
                message_count INTEGER NOT NULL,
                label_count INTEGER NOT NULL,
                indexed_message_count INTEGER NOT NULL
            );
            CREATE TABLE gmail_messages (
                message_rowid INTEGER PRIMARY KEY,
                account_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                history_id TEXT NOT NULL,
                internal_date_epoch_ms INTEGER NOT NULL,
                snippet TEXT NOT NULL,
                subject TEXT NOT NULL,
                from_header TEXT NOT NULL,
                from_address TEXT,
                recipient_headers TEXT NOT NULL,
                to_header TEXT NOT NULL,
                cc_header TEXT NOT NULL,
                bcc_header TEXT NOT NULL,
                reply_to_header TEXT NOT NULL,
                size_estimate INTEGER NOT NULL,
                label_names_text TEXT NOT NULL
            );
            CREATE TABLE gmail_labels (
                label_rowid INTEGER PRIMARY KEY,
                account_id TEXT NOT NULL,
                label_id TEXT NOT NULL,
                name TEXT NOT NULL,
                label_type TEXT NOT NULL
            );
            CREATE VIRTUAL TABLE gmail_message_search USING fts5(
                subject,
                from_header,
                recipient_headers,
                snippet,
                label_names_text
            );
            ",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO gmail_sync_state (
                account_id,
                cursor_history_id,
                bootstrap_query,
                last_sync_mode,
                last_sync_status,
                last_error,
                last_sync_epoch_s,
                last_full_sync_success_epoch_s,
                last_incremental_sync_success_epoch_s,
                message_count,
                label_count,
                indexed_message_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, NULL, ?8, ?9, ?10)",
            rusqlite::params![
                "gmail:operator@example.com",
                "201",
                "newer_than:90d",
                "full",
                "ok",
                200_i64,
                200_i64,
                1_i64,
                1_i64,
                1_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO gmail_messages (
                message_rowid,
                account_id,
                message_id,
                thread_id,
                history_id,
                internal_date_epoch_ms,
                snippet,
                subject,
                from_header,
                from_address,
                recipient_headers,
                to_header,
                cc_header,
                bcc_header,
                reply_to_header,
                size_estimate,
                label_names_text
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, '', '', '', ?13, ?14)",
            rusqlite::params![
                1_i64,
                "gmail:operator@example.com",
                "m-1",
                "t-1",
                "201",
                1_700_000_000_000_i64,
                "Project alpha launch checklist",
                "Alpha launch",
                "Alice <alice@example.com>",
                "alice@example.com",
                "operator@example.com",
                "operator@example.com",
                123_i64,
                "INBOX",
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO gmail_labels (label_rowid, account_id, label_id, name, label_type)
             VALUES (1, ?1, ?2, ?3, ?4)",
            rusqlite::params!["gmail:operator@example.com", "INBOX", "INBOX", "system",],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO gmail_message_search (
                rowid,
                subject,
                from_header,
                recipient_headers,
                snippet,
                label_names_text
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                1_i64,
                "Alpha launch",
                "Alice <alice@example.com>",
                "operator@example.com",
                "Project alpha launch checklist",
                "INBOX",
            ],
        )
        .unwrap();

    let report = inspect_mailbox(&database_path, 5_000).unwrap().unwrap();

    assert_eq!(report.message_count, 1);
    assert_eq!(report.label_count, 1);
    assert_eq!(report.indexed_message_count, 1);
    assert_eq!(report.attachment_count, 0);
    assert_eq!(report.vaulted_attachment_count, 0);
    assert_eq!(report.attachment_export_count, 0);
    assert_eq!(
        report
            .sync_state
            .as_ref()
            .map(|state| state.account_id.as_str()),
        Some("gmail:operator@example.com")
    );
}

#[test]
fn upsert_sync_state_preserves_prior_success_timestamps() {
    let repo_root = unique_temp_dir("mailroom-mailbox-sync-state");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("200"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 200,
        },
    )
    .unwrap();

    upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(String::from("200")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Full,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 200,
            last_full_sync_success_epoch_s: Some(200),
            last_incremental_sync_success_epoch_s: None,
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

    let after_incremental = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(String::from("250")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 300,
            last_full_sync_success_epoch_s: None,
            last_incremental_sync_success_epoch_s: Some(300),
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
    assert_eq!(after_incremental.last_full_sync_success_epoch_s, Some(200));
    assert_eq!(
        after_incremental.last_incremental_sync_success_epoch_s,
        Some(300)
    );

    let after_failed_incremental = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(String::from("260")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Failed,
            last_error: Some(String::from("boom")),
            last_sync_epoch_s: 350,
            last_full_sync_success_epoch_s: None,
            last_incremental_sync_success_epoch_s: None,
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
    assert_eq!(
        after_failed_incremental.last_full_sync_success_epoch_s,
        Some(200)
    );
    assert_eq!(
        after_failed_incremental.last_incremental_sync_success_epoch_s,
        Some(300)
    );

    let after_full = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(String::from("300")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Full,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 400,
            last_full_sync_success_epoch_s: Some(400),
            last_incremental_sync_success_epoch_s: None,
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
    assert_eq!(after_full.last_full_sync_success_epoch_s, Some(400));
    assert_eq!(after_full.last_incremental_sync_success_epoch_s, Some(300));
}

#[test]
fn get_sync_state_rejects_invalid_persisted_mode() {
    let repo_root = unique_temp_dir("mailroom-invalid-sync-mode");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();

    upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(String::from("100")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Full,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 100,
            last_full_sync_success_epoch_s: Some(100),
            last_incremental_sync_success_epoch_s: None,
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

    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
    connection
        .execute(
            "UPDATE gmail_sync_state SET last_sync_mode = 'bogus' WHERE account_id = ?1",
            ["gmail:operator@example.com"],
        )
        .unwrap();

    let error = get_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap_err();
    assert!(error.to_string().contains("invalid mailbox sync mode"));
}

#[test]
fn delete_messages_removes_large_deduplicated_batches() {
    let repo_root = unique_temp_dir("mailroom-mailbox-delete-batch");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();

    let messages = (0..450)
        .map(|index| GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: format!("m-{index}"),
            thread_id: format!("t-{index}"),
            history_id: format!("{}", 200 + index),
            internal_date_epoch_ms: 1_700_000_000_000 + i64::from(index),
            snippet: format!("Snippet {index}"),
            subject: format!("Subject {index}"),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
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
        })
        .collect::<Vec<_>>();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &messages,
        100,
    )
    .unwrap();

    let mut message_ids = (0..450)
        .map(|index| format!("m-{index}"))
        .collect::<Vec<_>>();
    message_ids.extend([String::from("m-1"), String::from("missing")]);

    let deleted = super::delete_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &message_ids,
    )
    .unwrap();
    assert_eq!(deleted, 450);

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 0);
    assert_eq!(mailbox.indexed_message_count, 0);
}

#[test]
fn staged_full_sync_does_not_change_live_mailbox_until_finalize() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-before-finalize");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
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

    let account_id = "gmail:operator@example.com";
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            account_id,
            "live-1",
            "Existing cached subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        100,
    )
    .unwrap();

    reset_full_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    stage_full_sync_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
    )
    .unwrap();
    stage_full_sync_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "stage-1",
            "Staged subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
    )
    .unwrap();

    let before_finalize = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("Existing"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(before_finalize.len(), 1);
    assert_eq!(before_finalize[0].message_id, "live-1");

    let staged_hits = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("Staged"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(staged_hits.is_empty());
}

#[test]
fn reset_full_sync_stage_clears_stale_rows_before_finalize() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-reset");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    reset_full_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    stage_full_sync_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
    )
    .unwrap();
    stage_full_sync_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "stale-message",
            "Stale subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
    )
    .unwrap();

    reset_full_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    stage_full_sync_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
    )
    .unwrap();
    stage_full_sync_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "fresh-message",
            "Fresh subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
    )
    .unwrap();
    finalize_full_sync_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        300,
        &full_sync_state(account_id, 300),
    )
    .unwrap();

    let results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("subject"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "fresh-message");
}

#[test]
fn finalize_full_sync_from_stage_with_no_messages_clears_existing_live_rows() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-empty-finalize");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            account_id,
            "live-1",
            "To be removed",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        100,
    )
    .unwrap();

    reset_full_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    stage_full_sync_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
    )
    .unwrap();
    finalize_full_sync_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        200,
        &full_sync_state(account_id, 200),
    )
    .unwrap();

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 0);

    let results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("removed"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(results.is_empty());
}

#[test]
fn prepare_full_sync_checkpoint_exposes_progress_in_mailbox_doctor() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-checkpoint-doctor");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    let checkpoint = prepare_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: None,
            cursor_history_id: Some("100"),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 1,
            started_at_epoch_s: 100,
            updated_at_epoch_s: 100,
        }),
    )
    .unwrap();

    assert_eq!(checkpoint.bootstrap_query, "newer_than:90d");
    assert_eq!(checkpoint.status, FullSyncCheckpointStatus::Paging);
    assert_eq!(checkpoint.staged_label_count, 1);
    assert_eq!(checkpoint.staged_message_count, 0);

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    let checkpoint = mailbox.full_sync_checkpoint.unwrap();
    assert_eq!(checkpoint.account_id, account_id);
    assert_eq!(checkpoint.bootstrap_query, "newer_than:90d");
    assert_eq!(checkpoint.status, FullSyncCheckpointStatus::Paging);
    assert_eq!(checkpoint.staged_label_count, 1);
    assert_eq!(checkpoint.staged_message_count, 0);
}

#[test]
fn update_full_sync_checkpoint_labels_preserves_staged_messages() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-checkpoint-label-refresh");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    let checkpoint = prepare_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-2"),
            cursor_history_id: Some("100"),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 1,
            started_at_epoch_s: 100,
            updated_at_epoch_s: 100,
        }),
    )
    .unwrap();
    let checkpoint = stage_full_sync_page_and_update_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "staged-1",
            "Staged checkpoint subject",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-3"),
            cursor_history_id: checkpoint.cursor_history_id.as_deref(),
            pages_fetched: 1,
            messages_listed: 1,
            messages_upserted: 1,
            labels_synced: 1,
            started_at_epoch_s: checkpoint.started_at_epoch_s,
            updated_at_epoch_s: 101,
        }),
    )
    .unwrap();
    assert_eq!(checkpoint.staged_message_count, 1);
    assert_eq!(checkpoint.staged_attachment_count, 1);

    let refreshed = update_full_sync_checkpoint_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[
            gmail_label("INBOX", "PrimaryInbox", "system"),
            gmail_label("STARRED", "STARRED", "system"),
        ],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-3"),
            cursor_history_id: checkpoint.cursor_history_id.as_deref(),
            pages_fetched: checkpoint.pages_fetched,
            messages_listed: checkpoint.messages_listed,
            messages_upserted: checkpoint.messages_upserted,
            labels_synced: 2,
            started_at_epoch_s: checkpoint.started_at_epoch_s,
            updated_at_epoch_s: 102,
        }),
    )
    .unwrap();

    assert_eq!(refreshed.staged_label_count, 2);
    assert_eq!(refreshed.staged_message_count, 1);
    assert_eq!(refreshed.staged_attachment_count, 1);

    let results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("checkpoint"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(results.is_empty());

    finalize_full_sync_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        103,
        &full_sync_state(account_id, 103),
    )
    .unwrap();

    let starred_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("PrimaryInbox"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(starred_results.len(), 1);
    assert_eq!(starred_results[0].message_id, "staged-1");

    let stale_label_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("INBOX"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(stale_label_results.is_empty());
}

#[test]
fn partial_full_sync_page_chunks_reject_checkpoint_updates() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-page-partial-checkpoint-guard");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    prepare_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-2"),
            cursor_history_id: Some("100"),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 1,
            started_at_epoch_s: 100,
            updated_at_epoch_s: 100,
        }),
    )
    .unwrap();

    let mut writer = MailboxWriterConnection::open(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    let error = writer
        .stage_full_sync_page_chunk_and_maybe_update_checkpoint(
            &super::FullSyncStagePageInput {
                page_seq: 0,
                listed_count: 1,
                next_page_token: Some(String::from("page-2")),
                updated_at_epoch_s: 101,
                page_complete: false,
            },
            &[mailbox_message(
                account_id,
                "partial-1",
                "Partial chunk subject",
                &["INBOX"],
                "INBOX",
                &["1.1"],
            )],
            Some(&full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
                bootstrap_query: "newer_than:90d",
                status: FullSyncCheckpointStatus::Paging,
                next_page_token: Some("page-3"),
                cursor_history_id: Some("101"),
                pages_fetched: 1,
                messages_listed: 1,
                messages_upserted: 1,
                labels_synced: 1,
                started_at_epoch_s: 100,
                updated_at_epoch_s: 101,
            })),
        )
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "partial full sync page chunks must not advance the checkpoint"
    );
}

#[test]
fn full_sync_stage_page_counts_are_idempotent_on_chunk_retry() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-page-retry-idempotent");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    prepare_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-2"),
            cursor_history_id: Some("100"),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 1,
            started_at_epoch_s: 100,
            updated_at_epoch_s: 100,
        }),
    )
    .unwrap();

    let mut writer = MailboxWriterConnection::open(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    let page_input = super::FullSyncStagePageInput {
        page_seq: 0,
        listed_count: 1,
        next_page_token: Some(String::from("page-2")),
        updated_at_epoch_s: 101,
        page_complete: false,
    };
    let messages = vec![mailbox_message(
        account_id,
        "retry-1",
        "Retry chunk subject",
        &["INBOX"],
        "INBOX",
        &["1.1"],
    )];

    writer
        .stage_full_sync_page_chunk_and_maybe_update_checkpoint(&page_input, &messages, None)
        .unwrap();
    writer
        .stage_full_sync_page_chunk_and_maybe_update_checkpoint(&page_input, &messages, None)
        .unwrap();

    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
    let staged_message_count: i64 = connection
        .query_row(
            "SELECT staged_message_count
             FROM gmail_full_sync_stage_pages
             WHERE account_id = ?1
               AND page_seq = ?2",
            rusqlite::params![account_id, 0_i64],
            |row| row.get(0),
        )
        .unwrap();
    let page_message_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*)
             FROM gmail_full_sync_stage_page_messages
             WHERE account_id = ?1
               AND page_seq = ?2",
            rusqlite::params![account_id, 0_i64],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(staged_message_count, 1);
    assert_eq!(page_message_rows, 1);
}

#[test]
fn persist_successful_sync_outcome_records_history_and_summary() {
    let repo_root = unique_temp_dir("mailroom-sync-history-success");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    let sync_state = upsert_sync_state(
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
            last_full_sync_success_epoch_s: Some(90),
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

    let (_, history, summary) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 100,
            finished_at_epoch_s: 110,
            messages_listed: 125,
            duration_ms: 100,
            messages_per_second: 125.0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();

    assert!(history.run_id > 0);
    assert_eq!(summary.latest_run_id, history.run_id);
    assert_eq!(summary.best_clean_run_id, Some(history.run_id));
    assert!(!summary.regression_detected);

    let history_rows = list_sync_run_history(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        10,
    )
    .unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(history_rows[0].run_id, history.run_id);

    let persisted_summary = get_sync_run_summary(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
    )
    .unwrap()
    .unwrap();
    assert_eq!(persisted_summary.best_clean_run_id, Some(history.run_id));
}

#[test]
fn persist_failed_sync_outcome_detects_failure_streak_regression() {
    let repo_root = unique_temp_dir("mailroom-sync-history-failure");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("200"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 200,
        },
    )
    .unwrap();

    for finished_at_epoch_s in [210, 220] {
        persist_failed_sync_outcome(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: account.account_id.clone(),
                cursor_history_id: Some(String::from("200")),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Incremental,
                last_sync_status: SyncStatus::Failed,
                last_error: Some(String::from("sync failed")),
                last_sync_epoch_s: finished_at_epoch_s,
                last_full_sync_success_epoch_s: Some(190),
                last_incremental_sync_success_epoch_s: Some(190),
                pipeline_enabled: true,
                pipeline_list_queue_high_water: 1,
                pipeline_write_queue_high_water: 1,
                pipeline_write_batch_count: 1,
                pipeline_writer_wait_ms: 1,
                pipeline_fetch_batch_count: 1,
                pipeline_fetch_batch_avg_ms: 1,
                pipeline_fetch_batch_max_ms: 1,
                pipeline_writer_tx_count: 1,
                pipeline_writer_tx_avg_ms: 1,
                pipeline_writer_tx_max_ms: 1,
                pipeline_reorder_buffer_high_water: 1,
                pipeline_staged_message_count: 1,
                pipeline_staged_delete_count: 0,
                pipeline_staged_attachment_count: 0,
            },
            &sample_sync_run_outcome(SampleSyncRunOutcome {
                account_id: account.account_id.clone(),
                sync_mode: SyncMode::Incremental,
                status: SyncStatus::Failed,
                started_at_epoch_s: finished_at_epoch_s - 5,
                finished_at_epoch_s,
                messages_listed: 0,
                duration_ms: 100,
                messages_per_second: 0.0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
            }),
        )
        .unwrap();
    }

    let summary = get_sync_run_summary(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
    )
    .unwrap()
    .unwrap();
    assert!(summary.regression_detected);
    assert_eq!(
        summary.regression_kind,
        Some(SyncRunRegressionKind::FailureStreak)
    );
    assert_eq!(summary.recent_failure_streak, 2);

    let history = list_sync_run_history(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        10,
    )
    .unwrap();
    assert_eq!(history.len(), 2);
    assert!(history.iter().all(|row| row.status == SyncStatus::Failed));
}

#[test]
fn sync_run_summary_caps_recent_streaks_to_summary_window() {
    let repo_root = unique_temp_dir("mailroom-sync-history-windowed-streaks");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("225"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 225,
        },
    )
    .unwrap();

    for finished_at_epoch_s in 226..=237 {
        persist_failed_sync_outcome(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: account.account_id.clone(),
                cursor_history_id: Some(String::from("225")),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Incremental,
                last_sync_status: SyncStatus::Failed,
                last_error: Some(String::from("sync failed")),
                last_sync_epoch_s: finished_at_epoch_s,
                last_full_sync_success_epoch_s: Some(220),
                last_incremental_sync_success_epoch_s: Some(220),
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
            &sample_sync_run_outcome(SampleSyncRunOutcome {
                account_id: account.account_id.clone(),
                sync_mode: SyncMode::Incremental,
                status: SyncStatus::Failed,
                started_at_epoch_s: finished_at_epoch_s - 5,
                finished_at_epoch_s,
                messages_listed: 0,
                duration_ms: 100,
                messages_per_second: 0.0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
            }),
        )
        .unwrap();
    }

    let summary = get_sync_run_summary(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
    )
    .unwrap()
    .unwrap();
    assert_eq!(summary.recent_failure_streak, 10);
}

#[test]
fn persist_successful_sync_outcome_detects_retry_pressure_regression() {
    let repo_root = unique_temp_dir("mailroom-sync-history-retry-pressure");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("250"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 250,
        },
    )
    .unwrap();
    let sync_state = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("250")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 250,
            last_full_sync_success_epoch_s: Some(240),
            last_incremental_sync_success_epoch_s: Some(250),
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

    for finished_at_epoch_s in [260, 270, 280] {
        persist_successful_sync_outcome(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &sync_state,
            &sample_sync_run_outcome(SampleSyncRunOutcome {
                account_id: account.account_id.clone(),
                sync_mode: SyncMode::Incremental,
                status: SyncStatus::Ok,
                started_at_epoch_s: finished_at_epoch_s - 10,
                finished_at_epoch_s,
                messages_listed: 120,
                duration_ms: 100,
                messages_per_second: 120.0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
            }),
        )
        .unwrap();
    }

    let (_, history, summary) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 281,
            finished_at_epoch_s: 290,
            messages_listed: 120,
            duration_ms: 100,
            messages_per_second: 120.0,
            quota_pressure_retry_count: 1,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();

    assert!(summary.regression_detected);
    assert_eq!(
        summary.regression_kind,
        Some(SyncRunRegressionKind::RetryPressure)
    );
    assert_eq!(summary.regression_run_id, Some(history.run_id));
}

#[test]
fn persist_successful_sync_outcome_detects_throughput_drop_regression() {
    let repo_root = unique_temp_dir("mailroom-sync-history-throughput-drop");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("350"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 350,
        },
    )
    .unwrap();
    let sync_state = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("350")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 350,
            last_full_sync_success_epoch_s: Some(340),
            last_incremental_sync_success_epoch_s: Some(350),
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

    for (index, finished_at_epoch_s) in [360, 370, 380, 390, 400].into_iter().enumerate() {
        persist_successful_sync_outcome(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &sync_state,
            &sample_sync_run_outcome(SampleSyncRunOutcome {
                account_id: account.account_id.clone(),
                sync_mode: SyncMode::Incremental,
                status: SyncStatus::Ok,
                started_at_epoch_s: finished_at_epoch_s - 10,
                finished_at_epoch_s,
                messages_listed: 200,
                duration_ms: 100 + i64::try_from(index).unwrap_or(0),
                messages_per_second: 200.0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
            }),
        )
        .unwrap();
    }

    let (_, history, summary) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 401,
            finished_at_epoch_s: 430,
            messages_listed: 200,
            duration_ms: 140,
            messages_per_second: 100.0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();

    assert!(summary.regression_detected);
    assert_eq!(
        summary.regression_kind,
        Some(SyncRunRegressionKind::ThroughputDrop)
    );
    assert_eq!(summary.regression_run_id, Some(history.run_id));
}

#[test]
fn persist_successful_sync_outcome_detects_duration_spike_regression() {
    let repo_root = unique_temp_dir("mailroom-sync-history-duration-spike");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("450"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 450,
        },
    )
    .unwrap();
    let sync_state = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("450")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 450,
            last_full_sync_success_epoch_s: Some(440),
            last_incremental_sync_success_epoch_s: Some(450),
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

    for (index, finished_at_epoch_s) in [460, 470, 480, 490, 500].into_iter().enumerate() {
        persist_successful_sync_outcome(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &sync_state,
            &sample_sync_run_outcome(SampleSyncRunOutcome {
                account_id: account.account_id.clone(),
                sync_mode: SyncMode::Incremental,
                status: SyncStatus::Ok,
                started_at_epoch_s: finished_at_epoch_s - 10,
                finished_at_epoch_s,
                messages_listed: 200,
                duration_ms: 100 + i64::try_from(index).unwrap_or(0),
                messages_per_second: 200.0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
            }),
        )
        .unwrap();
    }

    let (_, history, summary) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 501,
            finished_at_epoch_s: 560,
            messages_listed: 200,
            duration_ms: 180,
            messages_per_second: 200.0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();

    assert!(summary.regression_detected);
    assert_eq!(
        summary.regression_kind,
        Some(SyncRunRegressionKind::DurationSpike)
    );
    assert_eq!(summary.regression_run_id, Some(history.run_id));
}

#[test]
fn sync_run_summary_tracks_separate_comparability_buckets() {
    let repo_root = unique_temp_dir("mailroom-sync-history-comparability");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    let account = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("300"),
            messages_total: 0,
            threads_total: 0,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 300,
        },
    )
    .unwrap();
    let sync_state = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("300")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 300,
            last_full_sync_success_epoch_s: Some(290),
            last_incremental_sync_success_epoch_s: Some(300),
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

    let (_, small_history, _) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 300,
            finished_at_epoch_s: 310,
            messages_listed: 10,
            duration_ms: 100,
            messages_per_second: 10.0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();
    let (_, large_history, _) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 320,
            finished_at_epoch_s: 330,
            messages_listed: 600,
            duration_ms: 100,
            messages_per_second: 60.0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();

    let small_summary = get_sync_run_summary_for_comparability(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
        SyncRunComparabilityKind::IncrementalWorkloadTier,
        "tiny",
    )
    .unwrap()
    .unwrap();
    let large_summary = get_sync_run_summary_for_comparability(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
        SyncRunComparabilityKind::IncrementalWorkloadTier,
        "large",
    )
    .unwrap()
    .unwrap();

    assert_eq!(small_summary.best_clean_run_id, Some(small_history.run_id));
    assert_eq!(large_summary.best_clean_run_id, Some(large_history.run_id));
    assert_eq!(small_summary.comparability_key, "tiny");
    assert_eq!(large_summary.comparability_key, "large");
}

#[test]
fn sync_run_history_pruning_keeps_account_wide_retention_and_clears_stale_bucket_summaries() {
    let repo_root = unique_temp_dir("mailroom-sync-history-prune-buckets");
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
    let sync_state = upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("500")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
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

    let (_, large_history, _) = persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &sample_sync_run_outcome(SampleSyncRunOutcome {
            account_id: account.account_id.clone(),
            sync_mode: SyncMode::Incremental,
            status: SyncStatus::Ok,
            started_at_epoch_s: 500,
            finished_at_epoch_s: 510,
            messages_listed: 600,
            duration_ms: 100,
            messages_per_second: 60.0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
        }),
    )
    .unwrap();

    for offset in 0..1_001_i64 {
        persist_successful_sync_outcome(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &sync_state,
            &sample_sync_run_outcome(SampleSyncRunOutcome {
                account_id: account.account_id.clone(),
                sync_mode: SyncMode::Incremental,
                status: SyncStatus::Ok,
                started_at_epoch_s: 511 + offset,
                finished_at_epoch_s: 512 + offset,
                messages_listed: 10,
                duration_ms: 100,
                messages_per_second: 10.0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
            }),
        )
        .unwrap();
    }

    let history = list_sync_run_history(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        1_100,
    )
    .unwrap();
    assert_eq!(history.len(), 1_000);
    assert!(
        history.iter().all(|row| row.comparability_key == "tiny"),
        "expected account-wide pruning to evict the older large bucket rows"
    );
    assert!(
        history.iter().all(|row| row.run_id != large_history.run_id),
        "expected the oldest large-bucket row to be pruned by account-wide retention"
    );

    let large_summary = get_sync_run_summary_for_comparability(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
        SyncRunComparabilityKind::IncrementalWorkloadTier,
        "large",
    )
    .unwrap();
    assert!(large_summary.is_none());

    let tiny_summary = get_sync_run_summary_for_comparability(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &account.account_id,
        SyncMode::Incremental,
        SyncRunComparabilityKind::IncrementalWorkloadTier,
        "tiny",
    )
    .unwrap()
    .unwrap();
    assert_eq!(tiny_summary.comparability_key, "tiny");
}

#[test]
fn finalize_full_sync_from_stage_clears_checkpoint_state() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-checkpoint-finalize-clear");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    let checkpoint = prepare_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-2"),
            cursor_history_id: Some("100"),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 1,
            started_at_epoch_s: 100,
            updated_at_epoch_s: 100,
        }),
    )
    .unwrap();
    stage_full_sync_page_and_update_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "finalize-1",
            "Finalize checkpoint subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        &full_sync_checkpoint_update(FullSyncCheckpointUpdateSpec {
            bootstrap_query: "newer_than:90d",
            status: FullSyncCheckpointStatus::ReadyToFinalize,
            next_page_token: None,
            cursor_history_id: checkpoint.cursor_history_id.as_deref(),
            pages_fetched: 1,
            messages_listed: 1,
            messages_upserted: 1,
            labels_synced: 1,
            started_at_epoch_s: checkpoint.started_at_epoch_s,
            updated_at_epoch_s: 101,
        }),
    )
    .unwrap();

    finalize_full_sync_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        200,
        &full_sync_state(account_id, 200),
    )
    .unwrap();

    assert!(
        get_full_sync_checkpoint(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
        )
        .unwrap()
        .is_none()
    );

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert!(mailbox.full_sync_checkpoint.is_none());
    assert_eq!(mailbox.message_count, 1);
}

#[test]
fn staged_incremental_batches_are_not_visible_until_finalize() {
    let repo_root = unique_temp_dir("mailroom-mailbox-incremental-stage-invisible");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            account_id,
            "live-1",
            "Live before finalize",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        100,
    )
    .unwrap();

    stage_incremental_sync_batch(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "staged-1",
            "Visible only after finalize",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        &[String::from("live-1")],
    )
    .unwrap();

    let mailbox_before_finalize = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox_before_finalize.message_count, 1);

    let live_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("Live"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(live_results.len(), 1);

    let staged_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("Visible"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(staged_results.is_empty());
}

#[test]
fn finalize_incremental_from_stage_applies_deletes_and_upserts_atomically() {
    let repo_root = unique_temp_dir("mailroom-mailbox-incremental-stage-finalize");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 2,
            threads_total: 2,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            account_id,
            "live-1",
            "Incremental live message",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        100,
    )
    .unwrap();

    stage_incremental_sync_batch(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "staged-1",
            "Incremental staged message",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
        &[String::from("live-1")],
    )
    .unwrap();

    let (sync_state, deleted_count) = finalize_incremental_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        200,
        &SyncStateUpdate {
            account_id: account_id.to_owned(),
            cursor_history_id: Some(String::from("200")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: SyncMode::Incremental,
            last_sync_status: SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 200,
            last_full_sync_success_epoch_s: None,
            last_incremental_sync_success_epoch_s: Some(200),
            pipeline_enabled: true,
            pipeline_list_queue_high_water: 2,
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
            pipeline_staged_message_count: 1,
            pipeline_staged_delete_count: 1,
            pipeline_staged_attachment_count: 0,
        },
    )
    .unwrap();

    assert_eq!(deleted_count, 1);
    assert_eq!(sync_state.cursor_history_id.as_deref(), Some("200"));
    assert!(sync_state.pipeline_enabled);
    assert_eq!(sync_state.pipeline_write_batch_count, 1);

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 1);
    assert_eq!(
        mailbox
            .sync_state
            .as_ref()
            .map(|state| state.pipeline_write_batch_count),
        Some(1)
    );

    let staged_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("staged"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(staged_results.len(), 1);

    let live_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: account_id.to_owned(),
            terms: String::from("live"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(live_results.is_empty());
}

#[test]
fn reset_incremental_sync_stage_clears_stale_stage_rows() {
    let repo_root = unique_temp_dir("mailroom-mailbox-incremental-stage-reset");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
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

    stage_incremental_sync_batch(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "staged-1",
            "Reset incremental stage",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
        &[String::from("delete-1")],
    )
    .unwrap();

    reset_incremental_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();

    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
    let staged_counts: (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                 (SELECT COUNT(*) FROM gmail_incremental_sync_stage_messages WHERE account_id = ?1),
                 (SELECT COUNT(*) FROM gmail_incremental_sync_stage_message_labels WHERE account_id = ?1),
                 (SELECT COUNT(*) FROM gmail_incremental_sync_stage_attachments WHERE account_id = ?1),
                 (SELECT COUNT(*) FROM gmail_incremental_sync_stage_delete_ids WHERE account_id = ?1)",
            [account_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(staged_counts, (0, 0, 0, 0));
}
