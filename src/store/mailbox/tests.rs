use super::{
    AttachmentExportEventInput, AttachmentListQuery, AttachmentVaultStateUpdate,
    FullSyncCheckpointStatus, FullSyncCheckpointUpdate, GmailAttachmentUpsertInput,
    GmailMessageUpsertInput, IncrementalSyncCommit, MailboxWriteError, SearchQuery, SyncMode,
    SyncPacingPressureKind, SyncPacingStateUpdate, SyncStateUpdate, SyncStatus, commit_full_sync,
    commit_incremental_sync, finalize_full_sync_from_stage, finalize_incremental_from_stage,
    get_attachment_detail, get_full_sync_checkpoint, get_sync_state, inspect_mailbox,
    list_attachments, list_label_usage, prepare_full_sync_checkpoint, record_attachment_export,
    replace_labels, replace_labels_and_report_reindex, replace_messages, reset_full_sync_stage,
    reset_incremental_sync_stage, search::build_plain_fts5_query, search_messages,
    set_attachment_vault_state, stage_full_sync_labels, stage_full_sync_messages,
    stage_full_sync_page_and_update_checkpoint, stage_incremental_sync_batch,
    update_full_sync_checkpoint_labels, upsert_messages, upsert_sync_pacing_state,
    upsert_sync_state,
};
use crate::config::resolve;
use crate::gmail::GmailLabel;
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
use std::time::Instant;
use tempfile::{Builder, TempDir};

#[test]
fn search_messages_returns_ranked_hits_with_filters() {
    let repo_root = unique_temp_dir("mailroom-mailbox-search");
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
        "gmail:operator@example.com",
        &[
            gmail_label("INBOX", "INBOX", "system"),
            gmail_label("Label_1", "Project/Alpha", "user"),
        ],
        100,
    )
    .unwrap();

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[
            GmailMessageUpsertInput {
                account_id: String::from("gmail:operator@example.com"),
                message_id: String::from("m-1"),
                thread_id: String::from("t-1"),
                history_id: String::from("101"),
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
                label_ids: vec![String::from("INBOX"), String::from("Label_1")],
                label_names_text: String::from("INBOX Project/Alpha"),
                attachments: Vec::new(),
            },
            GmailMessageUpsertInput {
                account_id: String::from("gmail:operator@example.com"),
                message_id: String::from("m-2"),
                thread_id: String::from("t-2"),
                history_id: String::from("102"),
                internal_date_epoch_ms: 1_600_000_000_000,
                snippet: String::from("Dinner plans"),
                subject: String::from("Weekend dinner"),
                from_header: String::from("Bob <bob@example.com>"),
                from_address: Some(String::from("bob@example.com")),
                recipient_headers: String::from("operator@example.com"),
                to_header: String::from("operator@example.com"),
                cc_header: String::new(),
                bcc_header: String::new(),
                reply_to_header: String::new(),
                size_estimate: 456,
                automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
                attachments: Vec::new(),
            },
        ],
        100,
    )
    .unwrap();

    let results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("alpha"),
            label: Some(String::from("Project/Alpha")),
            from_address: Some(String::from("alice@example.com")),
            after_epoch_ms: Some(1_650_000_000_000),
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "m-1");
    assert_eq!(results[0].label_names, vec!["INBOX", "Project/Alpha"]);
}

#[test]
fn build_plain_fts5_query_quotes_special_characters() {
    assert_eq!(
        build_plain_fts5_query("alice@example.com foo-bar C++"),
        "\"alice@example.com\" \"foo-bar\" \"C++\""
    );
    assert_eq!(
        build_plain_fts5_query(" say \"hello\" "),
        "\"say\" \"\"\"hello\"\"\""
    );
}

#[test]
fn search_messages_matches_common_punctuated_terms() {
    let repo_root = unique_temp_dir("mailroom-mailbox-search-punctuated");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("110"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 110,
        },
    )
    .unwrap();

    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        110,
    )
    .unwrap();

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("111"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Tracking foo-bar rollout"),
            subject: String::from("C++ rollout"),
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
        110,
    )
    .unwrap();

    for terms in ["alice@example.com", "foo-bar", "C++"] {
        let results = search_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SearchQuery {
                account_id: String::from("gmail:operator@example.com"),
                terms: terms.to_owned(),
                label: None,
                from_address: None,
                after_epoch_ms: None,
                before_epoch_ms: None,
                limit: 10,
            },
        )
        .unwrap();

        assert_eq!(
            results.len(),
            1,
            "terms {terms} should match the seeded message"
        );
        assert_eq!(results[0].message_id, "m-1");
    }
}

#[test]
fn replace_labels_reindexes_search_label_names() {
    let repo_root = unique_temp_dir("mailroom-mailbox-label-reindex");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("120"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 120,
        },
    )
    .unwrap();

    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[
            gmail_label("INBOX", "INBOX", "system"),
            gmail_label("Label_1", "ProjectAlpha", "user"),
        ],
        120,
    )
    .unwrap();

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("121"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Mailbox cleanup plan"),
            subject: String::from("Cleanup"),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX"), String::from("Label_1")],
            label_names_text: String::from("INBOX ProjectAlpha"),
            attachments: Vec::new(),
        }],
        120,
    )
    .unwrap();

    commit_incremental_sync(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &IncrementalSyncCommit {
            labels: &[
                gmail_label("INBOX", "INBOX", "system"),
                gmail_label("Label_1", "ProjectBeta", "user"),
            ],
            messages_to_upsert: &[],
            message_ids_to_delete: &[],
            updated_at_epoch_s: 121,
            sync_state_update: &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(String::from("121")),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Incremental,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 121,
                last_full_sync_success_epoch_s: None,
                last_incremental_sync_success_epoch_s: Some(121),
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
        },
    )
    .unwrap();

    let results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("ProjectBeta"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].label_names, vec!["INBOX", "ProjectBeta"]);
}

#[test]
fn replace_labels_skips_search_reindex_when_label_names_are_unchanged() {
    let repo_root = unique_temp_dir("mailroom-mailbox-label-refresh");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("120"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 120,
        },
    )
    .unwrap();

    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[
            gmail_label("INBOX", "INBOX", "system"),
            gmail_label("Label_1", "ProjectAlpha", "user"),
        ],
        120,
    )
    .unwrap();

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("121"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Mailbox cleanup plan"),
            subject: String::from("Cleanup"),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX"), String::from("Label_1")],
            label_names_text: String::from("INBOX ProjectAlpha"),
            attachments: Vec::new(),
        }],
        120,
    )
    .unwrap();

    let mut inbox = gmail_label("INBOX", "INBOX", "system");
    inbox.messages_total = Some(99);
    inbox.messages_unread = Some(5);
    let mut project = gmail_label("Label_1", "ProjectAlpha", "user");
    project.messages_total = Some(42);
    project.messages_unread = Some(2);

    let reindexed = replace_labels_and_report_reindex(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[inbox, project],
        121,
    )
    .unwrap();

    assert!(!reindexed);

    let results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("ProjectAlpha"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].label_names, vec!["INBOX", "ProjectAlpha"]);
}

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

    assert!(result.is_err());
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

    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
    connection
        .execute_batch(
            "DROP INDEX IF EXISTS gmail_sync_pacing_state_updated_at_idx;
             DROP TABLE gmail_sync_pacing_state;
             CREATE TABLE gmail_sync_pacing_state (
                 account_id TEXT PRIMARY KEY,
                 learned_quota_units_per_minute INTEGER NOT NULL,
                 learned_message_fetch_concurrency INTEGER NOT NULL,
                 clean_run_streak INTEGER NOT NULL,
                 last_pressure_kind TEXT,
                 updated_at_epoch_s INTEGER NOT NULL,
                 FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
             ) STRICT;
             CREATE INDEX gmail_sync_pacing_state_updated_at_idx
                 ON gmail_sync_pacing_state (updated_at_epoch_s DESC);",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO gmail_sync_pacing_state (
                 account_id,
                 learned_quota_units_per_minute,
                 learned_message_fetch_concurrency,
                 clean_run_streak,
                 last_pressure_kind,
                 updated_at_epoch_s
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                "gmail:operator@example.com",
                12_000_i64,
                4_i64,
                1_i64,
                "bogus",
                110_i64
            ],
        )
        .unwrap();
    drop(connection);

    let result = super::get_sync_pacing_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    );

    assert!(result.is_err());
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
fn replace_messages_rejects_mixed_account_batches() {
    let repo_root = unique_temp_dir("mailroom-mailbox-replace-account-guard");
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

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Alpha launch checklist"),
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
            label_ids: vec![],
            label_names_text: String::new(),
            attachments: Vec::new(),
        }],
        100,
    )
    .unwrap();

    let error = replace_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:second@example.com"),
            message_id: String::from("m-2"),
            thread_id: String::from("t-2"),
            history_id: String::from("102"),
            internal_date_epoch_ms: 1_700_000_100_000,
            snippet: String::from("Beta launch checklist"),
            subject: String::from("Beta launch"),
            from_header: String::from("Bob <bob@example.com>"),
            from_address: Some(String::from("bob@example.com")),
            recipient_headers: String::from("second@example.com"),
            to_header: String::from("second@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 456,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![],
            label_names_text: String::new(),
            attachments: Vec::new(),
        }],
        100,
    )
    .unwrap_err();

    assert!(error.to_string().contains("does not match batch account"));

    let operator_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("alpha"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(operator_results.len(), 1);

    let second_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: String::from("gmail:second@example.com"),
            terms: String::from("beta"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(second_results.is_empty());
}

#[test]
fn upsert_sync_state_scopes_indexed_message_count_to_the_account() {
    let repo_root = unique_temp_dir("mailroom-mailbox-sync-state-indexed-count");
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
            snippet: String::from("Operator mailbox"),
            subject: String::from("Operator mailbox"),
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

    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("second@example.com"),
            history_id: String::from("300"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:b"),
            refreshed_at_epoch_s: 300,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:second@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        300,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:second@example.com"),
            message_id: String::from("m-2"),
            thread_id: String::from("t-2"),
            history_id: String::from("301"),
            internal_date_epoch_ms: 1_700_000_100_000,
            snippet: String::from("Second mailbox"),
            subject: String::from("Second mailbox"),
            from_header: String::from("Bob <bob@example.com>"),
            from_address: Some(String::from("bob@example.com")),
            recipient_headers: String::from("second@example.com"),
            to_header: String::from("second@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 456,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: Vec::new(),
        }],
        300,
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

    assert_eq!(sync_state.message_count, 1);
    assert_eq!(sync_state.label_count, 1);
    assert_eq!(sync_state.indexed_message_count, 1);

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 2);
    assert_eq!(mailbox.label_count, 2);
    assert_eq!(mailbox.indexed_message_count, 2);
}

#[test]
fn apply_incremental_changes_rejects_messages_for_a_different_account() {
    let repo_root = unique_temp_dir("mailroom-mailbox-incremental-account-mismatch");
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

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Alpha launch checklist"),
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
            label_ids: vec![],
            label_names_text: String::new(),
            attachments: Vec::new(),
        }],
        100,
    )
    .unwrap();

    let error = super::apply_incremental_changes(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:second@example.com"),
            message_id: String::from("m-2"),
            thread_id: String::from("t-2"),
            history_id: String::from("102"),
            internal_date_epoch_ms: 1_700_000_100_000,
            snippet: String::from("Beta launch checklist"),
            subject: String::from("Beta launch"),
            from_header: String::from("Bob <bob@example.com>"),
            from_address: Some(String::from("bob@example.com")),
            recipient_headers: String::from("second@example.com"),
            to_header: String::from("second@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 456,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![],
            label_names_text: String::new(),
            attachments: Vec::new(),
        }],
        &[String::from("m-1")],
        100,
    )
    .unwrap_err();

    assert!(error.to_string().contains("does not match batch account"));

    let operator_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("alpha"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(operator_results.len(), 1);
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

fn gmail_label(id: &str, name: &str, label_type: &str) -> GmailLabel {
    GmailLabel {
        id: id.to_owned(),
        name: name.to_owned(),
        label_type: label_type.to_owned(),
        message_list_visibility: None,
        label_list_visibility: None,
        messages_total: None,
        messages_unread: None,
        threads_total: None,
        threads_unread: None,
    }
}

fn mailbox_message(
    account_id: &str,
    message_id: &str,
    subject: &str,
    label_ids: &[&str],
    label_names_text: &str,
    attachment_part_ids: &[&str],
) -> GmailMessageUpsertInput {
    GmailMessageUpsertInput {
        account_id: account_id.to_owned(),
        message_id: message_id.to_owned(),
        thread_id: format!("thread-{message_id}"),
        history_id: format!("history-{message_id}"),
        internal_date_epoch_ms: 1_700_000_000_000,
        snippet: format!("snippet for {subject}"),
        subject: subject.to_owned(),
        from_header: String::from("Operator <operator@example.com>"),
        from_address: Some(String::from("operator@example.com")),
        recipient_headers: String::from("operator@example.com"),
        to_header: String::from("operator@example.com"),
        cc_header: String::new(),
        bcc_header: String::new(),
        reply_to_header: String::new(),
        size_estimate: 1024,
        automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
        label_ids: label_ids
            .iter()
            .map(|label_id| (*label_id).to_owned())
            .collect(),
        label_names_text: label_names_text.to_owned(),
        attachments: attachment_part_ids
            .iter()
            .map(|part_id| GmailAttachmentUpsertInput {
                attachment_key: format!("{message_id}:{part_id}"),
                part_id: (*part_id).to_owned(),
                gmail_attachment_id: Some(format!("att-{message_id}-{part_id}")),
                filename: format!("{message_id}-{part_id}.bin"),
                mime_type: String::from("application/octet-stream"),
                size_bytes: 256,
                content_disposition: Some(String::from("attachment")),
                content_id: None,
                is_inline: false,
            })
            .collect(),
    }
}

fn full_sync_state(account_id: &str, epoch_s: i64) -> SyncStateUpdate {
    SyncStateUpdate {
        account_id: account_id.to_owned(),
        cursor_history_id: Some(format!("cursor-{epoch_s}")),
        bootstrap_query: String::from("newer_than:90d"),
        last_sync_mode: SyncMode::Full,
        last_sync_status: SyncStatus::Ok,
        last_error: None,
        last_sync_epoch_s: epoch_s,
        last_full_sync_success_epoch_s: Some(epoch_s),
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
    }
}

struct FullSyncCheckpointUpdateSpec<'a> {
    bootstrap_query: &'a str,
    status: FullSyncCheckpointStatus,
    next_page_token: Option<&'a str>,
    cursor_history_id: Option<&'a str>,
    pages_fetched: i64,
    messages_listed: i64,
    messages_upserted: i64,
    labels_synced: i64,
    started_at_epoch_s: i64,
    updated_at_epoch_s: i64,
}

fn full_sync_checkpoint_update(spec: FullSyncCheckpointUpdateSpec<'_>) -> FullSyncCheckpointUpdate {
    FullSyncCheckpointUpdate {
        bootstrap_query: spec.bootstrap_query.to_owned(),
        status: spec.status,
        next_page_token: spec.next_page_token.map(str::to_owned),
        cursor_history_id: spec.cursor_history_id.map(str::to_owned),
        pages_fetched: spec.pages_fetched,
        messages_listed: spec.messages_listed,
        messages_upserted: spec.messages_upserted,
        labels_synced: spec.labels_synced,
        started_at_epoch_s: spec.started_at_epoch_s,
        updated_at_epoch_s: spec.updated_at_epoch_s,
    }
}

#[test]
fn list_label_usage_counts_only_the_requested_account() {
    let repo_root = unique_temp_dir("mailroom-mailbox-label-usage-scope");
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
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
            history_id: String::from("200"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:b"),
            refreshed_at_epoch_s: 200,
        },
    )
    .unwrap();

    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[
            gmail_label("INBOX", "INBOX", "system"),
            gmail_label("Label_1", "Project", "user"),
        ],
        100,
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:other@example.com",
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
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Project launch"),
            subject: String::from("Operator"),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX"), String::from("Label_1")],
            label_names_text: String::from("INBOX Project"),
            attachments: Vec::new(),
        }],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:other@example.com"),
            message_id: String::from("m-2"),
            thread_id: String::from("t-2"),
            history_id: String::from("201"),
            internal_date_epoch_ms: 1_700_000_000_100,
            snippet: String::from("Inbox only"),
            subject: String::from("Other"),
            from_header: String::from("Bob <bob@example.com>"),
            from_address: Some(String::from("bob@example.com")),
            recipient_headers: String::from("other@example.com"),
            to_header: String::from("other@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 456,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: Vec::new(),
        }],
        200,
    )
    .unwrap();

    let labels = list_label_usage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap();

    let inbox = labels
        .iter()
        .find(|label| label.label_id == "INBOX")
        .unwrap();
    assert_eq!(inbox.local_message_count, 1);
    assert_eq!(inbox.local_thread_count, 1);

    let project = labels
        .iter()
        .find(|label| label.label_id == "Label_1")
        .unwrap();
    assert_eq!(project.local_message_count, 1);
    assert_eq!(project.local_thread_count, 1);
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
fn attachment_catalog_round_trips_through_message_upserts() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachments-catalog");
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
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Quarterly statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("m-1:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    let attachments = list_attachments(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentListQuery {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: None,
            message_id: None,
            filename: None,
            mime_type: None,
            fetched_only: false,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].attachment_key, "m-1:1.2");
    assert_eq!(attachments[0].filename, "statement.pdf");

    let detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "m-1:1.2",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.gmail_attachment_id.as_deref(), Some("att-1"));
    assert_eq!(detail.mime_type, "application/pdf");

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.attachment_count, 1);
    assert_eq!(mailbox.vaulted_attachment_count, 0);
    assert_eq!(mailbox.attachment_export_count, 0);
}

#[test]
fn attachment_vault_state_and_export_events_are_reflected_in_reads() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachments-vault");
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
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Quarterly statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("m-1:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("m-1:1.2"),
            content_hash: String::from("abc123"),
            relative_path: String::from("blake3/ab/abc123"),
            size_bytes: 42,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap();
    record_attachment_export(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentExportEventInput {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("m-1:1.2"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            destination_path: String::from("/tmp/export/statement.pdf"),
            content_hash: String::from("abc123"),
            exported_at_epoch_s: 102,
        },
    )
    .unwrap();

    let fetched_only = list_attachments(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentListQuery {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: None,
            message_id: None,
            filename: None,
            mime_type: None,
            fetched_only: true,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(fetched_only.len(), 1);
    assert_eq!(fetched_only[0].export_count, 1);

    let detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "m-1:1.2",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.vault_content_hash.as_deref(), Some("abc123"));
    assert_eq!(detail.export_count, 1);

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.attachment_count, 1);
    assert_eq!(mailbox.vaulted_attachment_count, 1);
    assert_eq!(mailbox.attachment_export_count, 1);
}

#[test]
fn set_attachment_vault_state_errors_when_attachment_key_is_missing() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachment-vault-missing");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let error = set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("missing:1.2"),
            content_hash: String::from("abc123"),
            relative_path: String::from("blake3/ab/abc123"),
            size_bytes: 42,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        MailboxWriteError::AttachmentNotFound {
            account_id,
            attachment_key
        } if account_id == "gmail:operator@example.com" && attachment_key == "missing:1.2"
    ));
}

#[test]
fn attachment_vault_updates_are_account_scoped_for_shared_attachment_keys() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachment-vault-account-scope");
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
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
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
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:other@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("shared:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:other@example.com"),
            message_id: String::from("m-2"),
            thread_id: String::from("t-2"),
            history_id: String::from("102"),
            internal_date_epoch_ms: 1_700_000_000_100,
            snippet: String::from("Statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("other@example.com"),
            to_header: String::from("other@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("shared:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-2")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("shared:1.2"),
            content_hash: String::from("hash-a"),
            relative_path: String::from("blake3/ha/hash-a"),
            size_bytes: 42,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap();

    let operator_detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "shared:1.2",
    )
    .unwrap()
    .unwrap();
    let other_detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:other@example.com",
        "shared:1.2",
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        operator_detail.vault_content_hash.as_deref(),
        Some("hash-a")
    );
    assert!(other_detail.vault_content_hash.is_none());
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
fn finalize_full_sync_from_stage_replaces_only_the_target_account() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-account-scope");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let operator = accounts::upsert_active(
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
    let other = accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
            history_id: String::from("200"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:b"),
            refreshed_at_epoch_s: 200,
        },
    )
    .unwrap();

    for account_id in [&operator.account_id, &other.account_id] {
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
            &[gmail_label("INBOX", "INBOX", "system")],
            200,
        )
        .unwrap();
    }
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            &operator.account_id,
            "operator-live",
            "Operator old subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        200,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            &other.account_id,
            "other-live",
            "Other untouched subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
        200,
    )
    .unwrap();

    reset_full_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &operator.account_id,
    )
    .unwrap();
    stage_full_sync_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &operator.account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
    )
    .unwrap();
    stage_full_sync_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &operator.account_id,
        &[mailbox_message(
            &operator.account_id,
            "operator-new",
            "Operator new subject",
            &["INBOX"],
            "INBOX",
            &[],
        )],
    )
    .unwrap();
    finalize_full_sync_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &operator.account_id,
        300,
        &full_sync_state(&operator.account_id, 300),
    )
    .unwrap();

    let operator_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: operator.account_id.clone(),
            terms: String::from("Operator"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(operator_results.len(), 1);
    assert_eq!(operator_results[0].message_id, "operator-new");

    let other_results = search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &SearchQuery {
            account_id: other.account_id.clone(),
            terms: String::from("Other"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(other_results.len(), 1);
    assert_eq!(other_results[0].message_id, "other-live");
}

#[test]
fn finalize_full_sync_from_stage_preserves_attachment_vault_state() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-vault-preserve");
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
            "message-1",
            "Before vault preserve",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
        100,
    )
    .unwrap();
    set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: account_id.to_owned(),
            attachment_key: String::from("message-1:1.1"),
            content_hash: String::from("hash-123"),
            relative_path: String::from("vault/message-1.bin"),
            size_bytes: 2048,
            fetched_at_epoch_s: 123,
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
            "message-1",
            "After vault preserve",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
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

    let detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        "message-1:1.1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.vault_content_hash.as_deref(), Some("hash-123"));
    assert_eq!(
        detail.vault_relative_path.as_deref(),
        Some("vault/message-1.bin")
    );
    assert_eq!(detail.vault_size_bytes, Some(2048));
    assert_eq!(detail.vault_fetched_at_epoch_s, Some(123));
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
            gmail_label("INBOX", "INBOX", "system"),
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

#[test]
#[ignore = "benchmark harness; run manually with: cargo test benchmark_attachment_lane_full_sync_tiers -- --ignored --nocapture"]
fn benchmark_attachment_lane_full_sync_tiers() {
    let tiers = [("small", 250_usize), ("medium", 1_000), ("large", 3_000)];

    for (tier_name, message_count) in tiers {
        let repo_root = unique_temp_dir("mailroom-bench-full-sync");
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
                messages_total: i64::try_from(message_count).unwrap(),
                threads_total: i64::try_from(message_count).unwrap(),
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();

        let labels = [gmail_label("INBOX", "INBOX", "system")];
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &labels,
            100,
        )
        .unwrap();

        let messages = (0..message_count)
            .map(|index| GmailMessageUpsertInput {
                account_id: String::from("gmail:operator@example.com"),
                message_id: format!("m-{index}"),
                thread_id: format!("t-{index}"),
                history_id: format!("{}", 200 + index),
                internal_date_epoch_ms: 1_700_000_000_000 + i64::try_from(index).unwrap(),
                snippet: format!("Benchmark mailbox payload {index}"),
                subject: format!("Benchmark subject {index}"),
                from_header: String::from("Benchmark <bench@example.com>"),
                from_address: Some(String::from("bench@example.com")),
                recipient_headers: String::from("operator@example.com"),
                to_header: String::from("operator@example.com"),
                cc_header: String::new(),
                bcc_header: String::new(),
                reply_to_header: String::new(),
                size_estimate: 4096,
                automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
                attachments: vec![GmailAttachmentUpsertInput {
                    attachment_key: format!("m-{index}:1.1"),
                    part_id: String::from("1.1"),
                    gmail_attachment_id: Some(format!("att-{index}")),
                    filename: format!("bench-{index}.bin"),
                    mime_type: String::from("application/octet-stream"),
                    size_bytes: 1024,
                    content_disposition: Some(String::from("attachment")),
                    content_id: None,
                    is_inline: false,
                }],
            })
            .collect::<Vec<_>>();

        let started_at = Instant::now();
        commit_full_sync(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &labels,
            &messages,
            300,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(format!("{}", 300 + message_count)),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Full,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 300,
                last_full_sync_success_epoch_s: Some(300),
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
        let elapsed = started_at.elapsed();

        let mailbox = inspect_mailbox(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
        )
        .unwrap()
        .unwrap();
        assert_eq!(mailbox.message_count, i64::try_from(message_count).unwrap());
        assert_eq!(
            mailbox.attachment_count,
            i64::try_from(message_count).unwrap()
        );

        let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
        let per_message_us = elapsed.as_secs_f64() * 1_000_000.0 / message_count as f64;
        println!(
            "{{\"bench\":\"attachment_lane.full_sync\",\"tier\":\"{tier_name}\",\"messages\":{message_count},\"attachments\":{message_count},\"elapsed_ms\":{elapsed_ms:.3},\"per_message_us\":{per_message_us:.3}}}"
        );
    }
}

fn unique_temp_dir(prefix: &str) -> TempDir {
    Builder::new().prefix(prefix).tempdir().unwrap()
}
