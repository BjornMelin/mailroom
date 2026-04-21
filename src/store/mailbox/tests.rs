use super::{
    AttachmentExportEventInput, AttachmentListQuery, AttachmentVaultStateUpdate,
    GmailAttachmentUpsertInput, GmailMessageUpsertInput, IncrementalSyncCommit, MailboxWriteError,
    SearchQuery, SyncMode, SyncStateUpdate, SyncStatus, commit_full_sync, commit_incremental_sync,
    get_attachment_detail, get_sync_state, inspect_mailbox, list_attachments,
    record_attachment_export, replace_labels, replace_labels_and_report_reindex, replace_messages,
    search::build_plain_fts5_query, search_messages, set_attachment_vault_state, upsert_messages,
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
