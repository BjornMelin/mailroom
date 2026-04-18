use super::{
    GmailMessageUpsertInput, IncrementalSyncCommit, SearchQuery, SyncMode, SyncStateUpdate,
    SyncStatus, commit_incremental_sync, get_sync_state, inspect_mailbox, replace_labels,
    replace_labels_and_report_reindex, replace_messages, search::build_plain_fts5_query,
    search_messages, upsert_messages, upsert_sync_state,
};
use crate::config::resolve;
use crate::gmail::GmailLabel;
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
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
                label_ids: vec![String::from("INBOX"), String::from("Label_1")],
                label_names_text: String::from("INBOX Project/Alpha"),
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
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
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
            label_ids: vec![String::from("INBOX"), String::from("Label_1")],
            label_names_text: String::from("INBOX ProjectAlpha"),
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
            label_ids: vec![String::from("INBOX"), String::from("Label_1")],
            label_names_text: String::from("INBOX ProjectAlpha"),
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
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
            label_ids: vec![],
            label_names_text: String::new(),
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
            label_ids: vec![],
            label_names_text: String::new(),
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
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
            label_ids: vec![],
            label_names_text: String::new(),
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
            label_ids: vec![],
            label_names_text: String::new(),
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
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

fn unique_temp_dir(prefix: &str) -> TempDir {
    Builder::new().prefix(prefix).tempdir().unwrap()
}
