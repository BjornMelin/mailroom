use super::*;

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

    assert_eq!(labels.len(), 2);
    assert_eq!(
        labels
            .iter()
            .map(|label| (
                label.label_id.as_str(),
                label.local_message_count,
                label.local_thread_count
            ))
            .collect::<Vec<_>>(),
        vec![("INBOX", 1, 1), ("Label_1", 1, 1)]
    );
}
