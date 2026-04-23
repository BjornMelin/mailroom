use super::*;

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
