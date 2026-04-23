use super::*;

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
