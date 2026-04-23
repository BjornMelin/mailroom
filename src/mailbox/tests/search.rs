use super::*;

#[tokio::test]
async fn search_rejects_whitespace_only_terms() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    store::init(&config_report).unwrap();

    let error = search(
        &config_report,
        SearchRequest {
            terms: String::from("   "),
            label: None,
            from_address: None,
            after: None,
            before: None,
            limit: 10,
        },
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("search terms cannot be empty"));
}

#[tokio::test]
async fn search_rejects_zero_limit_before_account_resolution() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = search(
        &config_report,
        SearchRequest {
            terms: String::from("alpha"),
            label: None,
            from_address: None,
            after: None,
            before: None,
            limit: 0,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.to_string(), "search limit must be greater than zero");
}

#[tokio::test]
async fn search_before_date_excludes_that_day() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    store::init(&config_report).unwrap();
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
    store::mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[store::mailbox::GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: parse_start_of_day_epoch_ms("1970-01-01").unwrap(),
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

    let report = search(
        &config_report,
        SearchRequest {
            terms: String::from("alpha"),
            label: None,
            from_address: None,
            after: None,
            before: Some(String::from("1970-01-01")),
            limit: 10,
        },
    )
    .await
    .unwrap();

    assert!(report.results.is_empty());
    assert_eq!(report.before_epoch_ms, Some(0));
}

#[tokio::test]
async fn search_migrates_schema_v2_store_before_querying_mailbox_tables() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    seed_schema_v2_store_with_active_account(&config_report);

    let report = search(
        &config_report,
        SearchRequest {
            terms: String::from("alpha"),
            label: None,
            from_address: None,
            after: None,
            before: None,
            limit: 10,
        },
    )
    .await
    .unwrap();

    assert!(report.results.is_empty());

    let store_report = store::inspect(config_report).unwrap();
    assert_eq!(store_report.schema_version, Some(16));
    assert_eq!(store_report.pending_migrations, Some(0));
}

#[tokio::test]
async fn search_without_active_account_initializes_store_before_failing() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = search(
        &config_report,
        SearchRequest {
            terms: String::from("alpha"),
            label: None,
            from_address: None,
            after: None,
            before: None,
            limit: 10,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "no active Gmail account found; run `mailroom auth login` first"
    );
    assert!(config_report.config.store.database_path.exists());
    assert!(config_report.config.workspace.runtime_root.exists());
}

#[tokio::test]
async fn search_uses_local_mailbox_cache_after_logout() {
    let temp_dir = TempDir::new().unwrap();
    let mock_server = MockServer::start().await;
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(&config_report, "250", "cached-1", "Existing cached message");

    let logout_report = auth::logout(&config_report).unwrap();
    assert!(logout_report.credential_removed);
    assert_eq!(logout_report.deactivated_accounts, 1);

    let report = search(
        &config_report,
        SearchRequest {
            terms: String::from("Existing"),
            label: None,
            from_address: None,
            after: None,
            before: None,
            limit: 10,
        },
    )
    .await
    .unwrap();

    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].message_id, "cached-1");
}
