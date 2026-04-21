use super::{
    DEFAULT_BOOTSTRAP_RECENT_DAYS, DEFAULT_SEARCH_LIMIT, SearchRequest, SyncRunOptions,
    newest_history_id, parse_start_of_day_epoch_ms, search, sync_run, sync_run_with_options,
};
use crate::auth;
use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::config::{ConfigReport, resolve};
use crate::mailbox::util::bootstrap_query;
use crate::store;
use crate::store::accounts;
use crate::workspace::WorkspacePaths;
use secrecy::SecretString;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct SeededMailboxLabels {
    labels: Vec<crate::gmail::GmailLabel>,
    label_ids: Vec<String>,
    label_names_text: String,
}

#[test]
fn parses_yyyy_mm_dd_date_bounds() {
    assert_eq!(parse_start_of_day_epoch_ms("1970-01-01").unwrap(), 0);
    assert_eq!(
        parse_start_of_day_epoch_ms("1970-01-02").unwrap(),
        86_400_000
    );
    assert!(parse_start_of_day_epoch_ms("1970-1-01").is_err());
    assert!(parse_start_of_day_epoch_ms("12345-01-01").is_err());
    assert!(parse_start_of_day_epoch_ms("1970-13-01").is_err());
}

#[test]
fn newest_history_id_keeps_the_highest_seen_cursor() {
    let cursor = newest_history_id(Some(String::from("250")), "400");
    let cursor = newest_history_id(cursor, "300");

    assert_eq!(cursor.as_deref(), Some("400"));
}

#[test]
fn search_request_default_limit_is_nonzero() {
    let request = SearchRequest {
        terms: String::from("alpha"),
        label: None,
        from_address: None,
        after: None,
        before: None,
        limit: DEFAULT_SEARCH_LIMIT,
    };

    assert!(request.limit > 0);
}

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
async fn sync_run_with_options_rejects_zero_message_fetch_concurrency() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = sync_run_with_options(
        &config_report,
        SyncRunOptions {
            force_full: false,
            recent_days: 30,
            quota_units_per_minute: 12_000,
            message_fetch_concurrency: 0,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "message_fetch_concurrency must be greater than zero"
    );
}

#[tokio::test]
async fn sync_run_with_options_rejects_quota_below_single_message_read_cost() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = sync_run_with_options(
        &config_report,
        SyncRunOptions {
            force_full: false,
            recent_days: 30,
            quota_units_per_minute: 4,
            message_fetch_concurrency: 4,
        },
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "gmail quota budget must be at least 5 units per minute; got 4"
    );
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
    assert_eq!(store_report.schema_version, Some(13));
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

#[tokio::test]
async fn full_sync_failure_preserves_existing_mailbox_cache() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "500").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient gmail failure"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(&config_report, "250", "cached-1", "Existing cached message");

    let error = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("users/me/messages"));

    let mailbox = store::mailbox::inspect_mailbox(
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
            .and_then(|state| state.cursor_history_id.as_deref()),
        Some("250")
    );
    assert_eq!(
        mailbox
            .sync_state
            .as_ref()
            .and_then(|state| state.last_full_sync_success_epoch_s),
        Some(100)
    );
    assert_eq!(
        mailbox
            .sync_state
            .as_ref()
            .and_then(|state| state.last_incremental_sync_success_epoch_s),
        None
    );

    let results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("Existing"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "cached-1");
}

#[tokio::test]
async fn sync_run_without_credentials_does_not_create_runtime_state() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    let config_report = resolve(&paths).unwrap();

    let error = sync_run(&config_report, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "mailroom is not authenticated; run `mailroom auth login` first"
    );
    assert!(!config_report.config.store.database_path.exists());
    assert!(!config_report.config.workspace.runtime_root.exists());
}

#[tokio::test]
async fn cursorless_failed_bootstrap_retries_with_full_sync() {
    let failing_server = MockServer::start().await;
    mount_profile(&failing_server, "500").await;
    mount_labels(&failing_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient gmail failure"))
        .mount(&failing_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let failing_config = config_report_for(&temp_dir, &failing_server);
    seed_credentials(&failing_config);

    let first_error = sync_run(&failing_config, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();
    assert!(first_error.to_string().contains("users/me/messages"));

    let failed_state = store::mailbox::get_sync_state(
        &failing_config.config.store.database_path,
        failing_config.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        failed_state.last_sync_status,
        store::mailbox::SyncStatus::Failed
    );
    assert!(failed_state.cursor_history_id.is_none());

    let success_server = MockServer::start().await;
    mount_profile(&success_server, "700").await;
    mount_labels(&success_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-recovered", "threadId": "t-recovered"}],
            "resultSizeEstimate": 1
        })))
        .mount(&success_server)
        .await;
    mount_message_metadata(&success_server, "m-recovered", "650", "Recovered message").await;

    let success_config = config_report_for(&temp_dir, &success_server);
    let report = sync_run(&success_config, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Full);
    assert_eq!(report.cursor_history_id, "700");
    assert_eq!(report.messages_upserted, 1);

    let stored_state = store::mailbox::get_sync_state(
        &success_config.config.store.database_path,
        success_config.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        stored_state.last_sync_status,
        store::mailbox::SyncStatus::Ok
    );
    assert_eq!(stored_state.cursor_history_id.as_deref(), Some("700"));
}

#[tokio::test]
async fn stale_history_failure_clears_cursor_before_retrying_full_sync() {
    let failing_server = MockServer::start().await;
    mount_profile(&failing_server, "350").await;
    mount_labels(&failing_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND"
            }
        })))
        .mount(&failing_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient gmail failure"))
        .mount(&failing_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let failing_config = config_report_for(&temp_dir, &failing_server);
    seed_credentials(&failing_config);
    seed_existing_mailbox(
        &failing_config,
        "250",
        "cached-1",
        "Existing cached message",
    );

    let first_error = sync_run(&failing_config, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();
    assert!(first_error.to_string().contains("users/me/messages"));

    let failed_state = store::mailbox::get_sync_state(
        &failing_config.config.store.database_path,
        failing_config.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        failed_state.last_sync_status,
        store::mailbox::SyncStatus::Failed
    );
    assert_eq!(failed_state.last_sync_mode, store::mailbox::SyncMode::Full);
    assert!(failed_state.cursor_history_id.is_none());

    let success_server = MockServer::start().await;
    mount_profile(&success_server, "700").await;
    mount_labels(&success_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .respond_with(
            ResponseTemplate::new(418).set_body_string("history endpoint should not be used"),
        )
        .mount(&success_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-recovered", "threadId": "t-recovered"}],
            "resultSizeEstimate": 1
        })))
        .mount(&success_server)
        .await;
    mount_message_metadata(&success_server, "m-recovered", "650", "Recovered message").await;

    let success_config = config_report_for(&temp_dir, &success_server);
    let report = sync_run(&success_config, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Full);
    assert!(!report.fallback_from_history);
    assert_eq!(report.cursor_history_id, "700");
}

#[tokio::test]
async fn stale_history_retry_keeps_persisted_bootstrap_query() {
    let stale_server = MockServer::start().await;
    mount_profile(&stale_server, "350").await;
    mount_labels(&stale_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND"
            }
        })))
        .mount(&stale_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("q", "newer_than:7d"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-recovered", "threadId": "t-recovered"}],
            "resultSizeEstimate": 1
        })))
        .mount(&stale_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("q", "newer_than:90d"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_string("stale-history retry widened bootstrap query"),
        )
        .mount(&stale_server)
        .await;
    mount_message_metadata(&stale_server, "m-recovered", "650", "Recovered message").await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &stale_server);
    seed_credentials(&config_report);
    seed_existing_mailbox_with_bootstrap_query(
        &config_report,
        "250",
        "cached-1",
        "Existing cached message",
        "newer_than:7d",
    );

    let report = sync_run(&config_report, false, 90).await.unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Full);
    assert!(report.fallback_from_history);
    assert_eq!(report.bootstrap_query, "newer_than:7d");
    assert_eq!(report.messages_upserted, 1);
    assert_eq!(report.cursor_history_id, "650");
}

#[tokio::test]
async fn forced_full_sync_uses_requested_bootstrap_query() {
    let mock_server = MockServer::start().await;
    let requested_bootstrap_query = bootstrap_query(90);
    mount_profile(&mock_server, "700").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("q", requested_bootstrap_query.as_str()))
        .and(query_param("maxResults", "500"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-forced", "threadId": "t-forced"}],
            "resultSizeEstimate": 1
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("q", "newer_than:7d"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_string("forced full sync reused persisted bootstrap query"),
        )
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-forced", "650", "Forced full sync").await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox_with_bootstrap_query(
        &config_report,
        "250",
        "cached-1",
        "Existing cached message",
        "newer_than:7d",
    );

    let report = sync_run(&config_report, true, 90).await.unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Full);
    assert!(!report.fallback_from_history);
    assert_eq!(report.bootstrap_query, requested_bootstrap_query);
    assert_eq!(report.messages_upserted, 1);
    assert_eq!(report.cursor_history_id, "700");
    assert!(report.pipeline_enabled);
    assert!(report.pipeline_list_queue_high_water >= 1);
    assert!(report.pipeline_write_queue_high_water >= 1);
    assert_eq!(report.pipeline_write_batch_count, 1);
    assert!(report.adaptive_pacing_enabled);
    assert_eq!(report.quota_units_budget_per_minute, 12_000);
    assert_eq!(report.message_fetch_concurrency, 4);
    assert_eq!(report.quota_units_cap_per_minute, 12_000);
    assert_eq!(report.message_fetch_concurrency_cap, 4);
    assert_eq!(report.starting_quota_units_per_minute, 12_000);
    assert_eq!(report.starting_message_fetch_concurrency, 4);
    assert_eq!(report.effective_quota_units_per_minute, 12_000);
    assert_eq!(report.effective_message_fetch_concurrency, 4);
    assert_eq!(report.adaptive_downshift_count, 0);
    assert_eq!(report.estimated_quota_units_reserved, 12);
    assert_eq!(report.http_attempt_count, 4);
    assert_eq!(report.retry_count, 0);
    assert_eq!(report.quota_pressure_retry_count, 0);
    assert_eq!(report.concurrency_pressure_retry_count, 0);
    assert_eq!(report.backend_retry_count, 0);
    assert_eq!(report.retry_after_wait_ms, 0);

    let stored_state = store::mailbox::get_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(stored_state.bootstrap_query, requested_bootstrap_query);
    assert!(stored_state.pipeline_enabled);
    assert_eq!(stored_state.pipeline_write_batch_count, 1);

    let pacing_state = store::mailbox::get_sync_pacing_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(pacing_state.learned_quota_units_per_minute, 12_000);
    assert_eq!(pacing_state.learned_message_fetch_concurrency, 4);
    assert_eq!(pacing_state.clean_run_streak, 1);
    assert!(pacing_state.last_pressure_kind.is_none());
}

#[tokio::test]
async fn label_refresh_failure_marks_existing_sync_state_as_failed() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "350").await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient labels failure"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox_with_bootstrap_query(
        &config_report,
        "250",
        "cached-1",
        "Existing cached message",
        "newer_than:7d",
    );

    let error = sync_run(&config_report, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("users/me/labels"));

    let failed_state = store::mailbox::get_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        failed_state.last_sync_status,
        store::mailbox::SyncStatus::Failed
    );
    assert_eq!(
        failed_state.last_sync_mode,
        store::mailbox::SyncMode::Incremental
    );
    assert_eq!(failed_state.bootstrap_query, "newer_than:7d");
    assert_eq!(failed_state.last_full_sync_success_epoch_s, Some(100));
    assert_eq!(failed_state.last_incremental_sync_success_epoch_s, None);
    assert!(failed_state.last_sync_epoch_s > 100);
    assert!(
        failed_state
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("users/me/labels"))
    );
}

#[tokio::test]
async fn full_sync_failure_keeps_cached_labels_until_mailbox_changes_commit() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "700").await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "labels": [{
                "id": "Label_1",
                "name": "Project/New",
                "type": "user"
            }]
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient messages failure"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox_with_custom_labels(
        &config_report,
        "250",
        "cached-1",
        "Existing cached message",
        "newer_than:7d",
        SeededMailboxLabels {
            labels: vec![crate::gmail::GmailLabel {
                id: String::from("Label_1"),
                name: String::from("Project/Old"),
                label_type: String::from("user"),
                message_list_visibility: None,
                label_list_visibility: None,
                messages_total: None,
                messages_unread: None,
                threads_total: None,
                threads_unread: None,
            }],
            label_ids: vec![String::from("Label_1")],
            label_names_text: String::from("Project/Old"),
        },
    );

    let error = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("users/me/messages"));

    let old_label_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("Existing"),
            label: Some(String::from("Project/Old")),
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(old_label_results.len(), 1);
    assert_eq!(old_label_results[0].label_names, vec!["Project/Old"]);

    let new_label_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("Existing"),
            label: Some(String::from("Project/New")),
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(new_label_results.is_empty());
}

#[tokio::test]
async fn full_sync_keeps_the_newest_history_cursor_across_pages() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-newer", "threadId": "t-newer"}],
            "nextPageToken": "page-2",
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("pageToken", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-older", "threadId": "t-older"}],
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-newer", "950", "Newest message").await;
    mount_message_metadata(&mock_server, "m-older", "900", "Older message").await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(&config_report, "250", "cached-1", "Stale cached message");

    let report = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert_eq!(report.cursor_history_id, "999");
    assert_eq!(report.messages_upserted, 2);

    let stored_state = store::mailbox::get_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(stored_state.cursor_history_id.as_deref(), Some("999"));

    let mailbox = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 2);

    let stale_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("Stale"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(stale_results.is_empty());
}

#[tokio::test]
async fn full_sync_failure_after_staging_a_page_preserves_existing_mailbox_cache() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-staged", "threadId": "t-staged"}],
            "nextPageToken": "page-2",
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("pageToken", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-fail", "threadId": "t-fail"}],
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-staged", "950", "Staged but not finalized").await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-fail"))
        .respond_with(ResponseTemplate::new(500).set_body_string("later page metadata failure"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(&config_report, "250", "cached-1", "Existing cached message");

    let error = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("users/me/messages/m-fail"));

    let mailbox = store::mailbox::inspect_mailbox(
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
            .and_then(|state| state.cursor_history_id.as_deref()),
        Some("250")
    );

    let cached_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("Existing"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(cached_results.len(), 1);
    assert_eq!(cached_results[0].message_id, "cached-1");

    let staged_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("finalized"),
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

#[tokio::test]
async fn full_sync_resume_reuses_saved_page_token_after_midstream_failure() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-stage-1", "threadId": "t-stage-1"}],
            "nextPageToken": "page-2",
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("pageToken", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-stage-2", "threadId": "t-stage-2"}],
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-stage-1", "950", "First staged page").await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-stage-2"))
        .respond_with(ResponseTemplate::new(500).set_body_string("second page failed"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);

    let error = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("users/me/messages/m-stage-2"));

    let checkpoint = store::mailbox::get_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(checkpoint.next_page_token.as_deref(), Some("page-2"));
    assert_eq!(checkpoint.pages_fetched, 1);
    assert_eq!(checkpoint.messages_upserted, 1);

    mock_server.reset().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("pageToken", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-stage-2", "threadId": "t-stage-2"}],
            "resultSizeEstimate": 2
        })))
        .expect(1)
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-stage-2", "960", "Resumed page").await;

    let report = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert!(report.resumed_from_checkpoint);
    assert_eq!(report.checkpoint_reused_pages, 1);
    assert_eq!(report.checkpoint_reused_messages_upserted, 1);
    assert_eq!(report.pages_fetched, 2);
    assert_eq!(report.messages_upserted, 2);
    assert_eq!(report.store_message_count, 2);
    assert!(
        store::mailbox::get_full_sync_checkpoint(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
        )
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn full_sync_ready_to_finalize_checkpoint_skips_relisting_pages() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_full_sync_checkpoint(
        &config_report,
        FullSyncCheckpointSeed {
            bootstrap_query: &bootstrap_query(DEFAULT_BOOTSTRAP_RECENT_DAYS),
            status: store::mailbox::FullSyncCheckpointStatus::ReadyToFinalize,
            next_page_token: None,
            pages_fetched: 1,
            messages_listed: 1,
            messages_upserted: 1,
            staged_messages: vec![seeded_mailbox_message(
                "gmail:operator@example.com",
                "m-ready",
                "950",
                "Checkpoint finalize subject",
            )],
        },
    );

    let report = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert!(report.resumed_from_checkpoint);
    assert_eq!(report.checkpoint_reused_pages, 1);
    assert_eq!(report.checkpoint_reused_messages_upserted, 1);
    assert_eq!(report.pages_fetched, 1);
    assert_eq!(report.messages_upserted, 1);
    assert_eq!(report.store_message_count, 1);
    assert!(
        store::mailbox::get_full_sync_checkpoint(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
        )
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn full_sync_invalid_resume_page_token_restarts_from_scratch() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param("pageToken", "expired-page"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": {
                "code": 400,
                "message": "Invalid pageToken value",
                "status": "INVALID_ARGUMENT"
            }
        })))
        .expect(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-fresh", "threadId": "t-fresh"}],
            "resultSizeEstimate": 1
        })))
        .expect(1)
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-fresh", "980", "Fresh after reset").await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_full_sync_checkpoint(
        &config_report,
        FullSyncCheckpointSeed {
            bootstrap_query: &bootstrap_query(DEFAULT_BOOTSTRAP_RECENT_DAYS),
            status: store::mailbox::FullSyncCheckpointStatus::Paging,
            next_page_token: Some("expired-page"),
            pages_fetched: 1,
            messages_listed: 1,
            messages_upserted: 1,
            staged_messages: vec![seeded_mailbox_message(
                "gmail:operator@example.com",
                "m-stale-stage",
                "970",
                "Stale staged message",
            )],
        },
    );

    let report = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert!(!report.resumed_from_checkpoint);
    assert_eq!(report.checkpoint_reused_pages, 0);
    assert_eq!(report.messages_upserted, 1);

    let stale_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("stale"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(stale_results.is_empty());

    let fresh_results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("fresh"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(fresh_results.len(), 1);
    assert_eq!(fresh_results[0].message_id, "m-fresh");
}

#[tokio::test]
async fn full_sync_checkpoint_query_mismatch_restarts_from_first_page() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "999").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [{"id": "m-fresh-query", "threadId": "t-fresh-query"}],
            "resultSizeEstimate": 1
        })))
        .expect(1)
        .mount(&mock_server)
        .await;
    mount_message_metadata(
        &mock_server,
        "m-fresh-query",
        "981",
        "Fresh after query mismatch",
    )
    .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_full_sync_checkpoint(
        &config_report,
        FullSyncCheckpointSeed {
            bootstrap_query: "in:anywhere newer_than:7d",
            status: store::mailbox::FullSyncCheckpointStatus::Paging,
            next_page_token: Some("page-9"),
            pages_fetched: 1,
            messages_listed: 1,
            messages_upserted: 1,
            staged_messages: vec![seeded_mailbox_message(
                "gmail:operator@example.com",
                "m-old-query",
                "970",
                "Old query staged message",
            )],
        },
    );

    let report = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert!(!report.resumed_from_checkpoint);
    assert_eq!(
        report.bootstrap_query,
        bootstrap_query(DEFAULT_BOOTSTRAP_RECENT_DAYS)
    );
    assert_eq!(report.checkpoint_reused_pages, 0);
    assert_eq!(report.store_message_count, 1);
}

#[tokio::test]
async fn incremental_sync_skips_messages_deleted_on_later_history_pages() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "350").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "history": [{
                "labelsAdded": [{
                    "message": {"id": "cached-1"}
                }]
            }],
            "nextPageToken": "page-2",
            "historyId": "300"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .and(query_param("pageToken", "page-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "history": [{
                "messagesDeleted": [{
                    "message": {"id": "cached-1"}
                }]
            }],
            "historyId": "350"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(
        &config_report,
        "250",
        "cached-1",
        "Message removed remotely",
    );

    let report = sync_run(&config_report, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Incremental);
    assert_eq!(report.cursor_history_id, "350");
    assert_eq!(report.messages_upserted, 0);
    assert_eq!(report.messages_deleted, 1);

    let mailbox = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 0);
    assert_eq!(
        mailbox
            .sync_state
            .as_ref()
            .and_then(|state| state.cursor_history_id.as_deref()),
        Some("350")
    );
}

#[tokio::test]
async fn full_sync_skips_messages_that_disappear_before_metadata_fetch() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "600").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messages": [
                {"id": "m-present", "threadId": "t-present"},
                {"id": "m-gone", "threadId": "t-gone"}
            ],
            "resultSizeEstimate": 2
        })))
        .mount(&mock_server)
        .await;
    mount_message_metadata(&mock_server, "m-present", "590", "Still available").await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-gone"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": {"code": 404, "message": "Requested entity was not found.", "status": "NOT_FOUND"}
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);

    let report = sync_run(&config_report, true, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Full);
    assert_eq!(report.messages_listed, 2);
    assert_eq!(report.messages_upserted, 1);
}

#[tokio::test]
async fn incremental_sync_deletes_messages_that_404_during_metadata_fetch() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "410").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "history": [{
                "messagesAdded": [{
                    "message": {"id": "cached-1"}
                }]
            }],
            "historyId": "410"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/cached-1"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": {"code": 404, "message": "Requested entity was not found.", "status": "NOT_FOUND"}
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(&config_report, "250", "cached-1", "Now missing in Gmail");

    let report = sync_run(&config_report, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Incremental);
    assert_eq!(report.messages_upserted, 0);
    assert_eq!(report.messages_deleted, 1);

    let mailbox = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 0);
}

#[tokio::test]
async fn incremental_sync_keeps_existing_bootstrap_query() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "350").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "history": [],
            "historyId": "350"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox_with_bootstrap_query(
        &config_report,
        "250",
        "cached-1",
        "Existing cached message",
        "newer_than:7d",
    );

    let report = sync_run(&config_report, false, 90).await.unwrap();

    assert_eq!(report.mode, store::mailbox::SyncMode::Incremental);
    assert_eq!(report.bootstrap_query, "newer_than:7d");
    assert_eq!(report.cursor_history_id, "350");

    let stored_state = store::mailbox::get_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
    )
    .unwrap()
    .unwrap();
    assert_eq!(stored_state.bootstrap_query, "newer_than:7d");
    assert_eq!(
        stored_state.last_sync_mode,
        store::mailbox::SyncMode::Incremental
    );
    assert_eq!(
        stored_state.last_sync_status,
        store::mailbox::SyncStatus::Ok
    );
}

#[tokio::test]
async fn incremental_sync_failure_preserves_deleted_messages_until_changes_commit() {
    let mock_server = MockServer::start().await;
    mount_profile(&mock_server, "410").await;
    mount_labels(&mock_server).await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/history"))
        .and(query_param_is_missing("pageToken"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "history": [{
                "messagesDeleted": [{
                    "message": {"id": "cached-1"}
                }],
                "messagesAdded": [{
                    "message": {"id": "m-updated"}
                }]
            }],
            "historyId": "410"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-updated"))
        .respond_with(ResponseTemplate::new(500).set_body_string("transient gmail failure"))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    seed_existing_mailbox(
        &config_report,
        "250",
        "cached-1",
        "Should survive failed sync",
    );

    let error = sync_run(&config_report, false, DEFAULT_BOOTSTRAP_RECENT_DAYS)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("users/me/messages/m-updated"));

    let mailbox = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.message_count, 1);

    let results = store::mailbox::search_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SearchQuery {
            account_id: String::from("gmail:operator@example.com"),
            terms: String::from("survive"),
            label: None,
            from_address: None,
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message_id, "cached-1");
}

fn config_report_for(temp_dir: &TempDir, mock_server: &MockServer) -> ConfigReport {
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let mut config_report = resolve(&paths).unwrap();
    config_report.config.gmail.api_base_url = format!("{}/gmail/v1", mock_server.uri());
    config_report.config.gmail.auth_url = format!("{}/oauth2/auth", mock_server.uri());
    config_report.config.gmail.token_url = format!("{}/oauth2/token", mock_server.uri());
    config_report.config.gmail.open_browser = false;
    config_report.config.gmail.client_id = Some(String::from("client-id"));
    config_report.config.gmail.client_secret = Some(String::from("client-secret"));
    config_report
}

fn seed_credentials(config_report: &ConfigReport) {
    let credential_store = FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    );
    credential_store
        .save(&StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("access-token")),
            refresh_token: Some(SecretString::from(String::from("refresh-token"))),
            expires_at_epoch_s: Some(u64::MAX),
            scopes: vec![String::from("scope:a")],
        })
        .unwrap();
}

fn seed_existing_mailbox(
    config_report: &ConfigReport,
    history_id: &str,
    message_id: &str,
    subject: &str,
) {
    seed_existing_mailbox_with_custom_labels(
        config_report,
        history_id,
        message_id,
        subject,
        "newer_than:90d",
        SeededMailboxLabels {
            labels: vec![crate::gmail::GmailLabel {
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
        },
    );
}

fn seed_existing_mailbox_with_bootstrap_query(
    config_report: &ConfigReport,
    history_id: &str,
    message_id: &str,
    subject: &str,
    bootstrap_query: &str,
) {
    seed_existing_mailbox_with_custom_labels(
        config_report,
        history_id,
        message_id,
        subject,
        bootstrap_query,
        SeededMailboxLabels {
            labels: vec![crate::gmail::GmailLabel {
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
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
        },
    );
}

fn seed_existing_mailbox_with_custom_labels(
    config_report: &ConfigReport,
    history_id: &str,
    message_id: &str,
    subject: &str,
    bootstrap_query: &str,
    seeded_labels: SeededMailboxLabels,
) {
    store::init(config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: history_id.to_owned(),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    store::mailbox::replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &seeded_labels.labels,
        100,
    )
    .unwrap();
    store::mailbox::upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[store::mailbox::GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: message_id.to_owned(),
            thread_id: String::from("t-cached"),
            history_id: history_id.to_owned(),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: subject.to_owned(),
            subject: subject.to_owned(),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: seeded_labels.label_ids,
            label_names_text: seeded_labels.label_names_text,
            attachments: Vec::new(),
        }],
        100,
    )
    .unwrap();
    store::mailbox::upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SyncStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            cursor_history_id: Some(history_id.to_owned()),
            bootstrap_query: bootstrap_query.to_owned(),
            last_sync_mode: store::mailbox::SyncMode::Full,
            last_sync_status: store::mailbox::SyncStatus::Ok,
            last_error: None,
            last_sync_epoch_s: 100,
            last_full_sync_success_epoch_s: Some(100),
            last_incremental_sync_success_epoch_s: None,
            pipeline_enabled: false,
            pipeline_list_queue_high_water: 0,
            pipeline_write_queue_high_water: 0,
            pipeline_write_batch_count: 0,
            pipeline_writer_wait_ms: 0,
        },
    )
    .unwrap();
}

fn seeded_mailbox_message(
    account_id: &str,
    message_id: &str,
    history_id: &str,
    subject: &str,
) -> store::mailbox::GmailMessageUpsertInput {
    store::mailbox::GmailMessageUpsertInput {
        account_id: account_id.to_owned(),
        message_id: message_id.to_owned(),
        thread_id: format!("thread-{message_id}"),
        history_id: history_id.to_owned(),
        internal_date_epoch_ms: 1_700_000_000_000,
        snippet: subject.to_owned(),
        subject: subject.to_owned(),
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
    }
}

struct FullSyncCheckpointSeed<'a> {
    bootstrap_query: &'a str,
    status: store::mailbox::FullSyncCheckpointStatus,
    next_page_token: Option<&'a str>,
    pages_fetched: i64,
    messages_listed: i64,
    messages_upserted: i64,
    staged_messages: Vec<store::mailbox::GmailMessageUpsertInput>,
}

fn seed_full_sync_checkpoint(config_report: &ConfigReport, seed: FullSyncCheckpointSeed<'_>) {
    store::init(config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("999"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();

    let account_id = "gmail:operator@example.com";
    let labels = vec![crate::gmail::GmailLabel {
        id: String::from("INBOX"),
        name: String::from("INBOX"),
        label_type: String::from("system"),
        message_list_visibility: None,
        label_list_visibility: None,
        messages_total: None,
        messages_unread: None,
        threads_total: None,
        threads_unread: None,
    }];
    let checkpoint = store::mailbox::prepare_full_sync_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &labels,
        &store::mailbox::FullSyncCheckpointUpdate {
            bootstrap_query: seed.bootstrap_query.to_owned(),
            status: store::mailbox::FullSyncCheckpointStatus::Paging,
            next_page_token: None,
            cursor_history_id: Some(String::from("999")),
            pages_fetched: 0,
            messages_listed: 0,
            messages_upserted: 0,
            labels_synced: 1,
            started_at_epoch_s: 100,
            updated_at_epoch_s: 100,
        },
    )
    .unwrap();

    if seed.staged_messages.is_empty() {
        store::mailbox::update_full_sync_checkpoint_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
            &labels,
            &store::mailbox::FullSyncCheckpointUpdate {
                bootstrap_query: seed.bootstrap_query.to_owned(),
                status: seed.status,
                next_page_token: seed.next_page_token.map(str::to_owned),
                cursor_history_id: checkpoint.cursor_history_id,
                pages_fetched: seed.pages_fetched,
                messages_listed: seed.messages_listed,
                messages_upserted: seed.messages_upserted,
                labels_synced: 1,
                started_at_epoch_s: checkpoint.started_at_epoch_s,
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        return;
    }

    store::mailbox::stage_full_sync_page_and_update_checkpoint(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &seed.staged_messages,
        &store::mailbox::FullSyncCheckpointUpdate {
            bootstrap_query: seed.bootstrap_query.to_owned(),
            status: seed.status,
            next_page_token: seed.next_page_token.map(str::to_owned),
            cursor_history_id: checkpoint.cursor_history_id,
            pages_fetched: seed.pages_fetched,
            messages_listed: seed.messages_listed,
            messages_upserted: seed.messages_upserted,
            labels_synced: 1,
            started_at_epoch_s: checkpoint.started_at_epoch_s,
            updated_at_epoch_s: 101,
        },
    )
    .unwrap();
}

fn seed_schema_v2_store_with_active_account(config_report: &ConfigReport) {
    store::init(config_report).unwrap();
    let connection = rusqlite::Connection::open(&config_report.config.store.database_path).unwrap();
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
            "../../migrations/06-attachment-catalog-export-foundation/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/05-workflow-version-cas/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/04-unified-thread-workflow/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../migrations/03-mailbox-sync-search-foundation/down.sql"
        ))
        .unwrap();
    connection
        .pragma_update(None, "user_version", 2_i64)
        .unwrap();

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
}

async fn mount_profile(mock_server: &MockServer, history_id: &str) {
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "emailAddress": "operator@example.com",
            "messagesTotal": 10,
            "threadsTotal": 7,
            "historyId": history_id
        })))
        .mount(mock_server)
        .await;
}

async fn mount_labels(mock_server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "labels": [{
                "id": "INBOX",
                "name": "INBOX",
                "type": "system"
            }]
        })))
        .mount(mock_server)
        .await;
}

async fn mount_message_metadata(
    mock_server: &MockServer,
    message_id: &str,
    history_id: &str,
    subject: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!("/gmail/v1/users/me/messages/{message_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": message_id,
            "threadId": format!("thread-{message_id}"),
            "labelIds": ["INBOX"],
            "snippet": subject,
            "historyId": history_id,
            "internalDate": "1700000000000",
            "sizeEstimate": 123,
            "payload": {
                "headers": [
                    {"name": "Subject", "value": subject},
                    {"name": "From", "value": "Alice <alice@example.com>"},
                    {"name": "To", "value": "operator@example.com"}
                ]
            }
        })))
        .mount(mock_server)
        .await;
}
