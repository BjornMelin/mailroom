use super::*;

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
async fn sync_history_returns_persisted_run_summary_for_active_account() {
    let temp_dir = TempDir::new().unwrap();
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    store::init(&config_report).unwrap();
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
    let sync_state = store::mailbox::upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("100")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: store::mailbox::SyncMode::Incremental,
            last_sync_status: store::mailbox::SyncStatus::Ok,
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
    let (_, history, _) = store::mailbox::persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &store::mailbox::SyncRunOutcomeInput {
            account_id: account.account_id.clone(),
            sync_mode: store::mailbox::SyncMode::Incremental,
            status: store::mailbox::SyncStatus::Ok,
            comparability_kind: store::mailbox::SyncRunComparabilityKind::IncrementalWorkloadTier,
            comparability_key: String::from("small"),
            startup_seed_run_id: None,
            started_at_epoch_s: 95,
            finished_at_epoch_s: 100,
            bootstrap_query: String::from("newer_than:90d"),
            cursor_history_id: Some(String::from("100")),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            pages_fetched: 1,
            messages_listed: 25,
            messages_upserted: 25,
            messages_deleted: 0,
            labels_synced: 3,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
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
            pipeline_staged_message_count: 25,
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
            estimated_quota_units_reserved: 100,
            http_attempt_count: 1,
            retry_count: 0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
            duration_ms: 500,
            pages_per_second: 2.0,
            messages_per_second: 50.0,
            error_message: None,
        },
    )
    .unwrap();

    let report = sync_history(&config_report, 10).await.unwrap();

    assert_eq!(report.account_id, account.account_id);
    assert_eq!(report.runs.len(), 1);
    assert_eq!(report.runs[0].run_id, history.run_id);
    assert_eq!(
        report
            .summary
            .as_ref()
            .and_then(|summary| summary.best_clean_run_id),
        Some(history.run_id)
    );
}

#[tokio::test]
async fn sync_perf_explain_uses_best_clean_baseline_for_latest_bucket() {
    let temp_dir = TempDir::new().unwrap();
    let mock_server = MockServer::start().await;
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    store::init(&config_report).unwrap();
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
    let sync_state = store::mailbox::upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("100")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: store::mailbox::SyncMode::Incremental,
            last_sync_status: store::mailbox::SyncStatus::Ok,
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
    let (_, history, _) = store::mailbox::persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &store::mailbox::SyncRunOutcomeInput {
            account_id: account.account_id.clone(),
            sync_mode: store::mailbox::SyncMode::Incremental,
            status: store::mailbox::SyncStatus::Ok,
            comparability_kind: store::mailbox::SyncRunComparabilityKind::IncrementalWorkloadTier,
            comparability_key: String::from("small"),
            startup_seed_run_id: None,
            started_at_epoch_s: 95,
            finished_at_epoch_s: 100,
            bootstrap_query: String::from("newer_than:90d"),
            cursor_history_id: Some(String::from("100")),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            pages_fetched: 1,
            messages_listed: 25,
            messages_upserted: 25,
            messages_deleted: 0,
            labels_synced: 3,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
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
            pipeline_staged_message_count: 25,
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
            estimated_quota_units_reserved: 100,
            http_attempt_count: 1,
            retry_count: 0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
            duration_ms: 500,
            pages_per_second: 2.0,
            messages_per_second: 50.0,
            error_message: None,
        },
    )
    .unwrap();

    let report = sync_perf_explain(&config_report, 10).await.unwrap();

    assert_eq!(
        report.latest_run.as_ref().map(|run| run.run_id),
        Some(history.run_id)
    );
    assert_eq!(
        report.baseline_run.as_ref().map(|run| run.run_id),
        Some(history.run_id)
    );
    assert!(report.comparable_to_baseline);
}

#[tokio::test]
async fn sync_perf_explain_suppresses_drift_for_tiny_incremental_workloads() {
    let temp_dir = TempDir::new().unwrap();
    let mock_server = MockServer::start().await;
    let config_report = config_report_for(&temp_dir, &mock_server);
    seed_credentials(&config_report);
    store::init(&config_report).unwrap();
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
    let sync_state = store::mailbox::upsert_sync_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::mailbox::SyncStateUpdate {
            account_id: account.account_id.clone(),
            cursor_history_id: Some(String::from("100")),
            bootstrap_query: String::from("newer_than:90d"),
            last_sync_mode: store::mailbox::SyncMode::Incremental,
            last_sync_status: store::mailbox::SyncStatus::Ok,
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
    let (_, baseline_history, _) = store::mailbox::persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &store::mailbox::SyncRunOutcomeInput {
            account_id: account.account_id.clone(),
            sync_mode: store::mailbox::SyncMode::Incremental,
            status: store::mailbox::SyncStatus::Ok,
            comparability_kind: store::mailbox::SyncRunComparabilityKind::IncrementalWorkloadTier,
            comparability_key: String::from("tiny"),
            startup_seed_run_id: None,
            started_at_epoch_s: 95,
            finished_at_epoch_s: 100,
            bootstrap_query: String::from("newer_than:90d"),
            cursor_history_id: Some(String::from("100")),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            pages_fetched: 1,
            messages_listed: 16,
            messages_upserted: 16,
            messages_deleted: 0,
            labels_synced: 3,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
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
            pipeline_staged_message_count: 16,
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
            estimated_quota_units_reserved: 64,
            http_attempt_count: 1,
            retry_count: 0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
            duration_ms: 400,
            pages_per_second: 2.5,
            messages_per_second: 40.0,
            error_message: None,
        },
    )
    .unwrap();
    let (_, latest_history, _) = store::mailbox::persist_successful_sync_outcome(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &sync_state,
        &store::mailbox::SyncRunOutcomeInput {
            account_id: account.account_id.clone(),
            sync_mode: store::mailbox::SyncMode::Incremental,
            status: store::mailbox::SyncStatus::Ok,
            comparability_kind: store::mailbox::SyncRunComparabilityKind::IncrementalWorkloadTier,
            comparability_key: String::from("tiny"),
            startup_seed_run_id: None,
            started_at_epoch_s: 101,
            finished_at_epoch_s: 103,
            bootstrap_query: String::from("newer_than:90d"),
            cursor_history_id: Some(String::from("101")),
            fallback_from_history: false,
            resumed_from_checkpoint: false,
            pages_fetched: 1,
            messages_listed: 2,
            messages_upserted: 2,
            messages_deleted: 0,
            labels_synced: 1,
            checkpoint_reused_pages: 0,
            checkpoint_reused_messages_upserted: 0,
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
            pipeline_staged_message_count: 2,
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
            estimated_quota_units_reserved: 10,
            http_attempt_count: 1,
            retry_count: 0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
            duration_ms: 500,
            pages_per_second: 2.0,
            messages_per_second: 4.0,
            error_message: None,
        },
    )
    .unwrap();

    let report = sync_perf_explain(&config_report, 10).await.unwrap();

    assert_eq!(
        report.latest_run.as_ref().map(|run| run.run_id),
        Some(latest_history.run_id)
    );
    assert_eq!(
        report.baseline_run.as_ref().map(|run| run.run_id),
        Some(baseline_history.run_id)
    );
    assert!(!report.comparable_to_baseline);
    assert!(report.drift.is_none());
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
    let failed_state = mailbox.sync_state.as_ref().unwrap();
    assert!(failed_state.pipeline_enabled);
    assert!(failed_state.pipeline_list_queue_high_water >= 1);
    assert_eq!(failed_state.pipeline_write_batch_count, 1);

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
    assert!(!report.pipeline_enabled);
    assert_eq!(report.pipeline_list_queue_high_water, 0);
    assert_eq!(report.pipeline_write_queue_high_water, 0);
    assert_eq!(report.pipeline_write_batch_count, 0);
    assert_eq!(report.pipeline_writer_wait_ms, 0);

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
    assert!(!stored_state.pipeline_enabled);
    assert_eq!(stored_state.pipeline_write_batch_count, 0);
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

    let history = sync_history(&config_report, 10).await.unwrap();
    let summary = history.summary.expect("failed sync summary should persist");
    assert_eq!(summary.comparability_key, "tiny");
    assert_eq!(summary.latest_status, store::mailbox::SyncStatus::Failed);
    assert_eq!(history.runs[0].comparability_key, "tiny");
    assert_eq!(history.runs[0].messages_listed, 1);
    assert_eq!(history.runs[0].messages_deleted, 1);
}
