use super::{
    DEFAULT_BOOTSTRAP_RECENT_DAYS, DEFAULT_SEARCH_LIMIT, SearchRequest, SyncRunOptions,
    newest_history_id, parse_start_of_day_epoch_ms, search, sync_history, sync_perf_explain,
    sync_run, sync_run_with_options,
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

#[path = "search.rs"]
mod search_orchestration;
mod sync;
mod unit;

fn config_report_for(temp_dir: &TempDir, mock_server_uri: &str) -> ConfigReport {
    let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let mut config_report = resolve(&paths).unwrap();
    config_report.config.gmail.api_base_url = format!("{mock_server_uri}/gmail/v1");
    config_report.config.gmail.auth_url = format!("{mock_server_uri}/oauth2/auth");
    config_report.config.gmail.token_url = format!("{mock_server_uri}/oauth2/token");
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
            "../../../migrations/15-sync-run-history/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/14-sync-pipeline-telemetry-and-page-manifests/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/13-bounded-sync-pipeline/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/12-sync-pacing-state-hardening/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/11-sync-pacing-state/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/10-full-sync-checkpoints/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/09-mailbox-full-sync-staging/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/08-automation-rules-and-bulk-actions/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/07-account-scoped-attachment-keys/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/06-attachment-catalog-export-foundation/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/05-workflow-version-cas/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/04-unified-thread-workflow/down.sql"
        ))
        .unwrap();
    connection
        .execute_batch(include_str!(
            "../../../migrations/03-mailbox-sync-search-foundation/down.sql"
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
