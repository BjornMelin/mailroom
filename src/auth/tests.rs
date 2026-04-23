use super::flow::{
    AuthorizationPrompt, CallbackListener, authorization_prompt, configured_workspace_paths,
    parse_callback_request, persist_login_state, redirect_url_for,
};
use super::{login, logout, setup, status};
use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::auth::oauth_client::{self, PreparedSetup, setup_guidance};
use crate::config::{GmailConfig, resolve};
use crate::store::accounts::UpsertAccountInput;
use crate::workspace::WorkspacePaths;
use oauth2::CsrfToken;
use rusqlite::Connection;
use secrecy::SecretString;
use std::fs;
use std::net::SocketAddr;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use url::Url;

#[test]
fn omits_authorization_prompt_for_json_output() {
    let authorize_url = Url::parse("https://example.com/oauth").unwrap();

    assert_eq!(authorization_prompt(&authorize_url, true, true), None);
}

#[test]
fn routes_headless_json_authorization_url_to_stderr() {
    let authorize_url = Url::parse("https://example.com/oauth").unwrap();

    assert_eq!(
        authorization_prompt(&authorize_url, true, false),
        Some(AuthorizationPrompt::StderrJson(String::from(
            r#"{"authorization_url":"https://example.com/oauth"}"#
        )))
    );
}

#[test]
fn renders_authorization_prompt_for_human_output() {
    let authorize_url = Url::parse("https://example.com/oauth").unwrap();

    assert_eq!(
        authorization_prompt(&authorize_url, false, false),
        Some(AuthorizationPrompt::StdoutText(String::from(
            "Complete Gmail authorization by visiting:\nhttps://example.com/oauth\n"
        )))
    );
}

#[test]
fn logout_clears_credentials_when_accounts_table_is_absent() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root);
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
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
            expires_at_epoch_s: Some(123),
            scopes: vec![String::from("scope:a")],
        })
        .unwrap();

    let connection = Connection::open(&config_report.config.store.database_path).unwrap();
    connection
        .execute_batch(
            "PRAGMA user_version = 1;
             CREATE TABLE app_metadata (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) STRICT;",
        )
        .unwrap();

    let report = logout(&config_report).unwrap();

    assert!(report.credential_removed);
    assert_eq!(report.deactivated_accounts, 0);
    assert!(credential_store.load().unwrap().is_none());
    assert!(config_report.config.store.database_path.exists());
}

#[tokio::test]
async fn login_without_oauth_client_does_not_create_runtime_state() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root);
    let config_report = resolve(&paths).unwrap();

    let error = login(&config_report, true, true).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "gmail OAuth client is not configured; run `mailroom auth setup` or set gmail.client_id"
    );
    assert!(!config_report.config.store.database_path.exists());
    assert!(!config_report.config.workspace.runtime_root.exists());
}

#[test]
fn status_reports_imported_oauth_client_as_configured() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root);
    let config_report = resolve(&paths).unwrap();
    let oauth_client_path = config_report
        .config
        .gmail
        .oauth_client_path(&config_report.config.workspace);
    fs::create_dir_all(oauth_client_path.parent().unwrap()).unwrap();
    fs::write(
        &oauth_client_path,
        r#"{
  "client_id": "desktop-client.apps.googleusercontent.com",
  "client_secret": "desktop-secret"
}"#,
    )
    .unwrap();

    let report = status(&config_report).unwrap();

    assert!(report.configured);
    assert_eq!(report.oauth_client_source, "workspace_file");
    assert!(report.oauth_client_exists);
}

#[test]
fn status_distinguishes_configured_auth_from_imported_client_file_presence() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let repo_config_path = repo_root.join(".mailroom/config.toml");
    fs::create_dir_all(repo_config_path.parent().unwrap()).unwrap();
    fs::write(
        &repo_config_path,
        r#"
[gmail]
client_id = "inline-client.apps.googleusercontent.com"
client_secret = "inline-secret"
"#,
    )
    .unwrap();

    let config_report = resolve(&paths).unwrap();
    let report = status(&config_report).unwrap();

    assert!(report.configured);
    assert_eq!(report.oauth_client_source, "config");
    assert!(!report.oauth_client_exists);
}

#[test]
fn status_errors_when_malformed_imported_oauth_client_exists() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let repo_config_path = repo_root.join(".mailroom/config.toml");
    let oauth_client_path = repo_root.join(".mailroom/auth/gmail-oauth-client.json");
    fs::create_dir_all(repo_config_path.parent().unwrap()).unwrap();
    fs::create_dir_all(oauth_client_path.parent().unwrap()).unwrap();
    fs::write(
        &repo_config_path,
        r#"
[gmail]
client_id = "inline-client.apps.googleusercontent.com"
client_secret = "inline-secret"
"#,
    )
    .unwrap();
    fs::write(&oauth_client_path, "{not-json").unwrap();

    let config_report = resolve(&paths).unwrap();
    let error = status(&config_report).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("failed to parse OAuth client from")
    );
}

#[test]
fn status_reports_valid_imported_oauth_client_as_authoritative_over_inline_config() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let repo_config_path = repo_root.join(".mailroom/config.toml");
    let oauth_client_path = repo_root.join(".mailroom/auth/gmail-oauth-client.json");
    fs::create_dir_all(repo_config_path.parent().unwrap()).unwrap();
    fs::create_dir_all(oauth_client_path.parent().unwrap()).unwrap();
    fs::write(
        &repo_config_path,
        r#"
[gmail]
client_id = "inline-client.apps.googleusercontent.com"
client_secret = "inline-secret"
"#,
    )
    .unwrap();
    fs::write(
        &oauth_client_path,
        r#"{
  "client_id": "imported-client.apps.googleusercontent.com",
  "client_secret": "imported-secret"
}"#,
    )
    .unwrap();

    let config_report = resolve(&paths).unwrap();
    let report = status(&config_report).unwrap();

    assert!(report.configured);
    assert_eq!(report.oauth_client_source, "workspace_file");
    assert!(report.oauth_client_exists);
}

#[tokio::test]
async fn setup_missing_credentials_file_reports_guidance() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let config_report = resolve(&paths).unwrap();

    let error = setup(
        &config_report,
        Some(repo_root.join("missing-client-secret.json")),
        true,
        true,
    )
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Google desktop-app credentials JSON was not found")
    );
    assert!(setup_guidance().contains("console.cloud.google.com"));
    assert!(!config_report.config.workspace.runtime_root.exists());
}

#[tokio::test]
async fn setup_preserves_existing_oauth_client_when_login_fails_after_staging_replacement() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let repo_config_path = repo_root.join(".mailroom/config.toml");
    let oauth_client_path = repo_root.join(".mailroom/auth/gmail-oauth-client.json");
    let credentials_path = repo_root.join("client_secret_replacement.json");
    fs::create_dir_all(repo_config_path.parent().unwrap()).unwrap();
    fs::create_dir_all(oauth_client_path.parent().unwrap()).unwrap();
    fs::write(
        &repo_config_path,
        r#"
[gmail]
auth_url = "not-a-valid-url"
"#,
    )
    .unwrap();
    fs::write(
        &oauth_client_path,
        r#"{
  "client_id": "existing-client.apps.googleusercontent.com",
  "client_secret": "existing-secret"
}"#,
    )
    .unwrap();
    fs::write(
        &credentials_path,
        r#"{
  "installed": {
    "client_id": "replacement-client.apps.googleusercontent.com",
    "client_secret": "replacement-secret"
  }
}"#,
    )
    .unwrap();

    let config_report = resolve(&paths).unwrap();
    let original_oauth_client = fs::read_to_string(&oauth_client_path).unwrap();
    let error = setup(&config_report, Some(credentials_path), true, true)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("relative URL without a base"));
    assert_eq!(
        fs::read_to_string(&oauth_client_path).unwrap(),
        original_oauth_client
    );
}

#[test]
fn setup_reuses_existing_imported_oauth_client_when_no_new_credentials_file_is_given() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root.clone());
    let oauth_client_path = repo_root.join(".mailroom/auth/gmail-oauth-client.json");
    fs::create_dir_all(oauth_client_path.parent().unwrap()).unwrap();
    fs::write(
        &oauth_client_path,
        r#"{
  "client_id": "imported-client.apps.googleusercontent.com",
  "client_secret": "imported-secret"
}"#,
    )
    .unwrap();

    let config_report = resolve(&paths).unwrap();
    let setup_action = oauth_client::prepare_setup(
        &config_report.config.gmail,
        &config_report.config.workspace,
        None,
        false,
    )
    .unwrap();

    assert!(matches!(setup_action, PreparedSetup::UseExisting));
}

#[tokio::test]
async fn persist_login_state_does_not_upsert_account_when_credential_save_fails() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root);
    let config_report = resolve(&paths).unwrap();
    let workspace_paths = configured_workspace_paths(&config_report).unwrap();
    let credential_store = FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    );

    workspace_paths.ensure_runtime_dirs().unwrap();
    fs::create_dir(credential_store.path()).unwrap();

    let error = persist_login_state(
        &config_report,
        &workspace_paths,
        &credential_store,
        &StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("access-token")),
            refresh_token: Some(SecretString::from(String::from("refresh-token"))),
            expires_at_epoch_s: Some(123),
            scopes: vec![String::from("scope:a")],
        },
        &UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("12345"),
            messages_total: 10,
            threads_total: 7,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .await
    .unwrap_err();

    assert!(!error.to_string().is_empty());
    assert!(!config_report.config.store.database_path.exists());
}

#[tokio::test]
async fn persist_login_state_rolls_back_new_credentials_when_store_init_fails() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root);
    let config_report = resolve(&paths).unwrap();
    let workspace_paths = configured_workspace_paths(&config_report).unwrap();
    let credential_store = FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    );

    workspace_paths.ensure_runtime_dirs().unwrap();
    fs::create_dir(&config_report.config.store.database_path).unwrap();

    let error = persist_login_state(
        &config_report,
        &workspace_paths,
        &credential_store,
        &StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("access-token")),
            refresh_token: Some(SecretString::from(String::from("refresh-token"))),
            expires_at_epoch_s: Some(123),
            scopes: vec![String::from("scope:a")],
        },
        &UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("12345"),
            messages_total: 10,
            threads_total: 7,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .await
    .unwrap_err();

    assert!(!error.to_string().is_empty());
    assert!(credential_store.load().unwrap().is_none());
    assert!(config_report.config.store.database_path.is_dir());
}

#[tokio::test]
async fn persist_login_state_restores_previous_credentials_when_store_init_fails() {
    let temp_dir = TempDir::new().unwrap();
    let repo_root = temp_dir.path().to_path_buf();
    let paths = WorkspacePaths::from_repo_root(repo_root);
    let config_report = resolve(&paths).unwrap();
    let workspace_paths = configured_workspace_paths(&config_report).unwrap();
    let credential_store = FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    );

    workspace_paths.ensure_runtime_dirs().unwrap();
    credential_store
        .save(&StoredCredentials {
            account_id: String::from("gmail:previous@example.com"),
            access_token: SecretString::from(String::from("previous-access-token")),
            refresh_token: Some(SecretString::from(String::from("previous-refresh-token"))),
            expires_at_epoch_s: Some(321),
            scopes: vec![String::from("scope:previous")],
        })
        .unwrap();
    fs::create_dir(&config_report.config.store.database_path).unwrap();

    let error = persist_login_state(
        &config_report,
        &workspace_paths,
        &credential_store,
        &StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("access-token")),
            refresh_token: Some(SecretString::from(String::from("refresh-token"))),
            expires_at_epoch_s: Some(123),
            scopes: vec![String::from("scope:a")],
        },
        &UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("12345"),
            messages_total: 10,
            threads_total: 7,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .await
    .unwrap_err();

    let restored = credential_store.load().unwrap().unwrap();

    assert!(!error.to_string().is_empty());
    assert_eq!(restored.account_id, "gmail:previous@example.com");
    assert_eq!(restored.expires_at_epoch_s, Some(321));
    assert_eq!(restored.scopes, vec![String::from("scope:previous")]);
}

#[test]
fn parses_successful_callback_request() {
    let callback = parse_callback_request(
        "GET /oauth2/callback?code=abc&state=def HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .unwrap()
    .unwrap();

    assert_eq!(callback.code, "abc");
    assert_eq!(callback.state, "def");
}

#[test]
fn parses_oauth_error_response() {
    let response = parse_callback_request(
        "GET /oauth2/callback?error=access_denied&error_description=nope HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .unwrap();

    assert_eq!(response.unwrap_err(), "access_denied: nope");
}

#[test]
fn redirect_url_brackets_ipv6_hosts() {
    let local_addr: SocketAddr = "[::1]:8181".parse().unwrap();

    assert_eq!(
        redirect_url_for(local_addr).unwrap().as_str(),
        "http://[::1]:8181/oauth2/callback"
    );
}

#[tokio::test]
async fn wait_for_code_returns_oauth_callback_error() {
    let listener = CallbackListener::bind(&GmailConfig {
        client_id: Some(String::from("client-id")),
        client_secret: None,
        auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
        token_url: String::from("https://oauth2.googleapis.com/token"),
        api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
        listen_host: String::from("127.0.0.1"),
        listen_port: 0,
        open_browser: false,
        request_timeout_secs: 30,
        scopes: vec![String::from("https://www.googleapis.com/auth/gmail.modify")],
    })
    .await
    .unwrap();
    let callback_url = Url::parse(&listener.redirect_url.to_string()).unwrap();
    let callback_host = callback_url.host_str().unwrap();
    let callback_port = callback_url.port().unwrap();
    let wait_for_code = tokio::spawn(async move {
        listener
            .wait_for_code(&CsrfToken::new(String::from("expected-state")))
            .await
            .unwrap_err()
            .to_string()
    });

    let mut stream = TcpStream::connect((callback_host, callback_port))
        .await
        .unwrap();
    stream
        .write_all(
            b"GET /oauth2/callback?error=access_denied&error_description=nope HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    assert!(response.contains("400 Bad Request"));
    assert!(response.contains("access_denied: nope"));
    assert_eq!(
        wait_for_code.await.unwrap(),
        String::from("oauth callback returned an error: access_denied: nope")
    );
}

#[tokio::test]
async fn wait_for_code_ignores_unrelated_requests_until_callback_arrives() {
    let listener = CallbackListener::bind(&GmailConfig {
        client_id: Some(String::from("client-id")),
        client_secret: None,
        auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
        token_url: String::from("https://oauth2.googleapis.com/token"),
        api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
        listen_host: String::from("127.0.0.1"),
        listen_port: 0,
        open_browser: false,
        request_timeout_secs: 30,
        scopes: vec![String::from("https://www.googleapis.com/auth/gmail.modify")],
    })
    .await
    .unwrap();
    let callback_url = Url::parse(&listener.redirect_url.to_string()).unwrap();
    let callback_host = callback_url.host_str().unwrap();
    let callback_port = callback_url.port().unwrap();
    let wait_for_code = tokio::spawn(async move {
        listener
            .wait_for_code(&CsrfToken::new(String::from("expected-state")))
            .await
            .unwrap()
            .secret()
            .to_owned()
    });

    let mut unrelated_stream = TcpStream::connect((callback_host, callback_port))
        .await
        .unwrap();
    unrelated_stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut unrelated_response = String::new();
    unrelated_stream
        .read_to_string(&mut unrelated_response)
        .await
        .unwrap();

    let mut callback_stream = TcpStream::connect((callback_host, callback_port))
        .await
        .unwrap();
    callback_stream
        .write_all(
            b"GET /oauth2/callback?code=real-code&state=expected-state HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await
        .unwrap();
    let mut callback_response = String::new();
    callback_stream
        .read_to_string(&mut callback_response)
        .await
        .unwrap();

    assert!(unrelated_response.contains("400 Bad Request"));
    assert!(unrelated_response.contains("/oauth2/callback"));
    assert!(callback_response.contains("200 OK"));
    assert_eq!(wait_for_code.await.unwrap(), String::from("real-code"));
}

#[tokio::test]
async fn wait_for_code_reads_callback_requests_across_multiple_tcp_reads() {
    let listener = CallbackListener::bind(&GmailConfig {
        client_id: Some(String::from("client-id")),
        client_secret: None,
        auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
        token_url: String::from("https://oauth2.googleapis.com/token"),
        api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
        listen_host: String::from("127.0.0.1"),
        listen_port: 0,
        open_browser: false,
        request_timeout_secs: 30,
        scopes: vec![String::from("https://www.googleapis.com/auth/gmail.modify")],
    })
    .await
    .unwrap();
    let callback_url = Url::parse(&listener.redirect_url.to_string()).unwrap();
    let callback_host = callback_url.host_str().unwrap();
    let callback_port = callback_url.port().unwrap();
    let wait_for_code = tokio::spawn(async move {
        listener
            .wait_for_code(&CsrfToken::new(String::from("expected-state")))
            .await
            .unwrap()
            .secret()
            .to_owned()
    });

    let mut callback_stream = TcpStream::connect((callback_host, callback_port))
        .await
        .unwrap();
    callback_stream
        .write_all(b"GET /oauth2/callback?code=split")
        .await
        .unwrap();
    callback_stream
        .write_all(b"-code&state=expected-state HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut callback_response = String::new();
    callback_stream
        .read_to_string(&mut callback_response)
        .await
        .unwrap();

    assert!(callback_response.contains("200 OK"));
    assert_eq!(wait_for_code.await.unwrap(), String::from("split-code"));
}
