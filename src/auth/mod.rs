pub mod file_store;
mod flow;
pub mod oauth_client;

use crate::config::{ConfigReport, GmailConfig};
use crate::gmail::GmailClient;
use crate::store;
use crate::store::accounts::{self, AccountRecord, UpsertAccountInput};
use crate::time::current_epoch_seconds;
use crate::workspace::{self, WorkspacePaths};
use anyhow::{Context, Result};
use file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use oauth_client::{
    ImportedOAuthClient, OAuthClientSource, PreparedSetup, resolve as resolve_oauth_client,
};
use oauth2::{
    AuthUrl, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge, Scope, TokenResponse, TokenUrl,
    basic::BasicClient,
};
use reqwest::redirect::Policy;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use std::io::Write;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone, Serialize)]
pub struct AuthStatusReport {
    pub configured: bool,
    pub oauth_client_source: String,
    pub oauth_client_path: std::path::PathBuf,
    pub oauth_client_exists: bool,
    pub credential_path: std::path::PathBuf,
    pub credential_exists: bool,
    pub access_token_expires_at_epoch_s: Option<u64>,
    pub scopes: Vec<String>,
    pub active_account: Option<AccountRecord>,
}

impl AuthStatusReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!("configured={}", self.configured);
            println!("oauth_client_source={}", self.oauth_client_source);
            println!("oauth_client_path={}", self.oauth_client_path.display());
            println!("oauth_client_exists={}", self.oauth_client_exists);
            println!("credential_path={}", self.credential_path.display());
            println!("credential_exists={}", self.credential_exists);
            match self.access_token_expires_at_epoch_s {
                Some(expires_at) => println!("access_token_expires_at_epoch_s={expires_at}"),
                None => println!("access_token_expires_at_epoch_s=<unknown>"),
            }
            println!("scopes={}", self.scopes.join(","));
            match &self.active_account {
                Some(account) => {
                    println!("active_account_id={}", account.account_id);
                    println!("active_account_email={}", account.email_address);
                    println!("active_account_history_id={}", account.history_id);
                }
                None => println!("active_account=<none>"),
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SetupReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imported_client: Option<ImportedOAuthClient>,
    pub login: LoginReport,
}

impl SetupReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!("oauth_client_imported={}", self.imported_client.is_some());
            if let Some(imported_client) = &self.imported_client {
                println!(
                    "oauth_client_source_kind={}",
                    imported_client.source_kind.as_str()
                );
                match &imported_client.source_path {
                    Some(source_path) => {
                        println!("oauth_client_source_path={}", source_path.display())
                    }
                    None => println!("oauth_client_source_path=<none>"),
                }
                println!(
                    "oauth_client_path={}",
                    imported_client.oauth_client_path.display()
                );
                println!(
                    "oauth_client_auto_discovered={}",
                    imported_client.auto_discovered
                );
                println!("oauth_client_id={}", imported_client.client_id);
                println!(
                    "oauth_client_secret_present={}",
                    imported_client.client_secret_present
                );
            }
            self.login.print(false)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginReport {
    pub opened_browser: bool,
    pub credential_path: std::path::PathBuf,
    pub access_token_expires_at_epoch_s: Option<u64>,
    pub scopes: Vec<String>,
    pub account: AccountRecord,
}

impl LoginReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!("opened_browser={}", self.opened_browser);
            println!("credential_path={}", self.credential_path.display());
            match self.access_token_expires_at_epoch_s {
                Some(expires_at) => println!("access_token_expires_at_epoch_s={expires_at}"),
                None => println!("access_token_expires_at_epoch_s=<unknown>"),
            }
            println!("scopes={}", self.scopes.join(","));
            println!("account_id={}", self.account.account_id);
            println!("email_address={}", self.account.email_address);
            println!("history_id={}", self.account.history_id);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LogoutReport {
    pub credential_path: std::path::PathBuf,
    pub credential_removed: bool,
    pub deactivated_accounts: usize,
}

impl LogoutReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!("credential_path={}", self.credential_path.display());
            println!("credential_removed={}", self.credential_removed);
            println!("deactivated_accounts={}", self.deactivated_accounts);
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum AuthError {
    #[error("oauth callback returned a malformed request")]
    MalformedCallbackRequest,
    #[error("oauth callback did not include an authorization code")]
    MissingAuthorizationCode,
    #[error("oauth callback returned an error: {0}")]
    OAuthCallback(String),
    #[error("oauth callback state did not match the original request")]
    StateMismatch,
    #[error("failed to bind or parse the loopback redirect URL")]
    InvalidRedirectUrl,
    #[error("opening the browser failed: {0}")]
    BrowserOpen(String),
    #[error("timed out waiting for the Gmail OAuth callback")]
    CallbackTimedOut,
    #[error("loopback callback I/O failed")]
    CallbackIo(#[source] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthorizationPrompt {
    StdoutText(String),
    StderrJson(String),
}

pub async fn setup(
    config_report: &ConfigReport,
    credentials_file: Option<std::path::PathBuf>,
    no_browser: bool,
    json: bool,
) -> Result<SetupReport> {
    let setup_action = oauth_client::prepare_setup(
        &config_report.config.gmail,
        &config_report.config.workspace,
        credentials_file,
        json,
    )?;
    let (imported_client, completed_login) = match &setup_action {
        PreparedSetup::UseExisting => (
            None,
            authenticate_with_client_override(config_report, None, no_browser, json).await?,
        ),
        PreparedSetup::ImportClient(prepared_import) => {
            let completed_login = authenticate_with_client_override(
                config_report,
                Some(prepared_import.resolved_client().clone()),
                no_browser,
                json,
            )
            .await?;
            oauth_client::persist_prepared_google_desktop_client(prepared_import)?;
            (
                Some(prepared_import.imported_client().clone()),
                completed_login,
            )
        }
        PreparedSetup::ImportAdc(prepared_import) => {
            let completed_login = authenticate_with_refresh_token_override(
                config_report,
                prepared_import.resolved_client().clone(),
                prepared_import.refresh_token().clone(),
            )
            .await?;
            oauth_client::persist_prepared_google_desktop_client(prepared_import.client_import())?;
            (
                Some(prepared_import.imported_client().clone()),
                completed_login,
            )
        }
    };
    let login = finalize_login(config_report, completed_login)?;

    Ok(SetupReport {
        imported_client,
        login,
    })
}

pub async fn login(
    config_report: &ConfigReport,
    no_browser: bool,
    json: bool,
) -> Result<LoginReport> {
    let completed_login =
        authenticate_with_client_override(config_report, None, no_browser, json).await?;
    finalize_login(config_report, completed_login)
}

async fn authenticate_with_client_override(
    config_report: &ConfigReport,
    resolved_client_override: Option<oauth_client::ResolvedOAuthClient>,
    no_browser: bool,
    json: bool,
) -> Result<CompletedOAuthLogin> {
    let resolved_client = match resolved_client_override {
        Some(resolved_client) => resolved_client,
        None => resolve_oauth_client(&config_report.config.gmail, &config_report.config.workspace)?,
    };
    let mut oauth_client = BasicClient::new(ClientId::new(resolved_client.client_id))
        .set_auth_uri(AuthUrl::new(config_report.config.gmail.auth_url.clone())?)
        .set_token_uri(TokenUrl::new(config_report.config.gmail.token_url.clone())?);
    if let Some(secret) = resolved_client.client_secret
        && !secret.is_empty()
    {
        oauth_client = oauth_client.set_client_secret(ClientSecret::new(secret));
    }
    let listener = flow::CallbackListener::bind(&config_report.config.gmail).await?;
    oauth_client = oauth_client.set_redirect_uri(listener.redirect_url.clone());
    let http_client = oauth_http_client(&config_report.config.gmail)?;
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let should_open_browser = config_report.config.gmail.open_browser && !no_browser;
    let mut authorization_request = oauth_client
        .authorize_url(CsrfToken::new_random)
        .set_pkce_challenge(pkce_challenge)
        .add_extra_param("access_type", "offline")
        .add_extra_param("prompt", "consent");
    for scope in &config_report.config.gmail.scopes {
        authorization_request = authorization_request.add_scope(Scope::new(scope.clone()));
    }
    let (authorize_url, csrf_state) = authorization_request.url();

    emit_authorization_prompt(authorization_prompt(
        &authorize_url,
        json,
        should_open_browser,
    ))?;
    let opened_browser = flow::open_browser_if_requested(&authorize_url, should_open_browser)?;
    let code = listener.wait_for_code(&csrf_state).await?;
    let token = oauth_client
        .exchange_code(code)
        .set_pkce_verifier(pkce_verifier)
        .request_async(&http_client)
        .await
        .context("failed to exchange Gmail OAuth authorization code")?;
    completed_login_from_token_response(config_report, &token, None, opened_browser).await
}

async fn authenticate_with_refresh_token_override(
    config_report: &ConfigReport,
    resolved_client: oauth_client::ResolvedOAuthClient,
    refresh_token: SecretString,
) -> Result<CompletedOAuthLogin> {
    let mut oauth_client = BasicClient::new(ClientId::new(resolved_client.client_id))
        .set_auth_uri(AuthUrl::new(config_report.config.gmail.auth_url.clone())?)
        .set_token_uri(TokenUrl::new(config_report.config.gmail.token_url.clone())?);
    if let Some(secret) = resolved_client.client_secret
        && !secret.is_empty()
    {
        oauth_client = oauth_client.set_client_secret(ClientSecret::new(secret));
    }
    let http_client = oauth_http_client(&config_report.config.gmail)?;
    let token = oauth_client
        .exchange_refresh_token(&oauth2::RefreshToken::new(
            refresh_token.clone().expose_secret().to_owned(),
        ))
        .request_async(&http_client)
        .await
        .context("failed to exchange gcloud ADC refresh token for a Gmail access token")?;
    completed_login_from_token_response(config_report, &token, Some(refresh_token), false).await
}

async fn completed_login_from_token_response<T>(
    config_report: &ConfigReport,
    token: &T,
    refresh_token_fallback: Option<SecretString>,
    opened_browser: bool,
) -> Result<CompletedOAuthLogin>
where
    T: TokenResponse,
{
    let profile = GmailClient::fetch_profile_with_access_token(
        &config_report.config.gmail,
        token.access_token().secret(),
    )
    .await?;
    let now_epoch_s = current_epoch_seconds()?;
    let mut account_input = UpsertAccountInput {
        email_address: profile.email_address,
        history_id: profile.history_id,
        messages_total: profile.messages_total,
        threads_total: profile.threads_total,
        access_scope: String::new(),
        refreshed_at_epoch_s: now_epoch_s,
    };
    let mut credentials = StoredCredentials::from_token_response(
        account_input.gmail_account_id(),
        token,
        &config_report.config.gmail.scopes,
    );
    if credentials.refresh_token.is_none() {
        credentials.refresh_token = refresh_token_fallback;
    }
    account_input.access_scope = credentials.scopes.join(" ");
    Ok(CompletedOAuthLogin {
        opened_browser,
        credentials,
        account_input,
    })
}

fn finalize_login(
    config_report: &ConfigReport,
    completed_login: CompletedOAuthLogin,
) -> Result<LoginReport> {
    let workspace_paths = configured_workspace_paths(config_report)?;
    let credential_store = credential_store(config_report);
    let account = persist_login_state(
        config_report,
        &workspace_paths,
        &credential_store,
        &completed_login.credentials,
        &completed_login.account_input,
    )?;

    Ok(LoginReport {
        opened_browser: completed_login.opened_browser,
        credential_path: credential_store.path().to_path_buf(),
        access_token_expires_at_epoch_s: completed_login.credentials.expires_at_epoch_s,
        scopes: completed_login.credentials.scopes,
        account,
    })
}

struct CompletedOAuthLogin {
    opened_browser: bool,
    credentials: StoredCredentials,
    account_input: UpsertAccountInput,
}

pub fn status(config_report: &ConfigReport) -> Result<AuthStatusReport> {
    let credential_store = credential_store(config_report);
    let credentials = credential_store.load()?;
    let oauth_client_path = config_report
        .config
        .gmail
        .oauth_client_path(&config_report.config.workspace);
    let source = oauth_client::oauth_client_source(
        &config_report.config.gmail,
        &config_report.config.workspace,
    )?;
    let oauth_client_exists = matches!(source, OAuthClientSource::WorkspaceFile);
    let configured = !matches!(source, OAuthClientSource::Unconfigured);
    let active_account = if config_report.config.store.database_path.exists() {
        accounts::get_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
        )?
    } else {
        None
    };

    Ok(AuthStatusReport {
        configured,
        oauth_client_source: source.as_str().to_owned(),
        oauth_client_path,
        oauth_client_exists,
        credential_path: credential_store.path().to_path_buf(),
        credential_exists: credentials.is_some(),
        access_token_expires_at_epoch_s: credentials
            .as_ref()
            .and_then(|credentials| credentials.expires_at_epoch_s),
        scopes: credentials
            .map(|credentials| credentials.scopes)
            .unwrap_or_else(|| config_report.config.gmail.scopes.clone()),
        active_account,
    })
}

pub fn logout(config_report: &ConfigReport) -> Result<LogoutReport> {
    let credential_store = credential_store(config_report);
    let credential_removed = credential_store.clear()?;
    let deactivated_accounts = if config_report.config.store.database_path.exists() {
        accounts::deactivate_all(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            current_epoch_seconds()?,
        )?
    } else {
        0
    };

    Ok(LogoutReport {
        credential_path: credential_store.path().to_path_buf(),
        credential_removed,
        deactivated_accounts,
    })
}

fn credential_store(config_report: &ConfigReport) -> FileCredentialStore {
    FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    )
}

fn configured_workspace_paths(config_report: &ConfigReport) -> Result<WorkspacePaths> {
    let repo_root =
        workspace::configured_repo_root_from_locations(&config_report.locations.repo_config_path)?;
    Ok(WorkspacePaths::from_config(
        repo_root,
        &config_report.config.workspace,
    ))
}

fn oauth_http_client(config: &GmailConfig) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .build()
        .context("failed to build OAuth reqwest client")
}

fn persist_login_state(
    config_report: &ConfigReport,
    workspace_paths: &WorkspacePaths,
    credential_store: &FileCredentialStore,
    credentials: &StoredCredentials,
    account_input: &UpsertAccountInput,
) -> Result<AccountRecord> {
    workspace_paths.ensure_runtime_dirs()?;
    let previous_credentials = credential_store.load()?;
    credential_store.save(credentials)?;
    match persist_active_account(config_report, account_input) {
        Ok(account) => Ok(account),
        Err(error) => {
            rollback_credentials(credential_store, previous_credentials).with_context(|| {
                format!(
                    "failed to roll back credential state after login persistence error: {error}"
                )
            })?;
            Err(error)
        }
    }
}

fn persist_active_account(
    config_report: &ConfigReport,
    account_input: &UpsertAccountInput,
) -> Result<AccountRecord> {
    store::init(config_report)?;
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_input,
    )
}

fn rollback_credentials(
    credential_store: &FileCredentialStore,
    previous_credentials: Option<StoredCredentials>,
) -> Result<()> {
    match previous_credentials {
        Some(previous_credentials) => credential_store.save(&previous_credentials),
        None => {
            credential_store.clear()?;
            Ok(())
        }
    }
}

fn authorization_prompt(
    authorize_url: &url::Url,
    json: bool,
    should_open_browser: bool,
) -> Option<AuthorizationPrompt> {
    match (json, should_open_browser) {
        (false, _) => Some(AuthorizationPrompt::StdoutText(format!(
            "Complete Gmail authorization by visiting:\n{authorize_url}\n"
        ))),
        (true, false) => Some(AuthorizationPrompt::StderrJson(
            serde_json::json!({ "authorization_url": authorize_url }).to_string(),
        )),
        (true, true) => None,
    }
}

fn emit_authorization_prompt(prompt: Option<AuthorizationPrompt>) -> Result<()> {
    match prompt {
        Some(AuthorizationPrompt::StdoutText(message)) => {
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{message}")?;
            stdout.flush()?;
        }
        Some(AuthorizationPrompt::StderrJson(message)) => {
            let mut stderr = std::io::stderr().lock();
            writeln!(stderr, "{message}")?;
            stderr.flush()?;
        }
        None => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationPrompt, authorization_prompt, configured_workspace_paths, login, logout,
        persist_login_state, setup, status,
    };
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::auth::oauth_client::{self, PreparedSetup, setup_guidance};
    use crate::config::resolve;
    use crate::store::accounts::UpsertAccountInput;
    use crate::workspace::WorkspacePaths;
    use rusqlite::Connection;
    use secrecy::SecretString;
    use std::fs;
    use tempfile::TempDir;
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

    #[test]
    fn persist_login_state_does_not_upsert_account_when_credential_save_fails() {
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
        .unwrap_err();

        assert!(!error.to_string().is_empty());
        assert!(!config_report.config.store.database_path.exists());
    }

    #[test]
    fn persist_login_state_rolls_back_new_credentials_when_store_init_fails() {
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
        .unwrap_err();

        assert!(!error.to_string().is_empty());
        assert!(credential_store.load().unwrap().is_none());
        assert!(config_report.config.store.database_path.is_dir());
    }

    #[test]
    fn persist_login_state_restores_previous_credentials_when_store_init_fails() {
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
        .unwrap_err();

        let restored = credential_store.load().unwrap().unwrap();

        assert!(!error.to_string().is_empty());
        assert_eq!(restored.account_id, "gmail:previous@example.com");
        assert_eq!(restored.expires_at_epoch_s, Some(321));
        assert_eq!(restored.scopes, vec![String::from("scope:previous")]);
    }
}
