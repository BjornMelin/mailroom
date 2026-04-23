use super::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use super::oauth_client::{
    ImportedOAuthClient, OAuthClientSource, PreparedSetup, resolve as resolve_oauth_client,
};
use crate::config::{ConfigReport, GmailConfig};
use crate::gmail::GmailClient;
use crate::store;
use crate::store::accounts::{self, AccountRecord, UpsertAccountInput};
use crate::time::current_epoch_seconds;
use crate::workspace::{self, WorkspacePaths};
use anyhow::{Context, Result};
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge, RedirectUrl,
    Scope, TokenResponse, TokenUrl, basic::BasicClient,
};
use reqwest::redirect::Policy;
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Instant, timeout};
use url::Url;

const CALLBACK_TIMEOUT_SECS: u64 = 180;
const CALLBACK_PATH: &str = "/oauth2/callback";

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
            crate::cli_output::print_json_success(self)?;
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
            crate::cli_output::print_json_success(self)?;
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
            crate::cli_output::print_json_success(self)?;
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
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("credential_path={}", self.credential_path.display());
            println!("credential_removed={}", self.credential_removed);
            println!("deactivated_accounts={}", self.deactivated_accounts);
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
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
pub(super) enum AuthorizationPrompt {
    StdoutText(String),
    StderrJson(String),
}

pub async fn setup(
    config_report: &ConfigReport,
    credentials_file: Option<std::path::PathBuf>,
    no_browser: bool,
    json: bool,
) -> Result<SetupReport> {
    let setup_action = super::oauth_client::prepare_setup(
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
            super::oauth_client::persist_prepared_google_desktop_client(prepared_import)?;
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
            super::oauth_client::persist_prepared_google_desktop_client(
                prepared_import.client_import(),
            )?;
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

pub fn status(config_report: &ConfigReport) -> Result<AuthStatusReport> {
    let credential_store = credential_store(config_report);
    let credentials = credential_store.load()?;
    let oauth_client_path = config_report
        .config
        .gmail
        .oauth_client_path(&config_report.config.workspace);
    let source = super::oauth_client::oauth_client_source(
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

async fn authenticate_with_client_override(
    config_report: &ConfigReport,
    resolved_client_override: Option<super::oauth_client::ResolvedOAuthClient>,
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
    let listener = CallbackListener::bind(&config_report.config.gmail).await?;
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
    let opened_browser = open_browser_if_requested(&authorize_url, should_open_browser)?;
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
    resolved_client: super::oauth_client::ResolvedOAuthClient,
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
            refresh_token.expose_secret().to_owned(),
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

fn credential_store(config_report: &ConfigReport) -> FileCredentialStore {
    FileCredentialStore::new(
        config_report
            .config
            .gmail
            .credential_path(&config_report.config.workspace),
    )
}

pub(super) fn configured_workspace_paths(config_report: &ConfigReport) -> Result<WorkspacePaths> {
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

pub(super) fn persist_login_state(
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

pub(super) fn authorization_prompt(
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

#[derive(Debug)]
pub struct CallbackListener {
    listener: TcpListener,
    pub redirect_url: RedirectUrl,
}

impl CallbackListener {
    pub async fn bind(config: &GmailConfig) -> Result<Self> {
        let listener = TcpListener::bind((config.listen_host.as_str(), config.listen_port)).await?;
        let local_addr = listener.local_addr()?;
        let redirect_url = redirect_url_for(local_addr)?;

        Ok(Self {
            listener,
            redirect_url,
        })
    }

    pub async fn wait_for_code(self, expected_state: &CsrfToken) -> Result<AuthorizationCode> {
        let deadline = Instant::now() + Duration::from_secs(CALLBACK_TIMEOUT_SECS);
        loop {
            let remaining = remaining_until(deadline)?;

            let (mut stream, _) = timeout(remaining, self.listener.accept())
                .await
                .map_err(|_| AuthError::CallbackTimedOut)?
                .map_err(AuthError::CallbackIo)?;

            let remaining = remaining_until(deadline)?;
            let request = timeout(remaining, read_callback_request(&mut stream))
                .await
                .map_err(|_| AuthError::CallbackTimedOut)??;
            let callback = match parse_callback_request(&request) {
                Ok(callback) => callback,
                Err(error) if is_malformed_callback_error(&error) => {
                    write_callback_response(
                        &mut stream,
                        "400 Bad Request",
                        "Mailroom is waiting for the Gmail OAuth callback on /oauth2/callback.",
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };

            let response = match callback {
                Ok(code) => {
                    if code.state != *expected_state.secret() {
                        write_callback_response(
                            &mut stream,
                            "400 Bad Request",
                            "OAuth state mismatch. You can close this tab and retry `mailroom auth login`.",
                        )
                        .await?;
                        return Err(AuthError::StateMismatch.into());
                    }

                    write_callback_response(
                        &mut stream,
                        "200 OK",
                        "Mailroom received the Gmail authorization response. You can close this tab.",
                    )
                    .await?;
                    return Ok(AuthorizationCode::new(code.code));
                }
                Err(error) => error,
            };

            write_callback_response(&mut stream, "400 Bad Request", &response).await?;
            return Err(AuthError::OAuthCallback(response).into());
        }
    }
}

fn remaining_until(deadline: Instant) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(AuthError::CallbackTimedOut.into());
    }
    Ok(remaining)
}

async fn read_callback_request(stream: &mut tokio::net::TcpStream) -> Result<String> {
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];

    loop {
        let bytes_read = stream
            .read(&mut chunk)
            .await
            .map_err(AuthError::CallbackIo)?;
        if bytes_read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") || request.len() >= 8 * 1024 {
            break;
        }
    }

    Ok(String::from_utf8_lossy(&request).into_owned())
}

pub fn open_browser_if_requested(url: &Url, enabled: bool) -> Result<bool> {
    if !enabled {
        return Ok(false);
    }

    webbrowser::open(url.as_str())
        .map(|_| true)
        .map_err(|error| AuthError::BrowserOpen(error.to_string()).into())
}

async fn write_callback_response(
    stream: &mut tokio::net::TcpStream,
    status: &str,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(AuthError::CallbackIo)?;
    Ok(())
}

pub(super) fn redirect_url_for(local_addr: SocketAddr) -> Result<RedirectUrl> {
    RedirectUrl::new(format!("http://{local_addr}{CALLBACK_PATH}"))
        .map_err(|_| AuthError::InvalidRedirectUrl.into())
}

fn is_malformed_callback_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<AuthError>()
        .is_some_and(|error| matches!(error, AuthError::MalformedCallbackRequest))
}

pub(super) fn parse_callback_request(request: &str) -> Result<Result<ParsedCallback, String>> {
    let request_line = request
        .lines()
        .next()
        .ok_or(AuthError::MalformedCallbackRequest)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or(AuthError::MalformedCallbackRequest)?;
    let target = parts.next().ok_or(AuthError::MalformedCallbackRequest)?;

    if method != "GET" {
        return Err(AuthError::MalformedCallbackRequest.into());
    }

    let url = Url::parse(&format!("http://localhost{target}"))
        .map_err(|_| AuthError::MalformedCallbackRequest)?;
    if url.path() != CALLBACK_PATH {
        return Err(AuthError::MalformedCallbackRequest.into());
    }

    let mut code = None;
    let mut state = None;
    let mut oauth_error = None;
    let mut oauth_error_description = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.into_owned()),
            "state" => state = Some(value.into_owned()),
            "error" => oauth_error = Some(value.into_owned()),
            "error_description" => oauth_error_description = Some(value.into_owned()),
            _ => {}
        }
    }

    if let Some(error) = oauth_error {
        let description = oauth_error_description
            .unwrap_or_else(|| String::from("Google rejected the authorization request."));
        return Ok(Err(format!("{error}: {description}")));
    }

    let code = code.ok_or(AuthError::MissingAuthorizationCode)?;
    let state = state.ok_or(AuthError::StateMismatch)?;
    Ok(Ok(ParsedCallback { code, state }))
}

#[derive(Debug)]
pub(super) struct ParsedCallback {
    pub(in crate::auth) code: String,
    pub(in crate::auth) state: String,
}
