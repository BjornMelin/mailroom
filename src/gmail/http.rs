use super::client::{GmailClient, GmailClientError, GmailResult};
use super::constants::{
    GMAIL_INITIAL_RETRY_DELAY_MS, GMAIL_MAX_RETRY_ATTEMPTS, TOKEN_REFRESH_LEEWAY_SECS,
};
use super::quota::{GmailQuotaPolicy, GmailRequestCost, GmailRetryClassification};
use super::response::GoogleApiErrorEnvelope;
use super::types::GmailProfile;
use crate::auth::file_store::{CredentialStore, StoredCredentials};
use crate::auth::oauth_client::resolve as resolve_oauth_client;
use crate::config::GmailConfig;
use oauth2::{AuthUrl, ClientId, ClientSecret, RefreshToken, TokenUrl, basic::BasicClient};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use secrecy::ExposeSecret;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

const GMAIL_MAX_RETRY_DELAY_MS: u64 = 32_000;
const GMAIL_RETRY_JITTER_MS: u64 = 250;

impl GmailClient {
    pub(super) async fn get_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::GET, path, query, None, request_cost)
            .await
    }

    pub(super) async fn post_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: serde_json::Value,
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::POST, path, query, Some(body), request_cost)
            .await
    }

    pub(super) async fn put_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
        body: serde_json::Value,
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.execute_json(Method::PUT, path, query, Some(body), request_cost)
            .await
    }

    pub(super) async fn execute_json<T>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<serde_json::Value>,
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let credentials = self.active_credentials().await?;
        match self
            .request_json::<T>(
                method.clone(),
                path,
                query,
                credentials.access_token.expose_secret(),
                body.as_ref(),
                request_cost,
            )
            .await
        {
            Ok(payload) => Ok(payload),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                self.request_json::<T>(
                    method,
                    path,
                    query,
                    refreshed.access_token.expose_secret(),
                    body.as_ref(),
                    request_cost,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn execute_empty(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        request_cost: GmailRequestCost,
    ) -> GmailResult<()> {
        let credentials = self.active_credentials().await?;
        match self
            .request_empty(
                method.clone(),
                path,
                query,
                credentials.access_token.expose_secret(),
                request_cost,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if matches_unauthorized(&error) => {
                let refreshed = self.refresh_credentials(&credentials).await?;
                self.request_empty(
                    method,
                    path,
                    query,
                    refreshed.access_token.expose_secret(),
                    request_cost,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn active_credentials(&self) -> GmailResult<StoredCredentials> {
        let credential_store = self.credential_store.clone();
        let credentials = tokio::task::spawn_blocking(move || credential_store.load())
            .await
            .map_err(|source| GmailClientError::CredentialLoad {
                source: source.into(),
            })?
            .map_err(|source| GmailClientError::CredentialLoad { source })?
            .ok_or(GmailClientError::MissingCredentials)?;

        let now_epoch_s = u64::try_from(
            crate::time::current_epoch_seconds()
                .map_err(|source| GmailClientError::Clock { source })?,
        )
        .map_err(|source| GmailClientError::ResponseDecode {
            path: String::from("credentials.expiry"),
            source: source.into(),
        })?;
        if credentials.should_refresh(TOKEN_REFRESH_LEEWAY_SECS, now_epoch_s) {
            return self.refresh_credentials(&credentials).await;
        }

        Ok(credentials)
    }

    pub(super) async fn refresh_credentials(
        &self,
        credentials: &StoredCredentials,
    ) -> GmailResult<StoredCredentials> {
        let refresh_token = credentials
            .refresh_token
            .as_ref()
            .ok_or(GmailClientError::MissingRefreshToken)?;
        let resolved_client = resolve_oauth_client(&self.config, &self.workspace)
            .map_err(|source| GmailClientError::OAuthClient { source })?;
        let mut oauth_client = BasicClient::new(ClientId::new(resolved_client.client_id))
            .set_auth_uri(
                AuthUrl::new(self.config.auth_url.clone()).map_err(|source| {
                    GmailClientError::OAuthClient {
                        source: source.into(),
                    }
                })?,
            )
            .set_token_uri(
                TokenUrl::new(self.config.token_url.clone()).map_err(|source| {
                    GmailClientError::OAuthClient {
                        source: source.into(),
                    }
                })?,
            );
        if let Some(secret) = resolved_client.client_secret
            && !secret.is_empty()
        {
            oauth_client = oauth_client.set_client_secret(ClientSecret::new(secret));
        }
        let token = oauth_client
            .exchange_refresh_token(&RefreshToken::new(refresh_token.expose_secret().to_owned()))
            .request_async(&self.http)
            .await
            .map_err(|source| GmailClientError::TokenRefresh {
                source: source.into(),
            })?;

        let mut refreshed = StoredCredentials::from_token_response(
            credentials.account_id.clone(),
            &token,
            &credentials.scopes,
        );
        if refreshed.refresh_token.is_none() {
            refreshed.refresh_token = credentials.refresh_token.clone();
        }
        let credential_store = self.credential_store.clone();
        let refreshed_for_save = refreshed.clone();
        tokio::task::spawn_blocking(move || credential_store.save(&refreshed_for_save))
            .await
            .map_err(|source| GmailClientError::CredentialSave {
                source: source.into(),
            })?
            .map_err(|source| GmailClientError::CredentialSave { source })?;
        Ok(refreshed)
    }

    pub(super) async fn request_json<T>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
        body: Option<&serde_json::Value>,
        request_cost: GmailRequestCost,
    ) -> GmailResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self
            .send_request(method, path, query, access_token, body, request_cost)
            .await?;
        response
            .json()
            .await
            .map_err(|source| GmailClientError::ResponseDecode {
                path: path.to_owned(),
                source: source.into(),
            })
    }

    async fn request_empty(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
        request_cost: GmailRequestCost,
    ) -> GmailResult<()> {
        self.send_request(method, path, query, access_token, None, request_cost)
            .await?;
        Ok(())
    }

    async fn send_request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        access_token: &str,
        body: Option<&serde_json::Value>,
        request_cost: GmailRequestCost,
    ) -> GmailResult<reqwest::Response> {
        send_request_with_retry(
            &self.http,
            self.request_policy.as_ref(),
            &self.config.api_base_url,
            method,
            path,
            query,
            access_token,
            body,
            request_cost,
        )
        .await
    }
}

pub(super) async fn fetch_profile_with_access_token(
    config: &GmailConfig,
    access_token: &str,
) -> GmailResult<GmailProfile> {
    let http = build_gmail_http_client(config)?;
    let response = send_request_with_retry(
        &http,
        None,
        &config.api_base_url,
        Method::GET,
        "users/me/profile",
        &[(
            "fields",
            String::from("emailAddress,messagesTotal,threadsTotal,historyId"),
        )],
        access_token,
        None,
        GmailRequestCost::ProfileGet,
    )
    .await?;

    response
        .json()
        .await
        .map_err(|source| GmailClientError::ResponseDecode {
            path: String::from("users/me/profile"),
            source: source.into(),
        })
}

pub(super) fn matches_unauthorized(error: &GmailClientError) -> bool {
    matches!(
        error,
        GmailClientError::Api { status, .. } if *status == StatusCode::UNAUTHORIZED
    )
}

pub(super) fn matches_missing_message_error(error: &GmailClientError, message_id: &str) -> bool {
    let expected_path = format!("users/me/messages/{message_id}");
    matches!(
        error,
        GmailClientError::Api { path, status, .. }
            if *status == StatusCode::NOT_FOUND && path == &expected_path
    )
}

pub(super) fn request_supports_automatic_retry(method: &Method) -> bool {
    *method == Method::GET
}

pub(super) fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

pub(super) fn is_retryable_transport_error(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout()
}

pub(super) fn retry_delay_duration(
    headers: &HeaderMap,
    default_delay_ms: u64,
    attempt: usize,
) -> Duration {
    retry_after_delay(headers).unwrap_or_else(|| jittered_retry_delay(default_delay_ms, attempt))
}

pub(super) fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let value = value.trim();

    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    let retry_at = httpdate::parse_http_date(value).ok()?;
    match retry_at.duration_since(SystemTime::now()) {
        Ok(duration) => Some(duration),
        Err(_) => Some(Duration::ZERO),
    }
}

pub(super) fn next_retry_delay_ms(current_delay_ms: u64) -> u64 {
    current_delay_ms
        .saturating_mul(2)
        .clamp(GMAIL_INITIAL_RETRY_DELAY_MS, GMAIL_MAX_RETRY_DELAY_MS)
}

pub(super) fn classify_retryable_api_response(
    status: StatusCode,
    body: &str,
) -> Option<GmailRetryClassification> {
    if is_retryable_status(status) {
        return Some(if matches_concurrent_request_limit(body) {
            GmailRetryClassification::ConcurrencyPressure
        } else if status == StatusCode::TOO_MANY_REQUESTS {
            GmailRetryClassification::QuotaPressure
        } else {
            GmailRetryClassification::Backend
        });
    }

    (status == StatusCode::FORBIDDEN)
        .then(|| classify_forbidden_retry(body))
        .flatten()
}

pub(super) fn build_gmail_http_client(config: &GmailConfig) -> GmailResult<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(config.request_timeout_secs))
        .user_agent(format!("mailroom/{} (gzip)", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|source| GmailClientError::HttpClientBuild {
            source: source.into(),
        })
}

#[allow(clippy::too_many_arguments)]
async fn send_request_with_retry(
    http: &reqwest::Client,
    request_policy: Option<&GmailQuotaPolicy>,
    api_base_url: &str,
    method: Method,
    path: &str,
    query: &[(&str, String)],
    access_token: &str,
    body: Option<&serde_json::Value>,
    request_cost: GmailRequestCost,
) -> GmailResult<reqwest::Response> {
    let url = format!("{}/{}", api_base_url.trim_end_matches('/'), path);
    let retryable_request = request_supports_automatic_retry(&method);
    let mut retry_delay_ms = GMAIL_INITIAL_RETRY_DELAY_MS;
    let mut attempt = 0usize;

    loop {
        attempt += 1;
        if let Some(policy) = request_policy {
            policy
                .acquire(request_cost)
                .await
                .map_err(|requested_units| GmailClientError::InvalidQuotaBudget {
                    units_per_minute: policy.units_per_minute(),
                    minimum_units_per_minute: requested_units,
                })?;
            policy.record_http_attempt();
        }

        let mut request = http
            .request(method.clone(), &url)
            .bearer_auth(access_token)
            .query(query);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await;

        match response {
            Ok(response) if response.status().is_success() => return Ok(response),
            Ok(response) => {
                let status = response.status();
                let retry_after = retry_after_delay(response.headers());
                let body = response.text().await.unwrap_or_default();
                let retry_classification = retryable_request
                    .then(|| classify_retryable_api_response(status, &body))
                    .flatten();
                if let Some(retry_classification) = retry_classification
                    && attempt < GMAIL_MAX_RETRY_ATTEMPTS
                {
                    let retry_delay = retry_after
                        .unwrap_or_else(|| jittered_retry_delay(retry_delay_ms, attempt));
                    if let Some(policy) = request_policy {
                        policy.record_retry(retry_classification);
                        if retry_after.is_some() {
                            policy.record_retry_after_wait(retry_delay);
                        }
                    }
                    sleep(retry_delay).await;
                    retry_delay_ms = next_retry_delay_ms(retry_delay_ms);
                    continue;
                }

                return Err(GmailClientError::Api {
                    path: path.to_owned(),
                    status,
                    body,
                });
            }
            Err(error) => {
                if retryable_request
                    && is_retryable_transport_error(&error)
                    && attempt < GMAIL_MAX_RETRY_ATTEMPTS
                {
                    if let Some(policy) = request_policy {
                        policy.record_retry(GmailRetryClassification::Backend);
                    }
                    sleep(retry_delay_duration(
                        &HeaderMap::new(),
                        retry_delay_ms,
                        attempt,
                    ))
                    .await;
                    retry_delay_ms = next_retry_delay_ms(retry_delay_ms);
                    continue;
                }

                return Err(GmailClientError::Transport {
                    path: path.to_owned(),
                    source: error,
                });
            }
        }
    }
}

fn classify_forbidden_retry(body: &str) -> Option<GmailRetryClassification> {
    serde_json::from_str::<GoogleApiErrorEnvelope>(body)
        .ok()
        .and_then(|payload| payload.error)
        .and_then(|error| {
            error
                .errors
                .into_iter()
                .find_map(|detail| match detail.reason.as_deref() {
                    Some("rateLimitExceeded" | "userRateLimitExceeded") => {
                        Some(GmailRetryClassification::QuotaPressure)
                    }
                    _ => None,
                })
        })
}

fn matches_concurrent_request_limit(body: &str) -> bool {
    if let Ok(payload) = serde_json::from_str::<GoogleApiErrorEnvelope>(body)
        && let Some(error) = payload.error
    {
        if error
            .message
            .as_deref()
            .is_some_and(is_concurrent_request_limit_message)
        {
            return true;
        }

        return error.errors.into_iter().any(|detail| {
            detail
                .message
                .as_deref()
                .is_some_and(is_concurrent_request_limit_message)
        });
    }

    is_concurrent_request_limit_message(body)
}

fn is_concurrent_request_limit_message(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("too many concurrent requests for user")
}

fn jittered_retry_delay(default_delay_ms: u64, attempt: usize) -> Duration {
    let jitter_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_millis() as u64)
        .unwrap_or((attempt as u64).saturating_mul(17))
        % GMAIL_RETRY_JITTER_MS.max(1);
    Duration::from_millis(default_delay_ms.saturating_add(jitter_ms))
}
