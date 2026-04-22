use super::{GmailClient, GmailClientError};
use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
use crate::config::{GmailConfig, WorkspaceConfig};
use crate::gmail::quota::GmailRetryClassification;
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde_json::json;
use std::time::{Duration, SystemTime};
use tempfile::TempDir;
use tokio::time::sleep;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::super::constants::{MESSAGE_CATALOG_FIELDS, MESSAGE_CATALOG_FULL_FIELDS};
use super::super::http::{
    classify_retryable_api_response, next_retry_delay_ms, request_supports_automatic_retry,
    retry_after_delay,
};
use super::super::response::{
    GmailMessagePayload, extract_email_address, payload_projection_truncated,
};
use super::super::types::GmailProfile;

fn workspace_for(temp_dir: &TempDir) -> WorkspaceConfig {
    let root = temp_dir.path().join(".mailroom");
    WorkspaceConfig {
        runtime_root: root.clone(),
        auth_dir: root.join("auth"),
        cache_dir: root.join("cache"),
        state_dir: root.join("state"),
        vault_dir: root.join("vault"),
        exports_dir: root.join("exports"),
        logs_dir: root.join("logs"),
    }
}

fn test_store(temp_dir: &TempDir) -> FileCredentialStore {
    let store = FileCredentialStore::new(temp_dir.path().join("gmail-credentials.json"));
    store
        .save(&StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("access-token")),
            refresh_token: Some(SecretString::from(String::from("refresh-token"))),
            expires_at_epoch_s: Some(u64::MAX),
            scopes: vec![String::from("scope:a")],
        })
        .unwrap();
    store
}

fn test_client(mock_server: &MockServer, temp_dir: &TempDir) -> GmailClient {
    GmailClient::new(
        GmailConfig {
            client_id: Some(String::from("client-id")),
            client_secret: None,
            auth_url: format!("{}/oauth2/auth", mock_server.uri()),
            token_url: format!("{}/oauth2/token", mock_server.uri()),
            api_base_url: format!("{}/gmail/v1", mock_server.uri()),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: false,
            request_timeout_secs: 30,
            scopes: vec![String::from("scope:a")],
        },
        workspace_for(temp_dir),
        test_store(temp_dir),
    )
    .unwrap()
}

#[tokio::test]
async fn get_profile_uses_stored_credentials() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "emailAddress": "operator@example.com",
            "messagesTotal": 10,
            "threadsTotal": 7,
            "historyId": "12345"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let (profile, access_scope) = client.get_profile_with_access_scope().await.unwrap();
    assert_eq!(
        profile,
        GmailProfile {
            email_address: String::from("operator@example.com"),
            messages_total: 10,
            threads_total: 7,
            history_id: String::from("12345"),
        }
    );
    assert_eq!(access_scope, "scope:a");
}

#[tokio::test]
async fn refresh_credentials_persists_rotated_refresh_tokens() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oauth2/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "fresh-access-token",
            "refresh_token": "rotated-refresh-token",
            "expires_in": 3600,
            "scope": "scope:a",
            "token_type": "Bearer"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "emailAddress": "operator@example.com",
            "messagesTotal": 10,
            "threadsTotal": 7,
            "historyId": "12345"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let store = test_store(&temp_dir);
    store
        .save(&StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("stale-access-token")),
            refresh_token: Some(SecretString::from(String::from("stale-refresh-token"))),
            expires_at_epoch_s: Some(0),
            scopes: vec![String::from("scope:a")],
        })
        .unwrap();

    let client = GmailClient::new(
        GmailConfig {
            client_id: Some(String::from("client-id")),
            client_secret: None,
            auth_url: format!("{}/oauth2/auth", mock_server.uri()),
            token_url: format!("{}/oauth2/token", mock_server.uri()),
            api_base_url: format!("{}/gmail/v1", mock_server.uri()),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: false,
            request_timeout_secs: 30,
            scopes: vec![String::from("scope:a")],
        },
        workspace_for(&temp_dir),
        store.clone(),
    )
    .unwrap();

    let (profile, access_scope) = client.get_profile_with_access_scope().await.unwrap();
    let refreshed = store.load().unwrap().unwrap();

    assert_eq!(profile.email_address, "operator@example.com");
    assert_eq!(access_scope, "scope:a");
    assert_eq!(
        refreshed.refresh_token.as_ref().unwrap().expose_secret(),
        "rotated-refresh-token"
    );
}

#[tokio::test]
async fn refresh_credentials_preserves_existing_refresh_token_when_response_omits_it() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/oauth2/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "fresh-access-token",
            "expires_in": 3600,
            "scope": "scope:a",
            "token_type": "Bearer"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/profile"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "emailAddress": "operator@example.com",
            "messagesTotal": 10,
            "threadsTotal": 7,
            "historyId": "12345"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let store = test_store(&temp_dir);
    store
        .save(&StoredCredentials {
            account_id: String::from("gmail:operator@example.com"),
            access_token: SecretString::from(String::from("stale-access-token")),
            refresh_token: Some(SecretString::from(String::from("stale-refresh-token"))),
            expires_at_epoch_s: Some(0),
            scopes: vec![String::from("scope:a")],
        })
        .unwrap();

    let client = GmailClient::new(
        GmailConfig {
            client_id: Some(String::from("client-id")),
            client_secret: None,
            auth_url: format!("{}/oauth2/auth", mock_server.uri()),
            token_url: format!("{}/oauth2/token", mock_server.uri()),
            api_base_url: format!("{}/gmail/v1", mock_server.uri()),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: false,
            request_timeout_secs: 30,
            scopes: vec![String::from("scope:a")],
        },
        workspace_for(&temp_dir),
        store.clone(),
    )
    .unwrap();

    let (profile, access_scope) = client.get_profile_with_access_scope().await.unwrap();
    let refreshed = store.load().unwrap().unwrap();

    assert_eq!(profile.email_address, "operator@example.com");
    assert_eq!(access_scope, "scope:a");
    assert_eq!(
        refreshed.refresh_token.as_ref().unwrap().expose_secret(),
        "stale-refresh-token"
    );
}

#[tokio::test]
async fn get_profile_with_access_scope_does_not_reload_credentials_after_success() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/profile"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(250))
                .set_body_json(serde_json::json!({
                    "emailAddress": "operator@example.com",
                    "messagesTotal": 10,
                    "threadsTotal": 7,
                    "historyId": "12345"
                })),
        )
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let store = test_store(&temp_dir);
    let client = GmailClient::new(
        GmailConfig {
            client_id: Some(String::from("client-id")),
            client_secret: None,
            auth_url: format!("{}/oauth2/auth", mock_server.uri()),
            token_url: format!("{}/oauth2/token", mock_server.uri()),
            api_base_url: format!("{}/gmail/v1", mock_server.uri()),
            listen_host: String::from("127.0.0.1"),
            listen_port: 0,
            open_browser: false,
            request_timeout_secs: 30,
            scopes: vec![String::from("scope:a")],
        },
        workspace_for(&temp_dir),
        store.clone(),
    )
    .unwrap();

    let profile_task = tokio::spawn(async move { client.get_profile_with_access_scope().await });

    let mut request_observed = false;
    for _ in 0..20 {
        if !mock_server.received_requests().await.unwrap().is_empty() {
            request_observed = true;
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        request_observed,
        "profile request did not reach the mock server"
    );

    assert!(store.clear().unwrap(), "expected credential file to exist");

    let (profile, access_scope) = profile_task.await.unwrap().unwrap();
    assert_eq!(profile.email_address, "operator@example.com");
    assert_eq!(access_scope, "scope:a");
}

#[test]
fn retry_after_delay_reads_delta_seconds_header_values() {
    let mut headers = HeaderMap::new();
    headers.insert(reqwest::header::RETRY_AFTER, "7".parse().unwrap());

    assert_eq!(retry_after_delay(&headers), Some(Duration::from_secs(7)));
}

#[test]
fn retry_after_delay_reads_http_date_header_values() {
    let mut headers = HeaderMap::new();
    let retry_at = SystemTime::now() + Duration::from_secs(2);
    headers.insert(
        reqwest::header::RETRY_AFTER,
        httpdate::fmt_http_date(retry_at).parse().unwrap(),
    );

    let delay = retry_after_delay(&headers).unwrap();
    assert!(delay <= Duration::from_secs(2));
    assert!(delay >= Duration::from_secs(1));
}

#[test]
fn next_retry_delay_ms_doubles_and_caps_backoff() {
    assert_eq!(next_retry_delay_ms(1_000), 2_000);
    assert_eq!(next_retry_delay_ms(16_000), 32_000);
    assert_eq!(next_retry_delay_ms(32_000), 32_000);
}

#[test]
fn automatic_retries_are_limited_to_get_requests() {
    assert!(request_supports_automatic_retry(&Method::GET));
    assert!(!request_supports_automatic_retry(&Method::POST));
    assert!(!request_supports_automatic_retry(&Method::PUT));
}

#[test]
fn gmail_usage_limit_forbidden_errors_are_classified_as_quota_pressure() {
    let body = serde_json::to_string(&json!({
        "error": {
            "errors": [
                {
                    "domain": "usageLimits",
                    "reason": "userRateLimitExceeded",
                    "message": "quota exhausted"
                }
            ]
        }
    }))
    .unwrap();

    assert_eq!(
        classify_retryable_api_response(StatusCode::FORBIDDEN, &body),
        Some(GmailRetryClassification::QuotaPressure)
    );
}

#[test]
fn gmail_daily_limit_forbidden_errors_are_not_classified_as_retryable() {
    let body = serde_json::to_string(&json!({
        "error": {
            "errors": [
                {
                    "domain": "usageLimits",
                    "reason": "dailyLimitExceeded",
                    "message": "daily quota exhausted"
                }
            ]
        }
    }))
    .unwrap();

    assert_eq!(
        classify_retryable_api_response(StatusCode::FORBIDDEN, &body),
        None
    );
}

#[test]
fn gmail_concurrent_request_429_errors_are_classified_as_concurrency_pressure() {
    let body = serde_json::to_string(&json!({
        "error": {
            "message": "Too many concurrent requests for user",
            "errors": [
                {
                    "domain": "usageLimits",
                    "reason": "rateLimitExceeded",
                    "message": "Too many concurrent requests for user"
                }
            ]
        }
    }))
    .unwrap();

    assert_eq!(
        classify_retryable_api_response(StatusCode::TOO_MANY_REQUESTS, &body),
        Some(GmailRetryClassification::ConcurrencyPressure)
    );
}

#[test]
fn gmail_backend_5xx_errors_are_classified_as_backend_pressure() {
    assert_eq!(
        classify_retryable_api_response(StatusCode::BAD_GATEWAY, "upstream failure"),
        Some(GmailRetryClassification::Backend)
    );
}

#[tokio::test]
async fn list_labels_waits_for_retry_after_before_retrying_throttled_requests() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(
            ResponseTemplate::new(429)
                .append_header("Retry-After", "2")
                .set_body_string("slow down"),
        )
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "labels": []
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);
    let retry_task = tokio::spawn(async move { client.list_labels().await });

    sleep(Duration::from_millis(1_200)).await;
    let requests = mock_server
        .received_requests()
        .await
        .expect("request recording should be enabled");
    assert_eq!(
        requests.len(),
        1,
        "client retried before honoring the Retry-After header"
    );

    let result = tokio::time::timeout(Duration::from_millis(2_500), retry_task)
        .await
        .expect("client should retry after Retry-After elapses")
        .expect("retry task should complete successfully");
    assert!(
        result.is_ok(),
        "client should succeed after the delayed retry"
    );
}

#[tokio::test]
async fn list_labels_retries_gmail_user_rate_limit_forbidden_errors() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": {
                "errors": [
                    {
                        "domain": "usageLimits",
                        "reason": "userRateLimitExceeded",
                        "message": "quota exhausted"
                    }
                ]
            }
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "labels": []
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir)
        .with_quota_budget(12_000)
        .unwrap();

    let labels = client.list_labels().await.unwrap();
    assert!(labels.is_empty());

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn get_thread_context_reports_quota_exhaustion_for_oversized_requests() {
    let mock_server = MockServer::start().await;
    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir)
        .with_quota_budget(5)
        .unwrap();

    let error = client.get_thread_context("thread-123").await.unwrap_err();

    assert!(matches!(
        error,
        GmailClientError::QuotaExhausted {
            requested_units: 10,
            available_units_per_minute: 5,
        }
    ));
}

#[tokio::test]
async fn list_labels_does_not_retry_non_quota_forbidden_errors() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/labels"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": {
                "errors": [
                    {
                        "domain": "global",
                        "reason": "insufficientPermissions",
                        "message": "forbidden"
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let error = client.list_labels().await.unwrap_err();
    assert!(matches!(
        error,
        GmailClientError::Api { status, .. } if status == StatusCode::FORBIDDEN
    ));

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn send_draft_does_not_retry_throttled_write_requests() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts/send"))
        .and(body_json(json!({
            "id": "draft-1"
        })))
        .respond_with(
            ResponseTemplate::new(429)
                .append_header("Retry-After", "1")
                .set_body_string("slow down"),
        )
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let error = client.send_draft("draft-1").await.unwrap_err();
    assert!(
        error.to_string().contains("users/me/drafts/send"),
        "unexpected error: {error:#}"
    );

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
}

#[test]
fn extract_email_address_accepts_supported_header_shapes() {
    assert_eq!(
        extract_email_address("\"Alice Example\" <Alice.Example+ops@example.com>"),
        Some(String::from("alice.example+ops@example.com"))
    );
    assert_eq!(
        extract_email_address("\"Alice, Example\" <alice@example.com>"),
        Some(String::from("alice@example.com"))
    );
    assert_eq!(
        extract_email_address("O'Hara <o'hara@example.com>"),
        Some(String::from("o'hara@example.com"))
    );
    assert_eq!(
        extract_email_address("bob@example.com"),
        Some(String::from("bob@example.com"))
    );
    assert_eq!(
        extract_email_address("<alerts@example.com>"),
        Some(String::from("alerts@example.com"))
    );
    assert_eq!(
        extract_email_address("\"o'hara@example.com\""),
        Some(String::from("o'hara@example.com"))
    );
}

#[test]
fn extract_email_address_rejects_ambiguous_or_unsupported_headers() {
    assert_eq!(
        extract_email_address("alice@example.com, bob@example.com"),
        None
    );
    assert_eq!(extract_email_address("Team: alice@example.com;"), None);
    assert_eq!(
        extract_email_address("Display Name <broken@example.com> trailing"),
        None
    );
}

#[tokio::test]
async fn get_thread_context_parses_and_sorts_thread_messages() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/threads/thread-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "thread-1",
            "historyId": "500",
            "messages": [
                {
                    "id": "m-2",
                    "threadId": "thread-1",
                    "historyId": "500",
                    "internalDate": "200",
                    "snippet": "Later message",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Re: Project"},
                            {"name": "From", "value": "Operator <operator@example.com>"},
                            {"name": "To", "value": "alice@example.com"},
                            {"name": "Message-ID", "value": "<m-2@example.com>"},
                            {"name": "References", "value": "<m-1@example.com>"}
                        ]
                    }
                },
                {
                    "id": "m-1",
                    "threadId": "thread-1",
                    "historyId": "400",
                    "internalDate": "100",
                    "snippet": "Earlier message",
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Project"},
                            {"name": "From", "value": "\"Alice Example\" <alice@example.com>"},
                            {"name": "To", "value": "operator@example.com"},
                            {"name": "Reply-To", "value": "replies@example.com"},
                            {"name": "Message-ID", "value": "<m-1@example.com>"}
                        ]
                    }
                }
            ]
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let thread = client.get_thread_context("thread-1").await.unwrap();

    assert_eq!(thread.id, "thread-1");
    assert_eq!(thread.history_id, "500");
    assert_eq!(thread.messages.len(), 2);
    assert_eq!(thread.messages[0].id, "m-1");
    assert_eq!(thread.messages[1].id, "m-2");
    assert_eq!(
        thread.messages[0].from_address.as_deref(),
        Some("alice@example.com")
    );
    assert_eq!(thread.messages[0].reply_to_header, "replies@example.com");
    assert_eq!(thread.messages[1].references_header, "<m-1@example.com>");
}

#[tokio::test]
async fn get_message_catalog_collects_nested_attachments() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-1"))
        .and(query_param("format", "full"))
        .and(query_param("fields", MESSAGE_CATALOG_FIELDS))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "m-1",
            "threadId": "thread-1",
            "labelIds": ["INBOX"],
            "snippet": "Quarterly statement attached",
            "historyId": "500",
            "internalDate": "1700000000000",
            "sizeEstimate": 123,
            "payload": {
                "headers": [
                    {"name": "Subject", "value": "Statement"},
                    {"name": "From", "value": "Billing <billing@example.com>"},
                    {"name": "To", "value": "operator@example.com"}
                ],
                "parts": [
                    {
                        "partId": "1.1",
                        "mimeType": "application/pdf",
                        "filename": "statement.pdf",
                        "headers": [
                            {"name": "Content-Disposition", "value": "attachment; filename=\"statement.pdf\""}
                        ],
                        "body": {
                            "attachmentId": "att-1",
                            "size": 42
                        }
                    },
                    {
                        "partId": "1.2",
                        "mimeType": "image/png",
                        "filename": "inline.png",
                        "headers": [
                            {"name": "Content-Id", "value": "<image-1>"}
                        ],
                        "body": {
                            "size": 5
                        }
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let catalog = client.get_message_catalog("m-1").await.unwrap();

    assert_eq!(catalog.metadata.subject, "Statement");
    assert_eq!(catalog.attachments.len(), 2);
    assert_eq!(catalog.attachments[0].attachment_key, "m-1:1.1");
    assert_eq!(
        catalog.attachments[0].gmail_attachment_id.as_deref(),
        Some("att-1")
    );
    assert!(catalog.attachments[1].is_inline);
}

#[tokio::test]
async fn get_message_catalog_collects_cid_only_inline_parts() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-inline-cid"))
        .and(query_param("format", "full"))
        .and(query_param("fields", MESSAGE_CATALOG_FIELDS))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "m-inline-cid",
            "threadId": "thread-inline",
            "labelIds": ["INBOX"],
            "snippet": "Inline image",
            "historyId": "501",
            "internalDate": "1700000000000",
            "sizeEstimate": 5,
            "payload": {
                "headers": [
                    {"name": "Subject", "value": "Inline image"},
                    {"name": "From", "value": "Billing <billing@example.com>"},
                    {"name": "To", "value": "operator@example.com"}
                ],
                "parts": [
                    {
                        "partId": "1.2",
                        "mimeType": "image/png",
                        "filename": "",
                        "headers": [
                            {"name": "Content-Id", "value": "<image-1>"}
                        ],
                        "body": {
                            "data": "aGVsbG8",
                            "size": 5
                        }
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let catalog = client.get_message_catalog("m-inline-cid").await.unwrap();

    assert_eq!(catalog.attachments.len(), 1);
    assert_eq!(catalog.attachments[0].attachment_key, "m-inline-cid:1.2");
    assert!(catalog.attachments[0].is_inline);
    assert_eq!(
        catalog.attachments[0].content_id.as_deref(),
        Some("<image-1>")
    );
}

#[tokio::test]
async fn get_message_catalog_refetches_full_payload_when_projection_hits_depth_marker() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-deep"))
        .and(query_param("format", "full"))
        .and(query_param("fields", MESSAGE_CATALOG_FIELDS))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "m-deep",
            "threadId": "thread-deep",
            "labelIds": ["INBOX"],
            "snippet": "deep part marker",
            "historyId": "700",
            "internalDate": "1700000000123",
            "sizeEstimate": 222,
            "payload": {
                "headers": [
                    {"name": "Subject", "value": "Deep"},
                    {"name": "From", "value": "Nested <nested@example.com>"},
                    {"name": "To", "value": "operator@example.com"}
                ],
                "parts": [{"partId": "1"}]
            }
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-deep"))
        .and(query_param("format", "full"))
        .and(query_param("fields", MESSAGE_CATALOG_FULL_FIELDS))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "m-deep",
            "threadId": "thread-deep",
            "labelIds": ["INBOX"],
            "snippet": "deep attachment",
            "historyId": "700",
            "internalDate": "1700000000123",
            "sizeEstimate": 333,
            "payload": {
                "headers": [
                    {"name": "Subject", "value": "Deep"},
                    {"name": "From", "value": "Nested <nested@example.com>"},
                    {"name": "To", "value": "operator@example.com"}
                ],
                "parts": [{
                    "partId": "1.1.1.1.1.1.1",
                    "mimeType": "application/pdf",
                    "filename": "deep.pdf",
                    "headers": [
                        {"name": "Content-Disposition", "value": "attachment; filename=\"deep.pdf\""}
                    ],
                    "body": {
                        "attachmentId": "att-deep",
                        "size": 99
                    }
                }]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let catalog = client.get_message_catalog("m-deep").await.unwrap();

    assert_eq!(catalog.attachments.len(), 1);
    assert_eq!(
        catalog.attachments[0].attachment_key,
        "m-deep:1.1.1.1.1.1.1"
    );
    assert_eq!(
        catalog.attachments[0].gmail_attachment_id.as_deref(),
        Some("att-deep")
    );
}

#[test]
fn message_catalog_fields_selector_is_parenthesis_balanced() {
    let mut depth = 0_i32;
    for character in MESSAGE_CATALOG_FIELDS.chars() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                assert!(
                    depth >= 0,
                    "MESSAGE_CATALOG_FIELDS has an unmatched closing parenthesis"
                );
            }
            _ => {}
        }
    }

    assert_eq!(
        depth, 0,
        "MESSAGE_CATALOG_FIELDS must have balanced parentheses"
    );
}

#[test]
fn payload_projection_truncated_detects_nested_depth_markers() {
    let payload: GmailMessagePayload = serde_json::from_value(json!({
        "partId": "0",
        "mimeType": "multipart/mixed",
        "parts": [
            {
                "partId": "1",
                "mimeType": "multipart/alternative",
                "parts": [
                    { "partId": "1.1" }
                ]
            }
        ]
    }))
    .unwrap();

    assert!(payload_projection_truncated(&payload));
}

#[tokio::test]
async fn get_attachment_bytes_supports_remote_attachment_and_inline_part_bodies() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/gmail/v1/users/me/messages/m-remote/attachments/att-1",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": "cGRmLWJ5dGVz"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/gmail/v1/users/me/messages/m-remote-padded/attachments/att-2",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": "cGRmLWJ5dGVzPQ=="
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-inline"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "m-inline",
            "threadId": "thread-inline",
            "labelIds": [],
            "historyId": "501",
            "internalDate": "1700000000000",
            "sizeEstimate": 5,
            "payload": {
                "parts": [
                    {
                        "partId": "1.2",
                        "body": {
                            "data": "aGVsbG8"
                        }
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let remote = client
        .get_attachment_bytes("m-remote", "1.1", Some("att-1"))
        .await
        .unwrap();
    let remote_padded = client
        .get_attachment_bytes("m-remote-padded", "1.1", Some("att-2"))
        .await
        .unwrap();
    let inline = client
        .get_attachment_bytes("m-inline", "1.2", None)
        .await
        .unwrap();

    assert_eq!(remote, b"pdf-bytes");
    assert_eq!(remote_padded, b"pdf-bytes=");
    assert_eq!(inline, b"hello");
}

#[tokio::test]
async fn get_attachment_bytes_returns_attachment_part_missing_for_unknown_inline_part() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-inline-missing"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "payload": {
                "parts": [
                    {
                        "partId": "1.1",
                        "body": {
                            "data": "aGVsbG8"
                        }
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let error = client
        .get_attachment_bytes("m-inline-missing", "9.9", None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        GmailClientError::AttachmentPartMissing { message_id, part_id }
        if message_id == "m-inline-missing" && part_id == "9.9"
    ));
}

#[tokio::test]
async fn get_attachment_bytes_returns_attachment_body_missing_for_empty_inline_part() {
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gmail/v1/users/me/messages/m-inline-empty-body"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "payload": {
                "parts": [
                    {
                        "partId": "1.2",
                        "body": {}
                    }
                ]
            }
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let error = client
        .get_attachment_bytes("m-inline-empty-body", "1.2", None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        GmailClientError::AttachmentBodyMissing { message_id, part_id }
        if message_id == "m-inline-empty-body" && part_id == "1.2"
    ));
}

#[tokio::test]
async fn draft_and_thread_write_operations_return_expected_refs() {
    let mock_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts"))
        .and(body_json(json!({
            "message": {
                "raw": "raw-1",
                "threadId": "thread-1"
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "draft-1",
            "message": {
                "id": "draft-message-1",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("PUT"))
        .and(path("/gmail/v1/users/me/drafts/draft-1"))
        .and(body_json(json!({
            "message": {
                "raw": "raw-2",
                "threadId": "thread-1"
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "draft-1",
            "message": {
                "id": "draft-message-2",
                "threadId": "thread-1"
            }
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/drafts/send"))
        .and(body_json(json!({
            "id": "draft-1"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "sent-message-1",
            "threadId": "thread-1",
            "historyId": "800"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
        .and(body_json(json!({
            "addLabelIds": ["Label_A"],
            "removeLabelIds": ["Label_B"]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "thread-1",
            "historyId": "801"
        })))
        .mount(&mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/gmail/v1/users/me/threads/thread-1/trash"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "thread-1",
            "historyId": "802"
        })))
        .mount(&mock_server)
        .await;

    let temp_dir = TempDir::new().unwrap();
    let client = test_client(&mock_server, &temp_dir);

    let created = client
        .create_draft("raw-1", Some("thread-1"))
        .await
        .unwrap();
    let updated = client
        .update_draft("draft-1", "raw-2", Some("thread-1"))
        .await
        .unwrap();
    let sent = client.send_draft("draft-1").await.unwrap();
    let modified = client
        .modify_thread_labels(
            "thread-1",
            &[String::from("Label_A")],
            &[String::from("Label_B")],
        )
        .await
        .unwrap();
    let trashed = client.trash_thread("thread-1").await.unwrap();

    assert_eq!(created.id, "draft-1");
    assert_eq!(created.message_id, "draft-message-1");
    assert_eq!(updated.message_id, "draft-message-2");
    assert_eq!(sent.message_id, "sent-message-1");
    assert_eq!(sent.history_id.as_deref(), Some("800"));
    assert_eq!(modified.thread_id, "thread-1");
    assert_eq!(modified.history_id.as_deref(), Some("801"));
    assert_eq!(trashed.history_id.as_deref(), Some("802"));
}
