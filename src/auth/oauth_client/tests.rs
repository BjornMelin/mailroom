use super::import::{
    import_google_desktop_client, prepare_google_desktop_client_from_adc,
    prepare_google_desktop_client_from_values, prepare_noninteractive_setup,
};
use super::interactive::should_use_interactive_setup;
use super::resolve::{
    OAuthClientError, OAuthClientSource, oauth_client_exists, oauth_client_source, resolve,
};
use super::storage::{default_download_dirs, detect_adc_path_from_env, normalize_candidate_paths};
use super::{
    GOOGLE_AUTH_CLIENTS_URL, GOOGLE_AUTH_OVERVIEW_URL, ImportedOAuthClient,
    ImportedOAuthClientSourceKind, PreparedSetup, setup_guidance,
};
use crate::config::{GmailConfig, WorkspaceConfig};
use secrecy::ExposeSecret;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

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

fn gmail_config() -> GmailConfig {
    GmailConfig {
        client_id: None,
        client_secret: None,
        auth_url: String::from("https://accounts.google.com/o/oauth2/v2/auth"),
        token_url: String::from("https://oauth2.googleapis.com/token"),
        api_base_url: String::from("https://gmail.googleapis.com/gmail/v1"),
        listen_host: String::from("127.0.0.1"),
        listen_port: 0,
        open_browser: true,
        request_timeout_secs: 30,
        scopes: vec![String::from("scope:a")],
    }
}

#[test]
fn imported_client_becomes_the_resolved_oauth_source() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    let config = gmail_config();
    let credentials_path = temp_dir.path().join("client_secret_test.json");
    fs::write(
        &credentials_path,
        r#"{
  "installed": {
    "client_id": "desktop-client.apps.googleusercontent.com",
    "client_secret": "desktop-secret",
    "project_id": "mailroom-dev",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "redirect_uris": ["http://localhost"]
  }
}"#,
    )
    .unwrap();

    let imported =
        import_google_desktop_client(&config, &workspace, Some(credentials_path)).unwrap();
    let resolved = resolve(&config, &workspace).unwrap();

    assert_eq!(
        imported,
        ImportedOAuthClient {
            source_kind: ImportedOAuthClientSourceKind::DownloadedJson,
            source_path: Some(temp_dir.path().join("client_secret_test.json")),
            oauth_client_path: workspace.auth_dir.join("gmail-oauth-client.json"),
            auto_discovered: false,
            client_id: String::from("desktop-client.apps.googleusercontent.com"),
            client_secret_present: true,
            project_id: Some(String::from("mailroom-dev")),
        }
    );
    assert_eq!(
        resolved.client_id,
        "desktop-client.apps.googleusercontent.com"
    );
    assert_eq!(resolved.client_secret.as_deref(), Some("desktop-secret"));
    assert!(oauth_client_exists(&config, &workspace).unwrap());

    let saved = fs::read_to_string(workspace.auth_dir.join("gmail-oauth-client.json")).unwrap();
    assert!(saved.contains("\"installed\""));
    assert!(saved.contains("\"project_id\": \"mailroom-dev\""));
}

#[test]
fn manual_paste_preparation_builds_standard_google_client_file() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    let config = gmail_config();

    let prepared = prepare_google_desktop_client_from_values(
        &config,
        &workspace,
        String::from("manual-client.apps.googleusercontent.com"),
        Some(String::from("manual-secret")),
    )
    .unwrap();

    assert_eq!(
        prepared.imported_client().source_kind,
        ImportedOAuthClientSourceKind::ManualPaste
    );
    assert_eq!(
        prepared.resolved_client().client_id,
        "manual-client.apps.googleusercontent.com"
    );
    assert_eq!(
        prepared.resolved_client().client_secret.as_deref(),
        Some("manual-secret")
    );
}

#[test]
fn adc_import_extracts_client_and_refresh_token() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    let config = gmail_config();
    let adc_path = temp_dir.path().join("application_default_credentials.json");
    fs::write(
        &adc_path,
        r#"{
  "type": "authorized_user",
  "client_id": "adc-client.apps.googleusercontent.com",
  "client_secret": "adc-secret",
  "refresh_token": "adc-refresh-token",
  "quota_project_id": "adc-project"
}"#,
    )
    .unwrap();

    let prepared =
        prepare_google_desktop_client_from_adc(&config, &workspace, adc_path.clone()).unwrap();

    assert_eq!(
        prepared.imported_client().source_kind,
        ImportedOAuthClientSourceKind::GcloudAdc
    );
    assert_eq!(
        prepared.imported_client().source_path.as_ref(),
        Some(&adc_path)
    );
    assert_eq!(
        prepared.resolved_client().client_id,
        "adc-client.apps.googleusercontent.com"
    );
    assert_eq!(
        prepared.refresh_token().expose_secret(),
        "adc-refresh-token"
    );
}

#[test]
fn detect_adc_path_ignores_missing_env_path_and_falls_back_to_well_known_adc() {
    let temp_dir = TempDir::new().unwrap();
    let missing_env_path = temp_dir.path().join("missing-adc.json");
    let well_known_adc = temp_dir
        .path()
        .join(".config/gcloud/application_default_credentials.json");
    fs::create_dir_all(well_known_adc.parent().unwrap()).unwrap();
    fs::write(&well_known_adc, "{}").unwrap();

    let detected = detect_adc_path_from_env(Some(missing_env_path), Some(temp_dir.path().into()));

    assert_eq!(detected, Some(well_known_adc));
}

#[test]
fn noninteractive_setup_uses_adc_when_no_downloaded_json_is_available() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    let config = gmail_config();
    let adc_path = temp_dir.path().join("application_default_credentials.json");
    fs::write(
        &adc_path,
        r#"{
  "type": "authorized_user",
  "client_id": "adc-client.apps.googleusercontent.com",
  "client_secret": "adc-secret",
  "refresh_token": "adc-refresh-token"
}"#,
    )
    .unwrap();

    let prepared =
        prepare_noninteractive_setup(&config, &workspace, Vec::new(), Some(adc_path)).unwrap();

    match prepared {
        PreparedSetup::ImportAdc(prepared) => {
            assert_eq!(
                prepared.imported_client().source_kind,
                ImportedOAuthClientSourceKind::GcloudAdc
            );
            assert_eq!(
                prepared.resolved_client().client_id,
                "adc-client.apps.googleusercontent.com"
            );
            assert_eq!(
                prepared.refresh_token().expose_secret(),
                "adc-refresh-token"
            );
        }
        PreparedSetup::UseExisting | PreparedSetup::ImportClient(_) => {
            panic!("expected non-interactive setup to import the ADC path")
        }
    }
}

#[test]
fn source_falls_back_to_unconfigured_when_workspace_file_disappears() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);

    assert_eq!(
        oauth_client_source(&gmail_config(), &workspace).unwrap(),
        OAuthClientSource::Unconfigured
    );
}

#[test]
fn prepare_setup_deduplicates_discovered_candidate_paths() {
    let candidate = PathBuf::from("/tmp/client_secret_duplicate.json");
    let normalized = normalize_candidate_paths(vec![candidate.clone(), candidate.clone()]);

    assert_eq!(normalized, vec![candidate]);
}

#[test]
fn default_download_dirs_only_include_download_locations() {
    let home_dir = PathBuf::from("/home/tester");
    let user_profile_dir = PathBuf::from("C:/Users/tester");
    let downloads_dirs = default_download_dirs(Some(home_dir), Some(user_profile_dir));

    assert_eq!(
        downloads_dirs,
        vec![
            PathBuf::from("/home/tester/Downloads"),
            PathBuf::from("/home/tester/downloads"),
            PathBuf::from("C:/Users/tester/Downloads"),
        ]
    );
}

#[test]
fn json_setup_never_uses_the_interactive_wizard() {
    assert!(!should_use_interactive_setup(true, true));
    assert!(!should_use_interactive_setup(true, false));
    assert!(should_use_interactive_setup(false, true));
    assert!(!should_use_interactive_setup(false, false));
}

#[test]
fn legacy_stored_client_file_is_still_resolved() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    fs::create_dir_all(&workspace.auth_dir).unwrap();
    fs::write(
        workspace.auth_dir.join("gmail-oauth-client.json"),
        r#"{
  "client_id": "legacy-client.apps.googleusercontent.com",
  "client_secret": "legacy-secret"
}"#,
    )
    .unwrap();

    let resolved = resolve(&gmail_config(), &workspace).unwrap();

    assert_eq!(
        resolved.client_id,
        "legacy-client.apps.googleusercontent.com"
    );
    assert_eq!(resolved.client_secret.as_deref(), Some("legacy-secret"));
}

#[test]
fn imported_client_takes_precedence_over_inline_config() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    fs::create_dir_all(&workspace.auth_dir).unwrap();
    fs::write(
        workspace.auth_dir.join("gmail-oauth-client.json"),
        r#"{
  "installed": {
    "client_id": "imported-client.apps.googleusercontent.com",
    "client_secret": "imported-secret",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}"#,
    )
    .unwrap();

    let resolved = resolve(
        &GmailConfig {
            client_id: Some(String::from("inline-client.apps.googleusercontent.com")),
            client_secret: Some(String::from("inline-secret")),
            ..gmail_config()
        },
        &workspace,
    )
    .unwrap();

    assert_eq!(
        resolved.client_id,
        "imported-client.apps.googleusercontent.com"
    );
    assert_eq!(resolved.client_secret.as_deref(), Some("imported-secret"));
}

#[test]
fn inline_config_reports_config_source_without_workspace_file() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    let config = GmailConfig {
        client_id: Some(String::from("inline-client.apps.googleusercontent.com")),
        client_secret: Some(String::from("inline-secret")),
        ..gmail_config()
    };

    let source = oauth_client_source(&config, &workspace).unwrap();

    assert_eq!(source, OAuthClientSource::InlineConfig);
    assert!(!oauth_client_exists(&config, &workspace).unwrap());
}

#[test]
fn source_reports_workspace_file_and_validates_it() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    fs::create_dir_all(&workspace.auth_dir).unwrap();
    fs::write(
        workspace.auth_dir.join("gmail-oauth-client.json"),
        r#"{
  "installed": {
    "client_id": "saved-client.apps.googleusercontent.com",
    "client_secret": "",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}"#,
    )
    .unwrap();

    assert_eq!(
        oauth_client_source(&gmail_config(), &workspace).unwrap(),
        OAuthClientSource::WorkspaceFile
    );
}

#[test]
fn malformed_workspace_file_returns_a_validation_error() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    fs::create_dir_all(&workspace.auth_dir).unwrap();
    fs::write(
        workspace.auth_dir.join("gmail-oauth-client.json"),
        r#"{
  "installed": {
    "client_id": "",
    "client_secret": "",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token"
  }
}"#,
    )
    .unwrap();

    let error = oauth_client_source(&gmail_config(), &workspace).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<OAuthClientError>(),
        Some(OAuthClientError::MissingStoredField("installed.client_id"))
    ));
}

#[test]
fn unsupported_adc_type_is_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let workspace = workspace_for(&temp_dir);
    let config = gmail_config();
    let adc_path = temp_dir.path().join("application_default_credentials.json");
    fs::write(
        &adc_path,
        r#"{
  "type": "service_account",
  "client_id": "adc-client.apps.googleusercontent.com",
  "client_secret": "adc-secret",
  "refresh_token": "adc-refresh-token"
}"#,
    )
    .unwrap();

    let error = prepare_google_desktop_client_from_adc(&config, &workspace, adc_path).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<OAuthClientError>(),
        Some(OAuthClientError::UnsupportedAdcType(kind)) if kind == "service_account"
    ));
}

#[test]
fn setup_guidance_points_to_console_urls_and_interactive_setup() {
    let guidance = setup_guidance();

    assert!(guidance.contains(GOOGLE_AUTH_OVERVIEW_URL));
    assert!(guidance.contains(GOOGLE_AUTH_CLIENTS_URL));
    assert!(guidance.contains("mailroom auth setup"));
    assert!(guidance.contains("interactive terminal"));
}
