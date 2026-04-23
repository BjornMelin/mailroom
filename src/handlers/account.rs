use crate::cli::AccountCommand;
use crate::config;
use crate::store;
use crate::workspace;
use anyhow::Result;
use serde::Serialize;

pub(crate) async fn handle_account_command(
    paths: &workspace::WorkspacePaths,
    command: AccountCommand,
) -> Result<()> {
    match command {
        AccountCommand::Show { json } => {
            refresh_active_account(&config::resolve(paths)?)
                .await?
                .print(json)?;
        }
    }

    Ok(())
}

async fn refresh_active_account(config_report: &config::ConfigReport) -> Result<AccountShowReport> {
    let account = crate::refresh_active_account_record(config_report).await?;

    Ok(AccountShowReport { account })
}

#[derive(Debug, Clone, Serialize)]
struct AccountShowReport {
    account: store::accounts::AccountRecord,
}

impl AccountShowReport {
    fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("account_id={}", self.account.account_id);
            println!("email_address={}", self.account.email_address);
            println!("history_id={}", self.account.history_id);
            println!("messages_total={}", self.account.messages_total);
            println!("threads_total={}", self.account.threads_total);
            println!(
                "last_profile_refresh_epoch_s={}",
                self.account.last_profile_refresh_epoch_s
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::refresh_active_account;
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use secrecy::SecretString;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn refresh_active_account_persists_stored_granted_scopes() {
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
        let repo_root = temp_dir.path().to_path_buf();
        let paths = WorkspacePaths::from_repo_root(repo_root);
        paths.ensure_runtime_dirs().unwrap();
        let mut config_report = resolve(&paths).unwrap();
        config_report.config.gmail.api_base_url = format!("{}/gmail/v1", mock_server.uri());
        config_report.config.gmail.scopes = vec![String::from("requested:scope")];
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
                scopes: vec![String::from("granted:scope")],
            })
            .unwrap();

        let report = refresh_active_account(&config_report).await.unwrap();

        assert_eq!(report.account.access_scope, "granted:scope");
    }

    #[tokio::test]
    async fn refresh_active_account_without_credentials_does_not_create_database() {
        let temp_dir = TempDir::new().unwrap();
        let repo_root = temp_dir.path().to_path_buf();
        let paths = WorkspacePaths::from_repo_root(repo_root);
        let config_report = resolve(&paths).unwrap();

        let error = refresh_active_account(&config_report).await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "mailroom is not authenticated; run `mailroom auth login` first"
        );
        assert!(!config_report.config.store.database_path.exists());
        assert!(!config_report.config.workspace.runtime_root.exists());
    }
}
