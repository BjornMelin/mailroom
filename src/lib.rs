mod auth;
mod cli;
mod config;
mod doctor;
mod gmail;
mod mailbox;
mod store;
mod time;
mod workspace;

use anyhow::Result;
use clap::Parser;
use cli::{
    AccountCommand, AuthCommand, Cli, Commands, ConfigCommand, GmailCommand, GmailLabelsCommand,
    SearchArgs, StoreCommand, SyncCommand, WorkspaceCommand,
};
use serde::Serialize;
use std::path::{Path, PathBuf};
use time::current_epoch_seconds;

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir()?;
    let repo_root = discover_repo_root(cwd)?;
    let paths = workspace::WorkspacePaths::from_repo_root(repo_root);

    match cli.command {
        Commands::Auth { command } => handle_auth_command(&paths, command).await?,
        Commands::Account { command } => handle_account_command(&paths, command).await?,
        Commands::Config { command } => handle_config_command(&paths, command)?,
        Commands::Paths { json } => {
            let config_report = config::resolve(&paths)?;
            configured_paths(&config_report)?.print(json)?;
        }
        Commands::Doctor { json } => handle_doctor_command(&paths, json)?,
        Commands::Gmail { command } => handle_gmail_command(&paths, command).await?,
        Commands::Roadmap => print_roadmap(),
        Commands::Search(args) => handle_search_command(&paths, args).await?,
        Commands::Sync { command } => handle_sync_command(&paths, command).await?,
        Commands::Workspace { command } => handle_workspace_command(&paths, command)?,
        Commands::Store { command } => handle_store_command(&paths, command)?,
    }

    Ok(())
}

async fn handle_auth_command(
    paths: &workspace::WorkspacePaths,
    command: AuthCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AuthCommand::Setup {
            credentials_file,
            json,
            no_browser,
        } => auth::setup(&config_report, credentials_file, no_browser, json)
            .await?
            .print(json)?,
        AuthCommand::Login { json, no_browser } => auth::login(&config_report, no_browser, json)
            .await?
            .print(json)?,
        AuthCommand::Status { json } => auth::status(&config_report)?.print(json)?,
        AuthCommand::Logout { json } => auth::logout(&config_report)?.print(json)?,
    }

    Ok(())
}

async fn handle_account_command(
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

fn handle_config_command(paths: &workspace::WorkspacePaths, command: ConfigCommand) -> Result<()> {
    match command {
        ConfigCommand::Show { json } => config::resolve(paths)?.print(json)?,
    }

    Ok(())
}

fn handle_doctor_command(paths: &workspace::WorkspacePaths, json: bool) -> Result<()> {
    let config_report = config::resolve(paths)?;
    let configured_paths = configured_paths(&config_report)?;
    doctor::DoctorReport::inspect(&configured_paths, config_report)?.print(json)?;
    Ok(())
}

async fn handle_gmail_command(
    paths: &workspace::WorkspacePaths,
    command: GmailCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        GmailCommand::Labels {
            command: GmailLabelsCommand::List { json },
        } => GmailLabelsReport {
            labels: gmail_client(&config_report)?.list_labels().await?,
        }
        .print(json)?,
    }

    Ok(())
}

fn print_roadmap() {
    println!(
        "v1 milestone: search + triage + draft queue\n\
         docs: docs/roadmap/v1-search-triage-draft-queue.md\n\
         architecture: docs/architecture/system-overview.md\n\
         plugin-assisted ops: docs/operations/plugin-assisted-workflows.md"
    );
}

async fn handle_search_command(paths: &workspace::WorkspacePaths, args: SearchArgs) -> Result<()> {
    let config_report = config::resolve(paths)?;
    mailbox::search(
        &config_report,
        mailbox::SearchRequest {
            terms: args.terms,
            label: args.label,
            from_address: args.from_address,
            after: args.after,
            before: args.before,
            limit: args.limit,
        },
    )
    .await?
    .print(args.json)?;

    Ok(())
}

async fn handle_sync_command(
    paths: &workspace::WorkspacePaths,
    command: SyncCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        SyncCommand::Run {
            full,
            recent_days,
            json,
        } => mailbox::sync_run(&config_report, full, recent_days)
            .await?
            .print(json)?,
    }

    Ok(())
}

fn handle_workspace_command(
    paths: &workspace::WorkspacePaths,
    command: WorkspaceCommand,
) -> Result<()> {
    match command {
        WorkspaceCommand::Init => {
            let config_report = config::resolve(paths)?;
            let configured_paths = configured_paths(&config_report)?;
            let created = configured_paths.ensure_runtime_dirs()?;
            println!(
                "initialized {} new runtime paths under {}",
                created.len(),
                configured_paths.runtime_root.display()
            );
            for path in created {
                println!("{}", path.display());
            }
        }
    }

    Ok(())
}

fn handle_store_command(paths: &workspace::WorkspacePaths, command: StoreCommand) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        StoreCommand::Init { json } => {
            let configured_paths = configured_paths(&config_report)?;
            configured_paths.ensure_runtime_dirs()?;
            store::init(&config_report)?.print(json)?;
        }
        StoreCommand::Doctor { json } => store::inspect(config_report)?.print(json)?,
    }

    Ok(())
}

fn gmail_client(config_report: &config::ConfigReport) -> Result<gmail::GmailClient> {
    gmail_client_for_config(config_report)
}

pub(crate) fn gmail_client_for_config(
    config_report: &config::ConfigReport,
) -> Result<gmail::GmailClient> {
    gmail::GmailClient::new(
        config_report.config.gmail.clone(),
        config_report.config.workspace.clone(),
        auth::file_store::FileCredentialStore::new(
            config_report
                .config
                .gmail
                .credential_path(&config_report.config.workspace),
        ),
    )
}

pub(crate) fn configured_paths(
    config_report: &config::ConfigReport,
) -> Result<workspace::WorkspacePaths> {
    let repo_root =
        workspace::configured_repo_root_from_locations(&config_report.locations.repo_config_path)?;
    Ok(workspace::WorkspacePaths::from_config(
        repo_root,
        &config_report.config.workspace,
    ))
}

pub(crate) async fn refresh_active_account_record(
    config_report: &config::ConfigReport,
) -> Result<store::accounts::AccountRecord> {
    let configured_paths = configured_paths(config_report)?;
    let gmail_client = gmail_client_for_config(config_report)?;
    let (profile, access_scope) = gmail_client.get_profile_with_access_scope().await?;
    configured_paths.ensure_runtime_dirs()?;
    store::init(config_report)?;
    store::accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &store::accounts::UpsertAccountInput {
            email_address: profile.email_address,
            history_id: profile.history_id,
            messages_total: profile.messages_total,
            threads_total: profile.threads_total,
            access_scope,
            refreshed_at_epoch_s: current_epoch_seconds()?,
        },
    )
}

async fn refresh_active_account(config_report: &config::ConfigReport) -> Result<AccountShowReport> {
    let account = refresh_active_account_record(config_report).await?;

    Ok(AccountShowReport { account })
}

fn discover_repo_root(start: PathBuf) -> Result<PathBuf> {
    start
        .ancestors()
        .find(|path| is_repo_root(path))
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow::anyhow!("could not locate repository root from {}", start.display()))
}

fn is_repo_root(path: &Path) -> bool {
    path.join(".git").exists()
}

#[cfg(test)]
mod tests {
    use super::{discover_repo_root, refresh_active_account};
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use secrecy::SecretString;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn repo_local_runtime_paths_are_stable() {
        let paths = WorkspacePaths::from_repo_root(PathBuf::from("/tmp/mailroom"));
        assert_eq!(paths.runtime_root, PathBuf::from("/tmp/mailroom/.mailroom"));
        assert_eq!(
            paths.state_dir,
            PathBuf::from("/tmp/mailroom/.mailroom/state")
        );
        assert_eq!(
            paths.vault_dir,
            PathBuf::from("/tmp/mailroom/.mailroom/vault")
        );
    }

    #[test]
    fn repo_root_discovery_walks_up_from_subdirectories() {
        let nested = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        assert_eq!(
            discover_repo_root(nested).unwrap(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        );
    }

    #[test]
    fn repo_root_discovery_ignores_nested_cargo_toml_without_git_metadata() {
        let root = unique_temp_dir("mailroom-root-discovery");
        let nested_crate = root.join("nested-crate");
        let nested_src = nested_crate.join("src");

        if root.exists() {
            fs::remove_dir_all(&root).unwrap();
        }

        fs::create_dir_all(&nested_src).unwrap();
        fs::write(root.join(".git"), "gitdir: /tmp/mailroom-test-git\n").unwrap();
        fs::write(
            nested_crate.join("Cargo.toml"),
            "[package]\nname = \"nested\"\n",
        )
        .unwrap();

        assert_eq!(discover_repo_root(nested_src).unwrap(), root);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn workspace_init_reports_only_new_runtime_paths() {
        let repo_root = unique_temp_dir("mailroom-test");
        if repo_root.exists() {
            fs::remove_dir_all(&repo_root).unwrap();
        }

        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let first = paths.ensure_runtime_dirs().unwrap();
        let second = paths.ensure_runtime_dirs().unwrap();

        assert_eq!(first.len(), 6);
        assert!(second.is_empty());

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn configured_runtime_paths_are_used_for_runtime_initialization() {
        let repo_root = unique_temp_dir("mailroom-configured-runtime-init");
        if repo_root.exists() {
            fs::remove_dir_all(&repo_root).unwrap();
        }

        let runtime_root = TempDir::new().unwrap();
        let runtime_root_path = runtime_root.path().to_path_buf();
        let runtime_root_toml = runtime_root_path.to_string_lossy().replace('\\', "\\\\");
        fs::create_dir_all(repo_root.join(".mailroom")).unwrap();
        fs::write(
            repo_root.join(".mailroom/config.toml"),
            format!(
                r#"
[workspace]
runtime_root = "{}"
"#,
                runtime_root_toml
            ),
        )
        .unwrap();

        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let config_report = resolve(&paths).unwrap();
        let configured_paths =
            WorkspacePaths::from_config(repo_root.clone(), &config_report.config.workspace);

        assert_eq!(configured_paths.runtime_root, runtime_root_path);
        assert_eq!(
            configured_paths.state_dir,
            runtime_root.path().join("state")
        );

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn configured_runtime_paths_are_used_for_workspace_doctor() {
        let repo_root = unique_temp_dir("mailroom-configured-runtime-doctor");
        if repo_root.exists() {
            fs::remove_dir_all(&repo_root).unwrap();
        }

        let runtime_root = TempDir::new().unwrap();
        let runtime_root_path = runtime_root.path().to_path_buf();
        let runtime_root_toml = runtime_root_path.to_string_lossy().replace('\\', "\\\\");
        fs::create_dir_all(repo_root.join(".mailroom")).unwrap();
        fs::write(
            repo_root.join(".mailroom/config.toml"),
            format!(
                r#"
[workspace]
runtime_root = "{}"
"#,
                runtime_root_toml
            ),
        )
        .unwrap();

        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let config_report = resolve(&paths).unwrap();
        let configured_paths =
            WorkspacePaths::from_config(repo_root.clone(), &config_report.config.workspace);
        configured_paths.ensure_runtime_dirs().unwrap();

        let report = crate::workspace::DoctorReport::inspect(&configured_paths);

        assert!(report.runtime_root_exists);
        assert!(report.path_statuses.iter().all(|status| status.exists));

        fs::remove_dir_all(repo_root).unwrap();
    }

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

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}

#[derive(Debug, Clone, Serialize)]
struct AccountShowReport {
    account: store::accounts::AccountRecord,
}

impl AccountShowReport {
    fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
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

#[derive(Debug, Clone, Serialize)]
struct GmailLabelsReport {
    labels: Vec<gmail::GmailLabel>,
}

impl GmailLabelsReport {
    fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            for label in &self.labels {
                println!("{} {} {}", label.id, label.name, label.label_type);
            }
        }
        Ok(())
    }
}
