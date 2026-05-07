mod attachments;
mod audit;
mod auth;
mod automation;
mod cli;
mod cli_output;
mod config;
mod doctor;
mod gmail;
mod handlers;
mod mailbox;
mod store;
mod time;
mod workflows;
mod workspace;

use anyhow::Result;
use clap::Parser;
use cli::{
    AccountCommand, AttachmentCommand, AuditCommand, AuthCommand, AutomationCommand,
    AutomationRulesCommand, CleanupCommand, Cli, Commands, ConfigCommand, DraftAttachmentCommand,
    DraftCommand, GmailCommand, GmailLabelsCommand, StoreCommand, SyncCommand, SyncPerfCommand,
    TriageCommand, WorkflowCommand,
};
use handlers::{
    handle_account_command, handle_attachment_command, handle_audit_command, handle_auth_command,
    handle_automation_command, handle_cleanup_command, handle_config_command,
    handle_doctor_command, handle_draft_command, handle_gmail_command, handle_paths_command,
    handle_search_command, handle_store_command, handle_sync_command, handle_triage_command,
    handle_workflow_command, handle_workspace_command,
};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use thiserror::Error;
use time::current_epoch_seconds;

#[derive(Debug, Error)]
pub(crate) enum CliInputError {
    #[error("use --until YYYY-MM-DD or --clear")]
    SnoozeRequiresUntilOrClear,
    #[error("use either --until or --clear, not both")]
    SnoozeUntilConflict,
    #[error("--recent-days must be greater than zero")]
    RecentDaysZero,
    #[error("--quota-units-per-minute must be greater than zero")]
    QuotaUnitsPerMinuteZero,
    #[error("--message-fetch-concurrency must be greater than zero")]
    MessageFetchConcurrencyZero,
    #[error("use exactly one of --text, --file, or --stdin")]
    DraftBodyInputSourceConflict,
    #[error("failed to read {path}: {source}")]
    DraftBodyFileRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read draft body from stdin: {source}")]
    DraftBodyStdinRead {
        #[source]
        source: std::io::Error,
    },
}

pub async fn run() -> ExitCode {
    let cli = Cli::parse();
    let metadata = command_metadata(&cli.command);

    match run_cli(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let report = cli_output::describe_error(&error, metadata.operation);
            if metadata.json {
                if let Err(output_error) = cli_output::print_json_failure(&report) {
                    eprintln!("{output_error:#}");
                    eprintln!("{error:#}");
                }
            } else {
                cli_output::print_human_failure(&error);
            }
            cli_output::exit_code(&report)
        }
    }
}

async fn run_cli(cli: Cli) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let repo_root = discover_repo_root(cwd)?;
    let paths = workspace::WorkspacePaths::from_repo_root(repo_root);

    match cli.command {
        Commands::Audit { command } => {
            let paths = paths.clone();
            tokio::task::spawn_blocking(move || handle_audit_command(&paths, command)).await??;
        }
        Commands::Auth { command } => handle_auth_command(&paths, command).await?,
        Commands::Account { command } => handle_account_command(&paths, command).await?,
        Commands::Config { command } => {
            let paths = paths.clone();
            tokio::task::spawn_blocking(move || handle_config_command(&paths, command)).await??;
        }
        Commands::Paths { json } => {
            let paths = paths.clone();
            tokio::task::spawn_blocking(move || handle_paths_command(&paths, json)).await??;
        }
        Commands::Doctor { json } => {
            let paths = paths.clone();
            tokio::task::spawn_blocking(move || handle_doctor_command(&paths, json)).await??;
        }
        Commands::Gmail { command } => handle_gmail_command(&paths, command).await?,
        Commands::Roadmap => print_roadmap(),
        Commands::Search(args) => handle_search_command(&paths, args).await?,
        Commands::Attachment { command } => handle_attachment_command(&paths, command).await?,
        Commands::Automation { command } => handle_automation_command(&paths, command).await?,
        Commands::Sync { command } => handle_sync_command(&paths, command).await?,
        Commands::Workflow { command } => handle_workflow_command(&paths, command).await?,
        Commands::Triage { command } => handle_triage_command(&paths, command).await?,
        Commands::Draft { command } => handle_draft_command(&paths, command).await?,
        Commands::Cleanup { command } => handle_cleanup_command(&paths, command).await?,
        Commands::Workspace { command } => {
            let paths = paths.clone();
            tokio::task::spawn_blocking(move || handle_workspace_command(&paths, command))
                .await??;
        }
        Commands::Store { command } => {
            let paths = paths.clone();
            tokio::task::spawn_blocking(move || handle_store_command(&paths, command)).await??;
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct CommandMetadata {
    json: bool,
    operation: &'static str,
}

fn command_metadata(command: &Commands) -> CommandMetadata {
    match command {
        Commands::Audit { command } => match command {
            AuditCommand::Labels { json } => CommandMetadata {
                json: *json,
                operation: "audit.labels",
            },
            AuditCommand::Verification { json } => CommandMetadata {
                json: *json,
                operation: "audit.verification",
            },
        },
        Commands::Auth { command } => match command {
            AuthCommand::Setup { json, .. } => CommandMetadata {
                json: *json,
                operation: "auth.setup",
            },
            AuthCommand::Login { json, .. } => CommandMetadata {
                json: *json,
                operation: "auth.login",
            },
            AuthCommand::Status { json } => CommandMetadata {
                json: *json,
                operation: "auth.status",
            },
            AuthCommand::Logout { json } => CommandMetadata {
                json: *json,
                operation: "auth.logout",
            },
        },
        Commands::Account { command } => match command {
            AccountCommand::Show { json } => CommandMetadata {
                json: *json,
                operation: "account.show",
            },
        },
        Commands::Config { command } => match command {
            ConfigCommand::Show { json } => CommandMetadata {
                json: *json,
                operation: "config.show",
            },
        },
        Commands::Paths { json } => CommandMetadata {
            json: *json,
            operation: "paths.show",
        },
        Commands::Doctor { json } => CommandMetadata {
            json: *json,
            operation: "doctor.show",
        },
        Commands::Roadmap => CommandMetadata {
            json: false,
            operation: "roadmap.show",
        },
        Commands::Search(args) => CommandMetadata {
            json: args.json,
            operation: "search.run",
        },
        Commands::Attachment { command } => match command {
            AttachmentCommand::List { json, .. } => CommandMetadata {
                json: *json,
                operation: "attachment.list",
            },
            AttachmentCommand::Show { json, .. } => CommandMetadata {
                json: *json,
                operation: "attachment.show",
            },
            AttachmentCommand::Fetch { json, .. } => CommandMetadata {
                json: *json,
                operation: "attachment.fetch",
            },
            AttachmentCommand::Export { json, .. } => CommandMetadata {
                json: *json,
                operation: "attachment.export",
            },
        },
        Commands::Automation { command } => match command {
            AutomationCommand::Rules {
                command: AutomationRulesCommand::Validate { json },
            } => CommandMetadata {
                json: *json,
                operation: "automation.rules.validate",
            },
            AutomationCommand::Run { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.run",
            },
            AutomationCommand::Rollout { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.rollout",
            },
            AutomationCommand::Show { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.show",
            },
            AutomationCommand::Apply { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.apply",
            },
            AutomationCommand::Prune { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.prune",
            },
        },
        Commands::Sync { command } => match command {
            SyncCommand::Run(args) => CommandMetadata {
                json: args.json,
                operation: "sync.run",
            },
            SyncCommand::Benchmark(args) => CommandMetadata {
                json: args.json,
                operation: "sync.benchmark",
            },
            SyncCommand::History { json, .. } => CommandMetadata {
                json: *json,
                operation: "sync.history",
            },
            SyncCommand::Perf {
                command: SyncPerfCommand::Explain { json, .. },
            } => CommandMetadata {
                json: *json,
                operation: "sync.perf_explain",
            },
            SyncCommand::PerfExplain { json, .. } => CommandMetadata {
                json: *json,
                operation: "sync.perf_explain",
            },
        },
        Commands::Workflow { command } => match command {
            WorkflowCommand::List { json, .. } => CommandMetadata {
                json: *json,
                operation: "workflow.list",
            },
            WorkflowCommand::Show { json, .. } => CommandMetadata {
                json: *json,
                operation: "workflow.show",
            },
            WorkflowCommand::Promote { json, .. } => CommandMetadata {
                json: *json,
                operation: "workflow.promote",
            },
            WorkflowCommand::Snooze { json, .. } => CommandMetadata {
                json: *json,
                operation: "workflow.snooze",
            },
        },
        Commands::Triage { command } => match command {
            TriageCommand::Set { json, .. } => CommandMetadata {
                json: *json,
                operation: "triage.set",
            },
        },
        Commands::Draft { command } => match command {
            DraftCommand::Start { json, .. } => CommandMetadata {
                json: *json,
                operation: "draft.start",
            },
            DraftCommand::Body { json, .. } => CommandMetadata {
                json: *json,
                operation: "draft.body.set",
            },
            DraftCommand::Send { json, .. } => CommandMetadata {
                json: *json,
                operation: "draft.send",
            },
            DraftCommand::Attach { command } => match command {
                DraftAttachmentCommand::Add { json, .. } => CommandMetadata {
                    json: *json,
                    operation: "draft.attachment.add",
                },
                DraftAttachmentCommand::Remove { json, .. } => CommandMetadata {
                    json: *json,
                    operation: "draft.attachment.remove",
                },
            },
        },
        Commands::Cleanup { command } => match command {
            CleanupCommand::Archive { json, .. } => CommandMetadata {
                json: *json,
                operation: "cleanup.archive",
            },
            CleanupCommand::Label { json, .. } => CommandMetadata {
                json: *json,
                operation: "cleanup.label",
            },
            CleanupCommand::Trash { json, .. } => CommandMetadata {
                json: *json,
                operation: "cleanup.trash",
            },
        },
        Commands::Workspace { .. } => CommandMetadata {
            json: false,
            operation: "workspace.init",
        },
        Commands::Gmail { command } => match command {
            GmailCommand::Labels {
                command: GmailLabelsCommand::List { json },
            } => CommandMetadata {
                json: *json,
                operation: "gmail.labels.list",
            },
        },
        Commands::Store { command } => match command {
            StoreCommand::Init { json } => CommandMetadata {
                json: *json,
                operation: "store.init",
            },
            StoreCommand::Doctor { json } => CommandMetadata {
                json: *json,
                operation: "store.doctor",
            },
        },
    }
}

fn print_roadmap() {
    println!(
        "v1 milestone: search + thread workflow + draft/send + reviewed cleanup + controlled attachment export\n\
         docs: docs/roadmap/v1-search-triage-draft-queue.md\n\
         architecture: docs/architecture/system-overview.md\n\
         hardening: docs/operations/verification-and-hardening.md\n\
         plugin-assisted ops: docs/operations/plugin-assisted-workflows.md"
    );
}

pub(crate) fn gmail_client_for_config(
    config_report: &config::ConfigReport,
) -> Result<gmail::GmailClient> {
    Ok(gmail::GmailClient::new(
        config_report.config.gmail.clone(),
        config_report.config.workspace.clone(),
        auth::file_store::FileCredentialStore::new(
            config_report
                .config
                .gmail
                .credential_path(&config_report.config.workspace),
        ),
    )?)
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
    let gmail_client = gmail_client_for_config(config_report)?;
    refresh_active_account_record_with_client(config_report, &gmail_client).await
}

pub(crate) async fn refresh_active_account_record_with_client(
    config_report: &config::ConfigReport,
    gmail_client: &gmail::GmailClient,
) -> Result<store::accounts::AccountRecord> {
    let configured_paths = configured_paths(config_report)?;
    let (profile, access_scope) = gmail_client.get_profile_with_access_scope().await?;
    let config_report = config_report.clone();
    tokio::task::spawn_blocking(move || {
        configured_paths.ensure_runtime_dirs()?;
        store::init(&config_report)?;
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
    })
    .await?
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
    use super::discover_repo_root;
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

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

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
