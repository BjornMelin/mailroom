mod attachments;
mod audit;
mod auth;
mod automation;
mod cli;
mod cli_output;
mod config;
mod doctor;
mod gmail;
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
    DraftCommand, GmailCommand, GmailLabelsCommand, SearchArgs, StoreCommand, SyncCommand,
    TriageBucketArg, TriageCommand, WorkflowCommand, WorkflowPromoteTargetArg, WorkflowStageArg,
    WorkspaceCommand,
};
use serde::Serialize;
use std::io::Read;
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
                eprintln!("{error:#}");
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
        Commands::Audit { command } => handle_audit_command(&paths, command)?,
        Commands::Auth { command } => handle_auth_command(&paths, command).await?,
        Commands::Account { command } => handle_account_command(&paths, command).await?,
        Commands::Config { command } => handle_config_command(&paths, command)?,
        Commands::Paths { json } => handle_paths_command(&paths, json)?,
        Commands::Doctor { json } => handle_doctor_command(&paths, json)?,
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
        Commands::Workspace { command } => handle_workspace_command(&paths, command)?,
        Commands::Store { command } => handle_store_command(&paths, command)?,
    }

    Ok(())
}

fn handle_paths_command(paths: &workspace::WorkspacePaths, json: bool) -> Result<()> {
    match config::resolve(paths) {
        Ok(config_report) => configured_paths(&config_report)?.print(json)?,
        Err(_) => paths.print(json)?,
    }

    Ok(())
}

fn resolve_snooze_until(until: Option<String>, clear: bool) -> Result<Option<String>> {
    if !clear && until.is_none() {
        return Err(CliInputError::SnoozeRequiresUntilOrClear.into());
    }
    if clear && until.is_some() {
        return Err(CliInputError::SnoozeUntilConflict.into());
    }

    if clear { Ok(None) } else { Ok(until) }
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
            AutomationCommand::Show { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.show",
            },
            AutomationCommand::Apply { json, .. } => CommandMetadata {
                json: *json,
                operation: "automation.apply",
            },
        },
        Commands::Sync { command } => match command {
            SyncCommand::Run { json, .. } => CommandMetadata {
                json: *json,
                operation: "sync.run",
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

fn handle_audit_command(paths: &workspace::WorkspacePaths, command: AuditCommand) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AuditCommand::Labels { json } => audit::labels(&config_report)?.print(json)?,
        AuditCommand::Verification { json } => audit::verification(&config_report)?.print(json)?,
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
        "v1 milestone: search + thread workflow + draft/send + reviewed cleanup + controlled attachment export\n\
         docs: docs/roadmap/v1-search-triage-draft-queue.md\n\
         architecture: docs/architecture/system-overview.md\n\
         hardening: docs/operations/verification-and-hardening.md\n\
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

async fn handle_attachment_command(
    paths: &workspace::WorkspacePaths,
    command: AttachmentCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AttachmentCommand::List {
            thread_id,
            message_id,
            filename,
            mime_type,
            fetched_only,
            limit,
            json,
        } => attachments::list(
            &config_report,
            attachments::AttachmentListRequest {
                thread_id,
                message_id,
                filename,
                mime_type,
                fetched_only,
                limit,
            },
        )
        .await?
        .print(json)?,
        AttachmentCommand::Show {
            attachment_key,
            json,
        } => attachments::show(&config_report, attachment_key)
            .await?
            .print(json)?,
        AttachmentCommand::Fetch {
            attachment_key,
            json,
        } => attachments::fetch(&config_report, attachment_key)
            .await?
            .print(json)?,
        AttachmentCommand::Export {
            attachment_key,
            to,
            json,
        } => attachments::export(&config_report, attachment_key, to)
            .await?
            .print(json)?,
    }

    Ok(())
}

async fn handle_automation_command(
    paths: &workspace::WorkspacePaths,
    command: AutomationCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AutomationCommand::Rules {
            command: AutomationRulesCommand::Validate { json },
        } => automation::validate_rules(&config_report)
            .await?
            .print(json)?,
        AutomationCommand::Run {
            rule_ids,
            limit,
            json,
        } => automation::run_preview(
            &config_report,
            automation::AutomationRunRequest { rule_ids, limit },
        )
        .await?
        .print(json)?,
        AutomationCommand::Show { run_id, json } => automation::show_run(&config_report, run_id)
            .await?
            .print(json)?,
        AutomationCommand::Apply {
            run_id,
            execute,
            json,
        } => automation::apply_run(&config_report, run_id, execute)
            .await?
            .print(json)?,
    }

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

async fn handle_workflow_command(
    paths: &workspace::WorkspacePaths,
    command: WorkflowCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        WorkflowCommand::List {
            stage,
            triage_bucket,
            json,
        } => workflows::list_workflows(
            &config_report,
            stage.map(workflow_stage_from_arg),
            triage_bucket.map(triage_bucket_from_arg),
        )
        .await?
        .print(json)?,
        WorkflowCommand::Show { thread_id, json } => {
            workflows::show_workflow(&config_report, thread_id)
                .await?
                .print(json)?
        }
        WorkflowCommand::Promote {
            thread_id,
            to,
            json,
        } => workflows::promote_workflow(
            &config_report,
            thread_id,
            workflow_promote_target_from_arg(to),
        )
        .await?
        .print(json)?,
        WorkflowCommand::Snooze {
            thread_id,
            until,
            clear,
            json,
        } => {
            let until = resolve_snooze_until(until, clear)?;
            workflows::snooze_workflow(&config_report, thread_id, until)
                .await?
                .print(json)?;
        }
    }

    Ok(())
}

async fn handle_triage_command(
    paths: &workspace::WorkspacePaths,
    command: TriageCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        TriageCommand::Set {
            thread_id,
            bucket,
            note,
            json,
        } => workflows::set_triage(
            &config_report,
            thread_id,
            triage_bucket_from_arg(bucket),
            note,
        )
        .await?
        .print(json)?,
    }

    Ok(())
}

async fn handle_draft_command(
    paths: &workspace::WorkspacePaths,
    command: DraftCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        DraftCommand::Start {
            thread_id,
            reply_all,
            json,
        } => workflows::draft_start(
            &config_report,
            thread_id,
            if reply_all {
                store::workflows::ReplyMode::ReplyAll
            } else {
                store::workflows::ReplyMode::Reply
            },
        )
        .await?
        .print(json)?,
        DraftCommand::Body {
            thread_id,
            text,
            file,
            stdin,
            json,
        } => {
            let body_text = resolve_draft_body_input(text, file, stdin)?;
            workflows::draft_body_set(&config_report, thread_id, body_text)
                .await?
                .print(json)?;
        }
        DraftCommand::Attach { command } => match command {
            DraftAttachmentCommand::Add {
                thread_id,
                path,
                json,
            } => workflows::draft_attach_add(&config_report, thread_id, path)
                .await?
                .print(json)?,
            DraftAttachmentCommand::Remove {
                thread_id,
                path,
                json,
            } => workflows::draft_attach_remove(&config_report, thread_id, path)
                .await?
                .print(json)?,
        },
        DraftCommand::Send { thread_id, json } => workflows::draft_send(&config_report, thread_id)
            .await?
            .print(json)?,
    }

    Ok(())
}

async fn handle_cleanup_command(
    paths: &workspace::WorkspacePaths,
    command: CleanupCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        CleanupCommand::Archive {
            thread_id,
            execute,
            json,
        } => workflows::cleanup_archive(&config_report, thread_id, execute)
            .await?
            .print(json)?,
        CleanupCommand::Label {
            thread_id,
            add_labels,
            remove_labels,
            execute,
            json,
        } => workflows::cleanup_label(
            &config_report,
            thread_id,
            execute,
            add_labels,
            remove_labels,
        )
        .await?
        .print(json)?,
        CleanupCommand::Trash {
            thread_id,
            execute,
            json,
        } => workflows::cleanup_trash(&config_report, thread_id, execute)
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

fn workflow_stage_from_arg(value: WorkflowStageArg) -> store::workflows::WorkflowStage {
    match value {
        WorkflowStageArg::Triage => store::workflows::WorkflowStage::Triage,
        WorkflowStageArg::FollowUp => store::workflows::WorkflowStage::FollowUp,
        WorkflowStageArg::Drafting => store::workflows::WorkflowStage::Drafting,
        WorkflowStageArg::ReadyToSend => store::workflows::WorkflowStage::ReadyToSend,
        WorkflowStageArg::Sent => store::workflows::WorkflowStage::Sent,
        WorkflowStageArg::Closed => store::workflows::WorkflowStage::Closed,
    }
}

fn workflow_promote_target_from_arg(
    value: WorkflowPromoteTargetArg,
) -> store::workflows::WorkflowStage {
    match value {
        WorkflowPromoteTargetArg::FollowUp => store::workflows::WorkflowStage::FollowUp,
        WorkflowPromoteTargetArg::ReadyToSend => store::workflows::WorkflowStage::ReadyToSend,
        WorkflowPromoteTargetArg::Closed => store::workflows::WorkflowStage::Closed,
    }
}

fn triage_bucket_from_arg(value: TriageBucketArg) -> store::workflows::TriageBucket {
    match value {
        TriageBucketArg::Urgent => store::workflows::TriageBucket::Urgent,
        TriageBucketArg::NeedsReplySoon => store::workflows::TriageBucket::NeedsReplySoon,
        TriageBucketArg::Waiting => store::workflows::TriageBucket::Waiting,
        TriageBucketArg::Fyi => store::workflows::TriageBucket::Fyi,
    }
}

fn resolve_draft_body_input(
    text: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<String> {
    let selected = usize::from(text.is_some()) + usize::from(file.is_some()) + usize::from(stdin);
    if selected != 1 {
        return Err(CliInputError::DraftBodyInputSourceConflict.into());
    }

    if let Some(text) = text {
        return Ok(text);
    }

    if let Some(file) = file {
        return std::fs::read_to_string(&file)
            .map_err(|source| CliInputError::DraftBodyFileRead { path: file, source }.into());
    }

    let mut buffer = String::new();
    std::io::stdin()
        .read_to_string(&mut buffer)
        .map_err(|source| CliInputError::DraftBodyStdinRead { source })?;
    Ok(buffer)
}

fn gmail_client(config_report: &config::ConfigReport) -> Result<gmail::GmailClient> {
    gmail_client_for_config(config_report)
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
    use super::{
        CliInputError, discover_repo_root, handle_paths_command, refresh_active_account,
        resolve_draft_body_input, resolve_snooze_until,
    };
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

    #[test]
    fn paths_command_still_prints_repo_local_paths_when_config_is_malformed() {
        let repo_root = unique_temp_dir("mailroom-paths-malformed-config");
        if repo_root.exists() {
            fs::remove_dir_all(&repo_root).unwrap();
        }

        fs::create_dir_all(repo_root.join(".mailroom")).unwrap();
        fs::write(repo_root.join(".mailroom/config.toml"), "[workspace\n").unwrap();

        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        assert!(resolve(&paths).is_err());
        handle_paths_command(&paths, true).unwrap();

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

    #[test]
    fn resolve_snooze_until_requires_explicit_until_or_clear() {
        let error = resolve_snooze_until(None, false).unwrap_err();

        assert_eq!(error.to_string(), "use --until YYYY-MM-DD or --clear");
    }

    #[test]
    fn resolve_snooze_until_rejects_conflicting_flags() {
        let error = resolve_snooze_until(Some(String::from("2026-05-01")), true).unwrap_err();

        assert_eq!(error.to_string(), "use either --until or --clear, not both");
    }

    #[test]
    fn resolve_draft_body_input_requires_exactly_one_source() {
        let error = resolve_draft_body_input(None, None, false).unwrap_err();

        assert_eq!(
            error.to_string(),
            "use exactly one of --text, --file, or --stdin"
        );
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::DraftBodyInputSourceConflict)
        ));
    }

    #[test]
    fn resolve_draft_body_input_reports_file_read_as_typed_validation_error() {
        let missing_path = PathBuf::from("/definitely/missing/mailroom-draft-body.txt");
        let error = resolve_draft_body_input(None, Some(missing_path.clone()), false).unwrap_err();

        assert!(
            error
                .to_string()
                .starts_with("failed to read /definitely/missing/mailroom-draft-body.txt:")
        );
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::DraftBodyFileRead { path, .. }) if path == &missing_path
        ));
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

#[derive(Debug, Clone, Serialize)]
struct GmailLabelsReport {
    labels: Vec<gmail::GmailLabel>,
}

impl GmailLabelsReport {
    fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            for label in &self.labels {
                println!("{} {} {}", label.id, label.name, label.label_type);
            }
        }
        Ok(())
    }
}
