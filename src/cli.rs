use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "mailroom",
    version,
    about = "Local-first mailbox operations workspace",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Authenticate Mailroom against Gmail
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Inspect the active Gmail account record
    Account {
        #[command(subcommand)]
        command: AccountCommand,
    },
    /// Show resolved configuration and config source locations
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Print canonical repo-local runtime paths
    Paths {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Inspect the repo-local runtime workspace
    Doctor {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Show the current milestone and key docs
    Roadmap,
    /// Search the local mailbox index
    Search(SearchArgs),
    /// Inspect, fetch, and export cataloged inbound attachments
    Attachment {
        #[command(subcommand)]
        command: AttachmentCommand,
    },
    /// Preview and apply review-first automation rules
    Automation {
        #[command(subcommand)]
        command: AutomationCommand,
    },
    /// Synchronize mailbox metadata into the local index
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    /// List, inspect, and move thread-scoped workflow items
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Set thread triage state
    Triage {
        #[command(subcommand)]
        command: TriageCommand,
    },
    /// Manage reply/draft workflow items
    Draft {
        #[command(subcommand)]
        command: DraftCommand,
    },
    /// Preview and execute reviewed cleanup actions
    Cleanup {
        #[command(subcommand)]
        command: CleanupCommand,
    },
    /// Manage the repo-local runtime workspace
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
    },
    /// Query Gmail through the native client
    Gmail {
        #[command(subcommand)]
        command: GmailCommand,
    },
    /// Manage the local SQLite store
    Store {
        #[command(subcommand)]
        command: StoreCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkspaceCommand {
    /// Create the repo-local runtime directories under .mailroom/
    Init,
}

#[derive(Debug, Subcommand)]
pub enum AuthCommand {
    /// Configure Gmail OAuth via downloaded JSON, pasted values, or imported gcloud ADC, then continue into Gmail login
    Setup {
        /// Path to the downloaded Google desktop-app credentials JSON
        #[arg(long)]
        credentials_file: Option<PathBuf>,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
        /// Print the authorization URL without trying to open a browser
        #[arg(long)]
        no_browser: bool,
    },
    /// Complete Gmail OAuth login and persist credentials locally
    Login {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
        /// Print the authorization URL without trying to open a browser
        #[arg(long)]
        no_browser: bool,
    },
    /// Inspect locally stored Gmail auth state
    Status {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Remove locally stored Gmail auth state
    Logout {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum AccountCommand {
    /// Refresh and print the active Gmail account profile
    Show {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print resolved configuration and source locations
    Show {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Args)]
pub struct SearchArgs {
    /// Full-text terms to match against the local mailbox index
    pub terms: String,
    /// Restrict matches to a specific Gmail label name
    #[arg(long)]
    pub label: Option<String>,
    /// Restrict matches to an exact sender email address
    #[arg(long = "from")]
    pub from_address: Option<String>,
    /// Restrict matches to messages on or after this UTC date (YYYY-MM-DD)
    #[arg(long)]
    pub after: Option<String>,
    /// Restrict matches to messages before the start of this UTC date (YYYY-MM-DD)
    #[arg(long)]
    pub before: Option<String>,
    /// Maximum number of search hits to return
    #[arg(long, default_value_t = crate::mailbox::DEFAULT_SEARCH_LIMIT)]
    pub limit: usize,
    /// Emit JSON instead of plain text
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum AttachmentCommand {
    /// List cataloged attachments from the local mailbox store
    List {
        /// Restrict results to a specific Gmail thread ID
        #[arg(long)]
        thread_id: Option<String>,
        /// Restrict results to a specific Gmail message ID
        #[arg(long)]
        message_id: Option<String>,
        /// Restrict results to filenames containing this substring
        #[arg(long)]
        filename: Option<String>,
        /// Restrict results to an exact MIME type
        #[arg(long)]
        mime_type: Option<String>,
        /// Only return attachments already fetched into the local vault
        #[arg(long)]
        fetched_only: bool,
        /// Maximum number of attachments to return
        #[arg(long, default_value_t = crate::attachments::DEFAULT_ATTACHMENT_LIST_LIMIT)]
        limit: usize,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Show one cataloged attachment in detail
    Show {
        /// Attachment key in `message_id:part_id` form
        attachment_key: String,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Fetch attachment bytes into the local vault
    Fetch {
        /// Attachment key in `message_id:part_id` form
        attachment_key: String,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Copy a fetched attachment into the exports directory or an explicit destination
    Export {
        /// Attachment key in `message_id:part_id` form
        attachment_key: String,
        /// Destination file path or existing directory
        #[arg(long)]
        to: Option<PathBuf>,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum AutomationCommand {
    /// Validate the active local automation rules file
    Rules {
        #[command(subcommand)]
        command: AutomationRulesCommand,
    },
    /// Evaluate rules against the local mailbox cache and persist a review snapshot
    Run {
        /// Restrict the run to one or more specific rule IDs
        #[arg(long = "rule")]
        rule_ids: Vec<String>,
        /// Maximum number of thread candidates to persist
        #[arg(long, default_value_t = crate::automation::DEFAULT_AUTOMATION_RUN_LIMIT)]
        limit: usize,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Inspect a persisted automation review snapshot
    Show {
        /// Numeric automation run ID
        run_id: i64,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Apply a persisted automation review snapshot
    Apply {
        /// Numeric automation run ID
        run_id: i64,
        /// Execute the reviewed mutations
        #[arg(long)]
        execute: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum AutomationRulesCommand {
    /// Validate and print the active local automation rules file
    Validate {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum SyncCommand {
    /// Run a one-shot mailbox sync into the local metadata index
    Run {
        /// Force a full recent-window resync instead of using the stored history cursor
        #[arg(long)]
        full: bool,
        /// Recent-window size in days for full bootstrap syncs
        #[arg(long, default_value_t = crate::mailbox::DEFAULT_BOOTSTRAP_RECENT_DAYS)]
        recent_days: u32,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum StoreCommand {
    /// Initialize the local store and apply migrations
    Init {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Inspect the local store, schema version, and hardening state
    Doctor {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum GmailCommand {
    /// List Gmail labels for the authenticated mailbox
    Labels {
        #[command(subcommand)]
        command: GmailLabelsCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum GmailLabelsCommand {
    /// Fetch labels from Gmail
    List {
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkflowCommand {
    /// List workflow items
    List {
        /// Restrict results to a specific workflow stage
        #[arg(long)]
        stage: Option<WorkflowStageArg>,
        /// Restrict results to a specific triage bucket
        #[arg(long)]
        triage_bucket: Option<TriageBucketArg>,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Show a single workflow item in detail
    Show {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Promote a workflow item to a new stage
    Promote {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Target stage
        #[arg(long)]
        to: WorkflowPromoteTargetArg,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Snooze or clear snooze on a workflow item
    Snooze {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Snooze until YYYY-MM-DD; omit together with --clear to clear the snooze
        #[arg(long)]
        until: Option<String>,
        /// Clear the current snooze
        #[arg(long)]
        clear: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum TriageCommand {
    /// Set the triage bucket for a thread
    Set {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Triage bucket to assign
        #[arg(long)]
        bucket: TriageBucketArg,
        /// Optional operator note
        #[arg(long)]
        note: Option<String>,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum DraftCommand {
    /// Start a reply or reply-all draft for a thread
    Start {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Use reply-all recipients instead of reply-to sender only
        #[arg(long)]
        reply_all: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Replace the current draft body text
    Body {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Inline body text
        #[arg(long)]
        text: Option<String>,
        /// Read body text from a local file
        #[arg(long)]
        file: Option<PathBuf>,
        /// Read body text from stdin
        #[arg(long)]
        stdin: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Manage draft attachments
    Attach {
        #[command(subcommand)]
        command: DraftAttachmentCommand,
    },
    /// Send the current Gmail-backed draft
    Send {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum DraftAttachmentCommand {
    /// Add an attachment to the current draft revision
    Add {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Attachment path
        #[arg(long)]
        path: PathBuf,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Remove an attachment from the current draft revision by path or filename
    Remove {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Attachment path or filename
        #[arg(long)]
        path: String,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum CleanupCommand {
    /// Archive a thread by removing INBOX after review
    Archive {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Execute the cleanup action; omit for preview only
        #[arg(long)]
        execute: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Add and/or remove thread labels after review
    Label {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Label names to add
        #[arg(long = "add")]
        add_labels: Vec<String>,
        /// Label names to remove
        #[arg(long = "remove")]
        remove_labels: Vec<String>,
        /// Execute the cleanup action; omit for preview only
        #[arg(long)]
        execute: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
    /// Move a thread to trash after review
    Trash {
        /// Gmail thread ID for the workflow item
        thread_id: String,
        /// Execute the cleanup action; omit for preview only
        #[arg(long)]
        execute: bool,
        /// Emit JSON instead of plain text
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum WorkflowStageArg {
    Triage,
    FollowUp,
    Drafting,
    ReadyToSend,
    Sent,
    Closed,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum WorkflowPromoteTargetArg {
    FollowUp,
    ReadyToSend,
    Closed,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum TriageBucketArg {
    Urgent,
    NeedsReplySoon,
    Waiting,
    Fyi,
}
