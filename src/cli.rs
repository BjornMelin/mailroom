use clap::{Args, Parser, Subcommand};
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
    /// Synchronize mailbox metadata into the local index
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
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
