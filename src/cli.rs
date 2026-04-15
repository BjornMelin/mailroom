use clap::{Parser, Subcommand};

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
    /// Manage the repo-local runtime workspace
    Workspace {
        #[command(subcommand)]
        command: WorkspaceCommand,
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
pub enum ConfigCommand {
    /// Print resolved configuration and source locations
    Show {
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
