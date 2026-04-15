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
}

#[derive(Debug, Subcommand)]
pub enum WorkspaceCommand {
    /// Create the repo-local runtime directories under .mailroom/
    Init,
}
