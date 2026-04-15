mod cli;
mod workspace;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, WorkspaceCommand};

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let repo_root = std::env::current_dir()?;
    let paths = workspace::WorkspacePaths::from_repo_root(repo_root);

    match cli.command {
        Commands::Paths { json } => {
            paths.print(json)?;
        }
        Commands::Doctor { json } => {
            let report = workspace::DoctorReport::inspect(&paths);
            report.print(json)?;
        }
        Commands::Roadmap => {
            println!(
                "v1 milestone: search + triage + draft queue\n\
                 docs: docs/roadmap/v1-search-triage-draft-queue.md\n\
                 architecture: docs/architecture/system-overview.md\n\
                 plugin-assisted ops: docs/operations/plugin-assisted-workflows.md"
            );
        }
        Commands::Workspace {
            command: WorkspaceCommand::Init,
        } => {
            let created = paths.ensure_runtime_dirs()?;
            println!(
                "initialized {} runtime paths under {}",
                created.len(),
                paths.runtime_root.display()
            );
            for path in created {
                println!("{}", path.display());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::workspace::WorkspacePaths;
    use std::path::PathBuf;

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
}
