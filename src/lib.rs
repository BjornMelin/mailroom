mod cli;
mod workspace;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, WorkspaceCommand};
use std::path::{Path, PathBuf};

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let repo_root = discover_repo_root(std::env::current_dir()?)?;
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

fn discover_repo_root(start: PathBuf) -> Result<PathBuf> {
    start
        .ancestors()
        .find(|path| is_repo_root(path))
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow::anyhow!("could not locate repository root from {}", start.display()))
}

fn is_repo_root(path: &Path) -> bool {
    path.join(".git").exists() || path.join("Cargo.toml").is_file()
}

#[cfg(test)]
mod tests {
    use super::discover_repo_root;
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

    #[test]
    fn repo_root_discovery_walks_up_from_subdirectories() {
        let nested = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        assert_eq!(
            discover_repo_root(nested).unwrap(),
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        );
    }
}
