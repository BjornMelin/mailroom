mod cli;
mod config;
mod store;
mod workspace;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Commands, ConfigCommand, StoreCommand, WorkspaceCommand};
use std::path::{Path, PathBuf};

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir()?;
    let repo_root = discover_repo_root(cwd)?;
    let paths = workspace::WorkspacePaths::from_repo_root(repo_root);

    match cli.command {
        Commands::Config {
            command: ConfigCommand::Show { json },
        } => {
            let config_report = config::resolve(&paths)?;
            config_report.print(json)?;
        }
        Commands::Paths { json } => {
            paths.print(json)?;
        }
        Commands::Doctor { json } => {
            let config_report = config::resolve(&paths)?;
            let configured_paths =
                runtime_paths_from_config(&paths.repo_root, &config_report.config.workspace);
            let report = workspace::DoctorReport::inspect(&configured_paths);
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
            let config_report = config::resolve(&paths)?;
            let configured_paths =
                runtime_paths_from_config(&paths.repo_root, &config_report.config.workspace);
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
        Commands::Store {
            command: StoreCommand::Init { json },
        } => {
            let config_report = config::resolve(&paths)?;
            let configured_paths =
                runtime_paths_from_config(&paths.repo_root, &config_report.config.workspace);
            configured_paths.ensure_runtime_dirs()?;
            let report = store::init(&config_report)?;
            report.print(json)?;
        }
        Commands::Store {
            command: StoreCommand::Doctor { json },
        } => {
            let config_report = config::resolve(&paths)?;
            let report = store::inspect(config_report)?;
            report.print(json)?;
        }
    }

    Ok(())
}

fn runtime_paths_from_config(
    repo_root: &Path,
    workspace: &config::WorkspaceConfig,
) -> workspace::WorkspacePaths {
    workspace::WorkspacePaths {
        repo_root: repo_root.to_path_buf(),
        runtime_root: workspace.runtime_root.clone(),
        auth_dir: workspace.auth_dir.clone(),
        cache_dir: workspace.cache_dir.clone(),
        state_dir: workspace.state_dir.clone(),
        vault_dir: workspace.vault_dir.clone(),
        exports_dir: workspace.exports_dir.clone(),
        logs_dir: workspace.logs_dir.clone(),
    }
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
    use super::{discover_repo_root, runtime_paths_from_config};
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

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

        let runtime_root = unique_temp_dir("mailroom-lib-alt-runtime");
        fs::create_dir_all(repo_root.join(".mailroom")).unwrap();
        fs::write(
            repo_root.join(".mailroom/config.toml"),
            format!(
                r#"
[workspace]
runtime_root = "{}"
"#,
                runtime_root.display()
            ),
        )
        .unwrap();

        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let config_report = resolve(&paths).unwrap();
        let configured_paths =
            runtime_paths_from_config(&repo_root, &config_report.config.workspace);

        assert_eq!(configured_paths.runtime_root, runtime_root);
        assert_eq!(configured_paths.state_dir, runtime_root.join("state"));

        fs::remove_dir_all(repo_root).unwrap();
        if runtime_root.exists() {
            fs::remove_dir_all(runtime_root).unwrap();
        }
    }

    #[test]
    fn configured_runtime_paths_are_used_for_workspace_doctor() {
        let repo_root = unique_temp_dir("mailroom-configured-runtime-doctor");
        if repo_root.exists() {
            fs::remove_dir_all(&repo_root).unwrap();
        }

        let runtime_root = unique_temp_dir("mailroom-doctor-alt-runtime");
        fs::create_dir_all(repo_root.join(".mailroom")).unwrap();
        fs::write(
            repo_root.join(".mailroom/config.toml"),
            format!(
                r#"
[workspace]
runtime_root = "{}"
"#,
                runtime_root.display()
            ),
        )
        .unwrap();

        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let config_report = resolve(&paths).unwrap();
        let configured_paths =
            runtime_paths_from_config(&repo_root, &config_report.config.workspace);
        configured_paths.ensure_runtime_dirs().unwrap();

        let report = crate::workspace::DoctorReport::inspect(&configured_paths);

        assert!(report.runtime_root_exists);
        assert!(report.path_statuses.iter().all(|status| status.exists));

        fs::remove_dir_all(repo_root).unwrap();
        if runtime_root.exists() {
            fs::remove_dir_all(runtime_root).unwrap();
        }
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
