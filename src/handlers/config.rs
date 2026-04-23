use crate::cli::ConfigCommand;
use crate::{config, configured_paths, workspace};
use anyhow::Result;

pub(crate) fn handle_config_command(
    paths: &workspace::WorkspacePaths,
    command: ConfigCommand,
) -> Result<()> {
    match command {
        ConfigCommand::Show { json } => config::resolve(paths)?.print(json)?,
    }

    Ok(())
}

pub(crate) fn handle_paths_command(paths: &workspace::WorkspacePaths, json: bool) -> Result<()> {
    match config::resolve(paths) {
        Ok(config_report) => configured_paths(&config_report)?.print(json)?,
        Err(error) => {
            eprintln!(
                "warning: config::resolve failed for `mailroom paths`; falling back to repo-local paths: {error}\n{error:#?}"
            );
            paths.print(json)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::handle_paths_command;
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

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

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
