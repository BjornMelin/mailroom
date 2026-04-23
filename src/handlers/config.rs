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
                "warning: config::resolve failed for `mailroom paths`; falling back to repo-local paths: {error}"
            );
            paths.print(json)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use serde_json::Value;
    use std::fs;
    use std::process::Command;
    use tempfile::TempDir;

    #[test]
    fn paths_command_still_prints_repo_local_paths_when_config_is_malformed() {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| String::from("cargo"));
        let manifest_path = format!("{}/Cargo.toml", env!("CARGO_MANIFEST_DIR"));
        let repo_root = TempDir::with_prefix("mailroom-paths-malformed-config").unwrap();
        std::fs::create_dir(repo_root.path().join(".git")).unwrap();
        let config_dir = repo_root.path().join("config");
        fs::create_dir_all(&config_dir).unwrap();
        fs::create_dir_all(repo_root.path().join(".mailroom")).unwrap();
        fs::write(
            repo_root.path().join(".mailroom/config.toml"),
            "[workspace\n",
        )
        .unwrap();

        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        assert!(resolve(&paths).is_err());

        let output = Command::new(&cargo)
            .args([
                "run",
                "--quiet",
                "--manifest-path",
                &manifest_path,
                "--",
                "paths",
                "--json",
            ])
            .env("XDG_CONFIG_HOME", &config_dir)
            .env_remove("HOME")
            .current_dir(repo_root.path())
            .output()
            .unwrap();
        assert!(output.status.success());

        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains(
            "warning: config::resolve failed for `mailroom paths`; falling back to repo-local paths:"
        ));
        assert!(!stderr.contains("{error:#?}"));

        let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(stdout["success"], Value::Bool(true));
        assert_eq!(
            stdout["data"]["repo_root"],
            Value::String(repo_root.path().display().to_string())
        );
        assert_eq!(
            stdout["data"]["runtime_root"],
            Value::String(repo_root.path().join(".mailroom").display().to_string())
        );
    }
}
