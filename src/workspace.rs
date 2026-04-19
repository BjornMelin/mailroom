use crate::config::WorkspaceConfig;
use anyhow::{Result, anyhow};
use serde::Serialize;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize)]
pub struct WorkspacePaths {
    pub repo_root: PathBuf,
    pub runtime_root: PathBuf,
    pub auth_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub state_dir: PathBuf,
    pub vault_dir: PathBuf,
    pub exports_dir: PathBuf,
    pub logs_dir: PathBuf,
}

impl WorkspacePaths {
    pub fn from_repo_root(repo_root: PathBuf) -> Self {
        let runtime_root = repo_root.join(".mailroom");
        Self {
            repo_root,
            auth_dir: runtime_root.join("auth"),
            cache_dir: runtime_root.join("cache"),
            state_dir: runtime_root.join("state"),
            vault_dir: runtime_root.join("vault"),
            exports_dir: runtime_root.join("exports"),
            logs_dir: runtime_root.join("logs"),
            runtime_root,
        }
    }

    pub fn from_config(repo_root: PathBuf, workspace: &WorkspaceConfig) -> Self {
        Self {
            repo_root,
            runtime_root: workspace.runtime_root.clone(),
            auth_dir: workspace.auth_dir.clone(),
            cache_dir: workspace.cache_dir.clone(),
            state_dir: workspace.state_dir.clone(),
            vault_dir: workspace.vault_dir.clone(),
            exports_dir: workspace.exports_dir.clone(),
            logs_dir: workspace.logs_dir.clone(),
        }
    }

    pub fn ensure_runtime_dirs(&self) -> Result<Vec<PathBuf>> {
        let mut created = Vec::new();
        for dir in self.runtime_dirs() {
            let existed = dir.exists();
            fs::create_dir_all(dir)?;
            if self.is_sensitive_runtime_dir(dir) {
                set_owner_only_permissions(dir)?;
            }
            if !existed {
                created.push(dir.to_path_buf());
            }
        }
        Ok(created)
    }

    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("repo_root={}", self.repo_root.display());
            println!("runtime_root={}", self.runtime_root.display());
            println!("auth_dir={}", self.auth_dir.display());
            println!("cache_dir={}", self.cache_dir.display());
            println!("state_dir={}", self.state_dir.display());
            println!("vault_dir={}", self.vault_dir.display());
            println!("exports_dir={}", self.exports_dir.display());
            println!("logs_dir={}", self.logs_dir.display());
        }
        Ok(())
    }

    fn runtime_dirs(&self) -> [&Path; 6] {
        [
            self.auth_dir.as_path(),
            self.cache_dir.as_path(),
            self.state_dir.as_path(),
            self.vault_dir.as_path(),
            self.exports_dir.as_path(),
            self.logs_dir.as_path(),
        ]
    }

    fn is_sensitive_runtime_dir(&self, path: &Path) -> bool {
        self.sensitive_runtime_dirs().contains(&path)
    }

    fn sensitive_runtime_dirs(&self) -> [&Path; 4] {
        [
            self.auth_dir.as_path(),
            self.state_dir.as_path(),
            self.vault_dir.as_path(),
            self.exports_dir.as_path(),
        ]
    }
}

pub fn configured_repo_root_from_locations(repo_config_path: &Path) -> Result<PathBuf> {
    repo_config_path
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            anyhow!(
                "could not derive repo root from {}",
                repo_config_path.display()
            )
        })
}

#[derive(Debug, Clone, Serialize)]
pub struct PathStatus {
    pub path: PathBuf,
    pub exists: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub repo_root: PathBuf,
    pub runtime_root_exists: bool,
    pub path_statuses: Vec<PathStatus>,
}

impl DoctorReport {
    pub fn inspect(paths: &WorkspacePaths) -> Self {
        let path_statuses = paths
            .runtime_dirs()
            .into_iter()
            .map(|path| PathStatus {
                exists: path_exists(path),
                path: path.to_path_buf(),
            })
            .collect();

        Self {
            repo_root: paths.repo_root.clone(),
            runtime_root_exists: path_exists(&paths.runtime_root),
            path_statuses,
        }
    }
}

fn path_exists(path: &Path) -> bool {
    path.exists()
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::WorkspacePaths;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(unix)]
    #[test]
    fn ensure_runtime_dirs_hardens_sensitive_runtime_permissions() {
        let repo_root = unique_temp_dir("mailroom-workspace-perms");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());

        paths.ensure_runtime_dirs().unwrap();

        for path in [
            &paths.auth_dir,
            &paths.state_dir,
            &paths.vault_dir,
            &paths.exports_dir,
        ] {
            let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o700,
                "expected owner-only permissions for {}",
                path.display()
            );
        }

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn ensure_runtime_dirs_hardens_configured_sensitive_directories() {
        let repo_root = unique_temp_dir("mailroom-workspace-custom-perms");
        let runtime_root = repo_root.join(".mailroom-custom");
        let paths = WorkspacePaths {
            repo_root: repo_root.clone(),
            runtime_root: runtime_root.clone(),
            auth_dir: runtime_root.join("gmail-secrets"),
            cache_dir: runtime_root.join("cache"),
            state_dir: runtime_root.join("gmail-state"),
            vault_dir: runtime_root.join("vault-storage"),
            exports_dir: runtime_root.join("exports-out"),
            logs_dir: runtime_root.join("logs"),
        };

        paths.ensure_runtime_dirs().unwrap();

        for path in [
            &paths.auth_dir,
            &paths.state_dir,
            &paths.vault_dir,
            &paths.exports_dir,
        ] {
            let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o700,
                "expected owner-only permissions for {}",
                path.display()
            );
        }

        fs::remove_dir_all(repo_root).unwrap();
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
