use anyhow::Result;
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

    pub fn ensure_runtime_dirs(&self) -> Result<Vec<PathBuf>> {
        let mut created = Vec::new();
        for dir in self.runtime_dirs() {
            let existed = dir.exists();
            fs::create_dir_all(dir)?;
            if is_sensitive_runtime_dir(dir) {
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
            println!("{}", serde_json::to_string_pretty(self)?);
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

    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!("repo_root={}", self.repo_root.display());
            println!("runtime_root_exists={}", self.runtime_root_exists);
            for status in &self.path_statuses {
                println!("{} exists={}", status.path.display(), status.exists);
            }
        }
        Ok(())
    }
}

fn path_exists(path: &Path) -> bool {
    path.exists()
}

fn is_sensitive_runtime_dir(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| matches!(name.to_str(), Some("auth" | "vault")))
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
