use crate::workspace::WorkspacePaths;
use anyhow::Result;
use directories::ProjectDirs;
use figment::{
    Figment, Provider,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const CONFIG_QUALIFIER: &str = "com";
const CONFIG_ORGANIZATION: &str = "BjornMelin";
const CONFIG_APPLICATION: &str = "mailroom";
const ENV_PREFIX: &str = "MAILROOM_";
const ENV_SPLIT: &str = "__";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailroomConfig {
    pub workspace: WorkspaceConfig,
    pub store: StoreConfig,
}

impl MailroomConfig {
    pub fn defaults_for(paths: &WorkspacePaths) -> Self {
        let workspace = WorkspaceConfig::defaults_for(paths);
        Self {
            store: StoreConfig::defaults_for(&workspace),
            workspace,
        }
    }

    fn with_overrides(defaults: Self, repo_root: &Path, overrides: PartialMailroomConfig) -> Self {
        let workspace = defaults
            .workspace
            .with_overrides(repo_root, overrides.workspace);
        let store = defaults
            .store
            .with_overrides(repo_root, &workspace, overrides.store);
        Self { workspace, store }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    pub runtime_root: PathBuf,
    pub auth_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub state_dir: PathBuf,
    pub vault_dir: PathBuf,
    pub exports_dir: PathBuf,
    pub logs_dir: PathBuf,
}

impl WorkspaceConfig {
    fn defaults_for(paths: &WorkspacePaths) -> Self {
        Self {
            runtime_root: paths.runtime_root.clone(),
            auth_dir: paths.auth_dir.clone(),
            cache_dir: paths.cache_dir.clone(),
            state_dir: paths.state_dir.clone(),
            vault_dir: paths.vault_dir.clone(),
            exports_dir: paths.exports_dir.clone(),
            logs_dir: paths.logs_dir.clone(),
        }
    }

    fn with_overrides(self, repo_root: &Path, overrides: PartialWorkspaceConfig) -> Self {
        let runtime_root = normalize_configured_path(
            overrides.runtime_root.unwrap_or(self.runtime_root),
            repo_root,
        );
        Self {
            auth_dir: overrides
                .auth_dir
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| runtime_root.join("auth")),
            cache_dir: overrides
                .cache_dir
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| runtime_root.join("cache")),
            state_dir: overrides
                .state_dir
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| runtime_root.join("state")),
            vault_dir: overrides
                .vault_dir
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| runtime_root.join("vault")),
            exports_dir: overrides
                .exports_dir
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| runtime_root.join("exports")),
            logs_dir: overrides
                .logs_dir
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| runtime_root.join("logs")),
            runtime_root,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreConfig {
    pub database_path: PathBuf,
    pub busy_timeout_ms: u64,
}

impl StoreConfig {
    fn defaults_for(workspace: &WorkspaceConfig) -> Self {
        Self {
            database_path: workspace.state_dir.join("mailroom.sqlite3"),
            busy_timeout_ms: 5_000,
        }
    }

    fn with_overrides(
        self,
        repo_root: &Path,
        workspace: &WorkspaceConfig,
        overrides: PartialStoreConfig,
    ) -> Self {
        Self {
            database_path: overrides
                .database_path
                .map(|path| normalize_configured_path(path, repo_root))
                .unwrap_or_else(|| workspace.state_dir.join("mailroom.sqlite3")),
            busy_timeout_ms: overrides.busy_timeout_ms.unwrap_or(self.busy_timeout_ms),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PartialMailroomConfig {
    #[serde(default)]
    workspace: PartialWorkspaceConfig,
    #[serde(default)]
    store: PartialStoreConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PartialWorkspaceConfig {
    runtime_root: Option<PathBuf>,
    auth_dir: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    vault_dir: Option<PathBuf>,
    exports_dir: Option<PathBuf>,
    logs_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PartialStoreConfig {
    database_path: Option<PathBuf>,
    busy_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigLocations {
    pub user_config_path: Option<PathBuf>,
    pub user_config_exists: bool,
    pub repo_config_path: PathBuf,
    pub repo_config_exists: bool,
}

impl ConfigLocations {
    pub fn discover(paths: &WorkspacePaths) -> Self {
        let user_config_path =
            ProjectDirs::from(CONFIG_QUALIFIER, CONFIG_ORGANIZATION, CONFIG_APPLICATION)
                .map(|dirs| dirs.config_dir().join("config.toml"));
        let user_config_exists = user_config_path.as_ref().is_some_and(|path| path.exists());

        let repo_config_path = paths.runtime_root.join("config.toml");
        let repo_config_exists = repo_config_path.exists();

        Self {
            user_config_path,
            user_config_exists,
            repo_config_path,
            repo_config_exists,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigReport {
    pub config: MailroomConfig,
    pub locations: ConfigLocations,
}

impl ConfigReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!(
                "user_config={}",
                display_optional_path(self.locations.user_config_path.as_deref())
            );
            println!("user_config_exists={}", self.locations.user_config_exists);
            println!("repo_config={}", self.locations.repo_config_path.display());
            println!("repo_config_exists={}", self.locations.repo_config_exists);
            println!(
                "database_path={}",
                self.config.store.database_path.display()
            );
            println!("busy_timeout_ms={}", self.config.store.busy_timeout_ms);
        }

        Ok(())
    }
}

pub fn resolve(paths: &WorkspacePaths) -> Result<ConfigReport> {
    let defaults = MailroomConfig::defaults_for(paths);
    let locations = ConfigLocations::discover(paths);
    resolve_with_override_provider(
        defaults,
        &paths.repo_root,
        locations,
        Env::prefixed(ENV_PREFIX).split(ENV_SPLIT),
    )
}

fn resolve_with_override_provider<P>(
    defaults: MailroomConfig,
    repo_root: &Path,
    locations: ConfigLocations,
    provider: P,
) -> Result<ConfigReport>
where
    P: Provider,
{
    let mut figment = Figment::from(Serialized::defaults(PartialMailroomConfig::default()));

    if locations.user_config_exists
        && let Some(user_config_path) = &locations.user_config_path
    {
        figment = figment.merge(Toml::file(user_config_path));
    }

    if locations.repo_config_exists {
        figment = figment.merge(Toml::file(&locations.repo_config_path));
    }

    figment = figment.merge(provider);

    let overrides: PartialMailroomConfig = figment.extract()?;
    let config = MailroomConfig::with_overrides(defaults, repo_root, overrides);
    Ok(ConfigReport { config, locations })
}

fn display_optional_path(path: Option<&Path>) -> String {
    match path {
        Some(path) => path.display().to_string(),
        None => String::from("<unavailable>"),
    }
}

fn normalize_configured_path(path: PathBuf, repo_root: &Path) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        repo_root.join(path)
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigLocations, MailroomConfig, resolve_with_override_provider};
    use crate::workspace::WorkspacePaths;
    use figment::providers::{Format, Toml};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn defaults_follow_repo_local_workspace_paths() {
        let repo_root = unique_temp_dir("mailroom-config-defaults");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let defaults = MailroomConfig::defaults_for(&paths);

        assert_eq!(defaults.workspace.runtime_root, repo_root.join(".mailroom"));
        assert_eq!(
            defaults.store.database_path,
            repo_root.join(".mailroom/state/mailroom.sqlite3")
        );
    }

    #[test]
    fn repo_config_overrides_user_config_and_override_provider_wins_last() {
        let repo_root = unique_temp_dir("mailroom-config-precedence");
        let user_config = repo_root.join("user.toml");
        let repo_config = repo_root.join(".mailroom/config.toml");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let defaults = MailroomConfig::defaults_for(&paths);

        fs::create_dir_all(&repo_root).unwrap();
        fs::create_dir_all(repo_config.parent().unwrap()).unwrap();
        fs::write(
            &user_config,
            r#"
[store]
busy_timeout_ms = 6000
"#,
        )
        .unwrap();
        fs::write(
            &repo_config,
            r#"
[store]
busy_timeout_ms = 7000
"#,
        )
        .unwrap();

        let locations = ConfigLocations {
            user_config_path: Some(user_config),
            user_config_exists: true,
            repo_config_path: repo_config,
            repo_config_exists: true,
        };

        let report = resolve_with_override_provider(
            defaults,
            &repo_root,
            locations,
            Toml::string(
                r#"
[store]
busy_timeout_ms = 8000
"#,
            ),
        )
        .unwrap();

        assert_eq!(report.config.store.busy_timeout_ms, 8_000);
    }

    #[test]
    fn runtime_root_override_recomputes_derived_workspace_and_store_paths() {
        let repo_root = unique_temp_dir("mailroom-config-runtime-root");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let defaults = MailroomConfig::defaults_for(&paths);
        let locations = ConfigLocations {
            user_config_path: None,
            user_config_exists: false,
            repo_config_path: repo_root.join(".mailroom/config.toml"),
            repo_config_exists: false,
        };

        let report = resolve_with_override_provider(
            defaults,
            &repo_root,
            locations,
            Toml::string(
                r#"
[workspace]
runtime_root = "/tmp/mailroom-alt"
"#,
            ),
        )
        .unwrap();

        assert_eq!(
            report.config.workspace.runtime_root,
            PathBuf::from("/tmp/mailroom-alt")
        );
        assert_eq!(
            report.config.workspace.auth_dir,
            PathBuf::from("/tmp/mailroom-alt/auth")
        );
        assert_eq!(
            report.config.workspace.state_dir,
            PathBuf::from("/tmp/mailroom-alt/state")
        );
        assert_eq!(
            report.config.store.database_path,
            PathBuf::from("/tmp/mailroom-alt/state/mailroom.sqlite3")
        );
    }

    #[test]
    fn explicit_state_dir_and_database_path_overrides_win_over_derived_defaults() {
        let repo_root = unique_temp_dir("mailroom-config-state-dir");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let defaults = MailroomConfig::defaults_for(&paths);
        let locations = ConfigLocations {
            user_config_path: None,
            user_config_exists: false,
            repo_config_path: repo_root.join(".mailroom/config.toml"),
            repo_config_exists: false,
        };

        let report = resolve_with_override_provider(
            defaults,
            &repo_root,
            locations,
            Toml::string(
                r#"
[workspace]
runtime_root = "/tmp/mailroom-alt"
state_dir = "/tmp/mailroom-custom-state"
logs_dir = "/tmp/mailroom-logs"

[store]
database_path = "/tmp/mailroom-custom-state/custom.sqlite3"
"#,
            ),
        )
        .unwrap();

        assert_eq!(
            report.config.workspace.auth_dir,
            PathBuf::from("/tmp/mailroom-alt/auth")
        );
        assert_eq!(
            report.config.workspace.state_dir,
            PathBuf::from("/tmp/mailroom-custom-state")
        );
        assert_eq!(
            report.config.workspace.logs_dir,
            PathBuf::from("/tmp/mailroom-logs")
        );
        assert_eq!(
            report.config.store.database_path,
            PathBuf::from("/tmp/mailroom-custom-state/custom.sqlite3")
        );
    }

    #[test]
    fn relative_runtime_root_and_database_path_are_resolved_from_repo_root() {
        let repo_root = unique_temp_dir("mailroom-config-relative-paths");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        let defaults = MailroomConfig::defaults_for(&paths);
        let locations = ConfigLocations {
            user_config_path: None,
            user_config_exists: false,
            repo_config_path: repo_root.join(".mailroom/config.toml"),
            repo_config_exists: false,
        };

        let report = resolve_with_override_provider(
            defaults,
            &repo_root,
            locations,
            Toml::string(
                r#"
[workspace]
runtime_root = ".mailroom-alt"

[store]
database_path = ".mailroom/custom.sqlite3"
"#,
            ),
        )
        .unwrap();

        assert_eq!(
            report.config.workspace.runtime_root,
            repo_root.join(".mailroom-alt")
        );
        assert_eq!(
            report.config.workspace.auth_dir,
            repo_root.join(".mailroom-alt/auth")
        );
        assert_eq!(
            report.config.store.database_path,
            repo_root.join(".mailroom/custom.sqlite3")
        );
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
