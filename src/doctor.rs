use crate::auth::{self, AuthStatusReport};
use crate::config::ConfigReport;
use crate::store::{self, StoreDoctorReport};
use crate::workspace::{DoctorReport as WorkspaceDoctorReport, WorkspacePaths};
use anyhow::Result;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub config: ConfigReport,
    pub workspace: WorkspaceDoctorReport,
    pub store: StoreDoctorReport,
    pub auth: AuthStatusReport,
}

impl DoctorReport {
    pub fn inspect(paths: &WorkspacePaths, config_report: ConfigReport) -> Result<Self> {
        let workspace = WorkspaceDoctorReport::inspect(paths);
        let store = store::inspect(config_report.clone())?;
        let auth = auth::status(&config_report)?;

        Ok(Self {
            config: config_report,
            workspace,
            store,
            auth,
        })
    }

    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            println!("repo_root={}", self.workspace.repo_root.display());
            println!("runtime_root_exists={}", self.workspace.runtime_root_exists);
            println!("database_exists={}", self.store.database_exists);
            println!("credential_exists={}", self.auth.credential_exists);
            println!("credential_path={}", self.auth.credential_path.display());
            println!("configured={}", self.auth.configured);
            match &self.auth.active_account {
                Some(account) => println!("active_account_email={}", account.email_address),
                None => println!("active_account_email=<none>"),
            }
        }

        Ok(())
    }
}
