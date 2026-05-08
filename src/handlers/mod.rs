mod account;
mod attachment;
mod audit;
mod auth;
mod automation;
mod config;
mod doctor;
mod gmail;
mod search;
mod store;
mod sync;
mod tui;
mod workflow;
mod workspace;

pub(crate) use account::handle_account_command;
pub(crate) use attachment::handle_attachment_command;
pub(crate) use audit::handle_audit_command;
pub(crate) use auth::handle_auth_command;
pub(crate) use automation::handle_automation_command;
pub(crate) use config::{handle_config_command, handle_paths_command};
pub(crate) use doctor::handle_doctor_command;
pub(crate) use gmail::handle_gmail_command;
pub(crate) use search::handle_search_command;
pub(crate) use store::handle_store_command;
pub(crate) use sync::handle_sync_command;
pub(crate) use tui::handle_tui_command;
pub(crate) use workflow::{
    handle_cleanup_command, handle_draft_command, handle_triage_command, handle_workflow_command,
};
pub(crate) use workspace::handle_workspace_command;
