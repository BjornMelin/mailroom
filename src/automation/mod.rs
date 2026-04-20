mod model;
mod output;
mod rules;
mod service;

pub use model::{AutomationRunRequest, DEFAULT_AUTOMATION_RUN_LIMIT};
pub(crate) use service::AutomationServiceError;
pub use service::{apply_run, run_preview, show_run, validate_rules};
