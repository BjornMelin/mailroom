mod model;
mod output;
mod rules;
mod service;

pub use model::{
    AutomationPruneRequest, AutomationPruneStatus, AutomationRolloutRequest, AutomationRunRequest,
    DEFAULT_AUTOMATION_ROLLOUT_LIMIT, DEFAULT_AUTOMATION_RUN_LIMIT,
};
pub(crate) use service::AutomationServiceError;
pub use service::{apply_run, prune_runs, rollout, run_preview, show_run, validate_rules};
