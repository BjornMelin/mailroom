mod headers;
mod model;
mod output;
mod rules;
mod service;
mod suggestions;

pub use model::{
    AutomationPruneRequest, AutomationPruneStatus, AutomationRolloutRequest,
    AutomationRulesSuggestRequest, AutomationRunRequest, DEFAULT_AUTOMATION_ROLLOUT_LIMIT,
    DEFAULT_AUTOMATION_RUN_LIMIT, DEFAULT_AUTOMATION_SUGGESTION_LIMIT,
    DEFAULT_AUTOMATION_SUGGESTION_MIN_THREAD_COUNT, DEFAULT_AUTOMATION_SUGGESTION_OLDER_THAN_DAYS,
    DEFAULT_AUTOMATION_SUGGESTION_SAMPLE_LIMIT,
};
pub(crate) use service::AutomationServiceError;
pub use service::{
    apply_run, prune_runs, rollout, run_preview, show_run, suggest_rules, validate_rules,
};
