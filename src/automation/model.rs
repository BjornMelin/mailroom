use crate::audit::VerificationAuditReport;
use crate::mailbox::SyncRunReport;
use crate::store::automation::{AutomationActionKind, AutomationRunDetail};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const DEFAULT_AUTOMATION_RUN_LIMIT: usize = 250;
pub const DEFAULT_AUTOMATION_ROLLOUT_LIMIT: usize = 10;

#[derive(Debug, Clone)]
pub struct AutomationRunRequest {
    pub rule_ids: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct AutomationRolloutRequest {
    pub rule_ids: Vec<String>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct AutomationPruneRequest {
    pub older_than_days: u32,
    pub statuses: Vec<AutomationPruneStatus>,
    pub execute: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutomationPruneStatus {
    Previewed,
    Applied,
    ApplyFailed,
}

impl AutomationPruneStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Previewed => "previewed",
            Self::Applied => "applied",
            Self::ApplyFailed => "apply_failed",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationRulesValidateReport {
    pub path: PathBuf,
    pub rule_file_hash: String,
    pub rule_count: usize,
    pub enabled_rule_count: usize,
    pub rules: Vec<AutomationRuleSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationRuleSummary {
    pub id: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub priority: i64,
    pub action_kind: AutomationActionKind,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationRunPreviewReport {
    pub detail: AutomationRunDetail,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationRolloutReport {
    pub verification: VerificationAuditReport,
    pub rules: Option<AutomationRulesValidateReport>,
    pub selected_rule_ids: Vec<String>,
    pub selected_rule_count: usize,
    pub candidate_count: usize,
    pub candidates: Vec<AutomationRolloutCandidateSummary>,
    pub blocked_rule_ids: Vec<String>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub next_steps: Vec<String>,
    pub command_plan: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationRolloutCandidateSummary {
    pub rule_id: String,
    pub thread_id: String,
    pub message_id: String,
    pub action_kind: String,
    pub subject: String,
    pub from_address: Option<String>,
    pub label_names: Vec<String>,
    pub has_list_unsubscribe: bool,
    pub matched_predicates: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationShowReport {
    pub detail: AutomationRunDetail,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationApplyReport {
    pub detail: AutomationRunDetail,
    pub execute: bool,
    pub applied_candidate_count: usize,
    pub failed_candidate_count: usize,
    pub sync_report: Option<SyncRunReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomationPruneReport {
    pub account_id: String,
    pub execute: bool,
    pub older_than_days: u32,
    pub cutoff_epoch_s: i64,
    pub statuses: Vec<String>,
    pub matched_run_count: i64,
    pub matched_candidate_count: i64,
    pub matched_event_count: i64,
    pub deleted_run_count: i64,
    pub warnings: Vec<String>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRuleSet {
    #[serde(default)]
    pub rules: Vec<AutomationRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationRule {
    pub id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "rule_enabled_default")]
    pub enabled: bool,
    #[serde(default = "rule_priority_default")]
    pub priority: i64,
    #[serde(rename = "match")]
    pub matcher: AutomationMatchRule,
    pub action: AutomationRuleAction,
}

impl AutomationRule {
    pub fn action_kind(&self) -> AutomationActionKind {
        match &self.action {
            AutomationRuleAction::Archive => AutomationActionKind::Archive,
            AutomationRuleAction::Trash => AutomationActionKind::Trash,
            AutomationRuleAction::Label { .. } => AutomationActionKind::Label,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AutomationMatchRule {
    #[serde(default)]
    pub from_address: Option<String>,
    #[serde(default)]
    pub subject_contains: Vec<String>,
    #[serde(default)]
    pub label_any: Vec<String>,
    #[serde(default)]
    pub older_than_days: Option<u32>,
    #[serde(default)]
    pub has_attachments: Option<bool>,
    #[serde(default)]
    pub has_list_unsubscribe: Option<bool>,
    #[serde(default)]
    pub list_id_contains: Vec<String>,
    #[serde(default)]
    pub precedence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AutomationRuleAction {
    Archive,
    Trash,
    Label {
        #[serde(default)]
        add: Vec<String>,
        #[serde(default)]
        remove: Vec<String>,
    },
}

fn rule_enabled_default() -> bool {
    true
}

fn rule_priority_default() -> i64 {
    100
}
