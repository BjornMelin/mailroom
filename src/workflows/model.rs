use crate::mailbox::SyncRunReport;
use crate::store;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowListReport {
    pub stage: Option<store::workflows::WorkflowStage>,
    pub triage_bucket: Option<store::workflows::TriageBucket>,
    pub workflows: Vec<store::workflows::WorkflowRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowShowReport {
    pub detail: store::workflows::WorkflowDetail,
}

#[derive(Debug, Clone, Serialize)]
pub struct CleanupPreview {
    pub action: store::workflows::CleanupAction,
    pub execute: bool,
    pub add_label_names: Vec<String>,
    pub remove_label_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowActionReport {
    pub action: String,
    pub workflow: store::workflows::WorkflowRecord,
    pub current_draft: Option<store::workflows::DraftRevisionDetail>,
    pub cleanup_preview: Option<CleanupPreview>,
    pub sync_report: Option<SyncRunReport>,
}
