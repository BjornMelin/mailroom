mod model;
mod output;
mod service;

pub use model::{
    CleanupPreview, WorkflowAction, WorkflowActionReport, WorkflowListReport, WorkflowShowReport,
};
pub(crate) use service::WorkflowServiceError;
pub use service::{
    cleanup_archive, cleanup_label, cleanup_trash, draft_attach_add, draft_attach_remove,
    draft_body_set, draft_send, draft_start, list_workflows, promote_workflow, set_triage,
    show_workflow, snooze_workflow,
};
