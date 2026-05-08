mod model;
mod output;
mod service;

pub use model::{
    CleanupPreview, WorkflowAction, WorkflowActionReport, WorkflowListReport, WorkflowShowReport,
};
pub(crate) use service::{WorkflowServiceError, cleanup_tracked_thread_for_automation};
pub use service::{
    cleanup_archive, cleanup_label, cleanup_trash, draft_attach_add, draft_attach_remove,
    draft_body_set, draft_send, draft_start, list_workflows, list_workflows_read_only,
    promote_workflow, set_triage, show_workflow, snooze_workflow,
};
