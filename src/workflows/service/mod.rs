use tokio::task::JoinHandle;

mod cleanup;
mod draft_local;
mod draft_remote;
mod error;
mod message_build;
mod queries;

pub(crate) use cleanup::cleanup_tracked_thread_for_automation;
pub use cleanup::{cleanup_archive, cleanup_label, cleanup_trash};
pub use draft_local::{
    draft_attach_add, draft_attach_remove, draft_body_set, draft_send, draft_start,
};
pub(crate) use error::WorkflowServiceError;
pub use queries::{
    clear_workflow_snooze, list_workflows, list_workflows_read_only, promote_workflow, set_triage,
    show_workflow, snooze_workflow,
};

use error::WorkflowResult;

#[cfg(test)]
use draft_local::{
    AttachmentRemovalResult, attachment_input_from_path, remove_attachment_by_path_or_name,
};
#[cfg(test)]
use draft_remote::{
    RemoteDraftUpsert, mark_sent_after_remote_send, persist_remote_draft_state,
    retire_local_draft_then_delete_remote,
};
#[cfg(test)]
use message_build::build_reply_recipients;
#[cfg(test)]
use queries::best_effort_sync_report;

#[cfg(test)]
mod tests;

async fn join_blocking<T, E>(
    handle: JoinHandle<std::result::Result<T, E>>,
    operation: &'static str,
) -> WorkflowResult<T>
where
    E: Into<WorkflowServiceError>,
{
    handle
        .await
        .map_err(|source| WorkflowServiceError::BlockingTask { operation, source })?
        .map_err(Into::into)
}

fn current_epoch_seconds() -> WorkflowResult<i64> {
    crate::time::current_epoch_seconds().map_err(|source| WorkflowServiceError::Time { source })
}
