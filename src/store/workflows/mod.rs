mod read;
#[cfg(test)]
mod tests;
mod types;
mod write;

pub(crate) use read::{
    get_workflow_detail, inspect_workflows, list_workflows, lookup_workflow_account_id,
};
pub(crate) use types::{
    ApplyCleanupInput, AttachmentInput, CleanupAction, DraftAttachmentRecord, DraftRevisionDetail,
    DraftRevisionRecord, MarkSentInput, PromoteWorkflowInput, RemoteDraftStateInput, ReplyMode,
    RestoreDraftStateInput, RetireDraftStateInput, SetTriageStateInput, SnoozeWorkflowInput,
    TriageBucket, UpsertDraftRevisionInput, WorkflowDetail, WorkflowDoctorReport,
    WorkflowEventRecord, WorkflowListFilter, WorkflowMessageSnapshot, WorkflowRecord,
    WorkflowStage, WorkflowStoreReadError, WorkflowStoreWriteError,
};
#[cfg(test)]
pub(crate) use write::set_remote_draft_state;
pub(crate) use write::{
    apply_cleanup, mark_sent, restore_draft_state_with_expected_version, retire_draft_state,
    set_remote_draft_state_with_expected_version, set_triage_state, snooze_workflow,
    upsert_draft_revision, upsert_stage,
};

fn is_missing_workflow_table_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("no such table: thread_workflows")
                || message.contains("no such table: thread_workflow_events")
                || message.contains("no such table: thread_draft_revisions")
                || message.contains("no such table: thread_draft_attachments")
    )
}
