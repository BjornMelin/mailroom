mod read;
mod search;
#[cfg(test)]
mod tests;
mod types;
mod write;

pub(crate) use read::{
    get_attachment_detail, get_full_sync_checkpoint, get_latest_thread_message,
    get_mailbox_coverage, get_sync_pacing_state, get_sync_state, inspect_mailbox,
    inspect_mailbox_account, list_attachments, list_label_usage, resolve_label_ids_by_names,
};
pub(crate) use search::search_messages;
pub(crate) use types::{
    AttachmentDetailRecord, AttachmentExportEventInput, AttachmentListItem, AttachmentListQuery,
    AttachmentVaultStateUpdate, FullSyncCheckpointRecord, FullSyncCheckpointStatus,
    FullSyncCheckpointUpdate, GmailAttachmentUpsertInput, GmailAutomationHeaders,
    GmailMessageUpsertInput, LabelUsageRecord, MailboxCoverageReport, MailboxDoctorReport,
    MailboxReadError, MailboxWriteError, SearchQuery, SearchResult, SyncMode,
    SyncPacingPressureKind, SyncPacingStateRecord, SyncPacingStateUpdate, SyncStateRecord,
    SyncStateUpdate, SyncStatus, ThreadMessageSnapshot,
};
pub(crate) use write::{
    IncrementalSyncCommit, commit_incremental_sync, finalize_full_sync_from_stage,
    prepare_full_sync_checkpoint, record_attachment_export, reset_full_sync_progress,
    set_attachment_vault_state, stage_full_sync_page_and_update_checkpoint,
    update_full_sync_checkpoint_labels, upsert_sync_pacing_state, upsert_sync_state,
};
#[cfg(test)]
pub(crate) use write::{
    apply_incremental_changes, commit_full_sync, delete_messages, replace_labels,
    replace_labels_and_report_reindex, replace_messages, reset_full_sync_stage,
    stage_full_sync_labels, stage_full_sync_messages, upsert_messages,
};

use std::collections::BTreeSet;

const LABEL_SEPARATOR: char = '\u{001F}';

fn is_missing_mailbox_table_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("no such table: gmail_")
                || message.contains("no such table: attachment_export_events")
    )
}

fn unique_sorted_strings(values: &[String]) -> Vec<String> {
    values
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
