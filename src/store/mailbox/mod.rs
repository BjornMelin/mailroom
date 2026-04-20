mod read;
mod search;
#[cfg(test)]
mod tests;
mod types;
mod write;

pub(crate) use read::{
    get_attachment_detail, get_latest_thread_message, get_sync_state, inspect_mailbox,
    list_attachments, resolve_label_ids_by_names,
};
pub(crate) use search::search_messages;
pub(crate) use types::{
    AttachmentDetailRecord, AttachmentExportEventInput, AttachmentListItem, AttachmentListQuery,
    AttachmentVaultStateUpdate, GmailAttachmentUpsertInput, GmailAutomationHeaders,
    GmailMessageUpsertInput, MailboxDoctorReport, MailboxReadError, MailboxWriteError, SearchQuery,
    SearchResult, SyncMode, SyncStateRecord, SyncStateUpdate, SyncStatus, ThreadMessageSnapshot,
};
pub(crate) use write::{
    IncrementalSyncCommit, commit_full_sync, commit_incremental_sync, record_attachment_export,
    set_attachment_vault_state, upsert_sync_state,
};
#[cfg(test)]
pub(crate) use write::{
    apply_incremental_changes, delete_messages, replace_labels, replace_labels_and_report_reindex,
    replace_messages, upsert_messages,
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
