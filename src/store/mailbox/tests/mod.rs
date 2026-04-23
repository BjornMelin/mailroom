use super::{
    AttachmentExportEventInput, AttachmentListQuery, AttachmentVaultStateUpdate,
    FullSyncCheckpointStatus, FullSyncCheckpointUpdate, FullSyncStagePageInput,
    GmailAttachmentUpsertInput, GmailMessageUpsertInput, IncrementalSyncCommit, MailboxReadError,
    MailboxWriteError, MailboxWriterConnection, SearchQuery, SyncMode, SyncPacingPressureKind,
    SyncPacingStateUpdate, SyncRunComparabilityKind, SyncRunOutcomeInput, SyncRunRegressionKind,
    SyncStateUpdate, SyncStatus, apply_incremental_changes, commit_full_sync,
    commit_incremental_sync, delete_messages, finalize_full_sync_from_stage,
    finalize_incremental_from_stage, get_attachment_detail, get_full_sync_checkpoint,
    get_sync_pacing_state, get_sync_run_summary, get_sync_run_summary_for_comparability,
    get_sync_state, inspect_mailbox, list_attachments, list_label_usage, list_sync_run_history,
    persist_failed_sync_outcome, persist_successful_sync_outcome, prepare_full_sync_checkpoint,
    record_attachment_export, replace_labels, replace_labels_and_report_reindex, replace_messages,
    reset_full_sync_stage, reset_incremental_sync_stage, search::build_plain_fts5_query,
    search_messages, set_attachment_vault_state, stage_full_sync_labels, stage_full_sync_messages,
    stage_full_sync_page_and_update_checkpoint, stage_incremental_sync_batch,
    update_full_sync_checkpoint_labels, upsert_messages, upsert_sync_pacing_state,
    upsert_sync_state,
};
use crate::config::resolve;
use crate::gmail::GmailLabel;
use crate::store::{accounts, init};
use crate::workspace::WorkspacePaths;
use std::time::Instant;
use tempfile::{Builder, TempDir};

#[path = "accounts.rs"]
mod account_isolation;
mod attachments;
mod labels;
mod search;
mod sync;

fn gmail_label(id: &str, name: &str, label_type: &str) -> GmailLabel {
    GmailLabel {
        id: id.to_owned(),
        name: name.to_owned(),
        label_type: label_type.to_owned(),
        message_list_visibility: None,
        label_list_visibility: None,
        messages_total: None,
        messages_unread: None,
        threads_total: None,
        threads_unread: None,
    }
}

fn mailbox_message(
    account_id: &str,
    message_id: &str,
    subject: &str,
    label_ids: &[&str],
    label_names_text: &str,
    attachment_part_ids: &[&str],
) -> GmailMessageUpsertInput {
    GmailMessageUpsertInput {
        account_id: account_id.to_owned(),
        message_id: message_id.to_owned(),
        thread_id: format!("thread-{message_id}"),
        history_id: format!("history-{message_id}"),
        internal_date_epoch_ms: 1_700_000_000_000,
        snippet: format!("snippet for {subject}"),
        subject: subject.to_owned(),
        from_header: String::from("Operator <operator@example.com>"),
        from_address: Some(String::from("operator@example.com")),
        recipient_headers: String::from("operator@example.com"),
        to_header: String::from("operator@example.com"),
        cc_header: String::new(),
        bcc_header: String::new(),
        reply_to_header: String::new(),
        size_estimate: 1024,
        automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
        label_ids: label_ids
            .iter()
            .map(|label_id| (*label_id).to_owned())
            .collect(),
        label_names_text: label_names_text.to_owned(),
        attachments: attachment_part_ids
            .iter()
            .map(|part_id| GmailAttachmentUpsertInput {
                attachment_key: format!("{message_id}:{part_id}"),
                part_id: (*part_id).to_owned(),
                gmail_attachment_id: Some(format!("att-{message_id}-{part_id}")),
                filename: format!("{message_id}-{part_id}.bin"),
                mime_type: String::from("application/octet-stream"),
                size_bytes: 256,
                content_disposition: Some(String::from("attachment")),
                content_id: None,
                is_inline: false,
            })
            .collect(),
    }
}

fn full_sync_state(account_id: &str, epoch_s: i64) -> SyncStateUpdate {
    SyncStateUpdate {
        account_id: account_id.to_owned(),
        cursor_history_id: Some(format!("cursor-{epoch_s}")),
        bootstrap_query: String::from("newer_than:90d"),
        last_sync_mode: SyncMode::Full,
        last_sync_status: SyncStatus::Ok,
        last_error: None,
        last_sync_epoch_s: epoch_s,
        last_full_sync_success_epoch_s: Some(epoch_s),
        last_incremental_sync_success_epoch_s: None,
        pipeline_enabled: false,
        pipeline_list_queue_high_water: 0,
        pipeline_write_queue_high_water: 0,
        pipeline_write_batch_count: 0,
        pipeline_writer_wait_ms: 0,
        pipeline_fetch_batch_count: 0,
        pipeline_fetch_batch_avg_ms: 0,
        pipeline_fetch_batch_max_ms: 0,
        pipeline_writer_tx_count: 0,
        pipeline_writer_tx_avg_ms: 0,
        pipeline_writer_tx_max_ms: 0,
        pipeline_reorder_buffer_high_water: 0,
        pipeline_staged_message_count: 0,
        pipeline_staged_delete_count: 0,
        pipeline_staged_attachment_count: 0,
    }
}

struct FullSyncCheckpointUpdateSpec<'a> {
    bootstrap_query: &'a str,
    status: FullSyncCheckpointStatus,
    next_page_token: Option<&'a str>,
    cursor_history_id: Option<&'a str>,
    pages_fetched: i64,
    messages_listed: i64,
    messages_upserted: i64,
    labels_synced: i64,
    started_at_epoch_s: i64,
    updated_at_epoch_s: i64,
}

fn full_sync_checkpoint_update(spec: FullSyncCheckpointUpdateSpec<'_>) -> FullSyncCheckpointUpdate {
    FullSyncCheckpointUpdate {
        bootstrap_query: spec.bootstrap_query.to_owned(),
        status: spec.status,
        next_page_token: spec.next_page_token.map(str::to_owned),
        cursor_history_id: spec.cursor_history_id.map(str::to_owned),
        pages_fetched: spec.pages_fetched,
        messages_listed: spec.messages_listed,
        messages_upserted: spec.messages_upserted,
        labels_synced: spec.labels_synced,
        started_at_epoch_s: spec.started_at_epoch_s,
        updated_at_epoch_s: spec.updated_at_epoch_s,
    }
}

fn unique_temp_dir(prefix: &str) -> TempDir {
    Builder::new().prefix(prefix).tempdir().unwrap()
}

struct SampleSyncRunOutcome {
    account_id: String,
    sync_mode: SyncMode,
    status: SyncStatus,
    started_at_epoch_s: i64,
    finished_at_epoch_s: i64,
    messages_listed: i64,
    duration_ms: i64,
    messages_per_second: f64,
    quota_pressure_retry_count: i64,
    concurrency_pressure_retry_count: i64,
    backend_retry_count: i64,
}

fn sample_sync_run_outcome(input: SampleSyncRunOutcome) -> SyncRunOutcomeInput {
    let comparability = match input.sync_mode {
        SyncMode::Full => {
            crate::store::mailbox::comparability_for_full_bootstrap_query("newer_than:90d")
        }
        SyncMode::Incremental => {
            crate::store::mailbox::comparability_for_incremental_workload(input.messages_listed, 0)
        }
    };
    SyncRunOutcomeInput {
        account_id: input.account_id,
        sync_mode: input.sync_mode,
        status: input.status,
        comparability_kind: comparability.kind,
        comparability_key: comparability.key,
        started_at_epoch_s: input.started_at_epoch_s,
        finished_at_epoch_s: input.finished_at_epoch_s,
        startup_seed_run_id: None,
        bootstrap_query: String::from("newer_than:90d"),
        cursor_history_id: Some(String::from("cursor-1")),
        fallback_from_history: false,
        resumed_from_checkpoint: false,
        pages_fetched: 1,
        messages_listed: input.messages_listed,
        messages_upserted: input.messages_listed,
        messages_deleted: 0,
        labels_synced: 3,
        checkpoint_reused_pages: 0,
        checkpoint_reused_messages_upserted: 0,
        pipeline_enabled: true,
        pipeline_list_queue_high_water: 1,
        pipeline_write_queue_high_water: 1,
        pipeline_write_batch_count: 1,
        pipeline_writer_wait_ms: 1,
        pipeline_fetch_batch_count: 1,
        pipeline_fetch_batch_avg_ms: 1,
        pipeline_fetch_batch_max_ms: 1,
        pipeline_writer_tx_count: 1,
        pipeline_writer_tx_avg_ms: 1,
        pipeline_writer_tx_max_ms: 1,
        pipeline_reorder_buffer_high_water: 1,
        pipeline_staged_message_count: input.messages_listed.max(0),
        pipeline_staged_delete_count: 0,
        pipeline_staged_attachment_count: 0,
        adaptive_pacing_enabled: true,
        quota_units_budget_per_minute: 12_000,
        message_fetch_concurrency: 4,
        quota_units_cap_per_minute: 12_000,
        message_fetch_concurrency_cap: 4,
        starting_quota_units_per_minute: 12_000,
        starting_message_fetch_concurrency: 4,
        effective_quota_units_per_minute: 12_000,
        effective_message_fetch_concurrency: 4,
        adaptive_downshift_count: 0,
        estimated_quota_units_reserved: 500,
        http_attempt_count: 3,
        retry_count: input.quota_pressure_retry_count
            + input.concurrency_pressure_retry_count
            + input.backend_retry_count,
        quota_pressure_retry_count: input.quota_pressure_retry_count,
        concurrency_pressure_retry_count: input.concurrency_pressure_retry_count,
        backend_retry_count: input.backend_retry_count,
        throttle_wait_count: 0,
        throttle_wait_ms: 0,
        retry_after_wait_ms: 0,
        duration_ms: input.duration_ms,
        pages_per_second: 10.0,
        messages_per_second: input.messages_per_second,
        error_message: (input.status == SyncStatus::Failed).then(|| String::from("sync failed")),
    }
}
