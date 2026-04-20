use super::read::load_workflow;
use super::{
    ApplyCleanupInput, AttachmentInput, CleanupAction, DraftRevisionRecord, MarkSentInput,
    PromoteWorkflowInput, RemoteDraftStateInput, RetireDraftStateInput, SetTriageStateInput,
    SnoozeWorkflowInput, TriageBucket, UpsertDraftRevisionInput, WorkflowMessageSnapshot,
    WorkflowRecord, WorkflowStage, WorkflowStoreWriteError,
};
use crate::store::connection;
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::json;
use std::path::Path;

struct WorkflowEventInsert<'a> {
    event_kind: &'a str,
    from_stage: Option<WorkflowStage>,
    to_stage: Option<WorkflowStage>,
    triage_bucket: Option<TriageBucket>,
    note: Option<&'a str>,
    payload_json: &'a str,
    created_at_epoch_s: i64,
}

pub(crate) fn set_triage_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &SetTriageStateInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let existing = load_workflow(&transaction, &input.account_id, &input.thread_id)?;
    let from_stage = existing.as_ref().map(|workflow| workflow.current_stage);
    let expected_workflow_version = existing.as_ref().map(|workflow| workflow.workflow_version);
    let mut workflow = existing.unwrap_or_else(|| {
        new_workflow(
            &input.account_id,
            &input.thread_id,
            WorkflowStage::Triage,
            &input.snapshot,
            input.updated_at_epoch_s,
        )
    });
    workflow.triage_bucket = Some(input.triage_bucket);
    if let Some(note) = &input.note {
        workflow.note = note.clone();
    }
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;
    apply_snapshot(&mut workflow, &input.snapshot);

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "triage_set",
            from_stage,
            to_stage: Some(workflow.current_stage),
            triage_bucket: workflow.triage_bucket,
            note: input.note.as_deref(),
            payload_json: &json!({
                "triage_bucket": input.triage_bucket,
            })
            .to_string(),
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok(workflow)
}

pub(crate) fn upsert_stage(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &PromoteWorkflowInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let existing = load_workflow(&transaction, &input.account_id, &input.thread_id)?;
    let from_stage = existing.as_ref().map(|workflow| workflow.current_stage);
    let expected_workflow_version = existing.as_ref().map(|workflow| workflow.workflow_version);
    let mut workflow = existing.unwrap_or_else(|| {
        new_workflow(
            &input.account_id,
            &input.thread_id,
            input.to_stage,
            &input.snapshot,
            input.updated_at_epoch_s,
        )
    });

    let has_sendable_draft = workflow.current_draft_revision_id.is_some()
        && workflow.gmail_draft_id.is_some()
        && workflow.last_remote_sync_epoch_s.is_some();
    if input.to_stage == WorkflowStage::ReadyToSend && !has_sendable_draft {
        return Err(WorkflowStoreWriteError::ReadyToSendRequiresSendableDraft);
    }
    workflow.current_stage = input.to_stage;
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;
    apply_snapshot(&mut workflow, &input.snapshot);

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "stage_promoted",
            from_stage,
            to_stage: Some(input.to_stage),
            triage_bucket: workflow.triage_bucket,
            note: None,
            payload_json: &json!({
                "to_stage": input.to_stage,
            })
            .to_string(),
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok(workflow)
}

pub(crate) fn snooze_workflow(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &SnoozeWorkflowInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let existing = load_workflow(&transaction, &input.account_id, &input.thread_id)?;
    let from_stage = existing.as_ref().map(|workflow| workflow.current_stage);
    let expected_workflow_version = existing.as_ref().map(|workflow| workflow.workflow_version);
    let mut workflow = existing.unwrap_or_else(|| {
        new_workflow(
            &input.account_id,
            &input.thread_id,
            WorkflowStage::FollowUp,
            &input.snapshot,
            input.updated_at_epoch_s,
        )
    });

    workflow.current_stage = WorkflowStage::FollowUp;
    workflow.snoozed_until_epoch_s = input.snoozed_until_epoch_s;
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;
    apply_snapshot(&mut workflow, &input.snapshot);

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "workflow_snoozed",
            from_stage,
            to_stage: Some(WorkflowStage::FollowUp),
            triage_bucket: workflow.triage_bucket,
            note: None,
            payload_json: &json!({
                "snoozed_until_epoch_s": input.snoozed_until_epoch_s,
            })
            .to_string(),
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok(workflow)
}

pub(crate) fn upsert_draft_revision(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &UpsertDraftRevisionInput,
) -> Result<(WorkflowRecord, DraftRevisionRecord), WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let existing = load_workflow(&transaction, &input.account_id, &input.thread_id)?;
    let from_stage = existing.as_ref().map(|workflow| workflow.current_stage);
    let expected_workflow_version = existing.as_ref().map(|workflow| workflow.workflow_version);
    let mut workflow = existing.unwrap_or_else(|| {
        new_workflow(
            &input.account_id,
            &input.thread_id,
            WorkflowStage::Drafting,
            &input.snapshot,
            input.updated_at_epoch_s,
        )
    });

    workflow.current_stage = WorkflowStage::Drafting;
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;
    apply_snapshot(&mut workflow, &input.snapshot);
    let mut workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;

    let draft_revision = insert_draft_revision(&transaction, workflow.workflow_id, input)?;
    workflow.current_draft_revision_id = Some(draft_revision.draft_revision_id);
    workflow.gmail_draft_message_id = None;
    workflow.gmail_draft_thread_id = None;
    workflow.last_remote_sync_epoch_s = None;
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;
    let expected_workflow_version = Some(workflow.workflow_version);
    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;

    let payload_json = json!({
        "draft_revision_id": draft_revision.draft_revision_id,
        "reply_mode": input.reply_mode,
        "attachment_count": input.attachments.len(),
    })
    .to_string();
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "draft_revision_upserted",
            from_stage,
            to_stage: Some(WorkflowStage::Drafting),
            triage_bucket: workflow.triage_bucket,
            note: None,
            payload_json: &payload_json,
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok((workflow, draft_revision))
}

#[cfg(test)]
pub(crate) fn set_remote_draft_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &RemoteDraftStateInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    set_remote_draft_state_with_expected_version(database_path, busy_timeout_ms, input, None)
}

pub(crate) fn set_remote_draft_state_with_expected_version(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &RemoteDraftStateInput,
    expected_workflow_version: Option<i64>,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let mut workflow = load_workflow(&transaction, &input.account_id, &input.thread_id)?
        .ok_or_else(|| WorkflowStoreWriteError::MissingWorkflow {
            thread_id: input.thread_id.clone(),
        })?;
    let expected_workflow_version = expected_workflow_version.or(Some(workflow.workflow_version));

    workflow.gmail_draft_id = input.gmail_draft_id.clone();
    workflow.gmail_draft_message_id = input.gmail_draft_message_id.clone();
    workflow.gmail_draft_thread_id = input.gmail_draft_thread_id.clone();
    workflow.last_remote_sync_epoch_s = Some(input.updated_at_epoch_s);
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    let payload_json = json!({
        "gmail_draft_id": input.gmail_draft_id,
        "gmail_draft_message_id": input.gmail_draft_message_id,
        "gmail_draft_thread_id": input.gmail_draft_thread_id,
    })
    .to_string();
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "remote_draft_synced",
            from_stage: Some(workflow.current_stage),
            to_stage: Some(workflow.current_stage),
            triage_bucket: workflow.triage_bucket,
            note: None,
            payload_json: &payload_json,
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok(workflow)
}

pub(crate) fn mark_sent(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &MarkSentInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let mut workflow = load_workflow(&transaction, &input.account_id, &input.thread_id)?
        .ok_or_else(|| WorkflowStoreWriteError::MissingWorkflow {
            thread_id: input.thread_id.clone(),
        })?;
    let from_stage = Some(workflow.current_stage);
    let expected_workflow_version = Some(workflow.workflow_version);

    workflow.current_stage = WorkflowStage::Sent;
    clear_draft_state(&mut workflow);
    workflow.last_remote_sync_epoch_s = Some(input.updated_at_epoch_s);
    workflow.last_sent_message_id = Some(input.sent_message_id.clone());
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    let payload_json = json!({
        "sent_message_id": input.sent_message_id,
    })
    .to_string();
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "draft_sent",
            from_stage,
            to_stage: Some(WorkflowStage::Sent),
            triage_bucket: workflow.triage_bucket,
            note: None,
            payload_json: &payload_json,
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok(workflow)
}

pub(crate) fn apply_cleanup(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &ApplyCleanupInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let mut workflow = load_workflow(&transaction, &input.account_id, &input.thread_id)?
        .ok_or_else(|| WorkflowStoreWriteError::MissingWorkflow {
            thread_id: input.thread_id.clone(),
        })?;
    let from_stage = Some(workflow.current_stage);
    let expected_workflow_version = Some(workflow.workflow_version);

    workflow.current_stage = WorkflowStage::Closed;
    workflow.last_cleanup_action = Some(input.cleanup_action);
    workflow.last_remote_sync_epoch_s = Some(input.updated_at_epoch_s);
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    insert_event(
        &transaction,
        &workflow,
        &WorkflowEventInsert {
            event_kind: "cleanup_applied",
            from_stage,
            to_stage: Some(WorkflowStage::Closed),
            triage_bucket: workflow.triage_bucket,
            note: None,
            payload_json: &input.payload_json,
            created_at_epoch_s: input.updated_at_epoch_s,
        },
    )?;
    transaction.commit()?;
    Ok(workflow)
}

pub(crate) fn retire_draft_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &RetireDraftStateInput,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let mut workflow = load_workflow(&transaction, &input.account_id, &input.thread_id)?
        .ok_or_else(|| WorkflowStoreWriteError::MissingWorkflow {
            thread_id: input.thread_id.clone(),
        })?;
    let expected_workflow_version = Some(workflow.workflow_version);

    clear_draft_state(&mut workflow);
    workflow.last_remote_sync_epoch_s = Some(input.updated_at_epoch_s);
    workflow.updated_at_epoch_s = input.updated_at_epoch_s;

    let workflow = persist_workflow(&transaction, workflow, expected_workflow_version)?;
    transaction.commit()?;
    Ok(workflow)
}

fn clear_draft_state(workflow: &mut WorkflowRecord) {
    workflow.current_draft_revision_id = None;
    workflow.gmail_draft_id = None;
    workflow.gmail_draft_message_id = None;
    workflow.gmail_draft_thread_id = None;
}

fn new_workflow(
    account_id: &str,
    thread_id: &str,
    stage: WorkflowStage,
    snapshot: &WorkflowMessageSnapshot,
    now_epoch_s: i64,
) -> WorkflowRecord {
    WorkflowRecord {
        workflow_id: 0,
        account_id: account_id.to_owned(),
        thread_id: thread_id.to_owned(),
        current_stage: stage,
        triage_bucket: None,
        note: String::new(),
        snoozed_until_epoch_s: None,
        follow_up_due_epoch_s: None,
        latest_message_id: Some(snapshot.message_id.clone()),
        latest_message_internal_date_epoch_ms: Some(snapshot.internal_date_epoch_ms),
        latest_message_subject: snapshot.subject.clone(),
        latest_message_from_header: snapshot.from_header.clone(),
        latest_message_snippet: snapshot.snippet.clone(),
        current_draft_revision_id: None,
        gmail_draft_id: None,
        gmail_draft_message_id: None,
        gmail_draft_thread_id: None,
        last_remote_sync_epoch_s: None,
        last_sent_message_id: None,
        last_cleanup_action: None,
        workflow_version: 0,
        created_at_epoch_s: now_epoch_s,
        updated_at_epoch_s: now_epoch_s,
    }
}

fn apply_snapshot(workflow: &mut WorkflowRecord, snapshot: &WorkflowMessageSnapshot) {
    // Keep newer stored snapshot fields when local mailbox state is stale.
    let should_replace_snapshot = workflow
        .latest_message_internal_date_epoch_ms
        .is_none_or(|current| snapshot.internal_date_epoch_ms >= current);

    if should_replace_snapshot {
        workflow.latest_message_id = Some(snapshot.message_id.clone());
        workflow.latest_message_internal_date_epoch_ms = Some(snapshot.internal_date_epoch_ms);
        workflow.latest_message_subject = snapshot.subject.clone();
        workflow.latest_message_from_header = snapshot.from_header.clone();
        workflow.latest_message_snippet = snapshot.snippet.clone();
    }
}

pub(super) fn persist_workflow(
    transaction: &Transaction<'_>,
    workflow: WorkflowRecord,
    expected_workflow_version: Option<i64>,
) -> Result<WorkflowRecord, WorkflowStoreWriteError> {
    let account_id = workflow.account_id.clone();
    let thread_id = workflow.thread_id.clone();
    let next_workflow_version = workflow.workflow_version.saturating_add(1);
    let mut workflow = workflow;
    workflow.workflow_version = next_workflow_version;

    if workflow.workflow_id == 0 {
        let rows_affected = transaction.execute(
            "INSERT INTO thread_workflows (
                 account_id,
                 thread_id,
                 current_stage,
                 triage_bucket,
                 note,
                 snoozed_until_epoch_s,
                 follow_up_due_epoch_s,
                 latest_message_id,
                 latest_message_internal_date_epoch_ms,
                 latest_message_subject,
                 latest_message_from_header,
                 latest_message_snippet,
                 current_draft_revision_id,
                 gmail_draft_id,
                 gmail_draft_message_id,
                 gmail_draft_thread_id,
                 last_remote_sync_epoch_s,
                 last_sent_message_id,
                 last_cleanup_action,
                 created_at_epoch_s,
                 updated_at_epoch_s,
                 workflow_version
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)
             ON CONFLICT(account_id, thread_id) DO NOTHING",
            params![
                account_id.clone(),
                thread_id.clone(),
                workflow.current_stage.as_str(),
                workflow.triage_bucket.map(TriageBucket::as_str),
                workflow.note,
                workflow.snoozed_until_epoch_s,
                workflow.follow_up_due_epoch_s,
                workflow.latest_message_id,
                workflow.latest_message_internal_date_epoch_ms,
                workflow.latest_message_subject,
                workflow.latest_message_from_header,
                workflow.latest_message_snippet,
                workflow.current_draft_revision_id,
                workflow.gmail_draft_id,
                workflow.gmail_draft_message_id,
                workflow.gmail_draft_thread_id,
                workflow.last_remote_sync_epoch_s,
                workflow.last_sent_message_id,
                workflow.last_cleanup_action.map(CleanupAction::as_str),
                workflow.created_at_epoch_s,
                workflow.updated_at_epoch_s,
                workflow.workflow_version,
            ],
        )?;
        if rows_affected == 0 {
            return Err(WorkflowStoreWriteError::Conflict { thread_id });
        }
    } else {
        let expected_workflow_version =
            expected_workflow_version.ok_or_else(|| WorkflowStoreWriteError::Conflict {
                thread_id: thread_id.clone(),
            })?;
        let rows_affected = transaction.execute(
            "UPDATE thread_workflows
             SET current_stage = ?2,
                 triage_bucket = ?3,
                 note = ?4,
                 snoozed_until_epoch_s = ?5,
                 follow_up_due_epoch_s = ?6,
                 latest_message_id = ?7,
                 latest_message_internal_date_epoch_ms = ?8,
                 latest_message_subject = ?9,
                 latest_message_from_header = ?10,
                 latest_message_snippet = ?11,
                 current_draft_revision_id = ?12,
                 gmail_draft_id = ?13,
                 gmail_draft_message_id = ?14,
                 gmail_draft_thread_id = ?15,
                 last_remote_sync_epoch_s = ?16,
                 last_sent_message_id = ?17,
                 last_cleanup_action = ?18,
                 updated_at_epoch_s = ?19,
                 workflow_version = ?20
             WHERE workflow_id = ?1
               AND workflow_version = ?21",
            params![
                workflow.workflow_id,
                workflow.current_stage.as_str(),
                workflow.triage_bucket.map(TriageBucket::as_str),
                workflow.note,
                workflow.snoozed_until_epoch_s,
                workflow.follow_up_due_epoch_s,
                workflow.latest_message_id,
                workflow.latest_message_internal_date_epoch_ms,
                workflow.latest_message_subject,
                workflow.latest_message_from_header,
                workflow.latest_message_snippet,
                workflow.current_draft_revision_id,
                workflow.gmail_draft_id,
                workflow.gmail_draft_message_id,
                workflow.gmail_draft_thread_id,
                workflow.last_remote_sync_epoch_s,
                workflow.last_sent_message_id,
                workflow.last_cleanup_action.map(CleanupAction::as_str),
                workflow.updated_at_epoch_s,
                workflow.workflow_version,
                expected_workflow_version,
            ],
        )?;
        if rows_affected == 0 {
            return Err(WorkflowStoreWriteError::Conflict { thread_id });
        }
    }

    load_workflow(transaction, &account_id, &thread_id)?
        .ok_or_else(|| WorkflowStoreWriteError::ReloadWorkflow { thread_id })
}

fn insert_draft_revision(
    transaction: &Transaction<'_>,
    workflow_id: i64,
    input: &UpsertDraftRevisionInput,
) -> Result<DraftRevisionRecord, WorkflowStoreWriteError> {
    let to_addresses_json = serde_json::to_string(&input.to_addresses)?;
    let cc_addresses_json = serde_json::to_string(&input.cc_addresses)?;
    let bcc_addresses_json = serde_json::to_string(&input.bcc_addresses)?;

    transaction.execute(
        "INSERT INTO thread_draft_revisions (
             workflow_id,
             account_id,
             thread_id,
             source_message_id,
             reply_mode,
             subject,
             to_addresses_json,
             cc_addresses_json,
             bcc_addresses_json,
             body_text,
             created_at_epoch_s
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            workflow_id,
            input.account_id,
            input.thread_id,
            input.source_message_id,
            input.reply_mode.as_str(),
            input.subject,
            to_addresses_json,
            cc_addresses_json,
            bcc_addresses_json,
            input.body_text,
            input.updated_at_epoch_s,
        ],
    )?;

    let draft_revision_id = transaction.last_insert_rowid();
    insert_attachments(
        transaction,
        draft_revision_id,
        &input.attachments,
        input.updated_at_epoch_s,
    )?;

    transaction
        .query_row(
            "SELECT
                 draft_revision_id,
                 workflow_id,
                 account_id,
                 thread_id,
                 source_message_id,
                 reply_mode,
                 subject,
                 to_addresses_json,
                 cc_addresses_json,
                 bcc_addresses_json,
                 body_text,
                 created_at_epoch_s
             FROM thread_draft_revisions
             WHERE draft_revision_id = ?1",
            [draft_revision_id],
            super::read::row_to_draft_revision,
        )
        .optional()?
        .ok_or_else(|| WorkflowStoreWriteError::ReloadDraftRevision {
            draft_revision_id: format!("draft_revision:{draft_revision_id}"),
        })
}

fn insert_attachments(
    transaction: &Transaction<'_>,
    draft_revision_id: i64,
    attachments: &[AttachmentInput],
    now_epoch_s: i64,
) -> Result<(), WorkflowStoreWriteError> {
    let mut statement = transaction.prepare_cached(
        "INSERT INTO thread_draft_attachments (
             draft_revision_id,
             path,
             file_name,
             mime_type,
             size_bytes,
             created_at_epoch_s
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;

    for attachment in attachments {
        statement.execute(params![
            draft_revision_id,
            &attachment.path,
            &attachment.file_name,
            &attachment.mime_type,
            attachment.size_bytes,
            now_epoch_s,
        ])?;
    }

    Ok(())
}

fn insert_event(
    transaction: &Transaction<'_>,
    workflow: &WorkflowRecord,
    event: &WorkflowEventInsert<'_>,
) -> Result<(), WorkflowStoreWriteError> {
    transaction.execute(
        "INSERT INTO thread_workflow_events (
             workflow_id,
             account_id,
             thread_id,
             event_kind,
             from_stage,
             to_stage,
             triage_bucket,
             note,
             payload_json,
             created_at_epoch_s
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            workflow.workflow_id,
            &workflow.account_id,
            &workflow.thread_id,
            event.event_kind,
            event.from_stage.map(WorkflowStage::as_str),
            event.to_stage.map(WorkflowStage::as_str),
            event.triage_bucket.map(TriageBucket::as_str),
            event.note,
            event.payload_json,
            event.created_at_epoch_s,
        ],
    )?;
    Ok(())
}
