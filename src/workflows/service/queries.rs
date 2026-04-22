use super::draft_remote::retire_local_draft_then_delete_remote;
use super::{WorkflowResult, join_blocking};
use crate::config::ConfigReport;
use crate::gmail::{GmailThreadContext, GmailThreadMessage};
use crate::mailbox;
use crate::store;
use crate::store::accounts::AccountRecord;
use crate::workflows::{
    WorkflowAction, WorkflowActionReport, WorkflowListReport, WorkflowServiceError,
    WorkflowShowReport,
};
use std::path::Path;
use tokio::task::spawn_blocking;

pub async fn list_workflows(
    config_report: &ConfigReport,
    stage: Option<store::workflows::WorkflowStage>,
    triage_bucket: Option<store::workflows::TriageBucket>,
) -> WorkflowResult<WorkflowListReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_workflow_account_id(config_report, None).await?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let workflows = join_blocking(
        spawn_blocking(move || {
            store::workflows::list_workflows(
                &database_path,
                busy_timeout_ms,
                &store::workflows::WorkflowListFilter {
                    account_id,
                    stage,
                    triage_bucket,
                },
            )
        }),
        "workflow.list",
    )
    .await?;

    Ok(WorkflowListReport {
        stage,
        triage_bucket,
        workflows,
    })
}

pub async fn show_workflow(
    config_report: &ConfigReport,
    thread_id: String,
) -> WorkflowResult<WorkflowShowReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_workflow_account_id(config_report, Some(&thread_id)).await?;
    let detail = workflow_detail(config_report, &account_id, &thread_id).await?;
    Ok(WorkflowShowReport { detail })
}

pub(super) async fn workflow_detail(
    config_report: &ConfigReport,
    account_id: &str,
    thread_id: &str,
) -> WorkflowResult<store::workflows::WorkflowDetail> {
    load_workflow_detail_if_present(config_report, account_id, thread_id)
        .await?
        .ok_or_else(|| WorkflowServiceError::WorkflowNotFound {
            thread_id: thread_id.to_owned(),
        })
}

pub(super) async fn action_report(
    config_report: &ConfigReport,
    action: WorkflowAction,
    workflow: store::workflows::WorkflowRecord,
    sync_report: Option<mailbox::SyncRunReport>,
) -> WorkflowResult<WorkflowActionReport> {
    let detail = workflow_detail(config_report, &workflow.account_id, &workflow.thread_id).await?;
    Ok(WorkflowActionReport {
        action,
        workflow,
        current_draft: detail.current_draft,
        cleanup_preview: None,
        sync_report,
    })
}

pub(super) async fn load_workflow_detail_if_present(
    config_report: &ConfigReport,
    account_id: &str,
    thread_id: &str,
) -> WorkflowResult<Option<store::workflows::WorkflowDetail>> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = account_id.to_owned();
    let thread_id = thread_id.to_owned();
    let thread_id_for_query = thread_id.clone();
    join_blocking(
        spawn_blocking(move || {
            store::workflows::get_workflow_detail(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &thread_id_for_query,
            )
        }),
        "workflow.detail",
    )
    .await
}

pub(super) async fn latest_thread_snapshot(
    config_report: &ConfigReport,
    account_id: &str,
    thread_id: &str,
) -> WorkflowResult<store::workflows::WorkflowMessageSnapshot> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = account_id.to_owned();
    let thread_id = thread_id.to_owned();
    let thread_id_for_query = thread_id.clone();
    let snapshot = join_blocking(
        spawn_blocking(move || {
            store::mailbox::get_latest_thread_message(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &thread_id_for_query,
            )
        }),
        "workflow.snapshot",
    )
    .await?;
    let snapshot = snapshot.ok_or_else(|| WorkflowServiceError::LocalSnapshotMissing {
        thread_id: thread_id.clone(),
    })?;
    Ok(store::workflows::WorkflowMessageSnapshot {
        message_id: snapshot.message_id,
        internal_date_epoch_ms: snapshot.internal_date_epoch_ms,
        subject: snapshot.subject,
        from_header: snapshot.from_header,
        snippet: snapshot.snippet,
    })
}

pub(super) fn workflow_snapshot_from_message(
    message: &GmailThreadMessage,
) -> store::workflows::WorkflowMessageSnapshot {
    store::workflows::WorkflowMessageSnapshot {
        message_id: message.id.clone(),
        internal_date_epoch_ms: message.internal_date_epoch_ms,
        subject: message.subject.clone(),
        from_header: message.from_header.clone(),
        snippet: message.snippet.clone(),
    }
}

pub(super) fn latest_thread_message(
    thread: &GmailThreadContext,
) -> WorkflowResult<&GmailThreadMessage> {
    thread
        .messages
        .last()
        .ok_or_else(|| WorkflowServiceError::ThreadHasNoMessages {
            thread_id: thread.id.clone(),
        })
}

pub(super) fn thread_message_by_id<'a>(
    thread: &'a GmailThreadContext,
    message_id: &str,
) -> WorkflowResult<&'a GmailThreadMessage> {
    thread
        .messages
        .iter()
        .find(|message| message.id == message_id)
        .ok_or_else(|| WorkflowServiceError::SourceMessageMissing {
            thread_id: thread.id.clone(),
            message_id: message_id.to_owned(),
        })
}

pub(super) fn best_effort_sync_report(
    sync_result: anyhow::Result<mailbox::SyncRunReport>,
    warning_context: &str,
) -> Option<mailbox::SyncRunReport> {
    match sync_result {
        Ok(report) => Some(report),
        Err(error) => {
            eprintln!("warning: {warning_context}: {error:#}");
            None
        }
    }
}

pub(super) async fn resolve_workflow_account_id(
    config_report: &ConfigReport,
    thread_id: Option<&str>,
) -> WorkflowResult<String> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let thread_id = thread_id.map(str::to_owned);
    join_blocking(
        spawn_blocking(move || {
            resolve_workflow_account_id_blocking(
                &database_path,
                busy_timeout_ms,
                thread_id.as_deref(),
            )
        }),
        "workflow.account.lookup",
    )
    .await
}

pub(super) async fn resolve_mutating_workflow_account_id(
    config_report: &ConfigReport,
    thread_id: &str,
) -> WorkflowResult<String> {
    let active_account = resolve_active_account(config_report).await?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let thread_id_owned = thread_id.to_owned();
    let thread_id_for_lookup = thread_id_owned.clone();
    let active_account_id = active_account.account_id.clone();
    let thread_account_id = join_blocking(
        spawn_blocking(move || {
            store::workflows::lookup_workflow_account_id(
                &database_path,
                busy_timeout_ms,
                Some(&thread_id_for_lookup),
            )
        }),
        "workflow.account.lookup_mutation",
    )
    .await?;

    if let Some(thread_account_id) = thread_account_id
        && thread_account_id != active_account_id
    {
        return Err(WorkflowServiceError::AuthenticatedAccountMismatch {
            thread_id: thread_id_owned,
            expected_account_id: thread_account_id,
            actual_account_id: active_account_id,
        });
    }

    Ok(active_account.account_id)
}

pub(super) fn resolve_workflow_account_id_blocking(
    database_path: &Path,
    busy_timeout_ms: u64,
    thread_id: Option<&str>,
) -> WorkflowResult<String> {
    if let Some(thread_id) = thread_id
        && let Some(account_id) = store::workflows::lookup_workflow_account_id(
            database_path,
            busy_timeout_ms,
            Some(thread_id),
        )?
    {
        return Ok(account_id);
    }

    if let Some(active_account) = store::accounts::get_active(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowServiceError::AccountState { source })?
    {
        return Ok(active_account.account_id);
    }

    if let Some(account_id) =
        store::workflows::lookup_workflow_account_id(database_path, busy_timeout_ms, None)?
    {
        return Ok(account_id);
    }

    if let Some(mailbox) = store::mailbox::inspect_mailbox(database_path, busy_timeout_ms)?
        && let Some(sync_state) = mailbox.sync_state
    {
        return Ok(sync_state.account_id);
    }

    Err(WorkflowServiceError::NoActiveAccount)
}

pub(super) async fn resolve_active_account(
    config_report: &ConfigReport,
) -> WorkflowResult<AccountRecord> {
    crate::refresh_active_account_record(config_report)
        .await
        .map_err(|source| WorkflowServiceError::ActiveAccountRefresh { source })
}

pub async fn set_triage(
    config_report: &ConfigReport,
    thread_id: String,
    triage_bucket: store::workflows::TriageBucket,
    note: Option<String>,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_mutating_workflow_account_id(config_report, &thread_id).await?;
    let snapshot = latest_thread_snapshot(config_report, &account_id, &thread_id).await?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
    let workflow = join_blocking(
        spawn_blocking(move || {
            store::workflows::set_triage_state(
                &database_path,
                busy_timeout_ms,
                &store::workflows::SetTriageStateInput {
                    account_id,
                    thread_id,
                    triage_bucket,
                    note,
                    snapshot,
                    updated_at_epoch_s,
                },
            )
        }),
        "triage.set",
    )
    .await?;
    action_report(config_report, WorkflowAction::TriageSet, workflow, None).await
}

pub async fn promote_workflow(
    config_report: &ConfigReport,
    thread_id: String,
    to_stage: store::workflows::WorkflowStage,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_mutating_workflow_account_id(config_report, &thread_id).await?;
    let snapshot = latest_thread_snapshot(config_report, &account_id, &thread_id).await?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
    let mut promoted_close_gmail_client: Option<crate::gmail::GmailClient> = None;
    if to_stage == store::workflows::WorkflowStage::Closed {
        let gmail_client = crate::gmail_client_for_config(config_report)?;
        gmail_client.get_profile_with_access_scope().await?;
        promoted_close_gmail_client = Some(gmail_client);
    }
    let mut workflow = join_blocking(
        spawn_blocking(move || {
            store::workflows::upsert_stage(
                &database_path,
                busy_timeout_ms,
                &store::workflows::PromoteWorkflowInput {
                    account_id,
                    thread_id,
                    to_stage,
                    snapshot,
                    updated_at_epoch_s,
                },
            )
        }),
        "workflow.promote",
    )
    .await?;
    if to_stage == store::workflows::WorkflowStage::Closed
        && (workflow.current_draft_revision_id.is_some() || workflow.gmail_draft_id.is_some())
    {
        let gmail_client = match promoted_close_gmail_client {
            Some(gmail_client) => gmail_client,
            None => {
                let gmail_client = crate::gmail_client_for_config(config_report)?;
                gmail_client.get_profile_with_access_scope().await?;
                gmail_client
            }
        };
        workflow = retire_local_draft_then_delete_remote(
            config_report,
            &gmail_client,
            workflow,
            "workflow.retire_draft_state",
        )
        .await?;
    }
    action_report(
        config_report,
        WorkflowAction::WorkflowPromoted,
        workflow,
        None,
    )
    .await
}

pub async fn snooze_workflow(
    config_report: &ConfigReport,
    thread_id: String,
    until: Option<String>,
) -> WorkflowResult<WorkflowActionReport> {
    use super::message_build::parse_day_to_epoch_s;

    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_mutating_workflow_account_id(config_report, &thread_id).await?;
    let snapshot = latest_thread_snapshot(config_report, &account_id, &thread_id).await?;
    let snoozed_until_epoch_s = until.as_deref().map(parse_day_to_epoch_s).transpose()?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
    let workflow = join_blocking(
        spawn_blocking(move || {
            store::workflows::snooze_workflow(
                &database_path,
                busy_timeout_ms,
                &store::workflows::SnoozeWorkflowInput {
                    account_id,
                    thread_id,
                    snoozed_until_epoch_s,
                    snapshot,
                    updated_at_epoch_s,
                },
            )
        }),
        "workflow.snooze",
    )
    .await?;
    action_report(
        config_report,
        WorkflowAction::WorkflowSnoozed,
        workflow,
        None,
    )
    .await
}
