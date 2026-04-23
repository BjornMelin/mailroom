use super::current_epoch_seconds;
use super::draft_remote::retire_local_draft_then_delete_remote;
use super::queries::{
    best_effort_sync_report, load_workflow_detail_if_present, resolve_mutating_workflow_account_id,
    workflow_detail,
};
use crate::config::ConfigReport;
use crate::mailbox;
use crate::store;
use crate::workflows::{
    CleanupPreview, WorkflowAction, WorkflowActionReport, WorkflowServiceError,
};
use tokio::task::spawn_blocking;

use super::{WorkflowResult, join_blocking};

pub async fn cleanup_archive(
    config_report: &ConfigReport,
    thread_id: String,
    execute: bool,
) -> WorkflowResult<WorkflowActionReport> {
    cleanup_impl(
        config_report,
        thread_id,
        execute,
        store::workflows::CleanupAction::Archive,
        Vec::new(),
        vec![String::from("INBOX")],
    )
    .await
}

pub async fn cleanup_label(
    config_report: &ConfigReport,
    thread_id: String,
    execute: bool,
    add_label_names: Vec<String>,
    remove_label_names: Vec<String>,
) -> WorkflowResult<WorkflowActionReport> {
    cleanup_impl(
        config_report,
        thread_id,
        execute,
        store::workflows::CleanupAction::Label,
        add_label_names,
        remove_label_names,
    )
    .await
}

pub async fn cleanup_trash(
    config_report: &ConfigReport,
    thread_id: String,
    execute: bool,
) -> WorkflowResult<WorkflowActionReport> {
    cleanup_impl(
        config_report,
        thread_id,
        execute,
        store::workflows::CleanupAction::Trash,
        Vec::new(),
        Vec::new(),
    )
    .await
}

async fn resolve_cleanup_label_ids_for_action(
    config_report: &ConfigReport,
    account_id: &str,
    action: store::workflows::CleanupAction,
    add_label_names: &[String],
    remove_label_names: &[String],
) -> WorkflowResult<Option<(Vec<String>, Vec<String>)>> {
    if action != store::workflows::CleanupAction::Label {
        return Ok(None);
    }
    if add_label_names.is_empty() && remove_label_names.is_empty() {
        return Err(WorkflowServiceError::CleanupLabelsRequired);
    }

    Ok(Some(
        resolve_cleanup_label_ids(
            config_report,
            account_id,
            add_label_names,
            remove_label_names,
        )
        .await?,
    ))
}

pub(crate) async fn cleanup_tracked_thread_for_automation(
    config_report: &ConfigReport,
    gmail_client: &crate::gmail::GmailClient,
    account_id: &str,
    thread_id: &str,
    action: store::workflows::CleanupAction,
    add_label_names: Vec<String>,
    remove_label_names: Vec<String>,
) -> WorkflowResult<bool> {
    let Some(detail) =
        load_workflow_detail_if_present(config_report, account_id, thread_id).await?
    else {
        return Ok(false);
    };

    let resolved_label_ids = resolve_cleanup_label_ids_for_action(
        config_report,
        account_id,
        action,
        &add_label_names,
        &remove_label_names,
    )
    .await?;
    execute_cleanup_after_auth(
        config_report,
        gmail_client,
        detail,
        action,
        add_label_names,
        remove_label_names,
        resolved_label_ids,
    )
    .await?;
    Ok(true)
}

async fn cleanup_impl(
    config_report: &ConfigReport,
    thread_id: String,
    execute: bool,
    action: store::workflows::CleanupAction,
    add_label_names: Vec<String>,
    remove_label_names: Vec<String>,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = if execute {
        resolve_mutating_workflow_account_id(config_report, &thread_id).await?
    } else {
        super::queries::resolve_workflow_account_id(config_report, Some(&thread_id)).await?
    };
    let detail = workflow_detail(config_report, &account_id, &thread_id).await?;
    let cleanup_preview = CleanupPreview {
        action,
        execute,
        add_label_names: add_label_names.clone(),
        remove_label_names: remove_label_names.clone(),
    };

    let resolved_label_ids = resolve_cleanup_label_ids_for_action(
        config_report,
        &account_id,
        action,
        &add_label_names,
        &remove_label_names,
    )
    .await?;

    if !execute {
        return Ok(WorkflowActionReport {
            action: WorkflowAction::CleanupPreview,
            workflow: detail.workflow,
            current_draft: detail.current_draft,
            cleanup_preview: Some(cleanup_preview),
            sync_report: None,
        });
    }

    let gmail_client = crate::gmail_client_for_config(config_report)
        .map_err(|source| WorkflowServiceError::GmailClientInit { source })?;
    gmail_client.get_profile_with_access_scope().await?;
    let workflow = execute_cleanup_after_auth(
        config_report,
        &gmail_client,
        detail,
        action,
        add_label_names,
        remove_label_names,
        resolved_label_ids,
    )
    .await?;
    let sync_report = best_effort_sync_report(
        mailbox::sync_run(config_report, false, mailbox::DEFAULT_BOOTSTRAP_RECENT_DAYS).await,
        "cleanup applied but mailbox sync failed; run `mailroom sync run` to refresh local state",
    );
    Ok(WorkflowActionReport {
        action: WorkflowAction::CleanupApplied,
        workflow,
        current_draft: None,
        cleanup_preview: Some(cleanup_preview),
        sync_report,
    })
}

async fn execute_cleanup_after_auth(
    config_report: &ConfigReport,
    gmail_client: &crate::gmail::GmailClient,
    detail: store::workflows::WorkflowDetail,
    action: store::workflows::CleanupAction,
    add_label_names: Vec<String>,
    remove_label_names: Vec<String>,
    resolved_label_ids: Option<(Vec<String>, Vec<String>)>,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    let thread_id = detail.workflow.thread_id.clone();
    let needs_draft_retirement = detail.workflow.current_draft_revision_id.is_some()
        || detail.workflow.gmail_draft_id.is_some();

    let mut workflow = persist_cleanup_state(
        config_report,
        &detail.workflow,
        action,
        add_label_names,
        remove_label_names,
    )
    .await?;

    match action {
        store::workflows::CleanupAction::Archive => {
            gmail_client
                .modify_thread_labels(&thread_id, &[], &[String::from("INBOX")])
                .await?;
        }
        store::workflows::CleanupAction::Trash => {
            gmail_client.trash_thread(&thread_id).await?;
        }
        store::workflows::CleanupAction::Label => {
            let (add_ids, remove_ids) = resolved_label_ids
                .as_ref()
                .ok_or(WorkflowServiceError::LabelCleanupInvariant)?;
            gmail_client
                .modify_thread_labels(&thread_id, add_ids, remove_ids)
                .await?;
        }
    }

    if needs_draft_retirement {
        workflow = retire_local_draft_then_delete_remote(
            config_report,
            gmail_client,
            workflow,
            "cleanup.retire_draft_state",
        )
        .await?;
    }

    Ok(workflow)
}

async fn persist_cleanup_state(
    config_report: &ConfigReport,
    workflow: &store::workflows::WorkflowRecord,
    action: store::workflows::CleanupAction,
    add_label_names: Vec<String>,
    remove_label_names: Vec<String>,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    let payload_json = serde_json::to_string(&serde_json::json!({
        "add_label_names": add_label_names,
        "remove_label_names": remove_label_names,
        "execute": true,
    }))?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let cleanup_account_id = workflow.account_id.clone();
    let cleanup_thread_id = workflow.thread_id.clone();
    let updated_at_epoch_s = current_epoch_seconds()?;

    join_blocking(
        spawn_blocking(move || {
            store::workflows::apply_cleanup(
                &database_path,
                busy_timeout_ms,
                &store::workflows::ApplyCleanupInput {
                    account_id: cleanup_account_id,
                    thread_id: cleanup_thread_id,
                    cleanup_action: action,
                    payload_json,
                    updated_at_epoch_s,
                },
            )
        }),
        "cleanup.apply",
    )
    .await
}

async fn resolve_cleanup_label_ids(
    config_report: &ConfigReport,
    account_id: &str,
    add_label_names: &[String],
    remove_label_names: &[String],
) -> WorkflowResult<(Vec<String>, Vec<String>)> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = account_id.to_owned();
    let add_names = add_label_names.to_owned();
    let remove_names = remove_label_names.to_owned();
    let (add_resolved, remove_resolved) = join_blocking(
        spawn_blocking(move || {
            Ok::<_, store::mailbox::MailboxReadError>((
                store::mailbox::resolve_label_ids_by_names(
                    &database_path,
                    busy_timeout_ms,
                    &account_id,
                    &add_names,
                )?,
                store::mailbox::resolve_label_ids_by_names(
                    &database_path,
                    busy_timeout_ms,
                    &account_id,
                    &remove_names,
                )?,
            ))
        }),
        "cleanup.label.resolve_labels",
    )
    .await?;
    if add_resolved.len() != add_label_names.len() {
        return Err(WorkflowServiceError::AddLabelsNotFoundLocally);
    }
    if remove_resolved.len() != remove_label_names.len() {
        return Err(WorkflowServiceError::RemoveLabelsNotFoundLocally);
    }

    Ok((
        add_resolved
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        remove_resolved
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
    ))
}
