use super::{WorkflowResult, join_blocking};
use crate::config::ConfigReport;
use crate::gmail::{GmailClient, GmailClientError};
use crate::store;
use crate::workflows::WorkflowServiceError;
use tokio::task::spawn_blocking;

#[derive(Debug, Clone)]
pub(super) struct RemoteDraftUpsert {
    pub(super) gmail_draft_id: String,
    pub(super) gmail_draft_message_id: String,
    pub(super) gmail_draft_thread_id: String,
    pub(super) created_new: bool,
}

struct RestoreDraftStateRequest {
    account_id: String,
    thread_id: String,
    current_draft_revision_id: Option<i64>,
    gmail_draft_id: Option<String>,
    gmail_draft_message_id: Option<String>,
    gmail_draft_thread_id: Option<String>,
    expected_workflow_version: i64,
}

pub(super) fn matches_missing_draft_error(error: &GmailClientError, draft_id: &str) -> bool {
    let expected_path = format!("users/me/drafts/{draft_id}");
    matches!(
        error,
        GmailClientError::Api { path, status, .. }
            if *status == reqwest::StatusCode::NOT_FOUND && path == &expected_path
    )
}

pub(super) async fn delete_remote_draft_if_present(
    gmail_client: &GmailClient,
    gmail_draft_id: Option<&str>,
) -> WorkflowResult<()> {
    let Some(gmail_draft_id) = gmail_draft_id else {
        return Ok(());
    };

    if let Err(error) = gmail_client.delete_draft(gmail_draft_id).await
        && !matches_missing_draft_error(&error, gmail_draft_id)
    {
        return Err(error.into());
    }

    Ok(())
}

pub(super) async fn upsert_remote_draft(
    gmail_client: &GmailClient,
    gmail_draft_id: Option<&str>,
    raw_message: &str,
    thread_id: &str,
) -> WorkflowResult<RemoteDraftUpsert> {
    let (gmail_draft, created_new) = match gmail_draft_id {
        Some(gmail_draft_id) => {
            match gmail_client
                .update_draft(gmail_draft_id, raw_message, Some(thread_id))
                .await
            {
                Ok(gmail_draft) => (gmail_draft, false),
                Err(error) if matches_missing_draft_error(&error, gmail_draft_id) => (
                    gmail_client
                        .create_draft(raw_message, Some(thread_id))
                        .await?,
                    true,
                ),
                Err(error) => return Err(error.into()),
            }
        }
        None => (
            gmail_client
                .create_draft(raw_message, Some(thread_id))
                .await?,
            true,
        ),
    };

    Ok(RemoteDraftUpsert {
        gmail_draft_id: gmail_draft.id,
        gmail_draft_message_id: gmail_draft.message_id,
        gmail_draft_thread_id: gmail_draft.thread_id,
        created_new,
    })
}

pub(super) async fn update_remote_draft_for_send(
    gmail_client: &GmailClient,
    gmail_draft_id: &str,
    raw_message: &str,
    thread_id: &str,
) -> WorkflowResult<RemoteDraftUpsert> {
    let gmail_draft = gmail_client
        .update_draft(gmail_draft_id, raw_message, Some(thread_id))
        .await
        .map_err(|error| {
            if matches_missing_draft_error(&error, gmail_draft_id) {
                WorkflowServiceError::RemoteDraftMissingBeforeSend {
                    thread_id: thread_id.to_owned(),
                    draft_id: gmail_draft_id.to_owned(),
                }
            } else {
                error.into()
            }
        })?;

    Ok(RemoteDraftUpsert {
        gmail_draft_id: gmail_draft.id,
        gmail_draft_message_id: gmail_draft.message_id,
        gmail_draft_thread_id: gmail_draft.thread_id,
        created_new: false,
    })
}

pub(super) async fn persist_remote_draft_state(
    config_report: &ConfigReport,
    workflow: store::workflows::WorkflowRecord,
    remote_draft: &RemoteDraftUpsert,
    gmail_client: &GmailClient,
    operation: &'static str,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    const REMOTE_DRAFT_STATE_MAX_ATTEMPTS: usize = 5;
    const REMOTE_DRAFT_STATE_RETRY_DELAY_MS: u64 = 50;

    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = workflow.account_id.clone();
    let thread_id = workflow.thread_id.clone();
    let expected_workflow_version = workflow.workflow_version;
    let mut last_error = None;
    for attempt in 0..REMOTE_DRAFT_STATE_MAX_ATTEMPTS {
        let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
        let database_path = database_path.clone();
        let account_id = account_id.clone();
        let thread_id = thread_id.clone();
        let gmail_draft_id = remote_draft.gmail_draft_id.clone();
        let gmail_draft_message_id = remote_draft.gmail_draft_message_id.clone();
        let gmail_draft_thread_id = remote_draft.gmail_draft_thread_id.clone();

        match join_blocking(
            spawn_blocking(move || {
                store::workflows::set_remote_draft_state_with_expected_version(
                    &database_path,
                    busy_timeout_ms,
                    &store::workflows::RemoteDraftStateInput {
                        account_id,
                        thread_id,
                        gmail_draft_id: Some(gmail_draft_id),
                        gmail_draft_message_id: Some(gmail_draft_message_id),
                        gmail_draft_thread_id: Some(gmail_draft_thread_id),
                        updated_at_epoch_s,
                    },
                    Some(expected_workflow_version),
                )
            }),
            operation,
        )
        .await
        {
            Ok(workflow) => return Ok(workflow),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < REMOTE_DRAFT_STATE_MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        REMOTE_DRAFT_STATE_RETRY_DELAY_MS,
                    ))
                    .await;
                }
            }
        }
    }

    let error = last_error.expect("persist_remote_draft_state must record an error");
    if !remote_draft.created_new {
        let database_path = config_report.config.store.database_path.clone();
        let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        let account_id = workflow.account_id.clone();
        let thread_id = workflow.thread_id.clone();
        let reloaded = join_blocking(
            spawn_blocking(move || {
                store::workflows::get_workflow_detail(
                    &database_path,
                    busy_timeout_ms,
                    &account_id,
                    &thread_id,
                )
            }),
            "draft.remote_state.reconcile_read",
        )
        .await;

        if let Ok(Some(detail)) = reloaded
            && detail.workflow.gmail_draft_id.as_deref() == Some(&remote_draft.gmail_draft_id)
            && detail.workflow.gmail_draft_message_id.as_deref()
                == Some(&remote_draft.gmail_draft_message_id)
            && detail.workflow.gmail_draft_thread_id.as_deref()
                == Some(&remote_draft.gmail_draft_thread_id)
        {
            return Ok(detail.workflow);
        }

        let source = anyhow::Error::new(error);
        return Err(WorkflowServiceError::RemoteDraftStateReconcile {
            thread_id: workflow.thread_id,
            draft_id: remote_draft.gmail_draft_id.clone(),
            source,
        });
    }

    let rollback_result =
        delete_remote_draft_if_present(gmail_client, Some(&remote_draft.gmail_draft_id)).await;
    match rollback_result {
        Ok(()) => Err(error),
        Err(rollback_error) => Err(WorkflowServiceError::RemoteDraftRollback {
            thread_id: workflow.thread_id,
            draft_id: remote_draft.gmail_draft_id.clone(),
            source: anyhow::Error::new(error).context(format!(
                "failed to delete Gmail draft {} after local state persistence failed: {rollback_error}",
                remote_draft.gmail_draft_id
            )),
        }),
    }
}

pub(super) async fn mark_sent_after_remote_send(
    config_report: &ConfigReport,
    workflow: &store::workflows::WorkflowRecord,
    sent_message_id: &str,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    const MARK_SENT_MAX_ATTEMPTS: usize = 5;
    const MARK_SENT_RETRY_DELAY_MS: u64 = 50;

    let mut last_error = None;
    for attempt in 0..MARK_SENT_MAX_ATTEMPTS {
        let database_path = config_report.config.store.database_path.clone();
        let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
        let account_id = workflow.account_id.clone();
        let thread_id = workflow.thread_id.clone();
        let sent_message_id = sent_message_id.to_owned();

        match join_blocking(
            spawn_blocking(move || {
                store::workflows::mark_sent(
                    &database_path,
                    busy_timeout_ms,
                    &store::workflows::MarkSentInput {
                        account_id,
                        thread_id,
                        sent_message_id,
                        updated_at_epoch_s,
                    },
                )
            }),
            "draft.send.mark_sent",
        )
        .await
        {
            Ok(workflow) => return Ok(workflow),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < MARK_SENT_MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(MARK_SENT_RETRY_DELAY_MS))
                        .await;
                }
            }
        }
    }

    let source =
        anyhow::Error::new(last_error.expect("mark_sent_after_remote_send must record an error"));
    Err(WorkflowServiceError::RemoteSendStateReconcile {
        thread_id: workflow.thread_id.clone(),
        sent_message_id: sent_message_id.to_owned(),
        source,
    })
}

pub(super) async fn retire_local_draft_state(
    config_report: &ConfigReport,
    account_id: &str,
    thread_id: &str,
    operation: &'static str,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
    let account_id = account_id.to_owned();
    let thread_id = thread_id.to_owned();

    join_blocking(
        spawn_blocking(move || {
            store::workflows::retire_draft_state(
                &database_path,
                busy_timeout_ms,
                &store::workflows::RetireDraftStateInput {
                    account_id,
                    thread_id,
                    updated_at_epoch_s,
                },
            )
        }),
        operation,
    )
    .await
}

pub(super) async fn retire_local_draft_then_delete_remote(
    config_report: &ConfigReport,
    gmail_client: &GmailClient,
    workflow: store::workflows::WorkflowRecord,
    operation: &'static str,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    let original_draft_revision_id = workflow.current_draft_revision_id;
    let gmail_draft_id = workflow.gmail_draft_id.clone();
    let gmail_draft_message_id = workflow.gmail_draft_message_id.clone();
    let gmail_draft_thread_id = workflow.gmail_draft_thread_id.clone();
    let retired_workflow = retire_local_draft_state(
        config_report,
        &workflow.account_id,
        &workflow.thread_id,
        operation,
    )
    .await?;
    if let Err(source) =
        delete_remote_draft_if_present(gmail_client, gmail_draft_id.as_deref()).await
    {
        let source_message = source.to_string();
        restore_local_draft_state(
            config_report,
            RestoreDraftStateRequest {
                account_id: workflow.account_id.clone(),
                thread_id: workflow.thread_id.clone(),
                current_draft_revision_id: original_draft_revision_id,
                gmail_draft_id: gmail_draft_id.clone(),
                gmail_draft_message_id,
                gmail_draft_thread_id,
                expected_workflow_version: retired_workflow.workflow_version,
            },
            "draft.restore_local_state",
        )
        .await
        .map_err(
            |restore_error| WorkflowServiceError::RemoteDraftStateReconcile {
                thread_id: workflow.thread_id.clone(),
                draft_id: gmail_draft_id
                    .clone()
                    .unwrap_or_else(|| String::from("<missing>")),
                source: anyhow::Error::new(restore_error)
                    .context(format!("remote draft delete failed: {source_message}")),
            },
        )?;
        return Err(source);
    }
    Ok(retired_workflow)
}

async fn restore_local_draft_state(
    config_report: &ConfigReport,
    request: RestoreDraftStateRequest,
    operation: &'static str,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;

    join_blocking(
        spawn_blocking(move || {
            store::workflows::restore_draft_state_with_expected_version(
                &database_path,
                busy_timeout_ms,
                &store::workflows::RestoreDraftStateInput {
                    account_id: request.account_id,
                    thread_id: request.thread_id,
                    current_draft_revision_id: request.current_draft_revision_id,
                    gmail_draft_id: request.gmail_draft_id,
                    gmail_draft_message_id: request.gmail_draft_message_id,
                    gmail_draft_thread_id: request.gmail_draft_thread_id,
                    updated_at_epoch_s,
                },
                Some(request.expected_workflow_version),
            )
        }),
        operation,
    )
    .await
}
