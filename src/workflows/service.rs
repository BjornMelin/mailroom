use crate::config::ConfigReport;
use crate::gmail::{GmailClientError, GmailThreadContext, GmailThreadMessage};
use crate::mailbox;
use crate::store;
use crate::store::accounts::AccountRecord;
use crate::workflows::{
    CleanupPreview, WorkflowAction, WorkflowActionReport, WorkflowListReport, WorkflowShowReport,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mail_builder::MessageBuilder;
use std::fs;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use tokio::task::{JoinError, JoinHandle, spawn_blocking};

type WorkflowResult<T> = std::result::Result<T, WorkflowServiceError>;

#[derive(Debug, Error)]
pub(crate) enum WorkflowServiceError {
    #[error(transparent)]
    Gmail(#[from] GmailClientError),
    #[error(transparent)]
    WorkflowStoreRead(#[from] store::workflows::WorkflowStoreReadError),
    #[error(transparent)]
    WorkflowStoreWrite(#[from] store::workflows::WorkflowStoreWriteError),
    #[error(transparent)]
    MailboxRead(#[from] store::mailbox::MailboxReadError),
    #[error("failed to initialize the local store")]
    StoreInit {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to refresh the active Gmail account")]
    ActiveAccountRefresh {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to access local account state")]
    AccountState {
        #[source]
        source: anyhow::Error,
    },
    #[error("workflow blocking task `{operation}` failed")]
    BlockingTask {
        operation: &'static str,
        #[source]
        source: JoinError,
    },
    #[error("no workflow found for thread {thread_id}")]
    WorkflowNotFound { thread_id: String },
    #[error("no current draft found for thread {thread_id}")]
    CurrentDraftNotFound { thread_id: String },
    #[error("no remote Gmail draft is associated with thread {thread_id}")]
    RemoteDraftNotFound { thread_id: String },
    #[error(
        "stored Gmail draft {draft_id} for thread {thread_id} no longer exists; refusing to recreate it during send because the previous send may have already succeeded; run `mailroom sync run` and inspect the thread before retrying"
    )]
    RemoteDraftMissingBeforeSend { thread_id: String, draft_id: String },
    #[error("no active Gmail account found; run `mailroom auth login` first")]
    NoActiveAccount,
    #[error(
        "no locally synced message found for thread {thread_id}; run `mailroom sync run` first"
    )]
    LocalSnapshotMissing { thread_id: String },
    #[error("thread {thread_id} has no messages")]
    ThreadHasNoMessages { thread_id: String },
    #[error("thread {thread_id} does not contain source message {message_id}")]
    SourceMessageMissing {
        thread_id: String,
        message_id: String,
    },
    #[error("could not determine reply recipient from thread headers")]
    ReplyRecipientUndetermined,
    #[error("reply draft has no recipients")]
    ReplyDraftWithoutRecipients,
    #[error("draft must have at least one To recipient")]
    DraftWithoutToRecipients,
    #[error("at least one label must be added or removed")]
    CleanupLabelsRequired,
    #[error("one or more add-label names were not found locally; run `mailroom sync run` first")]
    AddLabelsNotFoundLocally,
    #[error("one or more remove-label names were not found locally; run `mailroom sync run` first")]
    RemoveLabelsNotFoundLocally,
    #[error("label cleanup executed without resolved label ids")]
    LabelCleanupInvariant,
    #[error("no draft attachment matched `{path_or_name}`")]
    DraftAttachmentNotFound { path_or_name: String },
    #[error(
        "attachment name `{file_name}` matches multiple draft attachments; use the stored attachment path instead"
    )]
    DraftAttachmentNameAmbiguous { file_name: String },
    #[error("failed to read attachment metadata for {path}")]
    AttachmentMetadata {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is not a file")]
    AttachmentNotFile { path: String },
    #[error("failed to normalize attachment path {path}")]
    AttachmentNormalize {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("attachment path {path} has no valid file name")]
    AttachmentFileName { path: String },
    #[error("failed to read attachment {path}")]
    AttachmentRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("date `{value}` must be in YYYY-MM-DD format")]
    InvalidDateFormat { value: String },
    #[error("date `{value}` has an invalid month")]
    InvalidDateMonth { value: String },
    #[error("date `{value}` has an invalid day")]
    InvalidDateDay { value: String },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error("failed to build Gmail message")]
    MessageBuild {
        #[source]
        source: anyhow::Error,
    },
    #[error(
        "created Gmail draft {draft_id} for thread {thread_id} but could not persist or roll it back locally"
    )]
    RemoteDraftRollback {
        thread_id: String,
        draft_id: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(
        "Gmail sent the draft for thread {thread_id} as message {sent_message_id}, but mailroom could not record the sent state locally; inspect the thread before retrying send"
    )]
    RemoteSendStateReconcile {
        thread_id: String,
        sent_message_id: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(
        "Gmail updated draft {draft_id} for thread {thread_id}, but mailroom could not record draft state locally; inspect the thread before retrying"
    )]
    RemoteDraftStateReconcile {
        thread_id: String,
        draft_id: String,
        #[source]
        source: anyhow::Error,
    },
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
struct RemoteDraftUpsert {
    gmail_draft_id: String,
    gmail_draft_message_id: String,
    gmail_draft_thread_id: String,
    created_new: bool,
}

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

fn matches_missing_draft_error(error: &GmailClientError, draft_id: &str) -> bool {
    let expected_path = format!("users/me/drafts/{draft_id}");
    matches!(
        error,
        GmailClientError::Api { path, status, .. }
            if *status == reqwest::StatusCode::NOT_FOUND && path == &expected_path
    )
}

async fn delete_remote_draft_if_present(
    gmail_client: &crate::gmail::GmailClient,
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

async fn upsert_remote_draft(
    gmail_client: &crate::gmail::GmailClient,
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

async fn update_remote_draft_for_send(
    gmail_client: &crate::gmail::GmailClient,
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

async fn persist_remote_draft_state(
    config_report: &ConfigReport,
    workflow: store::workflows::WorkflowRecord,
    remote_draft: &RemoteDraftUpsert,
    gmail_client: &crate::gmail::GmailClient,
    operation: &'static str,
) -> WorkflowResult<store::workflows::WorkflowRecord> {
    const REMOTE_DRAFT_STATE_MAX_ATTEMPTS: usize = 5;
    const REMOTE_DRAFT_STATE_RETRY_DELAY_MS: u64 = 50;

    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = workflow.account_id.clone();
    let thread_id = workflow.thread_id.clone();
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
                store::workflows::set_remote_draft_state(
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

async fn mark_sent_after_remote_send(
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

async fn retire_local_draft_state(
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
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let thread_id_for_query = thread_id.clone();
    let detail = join_blocking(
        spawn_blocking(move || {
            store::workflows::get_workflow_detail(
                &database_path,
                busy_timeout_ms,
                &account_id,
                &thread_id_for_query,
            )
        }),
        "workflow.show",
    )
    .await?;

    let detail = detail.ok_or_else(|| WorkflowServiceError::WorkflowNotFound {
        thread_id: thread_id.clone(),
    })?;
    Ok(WorkflowShowReport { detail })
}

pub async fn set_triage(
    config_report: &ConfigReport,
    thread_id: String,
    triage_bucket: store::workflows::TriageBucket,
    note: Option<String>,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_workflow_account_id(config_report, Some(&thread_id)).await?;
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
    let account_id = resolve_workflow_account_id(config_report, Some(&thread_id)).await?;
    let snapshot = latest_thread_snapshot(config_report, &account_id, &thread_id).await?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
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
        let gmail_client = crate::gmail_client_for_config(config_report)?;
        delete_remote_draft_if_present(&gmail_client, workflow.gmail_draft_id.as_deref()).await?;
        workflow = retire_local_draft_state(
            config_report,
            &workflow.account_id,
            &workflow.thread_id,
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
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_workflow_account_id(config_report, Some(&thread_id)).await?;
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

pub async fn draft_start(
    config_report: &ConfigReport,
    thread_id: String,
    reply_mode: store::workflows::ReplyMode,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account = resolve_active_account(config_report).await?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
    let thread = gmail_client.get_thread_context(&thread_id).await?;
    let latest_message = latest_thread_message(&thread)?;
    let snapshot = workflow_snapshot_from_message(latest_message);
    let recipients = build_reply_recipients(&account.email_address, latest_message, reply_mode)?;
    let subject = normalize_reply_subject(&latest_message.subject);
    let draft_input = store::workflows::UpsertDraftRevisionInput {
        account_id: account.account_id.clone(),
        thread_id: thread_id.clone(),
        reply_mode,
        source_message_id: latest_message.id.clone(),
        subject: subject.clone(),
        to_addresses: recipients.to_addresses,
        cc_addresses: recipients.cc_addresses,
        bcc_addresses: Vec::new(),
        body_text: String::new(),
        attachments: Vec::new(),
        snapshot,
        updated_at_epoch_s: crate::time::current_epoch_seconds()?,
    };
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let (workflow, draft_revision) = join_blocking(
        spawn_blocking(move || {
            store::workflows::upsert_draft_revision(&database_path, busy_timeout_ms, &draft_input)
        }),
        "draft.start.persist",
    )
    .await?;
    let raw_message =
        build_raw_message(&account.email_address, latest_message, &draft_revision, &[]).await?;
    let remote_draft = upsert_remote_draft(
        &gmail_client,
        workflow.gmail_draft_id.as_deref(),
        &raw_message,
        &workflow.thread_id,
    )
    .await?;
    let workflow = persist_remote_draft_state(
        config_report,
        workflow,
        &remote_draft,
        &gmail_client,
        "draft.start.remote_state",
    )
    .await?;

    action_report(config_report, WorkflowAction::DraftStarted, workflow, None).await
}

pub async fn draft_body_set(
    config_report: &ConfigReport,
    thread_id: String,
    body_text: String,
) -> WorkflowResult<WorkflowActionReport> {
    update_draft_revision(
        config_report,
        thread_id,
        |draft| {
            draft.body_text = body_text;
            Ok(())
        },
        WorkflowAction::DraftBodySet,
    )
    .await
}

pub async fn draft_attach_add(
    config_report: &ConfigReport,
    thread_id: String,
    path: PathBuf,
) -> WorkflowResult<WorkflowActionReport> {
    let attachment = join_blocking(
        spawn_blocking(move || attachment_input_from_path(&path)),
        "draft.attach.input",
    )
    .await?;
    update_draft_revision(
        config_report,
        thread_id,
        |draft| {
            if !draft
                .attachments
                .iter()
                .any(|existing| existing.path == attachment.path)
            {
                draft.attachments.push(attachment);
            }
            Ok(())
        },
        WorkflowAction::DraftAttachmentAdded,
    )
    .await
}

pub async fn draft_attach_remove(
    config_report: &ConfigReport,
    thread_id: String,
    path_or_name: String,
) -> WorkflowResult<WorkflowActionReport> {
    let repo_root = crate::workspace::configured_repo_root_from_locations(
        &config_report.locations.repo_config_path,
    )?;
    let invocation_dir = std::env::current_dir().unwrap_or_else(|_| repo_root.clone());
    update_draft_revision(
        config_report,
        thread_id,
        |draft| match remove_attachment_by_path_or_name(
            &mut draft.attachments,
            &path_or_name,
            &invocation_dir,
        ) {
            AttachmentRemovalResult::Removed => Ok(()),
            AttachmentRemovalResult::NotFound => {
                Err(WorkflowServiceError::DraftAttachmentNotFound {
                    path_or_name: path_or_name.clone(),
                })
            }
            AttachmentRemovalResult::AmbiguousFileName => {
                Err(WorkflowServiceError::DraftAttachmentNameAmbiguous {
                    file_name: path_or_name.clone(),
                })
            }
        },
        WorkflowAction::DraftAttachmentRemoved,
    )
    .await
}

pub async fn draft_send(
    config_report: &ConfigReport,
    thread_id: String,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account = resolve_active_account(config_report).await?;
    let detail = workflow_detail(config_report, &account.account_id, &thread_id).await?;
    let draft = detail
        .current_draft
        .ok_or_else(|| WorkflowServiceError::CurrentDraftNotFound {
            thread_id: thread_id.clone(),
        })?;
    let gmail_draft_id = detail.workflow.gmail_draft_id.clone().ok_or_else(|| {
        WorkflowServiceError::RemoteDraftNotFound {
            thread_id: thread_id.clone(),
        }
    })?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
    let thread = gmail_client.get_thread_context(&thread_id).await?;
    let source_message = thread_message_by_id(&thread, &draft.revision.source_message_id)?;
    let attachments = draft
        .attachments
        .iter()
        .map(|attachment| store::workflows::AttachmentInput {
            path: attachment.path.clone(),
            file_name: attachment.file_name.clone(),
            mime_type: attachment.mime_type.clone(),
            size_bytes: attachment.size_bytes,
        })
        .collect::<Vec<_>>();
    let raw_message = build_raw_message(
        &account.email_address,
        source_message,
        &draft.revision,
        &attachments,
    )
    .await?;
    let remote_draft =
        update_remote_draft_for_send(&gmail_client, &gmail_draft_id, &raw_message, &thread_id)
            .await?;
    persist_remote_draft_state(
        config_report,
        detail.workflow.clone(),
        &remote_draft,
        &gmail_client,
        "draft.send.remote_state",
    )
    .await?;
    let sent = gmail_client
        .send_draft(&remote_draft.gmail_draft_id)
        .await?;
    let workflow =
        mark_sent_after_remote_send(config_report, &detail.workflow, &sent.message_id).await?;
    let sync_report = best_effort_sync_report(
        mailbox::sync_run(config_report, false, mailbox::DEFAULT_BOOTSTRAP_RECENT_DAYS).await,
        "draft sent but mailbox sync failed; run `mailroom sync run` to refresh local state",
    );
    action_report(
        config_report,
        WorkflowAction::DraftSent,
        workflow,
        sync_report,
    )
    .await
}

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
    if add_label_names.is_empty() && remove_label_names.is_empty() {
        return Err(WorkflowServiceError::CleanupLabelsRequired);
    }
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

async fn cleanup_impl(
    config_report: &ConfigReport,
    thread_id: String,
    execute: bool,
    action: store::workflows::CleanupAction,
    add_label_names: Vec<String>,
    remove_label_names: Vec<String>,
) -> WorkflowResult<WorkflowActionReport> {
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account_id = resolve_workflow_account_id(config_report, Some(&thread_id)).await?;
    let detail = workflow_detail(config_report, &account_id, &thread_id).await?;
    let cleanup_preview = CleanupPreview {
        action,
        execute,
        add_label_names: add_label_names.clone(),
        remove_label_names: remove_label_names.clone(),
    };

    if !execute {
        return Ok(WorkflowActionReport {
            action: WorkflowAction::CleanupPreview,
            workflow: detail.workflow,
            current_draft: detail.current_draft,
            cleanup_preview: Some(cleanup_preview),
            sync_report: None,
        });
    }

    let resolved_label_ids = if action == store::workflows::CleanupAction::Label {
        let database_path = config_report.config.store.database_path.clone();
        let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        let account_id = account_id.clone();
        let add_names = add_label_names.clone();
        let remove_names = remove_label_names.clone();
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
        Some((
            add_resolved
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            remove_resolved
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
        ))
    } else {
        None
    };

    let payload_json = serde_json::to_string(&serde_json::json!({
        "add_label_names": add_label_names,
        "remove_label_names": remove_label_names,
        "execute": execute,
    }))?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let updated_at_epoch_s = crate::time::current_epoch_seconds()?;
    let mut workflow = join_blocking(
        spawn_blocking(move || {
            store::workflows::apply_cleanup(
                &database_path,
                busy_timeout_ms,
                &store::workflows::ApplyCleanupInput {
                    account_id: detail.workflow.account_id,
                    thread_id: detail.workflow.thread_id,
                    cleanup_action: action,
                    payload_json,
                    updated_at_epoch_s,
                },
            )
        }),
        "cleanup.apply",
    )
    .await?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
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
            let (add_ids, remove_ids) =
                resolved_label_ids.ok_or(WorkflowServiceError::LabelCleanupInvariant)?;
            gmail_client
                .modify_thread_labels(&thread_id, &add_ids, &remove_ids)
                .await?;
        }
    }
    let needs_draft_retirement =
        workflow.current_draft_revision_id.is_some() || workflow.gmail_draft_id.is_some();
    if needs_draft_retirement {
        delete_remote_draft_if_present(&gmail_client, workflow.gmail_draft_id.as_deref()).await?;
        workflow = retire_local_draft_state(
            config_report,
            &workflow.account_id,
            &workflow.thread_id,
            "cleanup.retire_draft_state",
        )
        .await?;
    }
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

async fn update_draft_revision<F>(
    config_report: &ConfigReport,
    thread_id: String,
    mutate: F,
    action: WorkflowAction,
) -> WorkflowResult<WorkflowActionReport>
where
    F: FnOnce(&mut DraftRevisionMutation) -> WorkflowResult<()>,
{
    store::init(config_report).map_err(|source| WorkflowServiceError::StoreInit { source })?;
    let account = resolve_active_account(config_report).await?;
    let detail = workflow_detail(config_report, &account.account_id, &thread_id).await?;
    let current_draft =
        detail
            .current_draft
            .ok_or_else(|| WorkflowServiceError::CurrentDraftNotFound {
                thread_id: thread_id.clone(),
            })?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
    let thread = gmail_client.get_thread_context(&thread_id).await?;
    let latest_message = latest_thread_message(&thread)?;
    let source_message = thread_message_by_id(&thread, &current_draft.revision.source_message_id)?;
    let snapshot = workflow_snapshot_from_message(latest_message);

    let mut draft = DraftRevisionMutation {
        reply_mode: current_draft.revision.reply_mode,
        source_message_id: current_draft.revision.source_message_id.clone(),
        subject: current_draft.revision.subject.clone(),
        to_addresses: current_draft.revision.to_addresses.clone(),
        cc_addresses: current_draft.revision.cc_addresses.clone(),
        bcc_addresses: current_draft.revision.bcc_addresses.clone(),
        body_text: current_draft.revision.body_text.clone(),
        attachments: current_draft
            .attachments
            .iter()
            .map(|attachment| store::workflows::AttachmentInput {
                path: attachment.path.clone(),
                file_name: attachment.file_name.clone(),
                mime_type: attachment.mime_type.clone(),
                size_bytes: attachment.size_bytes,
            })
            .collect(),
    };
    mutate(&mut draft)?;
    if draft.to_addresses.is_empty() {
        return Err(WorkflowServiceError::DraftWithoutToRecipients);
    }

    let upsert_input = store::workflows::UpsertDraftRevisionInput {
        account_id: account.account_id.clone(),
        thread_id: thread_id.clone(),
        reply_mode: draft.reply_mode,
        source_message_id: draft.source_message_id.clone(),
        subject: draft.subject.clone(),
        to_addresses: draft.to_addresses.clone(),
        cc_addresses: draft.cc_addresses.clone(),
        bcc_addresses: draft.bcc_addresses.clone(),
        body_text: draft.body_text.clone(),
        attachments: draft.attachments.clone(),
        snapshot,
        updated_at_epoch_s: crate::time::current_epoch_seconds()?,
    };
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let (workflow, draft_revision) = join_blocking(
        spawn_blocking(move || {
            store::workflows::upsert_draft_revision(&database_path, busy_timeout_ms, &upsert_input)
        }),
        "draft.update.persist",
    )
    .await?;
    let raw_message = build_raw_message(
        &account.email_address,
        source_message,
        &draft_revision,
        &draft.attachments,
    )
    .await?;
    let remote_draft = upsert_remote_draft(
        &gmail_client,
        detail.workflow.gmail_draft_id.as_deref(),
        &raw_message,
        &thread_id,
    )
    .await?;
    let workflow = persist_remote_draft_state(
        config_report,
        workflow,
        &remote_draft,
        &gmail_client,
        "draft.update.remote_state",
    )
    .await?;
    action_report(config_report, action, workflow, None).await
}

async fn action_report(
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

async fn workflow_detail(
    config_report: &ConfigReport,
    account_id: &str,
    thread_id: &str,
) -> WorkflowResult<store::workflows::WorkflowDetail> {
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
    .await?
    .ok_or_else(|| WorkflowServiceError::WorkflowNotFound {
        thread_id: thread_id.clone(),
    })
}

async fn latest_thread_snapshot(
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

fn workflow_snapshot_from_message(
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

fn build_reply_recipients(
    account_email: &str,
    latest_message: &GmailThreadMessage,
    reply_mode: store::workflows::ReplyMode,
) -> WorkflowResult<ReplyRecipients> {
    let primary = first_non_self_reply_recipient(account_email, latest_message)
        .ok_or(WorkflowServiceError::ReplyRecipientUndetermined)?;

    let mut to_addresses = vec![primary];
    let mut cc_addresses = Vec::new();

    if reply_mode == store::workflows::ReplyMode::ReplyAll {
        for address in split_address_list(&latest_message.to_header) {
            push_unique_address(&mut to_addresses, &address, account_email);
        }
        for address in split_address_list(&latest_message.cc_header) {
            if address.eq_ignore_ascii_case(account_email)
                || to_addresses
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&address))
            {
                continue;
            }
            push_unique_address(&mut cc_addresses, &address, account_email);
        }
    }

    if to_addresses.is_empty() {
        return Err(WorkflowServiceError::ReplyDraftWithoutRecipients);
    }
    Ok(ReplyRecipients {
        to_addresses,
        cc_addresses,
    })
}

fn first_non_self_reply_recipient(
    account_email: &str,
    latest_message: &GmailThreadMessage,
) -> Option<String> {
    first_address(&latest_message.reply_to_header)
        .into_iter()
        .chain(latest_message.from_address.clone())
        .chain(first_address(&latest_message.from_header))
        .chain(split_address_list(&latest_message.to_header))
        .chain(split_address_list(&latest_message.cc_header))
        .find(|address| !address.eq_ignore_ascii_case(account_email))
}

fn normalize_reply_subject(subject: &str) -> String {
    let trimmed = subject.trim();
    if trimmed.is_empty() {
        return String::from("Re:");
    }
    if trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("re:"))
    {
        trimmed.to_owned()
    } else {
        format!("Re: {trimmed}")
    }
}

async fn build_raw_message(
    account_email: &str,
    source_message: &GmailThreadMessage,
    draft_revision: &store::workflows::DraftRevisionRecord,
    attachments: &[store::workflows::AttachmentInput],
) -> WorkflowResult<String> {
    let message_id = source_message
        .message_id_header
        .as_ref()
        .and_then(|header| normalize_message_id(header));
    let references_header = source_message.references_header.clone();
    let attachments: Vec<(store::workflows::AttachmentInput, Vec<u8>)> = if attachments.is_empty() {
        Vec::new()
    } else {
        let attachments = attachments.to_vec();
        // Keep filesystem reads off the async runtime thread.
        join_blocking(
            spawn_blocking(move || {
                attachments
                    .into_iter()
                    .map(|attachment| {
                        let bytes = fs::read(&attachment.path).map_err(|source| {
                            WorkflowServiceError::AttachmentRead {
                                path: attachment.path.clone(),
                                source,
                            }
                        })?;
                        Ok::<_, WorkflowServiceError>((attachment, bytes))
                    })
                    .collect::<WorkflowResult<Vec<_>>>()
            }),
            "workflow.attachments.read",
        )
        .await?
    };

    let mut builder = MessageBuilder::new()
        .from(account_email.to_owned())
        .to(draft_revision.to_addresses.clone())
        .subject(draft_revision.subject.clone())
        .text_body(draft_revision.body_text.clone());

    if !draft_revision.cc_addresses.is_empty() {
        builder = builder.cc(draft_revision.cc_addresses.clone());
    }
    if !draft_revision.bcc_addresses.is_empty() {
        builder = builder.bcc(draft_revision.bcc_addresses.clone());
    }
    if let Some(message_id) = message_id {
        let references = build_references(&references_header, &message_id);
        builder = builder.in_reply_to(message_id);
        if !references.is_empty() {
            builder = builder.references(references);
        }
    }

    for attachment in attachments {
        let (attachment, bytes) = attachment;
        builder = builder.attachment(attachment.mime_type, attachment.file_name, bytes);
    }

    let mut output = Vec::new();
    builder
        .write_to(&mut output)
        .map_err(|source| WorkflowServiceError::MessageBuild {
            source: source.into(),
        })?;
    Ok(URL_SAFE_NO_PAD.encode(output))
}

fn build_references(existing: &str, message_id: &str) -> Vec<String> {
    let mut ids = parse_message_id_header(existing);
    if !ids.iter().any(|existing_id| existing_id == message_id) {
        ids.push(message_id.to_owned());
    }
    ids
}

fn parse_message_id_header(value: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut current = String::new();
    let mut in_brackets = false;

    for character in value.chars() {
        match character {
            '<' => {
                current.clear();
                in_brackets = true;
            }
            '>' if in_brackets => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    ids.push(trimmed.to_owned());
                }
                current.clear();
                in_brackets = false;
            }
            _ if in_brackets => current.push(character),
            _ => {}
        }
    }

    ids
}

fn normalize_message_id(value: &str) -> Option<String> {
    parse_message_id_header(value).into_iter().next()
}

fn split_address_list(header: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut angle_depth = 0usize;

    for character in header.chars() {
        match character {
            '"' => {
                in_quotes = !in_quotes;
                current.push(character);
            }
            '<' if !in_quotes => {
                angle_depth += 1;
                current.push(character);
            }
            '>' if !in_quotes && angle_depth > 0 => {
                angle_depth -= 1;
                current.push(character);
            }
            ',' if !in_quotes && angle_depth == 0 => {
                if let Some(value) = normalize_address_candidate(&current) {
                    values.push(value);
                }
                current.clear();
            }
            _ => current.push(character),
        }
    }
    if let Some(value) = normalize_address_candidate(&current) {
        values.push(value);
    }
    values
}

fn first_address(header: &str) -> Option<String> {
    split_address_list(header).into_iter().next()
}

fn normalize_address_candidate(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let candidate = if let Some((_, remainder)) = value.split_once('<') {
        remainder.split_once('>')?.0.trim()
    } else {
        value.trim_matches('"')
    };
    if candidate.is_empty()
        || candidate.contains(char::is_whitespace)
        || candidate.matches('@').count() != 1
    {
        return None;
    }
    Some(candidate.to_ascii_lowercase())
}

fn push_unique_address(target: &mut Vec<String>, address: &str, account_email: &str) {
    if address.eq_ignore_ascii_case(account_email)
        || target
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(address))
    {
        return;
    }
    target.push(address.to_owned());
}

fn latest_thread_message(thread: &GmailThreadContext) -> WorkflowResult<&GmailThreadMessage> {
    thread
        .messages
        .last()
        .ok_or_else(|| WorkflowServiceError::ThreadHasNoMessages {
            thread_id: thread.id.clone(),
        })
}

fn thread_message_by_id<'a>(
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

fn attachment_input_from_path(path: &Path) -> WorkflowResult<store::workflows::AttachmentInput> {
    let path_display = path.display().to_string();
    let metadata =
        fs::metadata(path).map_err(|source| WorkflowServiceError::AttachmentMetadata {
            path: path_display.clone(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(WorkflowServiceError::AttachmentNotFile { path: path_display });
    }
    let normalized_path =
        path.canonicalize()
            .map_err(|source| WorkflowServiceError::AttachmentNormalize {
                path: path.display().to_string(),
                source,
            })?;
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .ok_or_else(|| WorkflowServiceError::AttachmentFileName {
            path: path.display().to_string(),
        })?;
    Ok(store::workflows::AttachmentInput {
        path: normalized_path.display().to_string(),
        file_name: file_name.to_owned(),
        mime_type: mime_guess::from_path(path)
            .first_or_octet_stream()
            .essence_str()
            .to_owned(),
        size_bytes: i64::try_from(metadata.len())?,
    })
}

fn best_effort_sync_report(
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

fn parse_day_to_epoch_s(value: &str) -> WorkflowResult<i64> {
    let epoch_ms = parse_start_of_day_epoch_ms(value)?;
    Ok(epoch_ms / 1000)
}

fn parse_start_of_day_epoch_ms(value: &str) -> WorkflowResult<i64> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .ok_or_else(|| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let month = parts
        .next()
        .ok_or_else(|| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let day = parts
        .next()
        .ok_or_else(|| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    if parts.next().is_some() || year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return Err(WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        });
    }
    let year = year
        .parse::<i64>()
        .map_err(|_| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let month = month
        .parse::<u32>()
        .map_err(|_| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let day = day
        .parse::<u32>()
        .map_err(|_| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => {
            return Err(WorkflowServiceError::InvalidDateMonth {
                value: value.to_owned(),
            });
        }
    };
    if day == 0 || day > max_day {
        return Err(WorkflowServiceError::InvalidDateDay {
            value: value.to_owned(),
        });
    }

    let month = i64::from(month);
    let day = i64::from(day);
    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Ok((era * 146_097 + day_of_era - 719_468) * 86_400_000)
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

async fn resolve_workflow_account_id(
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

fn resolve_workflow_account_id_blocking(
    database_path: &Path,
    busy_timeout_ms: u64,
    thread_id: Option<&str>,
) -> WorkflowResult<String> {
    if let Some(active_account) = store::accounts::get_active(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowServiceError::AccountState { source })?
    {
        return Ok(active_account.account_id);
    }

    if let Some(thread_id) = thread_id
        && let Some(account_id) = store::workflows::lookup_workflow_account_id(
            database_path,
            busy_timeout_ms,
            Some(thread_id),
        )?
    {
        return Ok(account_id);
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

async fn resolve_active_account(config_report: &ConfigReport) -> WorkflowResult<AccountRecord> {
    crate::refresh_active_account_record(config_report)
        .await
        .map_err(|source| WorkflowServiceError::ActiveAccountRefresh { source })
}

#[derive(Debug)]
struct ReplyRecipients {
    to_addresses: Vec<String>,
    cc_addresses: Vec<String>,
}

#[derive(Debug)]
struct DraftRevisionMutation {
    reply_mode: store::workflows::ReplyMode,
    source_message_id: String,
    subject: String,
    to_addresses: Vec<String>,
    cc_addresses: Vec<String>,
    bcc_addresses: Vec<String>,
    body_text: String,
    attachments: Vec<store::workflows::AttachmentInput>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttachmentRemovalResult {
    Removed,
    NotFound,
    AmbiguousFileName,
}

fn remove_attachment_by_path_or_name(
    attachments: &mut Vec<store::workflows::AttachmentInput>,
    path_or_name: &str,
    base_dir: &Path,
) -> AttachmentRemovalResult {
    if let Some(index) = attachments
        .iter()
        .position(|attachment| attachment.path == path_or_name)
    {
        attachments.remove(index);
        return AttachmentRemovalResult::Removed;
    }

    let normalized_path = normalize_attachment_match_path(path_or_name, base_dir)
        .map(|path| path.display().to_string());
    if let Some(normalized_path) = normalized_path.as_deref()
        && let Some(index) = attachments
            .iter()
            .position(|attachment| attachment.path == normalized_path)
    {
        attachments.remove(index);
        return AttachmentRemovalResult::Removed;
    }

    let mut matching_indexes = attachments
        .iter()
        .enumerate()
        .filter(|(_, attachment)| attachment.file_name == path_or_name)
        .map(|(index, _)| index);

    match (matching_indexes.next(), matching_indexes.next()) {
        (Some(index), None) => {
            attachments.remove(index);
            AttachmentRemovalResult::Removed
        }
        (Some(_), Some(_)) => AttachmentRemovalResult::AmbiguousFileName,
        (None, None) => AttachmentRemovalResult::NotFound,
        (None, Some(_)) => unreachable!("iterator cannot yield a second value without a first"),
    }
}

fn normalize_attachment_match_path(path_or_name: &str, base_dir: &Path) -> Option<PathBuf> {
    lexical_absolute_path(Path::new(path_or_name), base_dir).ok()
}

fn lexical_absolute_path(path: &Path, base_dir: &Path) -> std::io::Result<PathBuf> {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    };
    Ok(normalize_absolute_path_components(absolute_path))
}

fn normalize_absolute_path_components(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::{
        AttachmentRemovalResult, RemoteDraftUpsert, WorkflowServiceError,
        attachment_input_from_path, best_effort_sync_report, build_reply_recipients,
        cleanup_archive, cleanup_label, draft_body_set, draft_send, draft_start, list_workflows,
        mark_sent_after_remote_send, persist_remote_draft_state, promote_workflow,
        remove_attachment_by_path_or_name, show_workflow,
    };
    use crate::auth;
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::config::{ConfigReport, resolve};
    use crate::gmail::{GmailLabel, GmailThreadMessage};
    use crate::mailbox::SyncRunReport;
    use crate::store::mailbox::{
        GmailMessageUpsertInput, SyncMode, SyncStateUpdate, SyncStatus, replace_labels,
        upsert_messages, upsert_sync_state,
    };
    use crate::store::workflows::{
        AttachmentInput, CleanupAction, ReplyMode, TriageBucket, UpsertDraftRevisionInput,
        get_workflow_detail, set_remote_draft_state, set_triage_state, upsert_draft_revision,
    };
    use crate::store::{accounts, init};
    use crate::workspace::WorkspacePaths;
    use anyhow::anyhow;
    use secrecy::SecretString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::sync_channel;
    use std::time::Duration;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn draft_start_reuses_existing_remote_gmail_draft() {
        let mock_server = MockServer::start().await;
        mount_profile(&mock_server).await;
        mount_thread(&mock_server).await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "draft-1",
                "message": {
                    "id": "draft-message-1",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "draft-1",
                "message": {
                    "id": "draft-message-2",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);

        let first = draft_start(&config_report, String::from("thread-1"), ReplyMode::Reply)
            .await
            .unwrap();
        let second = draft_start(&config_report, String::from("thread-1"), ReplyMode::Reply)
            .await
            .unwrap();

        assert_eq!(first.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
        assert_eq!(second.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
        assert_eq!(
            second.workflow.gmail_draft_message_id.as_deref(),
            Some("draft-message-2")
        );

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
        assert_eq!(
            detail.workflow.gmail_draft_message_id.as_deref(),
            Some("draft-message-2")
        );

        let requests = mock_server.received_requests().await.unwrap();
        let create_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST"
                    && request.url.path() == "/gmail/v1/users/me/drafts"
            })
            .count();
        let update_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "PUT"
                    && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
            })
            .count();

        assert_eq!(create_count, 1);
        assert_eq!(update_count, 1);
    }

    #[tokio::test]
    async fn draft_start_and_body_set_persist_live_thread_metadata_when_local_snapshot_is_stale() {
        let mock_server = MockServer::start().await;
        mount_profile(&mock_server).await;
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/threads/thread-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "500",
                "messages": [
                    {
                        "id": "m-2",
                        "threadId": "thread-1",
                        "historyId": "401",
                        "internalDate": "200",
                        "snippet": "Fresh status",
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Project updated"},
                                {"name": "From", "value": "\"Alice Example\" <alice@example.com>"},
                                {"name": "To", "value": "operator@example.com"},
                                {"name": "Message-ID", "value": "<m-2@example.com>"}
                            ]
                        }
                    }
                ]
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "draft-1",
                "message": {
                    "id": "draft-message-1",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "draft-1",
                "message": {
                    "id": "draft-message-2",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot_with_message(
            &config_report,
            "m-1",
            100,
            "Project",
            "Alice <alice@example.com>",
            Some("alice@example.com"),
            "Stale status",
        );

        let report = draft_start(&config_report, String::from("thread-1"), ReplyMode::Reply)
            .await
            .unwrap();
        draft_body_set(
            &config_report,
            String::from("thread-1"),
            String::from("Updated body"),
        )
        .await
        .unwrap();

        assert_eq!(report.workflow.latest_message_id.as_deref(), Some("m-2"));
        assert_eq!(
            report.workflow.latest_message_internal_date_epoch_ms,
            Some(200)
        );
        assert_eq!(report.workflow.latest_message_subject, "Project updated");
        assert_eq!(
            report.workflow.latest_message_from_header,
            "\"Alice Example\" <alice@example.com>"
        );
        assert_eq!(report.workflow.latest_message_snippet, "Fresh status");

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.latest_message_id.as_deref(), Some("m-2"));
        assert_eq!(detail.workflow.latest_message_subject, "Project updated");
        assert_eq!(detail.workflow.latest_message_snippet, "Fresh status");
    }

    #[tokio::test]
    async fn draft_body_set_recreates_missing_remote_gmail_draft() {
        let mock_server = MockServer::start().await;
        mount_profile(&mock_server).await;
        mount_thread(&mock_server).await;
        Mock::given(method("PUT"))
            .and(path("/gmail/v1/users/me/drafts/draft-stale"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "draft-2",
                "message": {
                    "id": "draft-message-2",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-stale")),
                gmail_draft_message_id: Some(String::from("draft-message-stale")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let report = draft_body_set(
            &config_report,
            String::from("thread-1"),
            String::from("Updated body"),
        )
        .await
        .unwrap();

        assert_eq!(report.workflow.gmail_draft_id.as_deref(), Some("draft-2"));
        assert_eq!(
            report.workflow.gmail_draft_message_id.as_deref(),
            Some("draft-message-2")
        );

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-2"));
        assert_eq!(
            detail.workflow.gmail_draft_message_id.as_deref(),
            Some("draft-message-2")
        );

        let requests = mock_server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "PUT"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-stale"
        }));
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/gmail/v1/users/me/drafts"
        }));
    }

    #[tokio::test]
    async fn persist_remote_draft_state_rolls_back_created_remote_draft_when_local_write_fails() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-created"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let mut config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        let (workflow, _) = upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
        let original_database_path = config_report.config.store.database_path.clone();
        let original_busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        config_report.config.store.database_path =
            temp_dir.path().join("missing").join("db.sqlite");

        let error = persist_remote_draft_state(
            &config_report,
            workflow,
            &RemoteDraftUpsert {
                gmail_draft_id: String::from("draft-created"),
                gmail_draft_message_id: String::from("draft-message-created"),
                gmail_draft_thread_id: String::from("thread-1"),
                created_new: true,
            },
            &gmail_client,
            "draft.test.remote_state",
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("failed to open workflow store"),
            "unexpected error: {error}"
        );

        let detail = get_workflow_detail(
            &original_database_path,
            original_busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.gmail_draft_id, None);
        assert!(detail.current_draft.is_some());

        let requests = mock_server.received_requests().await.unwrap();
        let create_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST"
                    && request.url.path() == "/gmail/v1/users/me/drafts"
            })
            .count();
        let delete_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "DELETE"
                    && request.url.path() == "/gmail/v1/users/me/drafts/draft-created"
            })
            .count();
        assert_eq!(create_count, 0);
        assert_eq!(delete_count, 1);
    }

    #[tokio::test]
    async fn persist_remote_draft_state_retries_existing_remote_draft_write_after_transient_lock() {
        let mock_server = MockServer::start().await;
        let temp_dir = TempDir::new().unwrap();
        let mut config_report = config_report_for(&temp_dir, &mock_server);
        config_report.config.store.busy_timeout_ms = 1;
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        let (workflow, _) = upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
        let lock_handle = lock_workflow_store_after_delay(
            config_report.config.store.database_path.clone(),
            Duration::from_millis(0),
            Duration::from_millis(80),
        );

        let persisted = persist_remote_draft_state(
            &config_report,
            workflow,
            &RemoteDraftUpsert {
                gmail_draft_id: String::from("draft-updated"),
                gmail_draft_message_id: String::from("draft-message-updated"),
                gmail_draft_thread_id: String::from("thread-1"),
                created_new: false,
            },
            &gmail_client,
            "draft.test.remote_state",
        )
        .await
        .unwrap();
        lock_handle.join().unwrap();

        assert_eq!(persisted.gmail_draft_id.as_deref(), Some("draft-updated"));
        assert_eq!(
            persisted.gmail_draft_message_id.as_deref(),
            Some("draft-message-updated")
        );
        assert_eq!(persisted.gmail_draft_thread_id.as_deref(), Some("thread-1"));

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.gmail_draft_id.as_deref(),
            Some("draft-updated")
        );
        assert_eq!(
            detail.workflow.gmail_draft_message_id.as_deref(),
            Some("draft-message-updated")
        );
        assert_eq!(
            detail.workflow.gmail_draft_thread_id.as_deref(),
            Some("thread-1")
        );
    }

    #[tokio::test]
    async fn persist_remote_draft_state_reports_reconcile_failure_after_retry_exhaustion() {
        let mock_server = MockServer::start().await;
        let temp_dir = TempDir::new().unwrap();
        let mut config_report = config_report_for(&temp_dir, &mock_server);
        config_report.config.store.busy_timeout_ms = 1;
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        let (workflow, _) = upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        let gmail_client = crate::gmail_client_for_config(&config_report).unwrap();
        let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
            config_report.config.store.database_path.clone(),
            Duration::from_millis(450),
        );
        lock_ready.recv().unwrap();

        let error = persist_remote_draft_state(
            &config_report,
            workflow,
            &RemoteDraftUpsert {
                gmail_draft_id: String::from("draft-updated"),
                gmail_draft_message_id: String::from("draft-message-updated"),
                gmail_draft_thread_id: String::from("thread-1"),
                created_new: false,
            },
            &gmail_client,
            "draft.test.remote_state",
        )
        .await
        .unwrap_err();
        lock_handle.join().unwrap();

        match error {
            WorkflowServiceError::RemoteDraftStateReconcile {
                thread_id,
                draft_id,
                ..
            } => {
                assert_eq!(thread_id, "thread-1");
                assert_eq!(draft_id, "draft-updated");
            }
            other => panic!("expected RemoteDraftStateReconcile, got {other}"),
        }

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.gmail_draft_id, None);
    }

    #[tokio::test]
    async fn draft_send_refuses_to_recreate_missing_remote_draft() {
        let mock_server = MockServer::start().await;
        mount_profile(&mock_server).await;
        mount_thread(&mock_server).await;
        Mock::given(method("PUT"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "draft-2",
                "message": {
                    "id": "draft-message-2",
                    "threadId": "thread-1"
                }
            })))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts/send"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(150))
                    .set_body_json(serde_json::json!({
                        "id": "sent-message-1",
                        "threadId": "thread-1",
                        "historyId": "900"
                    })),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/drafts/send"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "sent-message-2",
                "threadId": "thread-1",
                "historyId": "901"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let error = draft_send(&config_report, String::from("thread-1"))
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "stored Gmail draft draft-1 for thread thread-1 no longer exists; refusing to recreate it during send because the previous send may have already succeeded; run `mailroom sync run` and inspect the thread before retrying"
        );

        let requests = mock_server.received_requests().await.unwrap();
        let update_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "PUT"
                    && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
            })
            .count();
        let create_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST"
                    && request.url.path() == "/gmail/v1/users/me/drafts"
            })
            .count();
        let send_count = requests
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST"
                    && request.url.path() == "/gmail/v1/users/me/drafts/send"
            })
            .count();
        assert_eq!(update_count, 1);
        assert_eq!(create_count, 0);
        assert_eq!(send_count, 0);
    }

    #[tokio::test]
    async fn draft_send_retries_mark_sent_after_transient_local_lock() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let mut config_report = resolve(&paths).unwrap();
        config_report.config.store.busy_timeout_ms = 1;
        init(&config_report).unwrap();
        accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();
        let workflow = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap()
        .workflow;
        let lock_handle = lock_workflow_store_after_delay(
            config_report.config.store.database_path.clone(),
            Duration::from_millis(0),
            Duration::from_millis(80),
        );

        let report = mark_sent_after_remote_send(&config_report, &workflow, "sent-message-1")
            .await
            .unwrap();
        lock_handle.join().unwrap();

        assert_eq!(
            report.current_stage,
            crate::store::workflows::WorkflowStage::Sent
        );
        assert_eq!(report.gmail_draft_id, None);
        assert_eq!(
            report.last_sent_message_id.as_deref(),
            Some("sent-message-1")
        );

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Sent
        );
        assert_eq!(detail.current_draft, None);
    }

    #[test]
    fn remove_attachment_by_path_or_name_removes_matching_filename() {
        let mut attachments = vec![
            sample_attachment("/tmp/one.txt", "one.txt"),
            sample_attachment("/tmp/two.txt", "two.txt"),
        ];

        let removed =
            remove_attachment_by_path_or_name(&mut attachments, "two.txt", Path::new("/tmp"));

        assert_eq!(removed, AttachmentRemovalResult::Removed);
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "one.txt");
    }

    #[test]
    fn remove_attachment_by_path_or_name_reports_when_nothing_matches() {
        let mut attachments = vec![sample_attachment("/tmp/one.txt", "one.txt")];

        let removed =
            remove_attachment_by_path_or_name(&mut attachments, "missing.txt", Path::new("/tmp"));

        assert_eq!(removed, AttachmentRemovalResult::NotFound);
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].file_name, "one.txt");
    }

    #[test]
    fn remove_attachment_by_path_or_name_resolves_relative_path_from_repo_root() {
        let repo_root = TempDir::new().unwrap();
        let relative_path = Path::new("notes").join("note.txt");
        let full_path = repo_root.path().join(&relative_path);
        fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        fs::write(&full_path, "hello").unwrap();

        let mut attachments = vec![sample_attachment(
            full_path.canonicalize().unwrap().to_str().unwrap(),
            "note.txt",
        )];

        let removed = remove_attachment_by_path_or_name(
            &mut attachments,
            relative_path.to_str().unwrap(),
            repo_root.path(),
        );

        assert_eq!(removed, AttachmentRemovalResult::Removed);
        assert!(attachments.is_empty());
    }

    #[test]
    fn remove_attachment_by_path_or_name_matches_relative_path_after_file_is_deleted() {
        let repo_root = TempDir::new().unwrap();
        let relative_path = Path::new("notes").join("note.txt");
        let full_path = repo_root.path().join(&relative_path);
        fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        fs::write(&full_path, "hello").unwrap();

        let mut attachments = vec![sample_attachment(
            full_path.canonicalize().unwrap().to_str().unwrap(),
            "note.txt",
        )];
        fs::remove_file(&full_path).unwrap();

        let removed = remove_attachment_by_path_or_name(
            &mut attachments,
            relative_path.to_str().unwrap(),
            repo_root.path(),
        );

        assert_eq!(removed, AttachmentRemovalResult::Removed);
        assert!(attachments.is_empty());
    }

    #[test]
    fn remove_attachment_by_path_or_name_rejects_ambiguous_filename_matches() {
        let mut attachments = vec![
            sample_attachment("/tmp/a/report.pdf", "report.pdf"),
            sample_attachment("/tmp/b/report.pdf", "report.pdf"),
        ];

        let removed =
            remove_attachment_by_path_or_name(&mut attachments, "report.pdf", Path::new("/tmp"));

        assert_eq!(removed, AttachmentRemovalResult::AmbiguousFileName);
        assert_eq!(attachments.len(), 2);
    }

    #[test]
    fn build_reply_recipients_uses_non_self_participant_when_latest_message_is_from_operator() {
        let recipients = build_reply_recipients(
            "operator@example.com",
            &sample_thread_message(
                "Operator <operator@example.com>",
                Some("operator@example.com"),
                "alice@example.com, operator@example.com",
                "carol@example.com",
                "",
            ),
            ReplyMode::Reply,
        )
        .unwrap();

        assert_eq!(
            recipients.to_addresses,
            vec![String::from("alice@example.com")]
        );
        assert!(recipients.cc_addresses.is_empty());
    }

    #[test]
    fn attachment_input_from_path_persists_a_normalized_absolute_path() {
        let current_dir = std::env::current_dir().unwrap();
        let temp_dir = tempfile::Builder::new()
            .prefix("mailroom-attachment-")
            .tempdir_in(&current_dir)
            .unwrap();
        let relative_dir = temp_dir.path().strip_prefix(&current_dir).unwrap();
        let relative_path = relative_dir.join("note.txt");
        fs::write(current_dir.join(&relative_path), "hello").unwrap();

        let attachment = attachment_input_from_path(&relative_path).unwrap();
        let expected_path = current_dir.join(&relative_path).canonicalize().unwrap();

        assert_eq!(Path::new(&attachment.path), expected_path.as_path());
        assert_eq!(attachment.file_name, "note.txt");
    }

    #[test]
    fn best_effort_sync_report_returns_none_when_sync_fails() {
        assert!(best_effort_sync_report(Err(anyhow!("stale history")), "sync failed").is_none());
    }

    #[test]
    fn best_effort_sync_report_preserves_successful_sync_results() {
        let report = SyncRunReport {
            mode: SyncMode::Incremental,
            fallback_from_history: false,
            bootstrap_query: String::from("newer_than:90d"),
            cursor_history_id: String::from("123"),
            pages_fetched: 1,
            messages_listed: 3,
            messages_upserted: 3,
            messages_deleted: 0,
            labels_synced: 4,
            store_message_count: 3,
            store_label_count: 4,
            store_indexed_message_count: 3,
        };

        let sync_report = best_effort_sync_report(Ok(report), "sync failed").unwrap();
        assert_eq!(sync_report.mode, SyncMode::Incremental);
        assert_eq!(sync_report.cursor_history_id, "123");
        assert_eq!(sync_report.messages_upserted, 3);
    }

    #[tokio::test]
    async fn list_workflows_uses_persisted_mailbox_account_after_logout() {
        let mock_server = MockServer::start().await;
        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        init(&config_report).unwrap();
        let account = accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        upsert_sync_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: account.account_id.clone(),
                cursor_history_id: Some(String::from("100")),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Incremental,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 100,
                last_full_sync_success_epoch_s: Some(100),
                last_incremental_sync_success_epoch_s: Some(100),
            },
        )
        .unwrap();
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();

        let logout_report = auth::logout(&config_report).unwrap();
        assert_eq!(logout_report.deactivated_accounts, 1);

        let report = list_workflows(&config_report, None, None).await.unwrap();

        assert_eq!(report.workflows.len(), 1);
        assert_eq!(report.workflows[0].thread_id, "thread-1");
    }

    #[tokio::test]
    async fn workflow_commands_use_persisted_workflow_account_after_logout_without_sync_state() {
        let mock_server = MockServer::start().await;
        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        init(&config_report).unwrap();
        let account = accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();

        let logout_report = auth::logout(&config_report).unwrap();
        assert_eq!(logout_report.deactivated_accounts, 1);

        let show = show_workflow(&config_report, String::from("thread-1"))
            .await
            .unwrap();
        assert_eq!(show.detail.workflow.thread_id, "thread-1");

        let report = list_workflows(&config_report, None, None).await.unwrap();
        assert_eq!(report.workflows.len(), 1);
        assert_eq!(report.workflows[0].thread_id, "thread-1");
    }

    #[tokio::test]
    async fn cleanup_archive_deletes_remote_draft_and_treats_sync_as_best_effort() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "710"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        init(&config_report).unwrap();
        let account = accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let report = cleanup_archive(&config_report, String::from("thread-1"), true)
            .await
            .unwrap();

        assert_eq!(
            report.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert!(report.sync_report.is_none());

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &account.account_id,
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.gmail_draft_id, None);
        assert_eq!(detail.current_draft, None);

        let requests = mock_server.received_requests().await.unwrap();
        let delete_index = requests
            .iter()
            .position(|request| {
                request.method.as_str() == "DELETE"
                    && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
            })
            .unwrap();
        let modify_index = requests
            .iter()
            .position(|request| {
                request.method.as_str() == "POST"
                    && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
            })
            .unwrap();
        assert!(modify_index < delete_index);
    }

    #[tokio::test]
    async fn promote_workflow_closed_deletes_remote_draft_after_local_close_persists() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let report = promote_workflow(
            &config_report,
            String::from("thread-1"),
            crate::store::workflows::WorkflowStage::Closed,
        )
        .await
        .unwrap();

        assert_eq!(
            report.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert_eq!(report.workflow.gmail_draft_id, None);

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert_eq!(detail.workflow.gmail_draft_id, None);

        let requests = mock_server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
    }

    #[tokio::test]
    async fn promote_workflow_closed_keeps_remote_draft_when_local_close_write_fails() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let mut config_report = config_report_for(&temp_dir, &mock_server);
        config_report.config.store.busy_timeout_ms = 1;
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();
        let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
            config_report.config.store.database_path.clone(),
            Duration::from_millis(150),
        );
        lock_ready.recv().unwrap();

        let _error = promote_workflow(
            &config_report,
            String::from("thread-1"),
            crate::store::workflows::WorkflowStage::Closed,
        )
        .await
        .unwrap_err();
        lock_handle.join().unwrap();

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Triage
        );
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));

        let requests = mock_server.received_requests().await.unwrap();
        assert!(!requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
    }

    #[tokio::test]
    async fn cleanup_label_validates_before_deleting_remote_draft() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "710"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        init(&config_report).unwrap();
        let account = accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &account.account_id,
            &[GmailLabel {
                id: String::from("Label_1"),
                name: String::from("Project/Alpha"),
                label_type: String::from("user"),
                message_list_visibility: None,
                label_list_visibility: None,
                messages_total: None,
                messages_unread: None,
                threads_total: None,
                threads_unread: None,
            }],
            100,
        )
        .unwrap();
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let error = cleanup_label(
            &config_report,
            String::from("thread-1"),
            true,
            vec![String::from("Missing/Label")],
            Vec::new(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "one or more add-label names were not found locally; run `mailroom sync run` first"
        );

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &account.account_id,
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));

        let requests = mock_server.received_requests().await.unwrap();
        assert!(!requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
        assert!(!requests.iter().any(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        }));
    }

    #[tokio::test]
    async fn cleanup_archive_treats_missing_remote_draft_as_already_deleted() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-stale"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "711"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        init(&config_report).unwrap();
        let account = accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::SetTriageStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Project"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Project status"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: account.account_id.clone(),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-stale")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 103,
            },
        )
        .unwrap();

        let report = cleanup_archive(&config_report, String::from("thread-1"), true)
            .await
            .unwrap();

        assert_eq!(
            report.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert_eq!(report.workflow.gmail_draft_id, None);

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &account.account_id,
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert_eq!(detail.workflow.gmail_draft_id, None);
        assert_eq!(detail.current_draft, None);

        let requests = mock_server.received_requests().await.unwrap();
        let delete_index = requests
            .iter()
            .position(|request| {
                request.method.as_str() == "DELETE"
                    && request.url.path() == "/gmail/v1/users/me/drafts/draft-stale"
            })
            .unwrap();
        let modify_index = requests
            .iter()
            .position(|request| {
                request.method.as_str() == "POST"
                    && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
            })
            .unwrap();
        assert!(modify_index < delete_index);
    }

    #[tokio::test]
    async fn cleanup_archive_keeps_local_remote_draft_state_when_cleanup_mutation_fails() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(500).set_body_string("mailbox mutation failed"))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let error = cleanup_archive(&config_report, String::from("thread-1"), true)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "gmail API request to users/me/threads/thread-1/modify failed with status 500 Internal Server Error: mailbox mutation failed"
        );

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert_eq!(
            detail.workflow.last_cleanup_action,
            Some(CleanupAction::Archive)
        );
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
        assert!(detail.current_draft.is_some());

        let requests = mock_server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        }));
        assert!(!requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
    }

    #[tokio::test]
    async fn cleanup_archive_keeps_remote_draft_when_local_cleanup_write_fails() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(150))
                    .set_body_json(serde_json::json!({
                        "id": "thread-1",
                        "historyId": "710"
                    })),
            )
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let mut config_report = config_report_for(&temp_dir, &mock_server);
        config_report.config.store.busy_timeout_ms = 1;
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();
        let (lock_handle, lock_ready) = lock_workflow_store_until_locked(
            config_report.config.store.database_path.clone(),
            Duration::from_millis(150),
        );
        lock_ready.recv().unwrap();

        let _error = cleanup_archive(&config_report, String::from("thread-1"), true)
            .await
            .unwrap_err();
        lock_handle.join().unwrap();

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Drafting
        );
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
        assert!(detail.current_draft.is_some());

        let requests = mock_server.received_requests().await.unwrap();
        assert!(!requests.iter().any(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        }));
        assert!(!requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
    }

    #[tokio::test]
    async fn cleanup_archive_keeps_draft_state_when_remote_delete_fails() {
        let mock_server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(500).set_body_string("draft delete failed"))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "710"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, &mock_server);
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report);
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Project"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: crate::store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Project"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &crate::store::workflows::RemoteDraftStateInput {
                account_id: String::from("gmail:operator@example.com"),
                thread_id: String::from("thread-1"),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from("thread-1")),
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();

        let _error = cleanup_archive(&config_report, String::from("thread-1"), true)
            .await
            .unwrap_err();

        let detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            detail.workflow.current_stage,
            crate::store::workflows::WorkflowStage::Closed
        );
        assert_eq!(
            detail.workflow.last_cleanup_action,
            Some(CleanupAction::Archive)
        );
        assert_eq!(detail.workflow.gmail_draft_id.as_deref(), Some("draft-1"));
        assert!(detail.current_draft.is_some());

        let requests = mock_server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        }));
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
    }

    fn sample_attachment(path: &str, file_name: &str) -> AttachmentInput {
        AttachmentInput {
            path: String::from(path),
            file_name: String::from(file_name),
            mime_type: String::from("text/plain"),
            size_bytes: 1,
        }
    }

    fn sample_thread_message(
        from_header: &str,
        from_address: Option<&str>,
        to_header: &str,
        cc_header: &str,
        reply_to_header: &str,
    ) -> GmailThreadMessage {
        GmailThreadMessage {
            id: String::from("m-1"),
            thread_id: String::from("thread-1"),
            history_id: String::from("400"),
            internal_date_epoch_ms: 100,
            snippet: String::from("snippet"),
            subject: String::from("Project"),
            from_header: String::from(from_header),
            from_address: from_address.map(String::from),
            to_header: String::from(to_header),
            cc_header: String::from(cc_header),
            bcc_header: String::new(),
            reply_to_header: String::from(reply_to_header),
            message_id_header: Some(String::from("<m-1@example.com>")),
            references_header: String::new(),
        }
    }

    fn config_report_for(temp_dir: &TempDir, mock_server: &MockServer) -> ConfigReport {
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let mut config_report = resolve(&paths).unwrap();
        config_report.config.gmail.api_base_url = format!("{}/gmail/v1", mock_server.uri());
        config_report.config.gmail.auth_url = format!("{}/oauth2/auth", mock_server.uri());
        config_report.config.gmail.token_url = format!("{}/oauth2/token", mock_server.uri());
        config_report.config.gmail.open_browser = false;
        config_report.config.gmail.client_id = Some(String::from("client-id"));
        config_report.config.gmail.client_secret = Some(String::from("client-secret"));
        config_report
    }

    fn seed_credentials(config_report: &ConfigReport) {
        let credential_store = FileCredentialStore::new(
            config_report
                .config
                .gmail
                .credential_path(&config_report.config.workspace),
        );
        credential_store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("access-token")),
                refresh_token: Some(SecretString::from(String::from("refresh-token"))),
                expires_at_epoch_s: Some(u64::MAX),
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();
    }

    fn seed_local_thread_snapshot(config_report: &ConfigReport) {
        seed_local_thread_snapshot_with_message(
            config_report,
            "m-1",
            1_700_000_000_000,
            "Project",
            "Alice <alice@example.com>",
            Some("alice@example.com"),
            "Project status",
        );
    }

    fn seed_local_thread_snapshot_with_message(
        config_report: &ConfigReport,
        message_id: &str,
        internal_date_epoch_ms: i64,
        subject: &str,
        from_header: &str,
        from_address: Option<&str>,
        snippet: &str,
    ) {
        init(config_report).unwrap();
        accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();
        upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &[GmailMessageUpsertInput {
                account_id: String::from("gmail:operator@example.com"),
                message_id: String::from(message_id),
                thread_id: String::from("thread-1"),
                history_id: String::from("101"),
                internal_date_epoch_ms,
                snippet: String::from(snippet),
                subject: String::from(subject),
                from_header: String::from(from_header),
                from_address: from_address.map(String::from),
                recipient_headers: String::from("operator@example.com"),
                to_header: String::from("operator@example.com"),
                cc_header: String::new(),
                bcc_header: String::new(),
                reply_to_header: String::new(),
                size_estimate: 123,
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
            }],
            100,
        )
        .unwrap();
    }

    async fn mount_profile(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/profile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "emailAddress": "operator@example.com",
                "messagesTotal": 1,
                "threadsTotal": 1,
                "historyId": "12345"
            })))
            .mount(mock_server)
            .await;
    }

    async fn mount_thread(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/threads/thread-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "500",
                "messages": [
                    {
                        "id": "m-1",
                        "threadId": "thread-1",
                        "historyId": "400",
                        "internalDate": "100",
                        "snippet": "Project status",
                        "payload": {
                            "headers": [
                                {"name": "Subject", "value": "Project"},
                                {"name": "From", "value": "\"Alice Example\" <alice@example.com>"},
                                {"name": "To", "value": "operator@example.com"},
                                {"name": "Message-ID", "value": "<m-1@example.com>"}
                            ]
                        }
                    }
                ]
            })))
            .mount(mock_server)
            .await;
    }

    fn lock_workflow_store_after_delay(
        database_path: PathBuf,
        start_delay: Duration,
        hold_for: Duration,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            std::thread::sleep(start_delay);
            let connection = rusqlite::Connection::open(database_path).unwrap();
            connection.busy_timeout(Duration::from_millis(1)).unwrap();
            loop {
                match connection.execute_batch("BEGIN IMMEDIATE;") {
                    Ok(()) => break,
                    Err(rusqlite::Error::SqliteFailure(error, _))
                        if error.code == rusqlite::ErrorCode::DatabaseBusy =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("failed to lock workflow store: {error}"),
                }
            }
            std::thread::sleep(hold_for);
            connection.execute_batch("ROLLBACK;").unwrap();
        })
    }

    fn lock_workflow_store_until_locked(
        database_path: PathBuf,
        hold_for: Duration,
    ) -> (std::thread::JoinHandle<()>, std::sync::mpsc::Receiver<()>) {
        let (ready_tx, ready_rx) = sync_channel(1);
        let handle = std::thread::spawn(move || {
            let connection = rusqlite::Connection::open(database_path).unwrap();
            connection.busy_timeout(Duration::from_millis(1)).unwrap();
            loop {
                match connection.execute_batch("BEGIN IMMEDIATE;") {
                    Ok(()) => {
                        ready_tx.send(()).unwrap();
                        break;
                    }
                    Err(rusqlite::Error::SqliteFailure(error, _))
                        if error.code == rusqlite::ErrorCode::DatabaseBusy =>
                    {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("failed to lock workflow store: {error}"),
                }
            }
            std::thread::sleep(hold_for);
            connection.execute_batch("ROLLBACK;").unwrap();
        });
        (handle, ready_rx)
    }
}
