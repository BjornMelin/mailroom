use super::draft_remote::{
    mark_sent_after_remote_send, persist_remote_draft_state, update_remote_draft_for_send,
    upsert_remote_draft,
};
use super::message_build::{build_raw_message, build_reply_recipients, normalize_reply_subject};
use super::queries::{
    action_report, best_effort_sync_report, latest_thread_message, resolve_active_account,
    thread_message_by_id, workflow_detail, workflow_snapshot_from_message,
};
use super::{WorkflowResult, join_blocking};
use crate::config::ConfigReport;
use crate::mailbox;
use crate::store;
use crate::workflows::{WorkflowAction, WorkflowActionReport, WorkflowServiceError};
use std::fs;
use std::path::{Component, Path, PathBuf};
use tokio::task::spawn_blocking;

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
pub(super) enum AttachmentRemovalResult {
    Removed,
    NotFound,
    AmbiguousFileName,
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
        workflow.gmail_draft_id.as_deref(),
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

pub(super) fn attachment_input_from_path(
    path: &Path,
) -> WorkflowResult<store::workflows::AttachmentInput> {
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

pub(super) fn remove_attachment_by_path_or_name(
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
    let path = Path::new(path_or_name);
    if let Ok(canonical_path) = path.canonicalize() {
        return Some(canonical_path);
    }
    lexical_absolute_path(path, base_dir).ok()
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
