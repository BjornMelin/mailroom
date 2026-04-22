use super::export::{
    cleanup_export_file_task, copy_from_vault, export_filename, resolve_export_destination_path,
};
use super::vault::{existing_vault_report, write_vault_bytes};
use super::{
    AttachmentExportReport, AttachmentFetchReport, AttachmentListReport, AttachmentListRequest,
    AttachmentServiceError, AttachmentShowReport,
};
use crate::config::ConfigReport;
use crate::store;
use crate::time::current_epoch_seconds;
use crate::workspace::WorkspacePaths;
use crate::{configured_paths, gmail_client_for_config};
use anyhow::Result;
use std::path::{Path, PathBuf};
use tokio::task::spawn_blocking;

pub async fn list(
    config_report: &ConfigReport,
    request: AttachmentListRequest,
) -> Result<AttachmentListReport> {
    if request.limit == 0 {
        return Err(AttachmentServiceError::InvalidLimit.into());
    }

    init_store_task(config_report).await?;
    let account_id = resolve_attachment_account_id_task(config_report).await?;
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let query = store::mailbox::AttachmentListQuery {
        account_id: account_id.clone(),
        thread_id: request.thread_id.clone(),
        message_id: request.message_id.clone(),
        filename: request.filename.clone(),
        mime_type: request.mime_type.clone(),
        fetched_only: request.fetched_only,
        limit: request.limit,
    };
    let items = spawn_blocking(move || {
        store::mailbox::list_attachments(&database_path, busy_timeout_ms, &query)
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })?
    .map_err(|source| AttachmentServiceError::StoreRead { source })?;

    Ok(AttachmentListReport {
        account_id,
        thread_id: request.thread_id,
        message_id: request.message_id,
        filename: request.filename,
        mime_type: request.mime_type,
        fetched_only: request.fetched_only,
        limit: request.limit,
        items,
    })
}

pub async fn show(
    config_report: &ConfigReport,
    attachment_key: String,
) -> Result<AttachmentShowReport> {
    init_store_task(config_report).await?;
    let account_id = resolve_attachment_account_id_task(config_report).await?;
    let detail = load_attachment_detail(config_report, &account_id, &attachment_key).await?;

    Ok(AttachmentShowReport {
        account_id,
        attachment: detail,
    })
}

pub async fn fetch(
    config_report: &ConfigReport,
    attachment_key: String,
) -> Result<AttachmentFetchReport> {
    init_store_task(config_report).await?;
    let account_id = resolve_attachment_account_id_task(config_report).await?;
    let workspace_paths = configured_paths(config_report)?;
    ensure_runtime_dirs_task(workspace_paths.clone()).await?;
    let detail = load_attachment_detail(config_report, &account_id, &attachment_key).await?;

    let existing_report = spawn_blocking({
        let workspace_paths = workspace_paths.clone();
        let account_id = account_id.clone();
        let detail = detail.clone();
        move || existing_vault_report(&workspace_paths, &account_id, &detail)
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })??;
    if let Some(existing_report) = existing_report {
        return Ok(existing_report);
    }

    let gmail_client = gmail_client_for_config(config_report)?;
    let bytes = gmail_client
        .get_attachment_bytes(
            &detail.message_id,
            &detail.part_id,
            detail.gmail_attachment_id.as_deref(),
        )
        .await?;
    let fetched_at_epoch_s = current_epoch_seconds()?;
    let vault_write = spawn_blocking({
        let vault_dir = workspace_paths.vault_dir.clone();
        move || write_vault_bytes(&vault_dir, bytes)
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })??;

    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let update = store::mailbox::AttachmentVaultStateUpdate {
        account_id: account_id.clone(),
        attachment_key: detail.attachment_key.clone(),
        content_hash: vault_write.content_hash.clone(),
        relative_path: vault_write.relative_path.clone(),
        size_bytes: vault_write.size_bytes,
        fetched_at_epoch_s,
    };
    let update_result = spawn_blocking(move || {
        store::mailbox::set_attachment_vault_state(&database_path, busy_timeout_ms, &update)
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })?;
    if let Err(error) = update_result {
        return Err(map_mailbox_write_error(error).into());
    }

    Ok(AttachmentFetchReport {
        account_id,
        attachment_key: detail.attachment_key,
        message_id: detail.message_id,
        thread_id: detail.thread_id,
        filename: detail.filename,
        mime_type: detail.mime_type,
        size_bytes: detail.size_bytes,
        content_hash: vault_write.content_hash,
        vault_relative_path: vault_write.relative_path,
        vault_path: vault_write.path,
        downloaded: true,
        fetched_at_epoch_s,
    })
}

pub async fn export(
    config_report: &ConfigReport,
    attachment_key: String,
    destination: Option<PathBuf>,
) -> Result<AttachmentExportReport> {
    let fetched = fetch(config_report, attachment_key).await?;
    let workspace_paths = configured_paths(config_report)?;
    let filename = export_filename(&fetched.filename, &fetched.attachment_key);
    let destination_path = spawn_blocking({
        let workspace_paths = workspace_paths.clone();
        let thread_id = fetched.thread_id.clone();
        let message_id = fetched.message_id.clone();
        let attachment_key = fetched.attachment_key.clone();
        let filename = filename.clone();
        move || {
            resolve_export_destination_path(
                &workspace_paths,
                &thread_id,
                &message_id,
                &attachment_key,
                &filename,
                destination,
            )
        }
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })??;
    let exported_at_epoch_s = current_epoch_seconds()?;
    let copy_result = spawn_blocking({
        let source_path = fetched.vault_path.clone();
        let destination_path = destination_path.clone();
        let content_hash = fetched.content_hash.clone();
        move || copy_from_vault(&source_path, &destination_path, &content_hash)
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })??;

    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let event = store::mailbox::AttachmentExportEventInput {
        account_id: fetched.account_id.clone(),
        attachment_key: fetched.attachment_key.clone(),
        message_id: fetched.message_id.clone(),
        thread_id: fetched.thread_id.clone(),
        destination_path: destination_path.display().to_string(),
        content_hash: fetched.content_hash.clone(),
        exported_at_epoch_s,
    };
    let record_result = spawn_blocking(move || {
        store::mailbox::record_attachment_export(&database_path, busy_timeout_ms, &event)
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })?;
    if let Err(error) = record_result {
        if copy_result.copied {
            cleanup_export_file_task(destination_path.clone()).await;
        }
        return Err(map_mailbox_write_error(error).into());
    }

    Ok(AttachmentExportReport {
        account_id: fetched.account_id,
        attachment_key: fetched.attachment_key,
        message_id: fetched.message_id,
        thread_id: fetched.thread_id,
        filename: fetched.filename,
        content_hash: fetched.content_hash,
        source_vault_path: fetched.vault_path,
        destination_path,
        copied: copy_result.copied,
        exported_at_epoch_s,
    })
}

async fn resolve_attachment_account_id_task(config_report: &ConfigReport) -> Result<String> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    spawn_blocking(move || resolve_attachment_account_id(&database_path, busy_timeout_ms))
        .await
        .map_err(|source| AttachmentServiceError::BlockingTask { source })?
}

async fn init_store_task(config_report: &ConfigReport) -> Result<()> {
    let config_report = config_report.clone();
    spawn_blocking(move || store::init(&config_report))
        .await
        .map_err(|source| AttachmentServiceError::BlockingTask { source })?
        .map(|_| ())
}

async fn ensure_runtime_dirs_task(workspace_paths: WorkspacePaths) -> Result<()> {
    spawn_blocking(move || workspace_paths.ensure_runtime_dirs())
        .await
        .map_err(|source| AttachmentServiceError::BlockingTask { source })?
        .map(|_| ())
}

fn resolve_attachment_account_id(database_path: &Path, busy_timeout_ms: u64) -> Result<String> {
    if let Some(active_account) = store::accounts::get_active(database_path, busy_timeout_ms)? {
        return Ok(active_account.account_id);
    }

    if let Some(mailbox) = store::mailbox::inspect_mailbox(database_path, busy_timeout_ms)?
        && let Some(sync_state) = mailbox.sync_state
    {
        return Ok(sync_state.account_id);
    }

    Err(AttachmentServiceError::NoActiveAccount.into())
}

async fn load_attachment_detail(
    config_report: &ConfigReport,
    account_id: &str,
    attachment_key: &str,
) -> Result<store::mailbox::AttachmentDetailRecord> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = account_id.to_owned();
    let attachment_key_owned = attachment_key.to_owned();
    let detail = spawn_blocking(move || {
        store::mailbox::get_attachment_detail(
            &database_path,
            busy_timeout_ms,
            &account_id,
            &attachment_key_owned,
        )
    })
    .await
    .map_err(|source| AttachmentServiceError::BlockingTask { source })?
    .map_err(|source| AttachmentServiceError::StoreRead { source })?;

    detail.ok_or_else(|| {
        AttachmentServiceError::AttachmentNotFound {
            attachment_key: attachment_key.to_owned(),
        }
        .into()
    })
}

pub(super) fn map_mailbox_write_error(
    error: store::mailbox::MailboxWriteError,
) -> AttachmentServiceError {
    match error {
        store::mailbox::MailboxWriteError::AttachmentNotFound { attachment_key, .. } => {
            AttachmentServiceError::AttachmentNotFound { attachment_key }
        }
        error => AttachmentServiceError::StoreWrite {
            source: error.into(),
        },
    }
}
