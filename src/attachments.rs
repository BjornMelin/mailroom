use crate::config::ConfigReport;
use crate::store;
use crate::time::current_epoch_seconds;
use crate::workspace::WorkspacePaths;
use crate::{configured_paths, gmail_client_for_config};
use anyhow::Result;
use sanitize_filename::sanitize;
use serde::Serialize;
use std::fs;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use tokio::task::spawn_blocking;

pub const DEFAULT_ATTACHMENT_LIST_LIMIT: usize = 50;

#[derive(Debug, Clone)]
pub struct AttachmentListRequest {
    pub thread_id: Option<String>,
    pub message_id: Option<String>,
    pub filename: Option<String>,
    pub mime_type: Option<String>,
    pub fetched_only: bool,
    pub limit: usize,
}

#[derive(Debug, Error)]
pub enum AttachmentServiceError {
    #[error("no active Gmail account found; run `mailroom auth login` first")]
    NoActiveAccount,
    #[error("attachment `{attachment_key}` was not found in the local mailbox catalog")]
    AttachmentNotFound { attachment_key: String },
    #[error("attachment list limit must be greater than zero")]
    InvalidLimit,
    #[error("attachment vault path `{relative_path}` is invalid")]
    InvalidVaultPath { relative_path: String },
    #[error("export destination already exists with different content: {path}")]
    DestinationConflict { path: PathBuf },
    #[error("failed to join blocking attachment task: {source}")]
    BlockingTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("failed to create directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write file {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to copy file from {source_path} to {destination_path}: {source}")]
    CopyFile {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to persist attachment store state: {source}")]
    StoreWrite {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to read attachment state from local mailbox store: {source}")]
    StoreRead {
        #[source]
        source: store::mailbox::MailboxReadError,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentListReport {
    pub account_id: String,
    pub thread_id: Option<String>,
    pub message_id: Option<String>,
    pub filename: Option<String>,
    pub mime_type: Option<String>,
    pub fetched_only: bool,
    pub limit: usize,
    pub items: Vec<store::mailbox::AttachmentListItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentShowReport {
    pub account_id: String,
    pub attachment: store::mailbox::AttachmentDetailRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentFetchReport {
    pub account_id: String,
    pub attachment_key: String,
    pub message_id: String,
    pub thread_id: String,
    pub filename: String,
    pub mime_type: String,
    pub size_bytes: i64,
    pub content_hash: String,
    pub vault_relative_path: String,
    pub vault_path: PathBuf,
    pub downloaded: bool,
    pub fetched_at_epoch_s: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttachmentExportReport {
    pub account_id: String,
    pub attachment_key: String,
    pub message_id: String,
    pub thread_id: String,
    pub filename: String,
    pub content_hash: String,
    pub source_vault_path: PathBuf,
    pub destination_path: PathBuf,
    pub copied: bool,
    pub exported_at_epoch_s: i64,
}

impl AttachmentListReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("account_id={}", self.account_id);
            println!("items={}", self.items.len());
            for item in &self.items {
                println!(
                    "{}\t{}\t{}\tfetched={}\texports={}",
                    item.attachment_key,
                    item.filename,
                    item.mime_type,
                    item.vault_relative_path.is_some(),
                    item.export_count,
                );
            }
        }
        Ok(())
    }
}

impl AttachmentShowReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            let attachment = &self.attachment;
            println!("account_id={}", self.account_id);
            println!("attachment_key={}", attachment.attachment_key);
            println!("message_id={}", attachment.message_id);
            println!("thread_id={}", attachment.thread_id);
            println!("filename={}", attachment.filename);
            println!("mime_type={}", attachment.mime_type);
            println!("size_bytes={}", attachment.size_bytes);
            println!("fetched={}", attachment.vault_relative_path.is_some());
            println!("export_count={}", attachment.export_count);
            match &attachment.vault_relative_path {
                Some(path) => println!("vault_relative_path={path}"),
                None => println!("vault_relative_path=<none>"),
            }
        }
        Ok(())
    }
}

impl AttachmentFetchReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("account_id={}", self.account_id);
            println!("attachment_key={}", self.attachment_key);
            println!("message_id={}", self.message_id);
            println!("thread_id={}", self.thread_id);
            println!("filename={}", self.filename);
            println!("mime_type={}", self.mime_type);
            println!("size_bytes={}", self.size_bytes);
            println!("content_hash={}", self.content_hash);
            println!("downloaded={}", self.downloaded);
            println!("vault_relative_path={}", self.vault_relative_path);
            println!("vault_path={}", self.vault_path.display());
            println!("fetched_at_epoch_s={}", self.fetched_at_epoch_s);
        }
        Ok(())
    }
}

impl AttachmentExportReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("account_id={}", self.account_id);
            println!("attachment_key={}", self.attachment_key);
            println!("filename={}", self.filename);
            println!("content_hash={}", self.content_hash);
            println!("copied={}", self.copied);
            println!("source_vault_path={}", self.source_vault_path.display());
            println!("destination_path={}", self.destination_path.display());
            println!("exported_at_epoch_s={}", self.exported_at_epoch_s);
        }
        Ok(())
    }
}

pub async fn list(
    config_report: &ConfigReport,
    request: AttachmentListRequest,
) -> Result<AttachmentListReport> {
    if request.limit == 0 {
        return Err(AttachmentServiceError::InvalidLimit.into());
    }

    store::init(config_report)?;
    let account_id = resolve_attachment_account_id(config_report)?;
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
    store::init(config_report)?;
    let account_id = resolve_attachment_account_id(config_report)?;
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
    store::init(config_report)?;
    let account_id = resolve_attachment_account_id(config_report)?;
    let workspace_paths = configured_paths(config_report)?;
    workspace_paths.ensure_runtime_dirs()?;
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
        return Err(map_vault_state_write_error(error).into());
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
    workspace_paths.ensure_runtime_dirs()?;
    let filename = export_filename(&fetched.filename, &fetched.attachment_key);
    let destination_path = match destination {
        Some(path) if path.is_dir() => path.join(&filename),
        Some(path) => path,
        None => default_export_path(
            &workspace_paths,
            &fetched.thread_id,
            &fetched.message_id,
            &filename,
        ),
    };
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
    if let Err(source) = record_result {
        return Err(AttachmentServiceError::StoreWrite { source }.into());
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

fn resolve_attachment_account_id(config_report: &ConfigReport) -> Result<String> {
    if let Some(active_account) = store::accounts::get_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )? {
        return Ok(active_account.account_id);
    }

    if let Some(mailbox) = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )? && let Some(sync_state) = mailbox.sync_state
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

fn existing_vault_report(
    workspace_paths: &WorkspacePaths,
    account_id: &str,
    detail: &store::mailbox::AttachmentDetailRecord,
) -> Result<Option<AttachmentFetchReport>> {
    let Some(relative_path) = detail.vault_relative_path.as_deref() else {
        return Ok(None);
    };
    let Some(content_hash) = detail.vault_content_hash.clone() else {
        return Ok(None);
    };
    let Some(fetched_at_epoch_s) = detail.vault_fetched_at_epoch_s else {
        return Ok(None);
    };
    let path = resolve_vault_relative_path(workspace_paths, relative_path)?;
    if !path.exists() {
        return Ok(None);
    }
    let metadata = fs::metadata(&path).map_err(|source| AttachmentServiceError::ReadFile {
        path: path.clone(),
        source,
    })?;
    if !metadata.is_file() {
        return Ok(None);
    }
    if let Some(expected_size) = detail.vault_size_bytes {
        let observed_size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);
        if observed_size != expected_size {
            return Ok(None);
        }
    }
    if hash_file_blake3(&path)? != content_hash {
        return Ok(None);
    }

    Ok(Some(AttachmentFetchReport {
        account_id: account_id.to_owned(),
        attachment_key: detail.attachment_key.clone(),
        message_id: detail.message_id.clone(),
        thread_id: detail.thread_id.clone(),
        filename: detail.filename.clone(),
        mime_type: detail.mime_type.clone(),
        size_bytes: detail.size_bytes,
        content_hash,
        vault_relative_path: relative_path.to_owned(),
        vault_path: path,
        downloaded: false,
        fetched_at_epoch_s,
    }))
}

fn resolve_vault_relative_path(
    workspace_paths: &WorkspacePaths,
    relative_path: &str,
) -> Result<PathBuf> {
    let path = Path::new(relative_path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(AttachmentServiceError::InvalidVaultPath {
            relative_path: relative_path.to_owned(),
        }
        .into());
    }

    Ok(workspace_paths.vault_dir.join(path))
}

fn export_filename(filename: &str, attachment_key: &str) -> String {
    let trimmed = filename.trim();
    if !trimmed.is_empty() {
        let sanitized = sanitize(trimmed).to_string();
        if !sanitized.trim().is_empty() {
            return sanitized;
        }
    }
    format!(
        "attachment-{}.bin",
        sanitize_non_empty(attachment_key, "attachment")
    )
}

fn default_export_path(
    workspace_paths: &WorkspacePaths,
    thread_id: &str,
    message_id: &str,
    filename: &str,
) -> PathBuf {
    workspace_paths
        .exports_dir
        .join(sanitize_non_empty(thread_id, "thread"))
        .join(format!(
            "{}--{}",
            sanitize_non_empty(message_id, "message"),
            filename
        ))
}

fn sanitize_non_empty(value: &str, fallback: &str) -> String {
    let sanitized = sanitize(value).to_string();
    if sanitized.trim().is_empty() {
        return fallback.to_owned();
    }
    sanitized
}

struct VaultWriteResult {
    content_hash: String,
    relative_path: String,
    path: PathBuf,
    size_bytes: i64,
}

fn write_vault_bytes(vault_dir: &Path, bytes: Vec<u8>) -> Result<VaultWriteResult> {
    let content_hash = blake3::hash(&bytes).to_hex().to_string();
    let relative_path = format!("blake3/{}/{}", &content_hash[..2], &content_hash);
    let path = vault_dir.join(&relative_path);
    let parent = path
        .parent()
        .ok_or_else(|| AttachmentServiceError::InvalidVaultPath {
            relative_path: relative_path.clone(),
        })?;
    fs::create_dir_all(parent).map_err(|source| AttachmentServiceError::CreateDirectory {
        path: parent.to_path_buf(),
        source,
    })?;
    if !path.exists() {
        fs::write(&path, &bytes).map_err(|source| AttachmentServiceError::WriteFile {
            path: path.clone(),
            source,
        })?;
        harden_vault_file_permissions(&path)?;
    }
    let size_bytes = i64::try_from(bytes.len()).unwrap_or(i64::MAX);

    Ok(VaultWriteResult {
        content_hash,
        relative_path,
        path,
        size_bytes,
    })
}

struct CopyFromVaultResult {
    copied: bool,
}

fn copy_from_vault(
    source_path: &Path,
    destination_path: &Path,
    content_hash: &str,
) -> Result<CopyFromVaultResult> {
    let parent =
        destination_path
            .parent()
            .ok_or_else(|| AttachmentServiceError::CreateDirectory {
                path: destination_path.to_path_buf(),
                source: std::io::Error::other("destination path has no parent"),
            })?;
    fs::create_dir_all(parent).map_err(|source| AttachmentServiceError::CreateDirectory {
        path: parent.to_path_buf(),
        source,
    })?;

    if destination_path.exists() {
        let existing_hash = hash_file_blake3(destination_path)?;
        if existing_hash == content_hash {
            return Ok(CopyFromVaultResult { copied: false });
        }
        return Err(AttachmentServiceError::DestinationConflict {
            path: destination_path.to_path_buf(),
        }
        .into());
    }

    fs::copy(source_path, destination_path).map_err(|source| AttachmentServiceError::CopyFile {
        source_path: source_path.to_path_buf(),
        destination_path: destination_path.to_path_buf(),
        source,
    })?;
    Ok(CopyFromVaultResult { copied: true })
}

fn hash_file_blake3(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).map_err(|source| AttachmentServiceError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| AttachmentServiceError::ReadFile {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn map_vault_state_write_error(error: store::mailbox::MailboxWriteError) -> AttachmentServiceError {
    match error {
        store::mailbox::MailboxWriteError::AttachmentNotFound { attachment_key, .. } => {
            AttachmentServiceError::AttachmentNotFound { attachment_key }
        }
        store::mailbox::MailboxWriteError::Query(source) => AttachmentServiceError::StoreWrite {
            source: source.into(),
        },
        store::mailbox::MailboxWriteError::Unexpected(source) => {
            AttachmentServiceError::StoreWrite { source }
        }
    }
}

#[cfg(unix)]
fn harden_vault_file_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
        AttachmentServiceError::WriteFile {
            path: path.to_path_buf(),
            source,
        }
        .into()
    })
}

#[cfg(not(unix))]
fn harden_vault_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        copy_from_vault, default_export_path, existing_vault_report, export_filename,
        hash_file_blake3, map_vault_state_write_error, resolve_vault_relative_path,
    };
    use crate::store::mailbox::AttachmentDetailRecord;
    use crate::workspace::WorkspacePaths;
    use std::fs;
    use std::path::PathBuf;
    use std::time::Instant;
    use tempfile::TempDir;

    #[test]
    fn export_filename_falls_back_when_gmail_filename_is_blank() {
        assert_eq!(export_filename("", "m-1:2"), "attachment-m-12.bin");
    }

    #[test]
    fn export_filename_falls_back_when_sanitized_filename_is_empty() {
        assert_eq!(export_filename("///", "m-1:2"), "attachment-m-12.bin");
    }

    #[test]
    fn default_export_path_uses_thread_and_message_partitions() {
        let paths = WorkspacePaths::from_repo_root(PathBuf::from("/tmp/mailroom"));
        let path = default_export_path(&paths, "thread-1", "message-1", "note.pdf");

        assert_eq!(
            path,
            PathBuf::from("/tmp/mailroom/.mailroom/exports/thread-1/message-1--note.pdf")
        );
    }

    #[test]
    fn default_export_path_falls_back_when_partition_ids_sanitize_to_empty() {
        let paths = WorkspacePaths::from_repo_root(PathBuf::from("/tmp/mailroom"));
        let path = default_export_path(&paths, "///", "\\\\", "note.pdf");

        assert_eq!(
            path,
            PathBuf::from("/tmp/mailroom/.mailroom/exports/thread/message--note.pdf")
        );
    }

    #[test]
    fn resolve_vault_relative_path_rejects_parent_traversal() {
        let paths = WorkspacePaths::from_repo_root(PathBuf::from("/tmp/mailroom"));
        let error = resolve_vault_relative_path(&paths, "../escape.bin").unwrap_err();

        assert!(error.to_string().contains("invalid"));
    }

    #[test]
    fn existing_vault_report_requires_hash_match_before_reuse() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();

        let relative_path = "blake3/ab/abc123";
        let vault_path = paths.vault_dir.join(relative_path);
        fs::create_dir_all(vault_path.parent().unwrap()).unwrap();
        fs::write(&vault_path, b"hello").unwrap();

        let report = existing_vault_report(
            &paths,
            "gmail:operator@example.com",
            &detail_with_vault(relative_path, "invalid-hash", 5),
        )
        .unwrap();

        assert!(report.is_none());
    }

    #[test]
    fn existing_vault_report_reuses_matching_vault_file() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();

        let bytes = b"hello";
        let content_hash = blake3::hash(bytes).to_hex().to_string();
        let relative_path = format!("blake3/{}/{}", &content_hash[..2], content_hash);
        let vault_path = paths.vault_dir.join(&relative_path);
        fs::create_dir_all(vault_path.parent().unwrap()).unwrap();
        fs::write(&vault_path, bytes).unwrap();

        let report = existing_vault_report(
            &paths,
            "gmail:operator@example.com",
            &detail_with_vault(&relative_path, &content_hash, 5),
        )
        .unwrap();

        assert!(report.is_some());
        assert!(!report.unwrap().downloaded);
    }

    #[test]
    fn map_vault_state_write_error_maps_missing_rows_to_attachment_not_found() {
        let mapped = map_vault_state_write_error(
            crate::store::mailbox::MailboxWriteError::AttachmentNotFound {
                account_id: String::from("gmail:operator@example.com"),
                attachment_key: String::from("m-1:1.2"),
            },
        );
        assert!(matches!(
            mapped,
            super::AttachmentServiceError::AttachmentNotFound { attachment_key }
            if attachment_key == "m-1:1.2"
        ));
    }

    #[test]
    #[ignore = "benchmark harness; run manually with: cargo test benchmark_attachment_export_hash_compare_tiers -- --ignored --nocapture"]
    fn benchmark_attachment_export_hash_compare_tiers() {
        const COPY_ITERATIONS: usize = 8;
        const HASH_COMPARE_ITERATIONS: usize = 20;
        let tiers = [
            ("small", 64 * 1024_usize),
            ("medium", 1024 * 1024_usize),
            ("large", 8 * 1024 * 1024_usize),
        ];

        for (tier_name, size_bytes) in tiers {
            let temp_dir = TempDir::new().unwrap();
            let source_path = temp_dir.path().join("source.bin");
            let destination_path = temp_dir.path().join("exports/export.bin");
            fs::write(&source_path, vec![0xAC_u8; size_bytes]).unwrap();
            let source_hash = hash_file_blake3(&source_path).unwrap();

            let copy_started_at = Instant::now();
            for _ in 0..COPY_ITERATIONS {
                if destination_path.exists() {
                    fs::remove_file(&destination_path).unwrap();
                }
                let copied =
                    copy_from_vault(&source_path, &destination_path, &source_hash).unwrap();
                assert!(copied.copied);
            }
            let copy_elapsed = copy_started_at.elapsed();

            let compare_started_at = Instant::now();
            for _ in 0..HASH_COMPARE_ITERATIONS {
                let copied =
                    copy_from_vault(&source_path, &destination_path, &source_hash).unwrap();
                assert!(!copied.copied);
            }
            let compare_elapsed = compare_started_at.elapsed();

            let copy_avg_ms = copy_elapsed.as_secs_f64() * 1_000.0 / COPY_ITERATIONS as f64;
            let compare_avg_ms =
                compare_elapsed.as_secs_f64() * 1_000.0 / HASH_COMPARE_ITERATIONS as f64;
            println!(
                "{{\"bench\":\"attachment_lane.export\",\"tier\":\"{tier_name}\",\"size_bytes\":{size_bytes},\"copy_avg_ms\":{copy_avg_ms:.3},\"hash_compare_avg_ms\":{compare_avg_ms:.3}}}"
            );
        }
    }

    fn detail_with_vault(
        relative_path: &str,
        content_hash: &str,
        vault_size_bytes: i64,
    ) -> AttachmentDetailRecord {
        AttachmentDetailRecord {
            attachment_key: String::from("m-1:1.2"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            part_id: String::from("1.2"),
            gmail_attachment_id: Some(String::from("att-1")),
            filename: String::from("statement.pdf"),
            mime_type: String::from("application/pdf"),
            size_bytes: 5,
            content_disposition: None,
            content_id: None,
            is_inline: false,
            internal_date_epoch_ms: 1_700_000_000_000,
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            vault_content_hash: Some(content_hash.to_owned()),
            vault_relative_path: Some(relative_path.to_owned()),
            vault_size_bytes: Some(vault_size_bytes),
            vault_fetched_at_epoch_s: Some(101),
            export_count: 0,
        }
    }
}
