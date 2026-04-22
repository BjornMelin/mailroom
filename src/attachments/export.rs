use super::error::AttachmentServiceError;
use super::vault::hash_file_blake3;
use crate::workspace::WorkspacePaths;
use anyhow::Result;
use sanitize_filename::sanitize;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::task::spawn_blocking;

pub(super) fn export_filename(filename: &str, attachment_key: &str) -> String {
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

pub(super) fn default_export_path(
    workspace_paths: &WorkspacePaths,
    thread_id: &str,
    message_id: &str,
    attachment_key: &str,
    filename: &str,
) -> PathBuf {
    workspace_paths
        .exports_dir
        .join(sanitize_non_empty(thread_id, "thread"))
        .join(format!(
            "{}--{}--{}",
            sanitize_non_empty(message_id, "message"),
            sanitize_non_empty(attachment_key, "attachment"),
            filename
        ))
}

pub(super) fn resolve_export_destination_path(
    workspace_paths: &WorkspacePaths,
    thread_id: &str,
    message_id: &str,
    attachment_key: &str,
    filename: &str,
    destination: Option<PathBuf>,
) -> Result<PathBuf> {
    Ok(match destination {
        Some(path) if path.is_dir() => path.join(filename),
        Some(path) => path,
        None => default_export_path(
            workspace_paths,
            thread_id,
            message_id,
            attachment_key,
            filename,
        ),
    })
}

fn sanitize_non_empty(value: &str, fallback: &str) -> String {
    let sanitized = sanitize(value).to_string();
    if sanitized.trim().is_empty() {
        return fallback.to_owned();
    }
    sanitized
}

#[derive(Debug)]
pub(super) struct CopyFromVaultResult {
    pub(super) copied: bool,
}

pub(super) fn copy_from_vault(
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

    let mut source_file =
        fs::File::open(source_path).map_err(|source| AttachmentServiceError::CopyFile {
            source_path: source_path.to_path_buf(),
            destination_path: destination_path.to_path_buf(),
            source,
        })?;
    let mut destination_file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination_path)
    {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing_hash = hash_file_blake3(destination_path)?;
            if existing_hash == content_hash {
                return Ok(CopyFromVaultResult { copied: false });
            }
            return Err(AttachmentServiceError::DestinationConflict {
                path: destination_path.to_path_buf(),
            }
            .into());
        }
        Err(source) => {
            return Err(AttachmentServiceError::CopyFile {
                source_path: source_path.to_path_buf(),
                destination_path: destination_path.to_path_buf(),
                source,
            }
            .into());
        }
    };
    let write_result = (|| -> Result<()> {
        std::io::copy(&mut source_file, &mut destination_file).map_err(|source| {
            AttachmentServiceError::CopyFile {
                source_path: source_path.to_path_buf(),
                destination_path: destination_path.to_path_buf(),
                source,
            }
        })?;
        destination_file
            .sync_all()
            .map_err(|source| AttachmentServiceError::CopyFile {
                source_path: source_path.to_path_buf(),
                destination_path: destination_path.to_path_buf(),
                source,
            })?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(destination_path);
    }
    write_result?;
    Ok(CopyFromVaultResult { copied: true })
}

pub(super) async fn cleanup_export_file_task(destination_path: PathBuf) {
    let cleanup_path = destination_path.clone();
    let join_result = spawn_blocking(move || fs::remove_file(&cleanup_path)).await;
    match join_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(Err(error)) => eprintln!(
            "warning: failed to remove exported attachment after persistence failure: {} ({error})",
            destination_path.display()
        ),
        Err(error) => eprintln!(
            "warning: failed to join export cleanup task for {}: {error}",
            destination_path.display()
        ),
    }
}
