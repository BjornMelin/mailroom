use super::error::AttachmentServiceError;
use super::reports::AttachmentFetchReport;
use crate::store;
use crate::workspace::WorkspacePaths;
use anyhow::Result;
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const TEMP_PATH_RETRY_LIMIT: usize = 8;

pub(super) fn existing_vault_report(
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

pub(super) fn resolve_vault_relative_path(
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

#[derive(Debug)]
pub(super) struct VaultWriteResult {
    pub(super) content_hash: String,
    pub(super) relative_path: String,
    pub(super) path: PathBuf,
    pub(super) size_bytes: i64,
}

pub(super) fn write_vault_bytes(vault_dir: &Path, bytes: Vec<u8>) -> Result<VaultWriteResult> {
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
    write_vault_file_atomically(&path, &bytes)?;
    let size_bytes = i64::try_from(bytes.len()).unwrap_or(i64::MAX);

    Ok(VaultWriteResult {
        content_hash,
        relative_path,
        path,
        size_bytes,
    })
}

pub(super) fn write_vault_file_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| AttachmentServiceError::WriteFile {
            path: path.to_path_buf(),
            source: std::io::Error::other("vault path has no parent"),
        })?;
    let (mut temp_file, temp_path) = create_unique_vault_temp_file(parent, path)?;
    let write_result = (|| -> Result<()> {
        temp_file
            .write_all(bytes)
            .map_err(|source| AttachmentServiceError::WriteFile {
                path: temp_path.clone(),
                source,
            })?;
        temp_file
            .sync_all()
            .map_err(|source| AttachmentServiceError::WriteFile {
                path: temp_path.clone(),
                source,
            })?;
        drop(temp_file);
        harden_vault_file_permissions(&temp_path)?;
        persist_vault_temp_file(&temp_path, path)?;
        harden_vault_file_permissions(path)
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

pub(super) fn create_unique_vault_temp_file(
    parent: &Path,
    path: &Path,
) -> Result<(fs::File, PathBuf)> {
    for attempt in 0..TEMP_PATH_RETRY_LIMIT {
        let temp_path = unique_vault_temp_path(parent, path, attempt)?;
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => return Ok((file, temp_path)),
            Err(source)
                if source.kind() == std::io::ErrorKind::AlreadyExists
                    && attempt + 1 < TEMP_PATH_RETRY_LIMIT =>
            {
                continue;
            }
            Err(source) => {
                return Err(AttachmentServiceError::WriteFile {
                    path: temp_path,
                    source,
                }
                .into());
            }
        }
    }

    Err(AttachmentServiceError::WriteFile {
        path: path.to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "failed to create a unique temporary vault file",
        ),
    }
    .into())
}

fn unique_vault_temp_path(parent: &Path, path: &Path, attempt: usize) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| AttachmentServiceError::WriteFile {
            path: path.to_path_buf(),
            source: std::io::Error::other("vault path has no filename"),
        })?;
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    Ok(parent.join(format!(
        ".{}.tmp-{}-{now_nanos}-{attempt}",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

#[cfg(windows)]
fn unique_vault_backup_path(parent: &Path, path: &Path) -> Result<PathBuf> {
    let file_name = path
        .file_name()
        .ok_or_else(|| AttachmentServiceError::WriteFile {
            path: path.to_path_buf(),
            source: std::io::Error::other("vault path has no filename"),
        })?;
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    Ok(parent.join(format!(
        ".{}.bak-{}-{now_nanos}",
        file_name.to_string_lossy(),
        std::process::id()
    )))
}

pub(super) fn persist_vault_temp_file(tmp_path: &Path, destination: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let parent = destination
            .parent()
            .ok_or_else(|| AttachmentServiceError::WriteFile {
                path: destination.to_path_buf(),
                source: std::io::Error::other("vault path has no parent"),
            })?;
        let backup_path = unique_vault_backup_path(parent, destination)?;
        let moved_destination_to_backup = if destination.exists() {
            fs::rename(destination, &backup_path).map_err(|source| {
                AttachmentServiceError::WriteFile {
                    path: destination.to_path_buf(),
                    source,
                }
            })?;
            true
        } else {
            false
        };

        match fs::rename(tmp_path, destination) {
            Ok(()) => {
                if moved_destination_to_backup {
                    fs::remove_file(&backup_path).map_err(|source| {
                        AttachmentServiceError::WriteFile {
                            path: backup_path,
                            source,
                        }
                    })?;
                }
                return Ok(());
            }
            Err(source) => {
                if moved_destination_to_backup {
                    let _ = fs::rename(&backup_path, destination);
                }
                return Err(AttachmentServiceError::WriteFile {
                    path: destination.to_path_buf(),
                    source,
                }
                .into());
            }
        }
    }

    fs::rename(tmp_path, destination).map_err(|source| AttachmentServiceError::WriteFile {
        path: destination.to_path_buf(),
        source,
    })?;
    Ok(())
}

pub(super) fn hash_file_blake3(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).map_err(|source| AttachmentServiceError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(&mut file)
        .map_err(|source| AttachmentServiceError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(unix)]
pub(super) fn harden_vault_file_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
        AttachmentServiceError::WriteFile {
            path: path.to_path_buf(),
            source,
        }
        .into()
    })
}

#[cfg(not(unix))]
pub(super) fn harden_vault_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}
