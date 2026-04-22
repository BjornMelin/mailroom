use crate::store;
use std::path::PathBuf;
use thiserror::Error;

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
