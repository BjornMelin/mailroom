use crate::store;
use anyhow::Result;
use serde::Serialize;
use std::path::PathBuf;

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
