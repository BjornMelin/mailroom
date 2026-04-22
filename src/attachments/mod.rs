mod error;
mod export;
mod reports;
mod service;
mod vault;

#[cfg(test)]
mod tests;

pub use error::AttachmentServiceError;
pub use reports::{
    AttachmentExportReport, AttachmentFetchReport, AttachmentListReport, AttachmentShowReport,
};
pub use service::{export, fetch, list, show};

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
