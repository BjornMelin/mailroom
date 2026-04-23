use crate::attachments;
use crate::cli::AttachmentCommand;
use crate::{config, workspace};
use anyhow::Result;

pub(crate) async fn handle_attachment_command(
    paths: &workspace::WorkspacePaths,
    command: AttachmentCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        AttachmentCommand::List {
            thread_id,
            message_id,
            filename,
            mime_type,
            fetched_only,
            limit,
            json,
        } => attachments::list(
            &config_report,
            attachments::AttachmentListRequest {
                thread_id,
                message_id,
                filename,
                mime_type,
                fetched_only,
                limit,
            },
        )
        .await?
        .print(json)?,
        AttachmentCommand::Show {
            attachment_key,
            json,
        } => attachments::show(&config_report, attachment_key)
            .await?
            .print(json)?,
        AttachmentCommand::Fetch {
            attachment_key,
            json,
        } => attachments::fetch(&config_report, attachment_key)
            .await?
            .print(json)?,
        AttachmentCommand::Export {
            attachment_key,
            to,
            json,
        } => attachments::export(&config_report, attachment_key, to)
            .await?
            .print(json)?,
    }

    Ok(())
}
