use crate::CliInputError;
use crate::cli::{
    CleanupCommand, DraftAttachmentCommand, DraftCommand, TriageBucketArg, TriageCommand,
    WorkflowCommand, WorkflowPromoteTargetArg, WorkflowStageArg,
};
use crate::store;
use crate::{config, workflows, workspace};
use anyhow::Result;
use std::io::Read;
use std::path::PathBuf;

pub(crate) async fn handle_workflow_command(
    paths: &workspace::WorkspacePaths,
    command: WorkflowCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        WorkflowCommand::List {
            stage,
            triage_bucket,
            json,
        } => workflows::list_workflows(
            &config_report,
            stage.map(workflow_stage_from_arg),
            triage_bucket.map(triage_bucket_from_arg),
        )
        .await?
        .print(json)?,
        WorkflowCommand::Show { thread_id, json } => {
            workflows::show_workflow(&config_report, thread_id)
                .await?
                .print(json)?
        }
        WorkflowCommand::Promote {
            thread_id,
            to,
            json,
        } => workflows::promote_workflow(
            &config_report,
            thread_id,
            workflow_promote_target_from_arg(to),
        )
        .await?
        .print(json)?,
        WorkflowCommand::Snooze {
            thread_id,
            until,
            clear,
            json,
        } => {
            let until = resolve_snooze_until(until, clear)?;
            workflows::snooze_workflow(&config_report, thread_id, until)
                .await?
                .print(json)?;
        }
    }

    Ok(())
}

pub(crate) async fn handle_triage_command(
    paths: &workspace::WorkspacePaths,
    command: TriageCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        TriageCommand::Set {
            thread_id,
            bucket,
            note,
            json,
        } => workflows::set_triage(
            &config_report,
            thread_id,
            triage_bucket_from_arg(bucket),
            note,
        )
        .await?
        .print(json)?,
    }

    Ok(())
}

pub(crate) async fn handle_draft_command(
    paths: &workspace::WorkspacePaths,
    command: DraftCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        DraftCommand::Start {
            thread_id,
            reply_all,
            json,
        } => workflows::draft_start(
            &config_report,
            thread_id,
            if reply_all {
                store::workflows::ReplyMode::ReplyAll
            } else {
                store::workflows::ReplyMode::Reply
            },
        )
        .await?
        .print(json)?,
        DraftCommand::Body {
            thread_id,
            text,
            file,
            stdin,
            json,
        } => {
            let body_text = resolve_draft_body_input(text, file, stdin)?;
            workflows::draft_body_set(&config_report, thread_id, body_text)
                .await?
                .print(json)?;
        }
        DraftCommand::Attach { command } => match command {
            DraftAttachmentCommand::Add {
                thread_id,
                path,
                json,
            } => workflows::draft_attach_add(&config_report, thread_id, path)
                .await?
                .print(json)?,
            DraftAttachmentCommand::Remove {
                thread_id,
                path,
                json,
            } => workflows::draft_attach_remove(&config_report, thread_id, path)
                .await?
                .print(json)?,
        },
        DraftCommand::Send { thread_id, json } => workflows::draft_send(&config_report, thread_id)
            .await?
            .print(json)?,
    }

    Ok(())
}

pub(crate) async fn handle_cleanup_command(
    paths: &workspace::WorkspacePaths,
    command: CleanupCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        CleanupCommand::Archive {
            thread_id,
            execute,
            json,
        } => workflows::cleanup_archive(&config_report, thread_id, execute)
            .await?
            .print(json)?,
        CleanupCommand::Label {
            thread_id,
            add_labels,
            remove_labels,
            execute,
            json,
        } => workflows::cleanup_label(
            &config_report,
            thread_id,
            execute,
            add_labels,
            remove_labels,
        )
        .await?
        .print(json)?,
        CleanupCommand::Trash {
            thread_id,
            execute,
            json,
        } => workflows::cleanup_trash(&config_report, thread_id, execute)
            .await?
            .print(json)?,
    }

    Ok(())
}

fn workflow_stage_from_arg(value: WorkflowStageArg) -> store::workflows::WorkflowStage {
    match value {
        WorkflowStageArg::Triage => store::workflows::WorkflowStage::Triage,
        WorkflowStageArg::FollowUp => store::workflows::WorkflowStage::FollowUp,
        WorkflowStageArg::Drafting => store::workflows::WorkflowStage::Drafting,
        WorkflowStageArg::ReadyToSend => store::workflows::WorkflowStage::ReadyToSend,
        WorkflowStageArg::Sent => store::workflows::WorkflowStage::Sent,
        WorkflowStageArg::Closed => store::workflows::WorkflowStage::Closed,
    }
}

fn workflow_promote_target_from_arg(
    value: WorkflowPromoteTargetArg,
) -> store::workflows::WorkflowStage {
    match value {
        WorkflowPromoteTargetArg::FollowUp => store::workflows::WorkflowStage::FollowUp,
        WorkflowPromoteTargetArg::ReadyToSend => store::workflows::WorkflowStage::ReadyToSend,
        WorkflowPromoteTargetArg::Closed => store::workflows::WorkflowStage::Closed,
    }
}

fn triage_bucket_from_arg(value: TriageBucketArg) -> store::workflows::TriageBucket {
    match value {
        TriageBucketArg::Urgent => store::workflows::TriageBucket::Urgent,
        TriageBucketArg::NeedsReplySoon => store::workflows::TriageBucket::NeedsReplySoon,
        TriageBucketArg::Waiting => store::workflows::TriageBucket::Waiting,
        TriageBucketArg::Fyi => store::workflows::TriageBucket::Fyi,
    }
}

fn resolve_snooze_until(until: Option<String>, clear: bool) -> Result<Option<String>> {
    if !clear && until.is_none() {
        return Err(CliInputError::SnoozeRequiresUntilOrClear.into());
    }
    if clear && until.is_some() {
        return Err(CliInputError::SnoozeUntilConflict.into());
    }

    if clear { Ok(None) } else { Ok(until) }
}

fn resolve_draft_body_input(
    text: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<String> {
    let selected = usize::from(text.is_some()) + usize::from(file.is_some()) + usize::from(stdin);
    if selected != 1 {
        return Err(CliInputError::DraftBodyInputSourceConflict.into());
    }

    if let Some(text) = text {
        return Ok(text);
    }

    if let Some(file) = file {
        return std::fs::read_to_string(&file)
            .map_err(|source| CliInputError::DraftBodyFileRead { path: file, source }.into());
    }

    let mut buffer = String::new();
    std::io::stdin()
        .read_to_string(&mut buffer)
        .map_err(|source| CliInputError::DraftBodyStdinRead { source })?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::{resolve_draft_body_input, resolve_snooze_until};
    use crate::CliInputError;
    use std::path::PathBuf;

    #[test]
    fn resolve_snooze_until_requires_explicit_until_or_clear() {
        let error = resolve_snooze_until(None, false).unwrap_err();

        assert_eq!(error.to_string(), "use --until YYYY-MM-DD or --clear");
    }

    #[test]
    fn resolve_snooze_until_rejects_conflicting_flags() {
        let error = resolve_snooze_until(Some(String::from("2026-05-01")), true).unwrap_err();

        assert_eq!(error.to_string(), "use either --until or --clear, not both");
    }

    #[test]
    fn resolve_draft_body_input_requires_exactly_one_source() {
        let error = resolve_draft_body_input(None, None, false).unwrap_err();

        assert_eq!(
            error.to_string(),
            "use exactly one of --text, --file, or --stdin"
        );
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::DraftBodyInputSourceConflict)
        ));
    }

    #[test]
    fn resolve_draft_body_input_reports_file_read_as_typed_validation_error() {
        let missing_path = PathBuf::from("/definitely/missing/mailroom-draft-body.txt");
        let error = resolve_draft_body_input(None, Some(missing_path.clone()), false).unwrap_err();

        assert!(
            error
                .to_string()
                .starts_with("failed to read /definitely/missing/mailroom-draft-body.txt:")
        );
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::DraftBodyFileRead { path, .. }) if path == &missing_path
        ));
    }
}
