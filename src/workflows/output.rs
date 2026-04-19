use crate::workflows::{WorkflowActionReport, WorkflowListReport, WorkflowShowReport};
use anyhow::Result;

impl WorkflowListReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            print!("{}", self.render_plain());
        }
        Ok(())
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![format!("workflow_count={}", self.workflows.len())];
        if let Some(stage) = self.stage {
            lines.push(format!("stage={stage}"));
        }
        if let Some(bucket) = self.triage_bucket {
            lines.push(format!("triage_bucket={bucket}"));
        }
        lines.push(String::from("results_format=tsv"));
        lines.push(String::from(
            "thread_id\tstage\tbucket\tupdated_at_epoch_s\tsubject",
        ));
        lines.extend(self.workflows.iter().map(|workflow| {
            format!(
                "{}\t{}\t{}\t{}\t{}",
                sanitize(&workflow.thread_id),
                workflow.current_stage,
                workflow
                    .triage_bucket
                    .map(|bucket| bucket.to_string())
                    .unwrap_or_default(),
                workflow.updated_at_epoch_s,
                sanitize(&workflow.latest_message_subject),
            )
        }));
        lines.join("\n") + "\n"
    }
}

impl WorkflowShowReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            print!("{}", self.render_plain());
        }
        Ok(())
    }

    fn render_plain(&self) -> String {
        let workflow = &self.detail.workflow;
        let mut lines = vec![
            format!("thread_id={}", workflow.thread_id),
            format!("stage={}", workflow.current_stage),
        ];
        if let Some(bucket) = workflow.triage_bucket {
            lines.push(format!("triage_bucket={bucket}"));
        }
        lines.push(format!("note={}", sanitize(&workflow.note)));
        lines.push(format!(
            "latest_subject={}",
            sanitize(&workflow.latest_message_subject)
        ));
        lines.push(format!("event_count={}", self.detail.events.len()));
        if let Some(draft) = &self.detail.current_draft {
            lines.push(format!(
                "current_draft_revision_id={}",
                draft.revision.draft_revision_id
            ));
            lines.push(format!(
                "current_draft_reply_mode={}",
                draft.revision.reply_mode
            ));
            lines.push(format!(
                "current_draft_attachment_count={}",
                draft.attachments.len()
            ));
        } else {
            lines.push(String::from("current_draft_revision_id=<none>"));
        }
        lines.join("\n") + "\n"
    }
}

impl WorkflowActionReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            print!("{}", self.render_plain());
        }
        Ok(())
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            format!("action={}", self.action),
            format!("thread_id={}", self.workflow.thread_id),
            format!("stage={}", self.workflow.current_stage),
        ];
        if let Some(bucket) = self.workflow.triage_bucket {
            lines.push(format!("triage_bucket={bucket}"));
        }
        if let Some(preview) = &self.cleanup_preview {
            lines.push(format!("cleanup_action={}", preview.action));
            lines.push(format!("cleanup_execute={}", preview.execute));
            if !preview.add_label_names.is_empty() {
                lines.push(format!(
                    "cleanup_add_labels={}",
                    preview.add_label_names.join(",")
                ));
            }
            if !preview.remove_label_names.is_empty() {
                lines.push(format!(
                    "cleanup_remove_labels={}",
                    preview.remove_label_names.join(",")
                ));
            }
        }
        if let Some(draft) = &self.current_draft {
            lines.push(format!(
                "draft_revision_id={}",
                draft.revision.draft_revision_id
            ));
            lines.push(format!("draft_reply_mode={}", draft.revision.reply_mode));
            lines.push(format!(
                "draft_attachment_count={}",
                draft.attachments.len()
            ));
        }
        if let Some(sync_report) = &self.sync_report {
            lines.push(format!("sync_mode={}", sync_report.mode));
            lines.push(format!(
                "sync_cursor_history_id={}",
                sanitize(&sync_report.cursor_history_id)
            ));
        }
        lines.join("\n") + "\n"
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{WorkflowListReport, sanitize};
    use crate::store::workflows::{TriageBucket, WorkflowRecord, WorkflowStage};

    #[test]
    fn sanitize_replaces_all_control_characters() {
        assert_eq!(sanitize("alpha\u{1b}[31m\nbeta\t"), "alpha [31m beta ");
    }

    #[test]
    fn render_plain_separates_tsv_metadata_from_header() {
        let report = WorkflowListReport {
            stage: None,
            triage_bucket: None,
            workflows: vec![sample_workflow_record()],
        };

        let rendered = report.render_plain();
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(lines[0], "workflow_count=1");
        assert_eq!(lines[1], "results_format=tsv");
        assert_eq!(
            lines[2],
            "thread_id\tstage\tbucket\tupdated_at_epoch_s\tsubject"
        );
        assert_eq!(
            lines[3],
            "thread-1\tdrafting\turgent\t123\tAlpha [31m launch"
        );
    }

    fn sample_workflow_record() -> WorkflowRecord {
        WorkflowRecord {
            workflow_id: 1,
            account_id: String::from("account-1"),
            thread_id: String::from("thread-1"),
            current_stage: WorkflowStage::Drafting,
            triage_bucket: Some(TriageBucket::Urgent),
            note: String::from("note"),
            snoozed_until_epoch_s: None,
            follow_up_due_epoch_s: None,
            latest_message_id: Some(String::from("message-1")),
            latest_message_internal_date_epoch_ms: Some(123),
            latest_message_subject: String::from("Alpha\u{1b}[31m launch"),
            latest_message_from_header: String::from("Alice <alice@example.com>"),
            latest_message_snippet: String::from("snippet"),
            current_draft_revision_id: None,
            gmail_draft_id: None,
            gmail_draft_message_id: None,
            gmail_draft_thread_id: None,
            last_remote_sync_epoch_s: None,
            last_sent_message_id: None,
            last_cleanup_action: None,
            created_at_epoch_s: 1,
            updated_at_epoch_s: 123,
        }
    }
}
