use crate::workflows::{WorkflowActionReport, WorkflowListReport, WorkflowShowReport};
use anyhow::Result;

impl WorkflowListReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("workflow_count={}", self.workflows.len());
            if let Some(stage) = self.stage {
                println!("stage={stage}");
            }
            if let Some(bucket) = self.triage_bucket {
                println!("triage_bucket={bucket}");
            }
            println!("results_format=tsv\tthread_id\tstage\tbucket\tupdated_at_epoch_s\tsubject");
            for workflow in &self.workflows {
                println!(
                    "{}\t{}\t{}\t{}\t{}",
                    sanitize(&workflow.thread_id),
                    workflow.current_stage,
                    workflow
                        .triage_bucket
                        .map(|bucket| bucket.to_string())
                        .unwrap_or_default(),
                    workflow.updated_at_epoch_s,
                    sanitize(&workflow.latest_message_subject),
                );
            }
        }
        Ok(())
    }
}

impl WorkflowShowReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            let workflow = &self.detail.workflow;
            println!("thread_id={}", workflow.thread_id);
            println!("stage={}", workflow.current_stage);
            if let Some(bucket) = workflow.triage_bucket {
                println!("triage_bucket={bucket}");
            }
            println!("note={}", sanitize(&workflow.note));
            println!(
                "latest_subject={}",
                sanitize(&workflow.latest_message_subject)
            );
            println!("event_count={}", self.detail.events.len());
            if let Some(draft) = &self.detail.current_draft {
                println!(
                    "current_draft_revision_id={}",
                    draft.revision.draft_revision_id
                );
                println!("current_draft_reply_mode={}", draft.revision.reply_mode);
                println!("current_draft_attachment_count={}", draft.attachments.len());
            } else {
                println!("current_draft_revision_id=<none>");
            }
        }
        Ok(())
    }
}

impl WorkflowActionReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            println!("action={}", self.action);
            println!("thread_id={}", self.workflow.thread_id);
            println!("stage={}", self.workflow.current_stage);
            if let Some(bucket) = self.workflow.triage_bucket {
                println!("triage_bucket={bucket}");
            }
            if let Some(preview) = &self.cleanup_preview {
                println!("cleanup_action={}", preview.action);
                println!("cleanup_execute={}", preview.execute);
                if !preview.add_label_names.is_empty() {
                    println!("cleanup_add_labels={}", preview.add_label_names.join(","));
                }
                if !preview.remove_label_names.is_empty() {
                    println!(
                        "cleanup_remove_labels={}",
                        preview.remove_label_names.join(",")
                    );
                }
            }
            if let Some(draft) = &self.current_draft {
                println!("draft_revision_id={}", draft.revision.draft_revision_id);
                println!("draft_reply_mode={}", draft.revision.reply_mode);
                println!("draft_attachment_count={}", draft.attachments.len());
            }
            if let Some(sync_report) = &self.sync_report {
                println!("sync_mode={}", sync_report.mode);
                println!(
                    "sync_cursor_history_id={}",
                    sanitize(&sync_report.cursor_history_id)
                );
            }
        }
        Ok(())
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' | '\r' | '\t' => ' ',
            character => character,
        })
        .collect()
}
