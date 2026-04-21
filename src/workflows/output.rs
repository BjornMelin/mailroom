use crate::workflows::{WorkflowActionReport, WorkflowListReport, WorkflowShowReport};
use anyhow::Result;
use std::io::{self, Write};

fn route_output_to_stdout<F>(json: bool, mut write_fn: F) -> Result<()>
where
    F: FnMut(bool, &mut io::StdoutLock<'_>) -> Result<()>,
{
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_fn(json, &mut stdout)
}

impl WorkflowListReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
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

    fn write<W: Write>(&self, json: bool, writer: &mut W) -> Result<()> {
        if json {
            crate::cli_output::write_json_success(writer, self)?;
        } else {
            writer.write_all(self.render_plain().as_bytes())?;
        }
        Ok(())
    }
}

impl WorkflowShowReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        let workflow = &self.detail.workflow;
        let mut lines = vec![
            format!("thread_id={}", sanitize(&workflow.thread_id)),
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

    fn write<W: Write>(&self, json: bool, writer: &mut W) -> Result<()> {
        if json {
            crate::cli_output::write_json_success(writer, self)?;
        } else {
            writer.write_all(self.render_plain().as_bytes())?;
        }
        Ok(())
    }
}

impl WorkflowActionReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            format!("action={}", self.action),
            format!("thread_id={}", sanitize(&self.workflow.thread_id)),
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
                    preview
                        .add_label_names
                        .iter()
                        .map(|label| sanitize_label(label))
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            if !preview.remove_label_names.is_empty() {
                lines.push(format!(
                    "cleanup_remove_labels={}",
                    preview
                        .remove_label_names
                        .iter()
                        .map(|label| sanitize_label(label))
                        .collect::<Vec<_>>()
                        .join(",")
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

    fn write<W: Write>(&self, json: bool, writer: &mut W) -> Result<()> {
        if json {
            crate::cli_output::write_json_success(writer, self)?;
        } else {
            writer.write_all(self.render_plain().as_bytes())?;
        }
        Ok(())
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

fn sanitize_label(value: &str) -> String {
    sanitize(value)
}

#[cfg(test)]
mod tests {
    use super::{WorkflowListReport, sanitize};
    use crate::mailbox::SyncRunReport;
    use crate::store::mailbox::SyncMode;
    use crate::store::workflows::{
        CleanupAction, DraftAttachmentRecord, DraftRevisionDetail, DraftRevisionRecord,
        TriageBucket, WorkflowDetail, WorkflowEventRecord, WorkflowRecord, WorkflowStage,
    };
    use crate::workflows::{WorkflowAction, WorkflowActionReport, WorkflowShowReport};
    use serde_json::{Value, json};
    use std::io::Cursor;

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

    #[test]
    fn render_plain_show_report_includes_workflow_and_draft_fields() {
        let report = sample_show_report();

        let rendered = report.render_plain();
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(
            lines,
            vec![
                "thread_id=thread-1",
                "stage=drafting",
                "triage_bucket=urgent",
                "note=Prepare release notes",
                "latest_subject=Alpha [31m launch",
                "event_count=1",
                "current_draft_revision_id=7",
                "current_draft_reply_mode=reply_all",
                "current_draft_attachment_count=1",
            ]
        );
    }

    #[test]
    fn render_plain_action_report_includes_cleanup_and_sync_sections() {
        let report = sample_action_report();

        let rendered = report.render_plain();
        let lines: Vec<&str> = rendered.lines().collect();

        assert_eq!(
            lines,
            vec![
                "action=cleanup_applied",
                "thread_id=thread-1",
                "stage=closed",
                "triage_bucket=urgent",
                "cleanup_action=archive",
                "cleanup_execute=true",
                "cleanup_add_labels=Important",
                "draft_revision_id=7",
                "draft_reply_mode=reply_all",
                "draft_attachment_count=1",
                "sync_mode=full",
                "sync_cursor_history_id=cursor-1",
            ]
        );
    }

    #[test]
    fn render_plain_action_report_sanitizes_cleanup_label_names() {
        let report = WorkflowActionReport {
            cleanup_preview: Some(crate::workflows::CleanupPreview {
                action: CleanupAction::Archive,
                execute: false,
                add_label_names: vec![
                    String::from("Important\u{1b}[31m"),
                    String::from("Ops\tTeam"),
                ],
                remove_label_names: vec![String::from("Old\nLabel")],
            }),
            ..sample_action_report()
        };

        let rendered = report.render_plain();

        assert!(rendered.contains("cleanup_add_labels=Important [31m,Ops Team"));
        assert!(rendered.contains("cleanup_remove_labels=Old Label"));
    }

    #[test]
    fn print_routes_show_report_to_json_and_plain_output() {
        let report = sample_show_report();

        let plain_output = render_into_bytes(|writer| report.write(false, writer));
        assert_eq!(plain_output, report.render_plain().as_bytes());

        let json_output = render_into_bytes(|writer| report.write(true, writer));
        let json_value: Value = serde_json::from_slice(&json_output).unwrap();
        assert_eq!(json_value["success"], json!(true));
        assert_eq!(
            json_value["data"]["detail"]["workflow"]["thread_id"],
            json!("thread-1")
        );
        assert_eq!(
            json_value["data"]["detail"]["workflow"]["current_stage"],
            json!("drafting")
        );
        assert_eq!(
            json_value["data"]["detail"]["current_draft"]["revision"]["reply_mode"],
            json!("reply_all")
        );
        assert_eq!(
            json_value["data"]["detail"]["events"][0]["event_kind"],
            json!("triage_set")
        );
    }

    #[test]
    fn print_routes_action_report_to_json_and_plain_output() {
        let report = sample_action_report();

        let plain_output = render_into_bytes(|writer| report.write(false, writer));
        assert_eq!(plain_output, report.render_plain().as_bytes());

        let json_output = render_into_bytes(|writer| report.write(true, writer));
        let json_value: Value = serde_json::from_slice(&json_output).unwrap();
        assert_eq!(json_value["success"], json!(true));
        assert_eq!(json_value["data"]["action"], json!("cleanup_applied"));
        assert_eq!(
            json_value["data"]["workflow"]["thread_id"],
            json!("thread-1")
        );
        assert_eq!(
            json_value["data"]["cleanup_preview"]["action"],
            json!("archive")
        );
        assert_eq!(json_value["data"]["sync_report"]["mode"], json!("full"));
    }

    #[test]
    fn print_routes_list_report_to_json_and_plain_output() {
        let report = sample_list_report();

        let plain_output = render_into_bytes(|writer| report.write(false, writer));
        assert_eq!(plain_output, report.render_plain().as_bytes());

        let json_output = render_into_bytes(|writer| report.write(true, writer));
        let json_value: Value = serde_json::from_slice(&json_output).unwrap();
        assert_eq!(json_value["success"], json!(true));
        assert!(json_value["data"]["workflows"].is_array());
        assert_eq!(
            json_value["data"]["workflows"][0]["thread_id"],
            json!("thread-1")
        );
    }

    fn sample_workflow_record() -> WorkflowRecord {
        WorkflowRecord {
            workflow_id: 1,
            account_id: String::from("account-1"),
            thread_id: String::from("thread-1"),
            current_stage: WorkflowStage::Drafting,
            triage_bucket: Some(TriageBucket::Urgent),
            note: String::from("Prepare release notes"),
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
            workflow_version: 1,
            created_at_epoch_s: 1,
            updated_at_epoch_s: 123,
        }
    }

    fn sample_show_report() -> WorkflowShowReport {
        WorkflowShowReport {
            detail: WorkflowDetail {
                workflow: sample_workflow_record(),
                current_draft: Some(sample_draft_detail()),
                events: vec![WorkflowEventRecord {
                    event_id: 1,
                    workflow_id: 1,
                    account_id: String::from("account-1"),
                    thread_id: String::from("thread-1"),
                    event_kind: String::from("triage_set"),
                    from_stage: None,
                    to_stage: Some(WorkflowStage::Drafting),
                    triage_bucket: Some(TriageBucket::Urgent),
                    note: Some(String::from("manually triaged")),
                    payload_json: String::from("{\"kind\":\"triage_set\"}"),
                    created_at_epoch_s: 123,
                }],
            },
        }
    }

    fn sample_list_report() -> WorkflowListReport {
        WorkflowListReport {
            stage: Some(WorkflowStage::Drafting),
            triage_bucket: Some(TriageBucket::Urgent),
            workflows: vec![sample_workflow_record()],
        }
    }

    fn sample_action_report() -> WorkflowActionReport {
        WorkflowActionReport {
            action: WorkflowAction::CleanupApplied,
            workflow: WorkflowRecord {
                current_stage: WorkflowStage::Closed,
                last_cleanup_action: Some(CleanupAction::Archive),
                ..sample_workflow_record()
            },
            current_draft: Some(sample_draft_detail()),
            cleanup_preview: Some(crate::workflows::CleanupPreview {
                action: CleanupAction::Archive,
                execute: true,
                add_label_names: vec![String::from("Important")],
                remove_label_names: vec![],
            }),
            sync_report: Some(SyncRunReport {
                mode: SyncMode::Full,
                fallback_from_history: false,
                resumed_from_checkpoint: false,
                bootstrap_query: String::from("newer_than:7d"),
                cursor_history_id: String::from("cursor-1"),
                pages_fetched: 1,
                messages_listed: 2,
                messages_upserted: 1,
                messages_deleted: 0,
                labels_synced: 3,
                checkpoint_reused_pages: 0,
                checkpoint_reused_messages_upserted: 0,
                pipeline_enabled: true,
                pipeline_list_queue_high_water: 1,
                pipeline_write_queue_high_water: 1,
                pipeline_write_batch_count: 1,
                pipeline_writer_wait_ms: 0,
                pipeline_fetch_batch_count: 1,
                pipeline_fetch_batch_avg_ms: 10,
                pipeline_fetch_batch_max_ms: 10,
                pipeline_writer_tx_count: 1,
                pipeline_writer_tx_avg_ms: 5,
                pipeline_writer_tx_max_ms: 5,
                pipeline_reorder_buffer_high_water: 1,
                pipeline_staged_message_count: 1,
                pipeline_staged_delete_count: 0,
                pipeline_staged_attachment_count: 0,
                store_message_count: 4,
                store_label_count: 5,
                store_indexed_message_count: 6,
                adaptive_pacing_enabled: true,
                quota_units_budget_per_minute: 12_000,
                message_fetch_concurrency: 4,
                quota_units_cap_per_minute: 12_000,
                message_fetch_concurrency_cap: 4,
                starting_quota_units_per_minute: 12_000,
                starting_message_fetch_concurrency: 4,
                effective_quota_units_per_minute: 12_000,
                effective_message_fetch_concurrency: 4,
                adaptive_downshift_count: 0,
                estimated_quota_units_reserved: 15,
                http_attempt_count: 3,
                retry_count: 0,
                quota_pressure_retry_count: 0,
                concurrency_pressure_retry_count: 0,
                backend_retry_count: 0,
                throttle_wait_count: 0,
                throttle_wait_ms: 0,
                retry_after_wait_ms: 0,
                duration_ms: 100,
                pages_per_second: 10.0,
                messages_per_second: 20.0,
            }),
        }
    }

    fn sample_draft_detail() -> DraftRevisionDetail {
        DraftRevisionDetail {
            revision: DraftRevisionRecord {
                draft_revision_id: 7,
                workflow_id: 1,
                account_id: String::from("account-1"),
                thread_id: String::from("thread-1"),
                source_message_id: String::from("message-1"),
                reply_mode: crate::store::workflows::ReplyMode::ReplyAll,
                subject: String::from("Re: Alpha launch"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: vec![String::from("bob@example.com")],
                bcc_addresses: vec![],
                body_text: String::from("Draft body"),
                created_at_epoch_s: 124,
            },
            attachments: vec![DraftAttachmentRecord {
                attachment_id: 1,
                draft_revision_id: 7,
                path: String::from("/tmp/reply.txt"),
                file_name: String::from("reply.txt"),
                mime_type: String::from("text/plain"),
                size_bytes: 42,
                created_at_epoch_s: 124,
            }],
        }
    }

    fn render_into_bytes<F>(mut write_report: F) -> Vec<u8>
    where
        F: FnMut(&mut Cursor<Vec<u8>>) -> anyhow::Result<()>,
    {
        let mut output = Cursor::new(Vec::new());
        write_report(&mut output).unwrap();
        output.into_inner()
    }
}
