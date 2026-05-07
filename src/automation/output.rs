use super::model::{
    AutomationApplyReport, AutomationPruneReport, AutomationRolloutReport,
    AutomationRulesValidateReport, AutomationRunPreviewReport, AutomationShowReport,
};
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

impl AutomationRulesValidateReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            format!("rules_path={}", self.path.display()),
            format!("rule_file_hash={}", self.rule_file_hash),
            format!("rule_count={}", self.rule_count),
            format!("enabled_rule_count={}", self.enabled_rule_count),
            String::from("results_format=tsv"),
            String::from("rule_id\tenabled\tpriority\taction_kind\tdescription"),
        ];
        lines.extend(self.rules.iter().map(|rule| {
            format!(
                "{}\t{}\t{}\t{}\t{}",
                sanitize(&rule.id),
                rule.enabled,
                rule.priority,
                rule.action_kind,
                sanitize(rule.description.as_deref().unwrap_or_default()),
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

impl AutomationRunPreviewReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        render_run_detail("preview", &self.detail)
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

impl AutomationRolloutReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            String::from("operation=rollout"),
            format!(
                "account_id={}",
                sanitize(self.verification.account_id.as_deref().unwrap_or("<none>"))
            ),
            format!("authenticated={}", self.verification.authenticated),
            format!("rules_file_exists={}", self.verification.rules_file_exists),
            format!("selected_rule_count={}", self.selected_rule_count),
            format!(
                "selected_rule_ids={}",
                sanitize(&self.selected_rule_ids.join(","))
            ),
            format!("candidate_count={}", self.candidate_count),
            format!("blocked_rule_count={}", self.blocked_rule_ids.len()),
        ];
        if !self.blocked_rule_ids.is_empty() {
            lines.push(format!(
                "blocked_rule_ids={}",
                sanitize(&self.blocked_rule_ids.join(","))
            ));
        }
        if !self.candidates.is_empty() {
            lines.push(String::from("results_format=tsv"));
            lines.push(String::from(
                "rule_id\tthread_id\tmessage_id\taction\thas_unsubscribe\tfrom_address\tsubject\tlabels\tmatched_predicates",
            ));
            lines.extend(self.candidates.iter().map(|candidate| {
                format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    sanitize(&candidate.rule_id),
                    sanitize(&candidate.thread_id),
                    sanitize(&candidate.message_id),
                    sanitize(&candidate.action_kind),
                    candidate.has_list_unsubscribe,
                    sanitize(candidate.from_address.as_deref().unwrap_or_default()),
                    sanitize(&candidate.subject),
                    sanitize(&candidate.label_names.join(" | ")),
                    sanitize(&candidate.matched_predicates.join(", ")),
                )
            }));
        }
        for blocker in &self.blockers {
            lines.push(format!("blocker={}", sanitize(blocker)));
        }
        for warning in &self.warnings {
            lines.push(format!("warning={}", sanitize(warning)));
        }
        for next_step in &self.next_steps {
            lines.push(format!("next_step={}", sanitize(next_step)));
        }
        for command in &self.command_plan {
            lines.push(format!("command={}", sanitize(command)));
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

impl AutomationShowReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        render_run_detail("show", &self.detail)
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

impl AutomationPruneReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            String::from("operation=prune"),
            format!("account_id={}", sanitize(&self.account_id)),
            format!("execute={}", self.execute),
            format!("older_than_days={}", self.older_than_days),
            format!("cutoff_epoch_s={}", self.cutoff_epoch_s),
            format!("statuses={}", sanitize(&self.statuses.join(","))),
            format!("matched_run_count={}", self.matched_run_count),
            format!("matched_candidate_count={}", self.matched_candidate_count),
            format!("matched_event_count={}", self.matched_event_count),
            format!("deleted_run_count={}", self.deleted_run_count),
        ];
        for warning in &self.warnings {
            lines.push(format!("warning={}", sanitize(warning)));
        }
        for next_step in &self.next_steps {
            lines.push(format!("next_step={}", sanitize(next_step)));
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

impl AutomationApplyReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| self.write(json, stdout))
    }

    fn render_plain(&self) -> String {
        let mut rendered = render_run_detail("apply", &self.detail);
        rendered.push_str(&format!("execute={}\n", self.execute));
        rendered.push_str(&format!(
            "applied_candidate_count={}\n",
            self.applied_candidate_count
        ));
        rendered.push_str(&format!(
            "failed_candidate_count={}\n",
            self.failed_candidate_count
        ));
        if let Some(sync_report) = &self.sync_report {
            rendered.push_str(&format!("sync_mode={}\n", sync_report.mode));
            rendered.push_str(&format!(
                "sync_cursor_history_id={}\n",
                sanitize(&sync_report.cursor_history_id)
            ));
        }
        rendered
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

fn render_run_detail(
    operation: &str,
    detail: &crate::store::automation::AutomationRunDetail,
) -> String {
    let run = &detail.run;
    let mut lines = vec![
        format!("operation={operation}"),
        format!("run_id={}", run.run_id),
        format!("account_id={}", sanitize(&run.account_id)),
        format!("status={}", run.status),
        format!("rule_file_path={}", sanitize(&run.rule_file_path)),
        format!("rule_file_hash={}", run.rule_file_hash),
        format!("selected_rule_count={}", run.selected_rule_ids.len()),
        format!(
            "selected_rule_ids={}",
            sanitize(&run.selected_rule_ids.join(","))
        ),
        format!("candidate_count={}", detail.candidates.len()),
        format!("event_count={}", detail.events.len()),
        String::from("results_format=tsv"),
        String::from(
            "candidate_id\trule_id\tthread_id\taction\tapply_status\tapply_error\thas_unsubscribe\tsubject",
        ),
    ];
    lines.extend(detail.candidates.iter().map(|candidate| {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            candidate.candidate_id,
            sanitize(&candidate.rule_id),
            sanitize(&candidate.thread_id),
            candidate.action.kind,
            candidate
                .apply_status
                .map(|status| status.to_string())
                .unwrap_or_default(),
            sanitize(candidate.apply_error.as_deref().unwrap_or_default()),
            candidate.has_list_unsubscribe,
            sanitize(&candidate.subject),
        )
    }));
    if !detail.candidates.is_empty() {
        lines.push(String::from("candidate_details_format=tsv"));
        lines.push(String::from(
            "candidate_id\tfrom_address\tattachment_count\tlabels\tmatched_predicates\taction_add_labels\taction_remove_labels\tlist_id_header\tprecedence_header",
        ));
        lines.extend(detail.candidates.iter().map(|candidate| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                candidate.candidate_id,
                sanitize(candidate.from_address.as_deref().unwrap_or_default()),
                candidate.attachment_count,
                sanitize(&candidate.label_names.join(" | ")),
                sanitize(&render_match_reason(&candidate.reason)),
                sanitize(&candidate.action.add_label_names.join(" | ")),
                sanitize(&candidate.action.remove_label_names.join(" | ")),
                sanitize(candidate.list_id_header.as_deref().unwrap_or_default()),
                sanitize(candidate.precedence_header.as_deref().unwrap_or_default()),
            )
        }));
    }
    if !detail.events.is_empty() {
        lines.push(String::from("events_format=tsv"));
        lines.push(String::from(
            "event_id\trun_id\tevent_kind\tcreated_at_epoch_s\tpayload_json",
        ));
        lines.extend(detail.events.iter().map(|event| {
            format!(
                "{}\t{}\t{}\t{}\t{}",
                event.event_id,
                event.run_id,
                sanitize(&event.event_kind),
                event.created_at_epoch_s,
                sanitize(&event.payload_json),
            )
        }));
    }
    lines.join("\n") + "\n"
}

fn render_match_reason(reason: &crate::store::automation::AutomationMatchReason) -> String {
    let mut predicates = Vec::new();
    if let Some(from_address) = &reason.from_address {
        predicates.push(format!("from={from_address}"));
    }
    if !reason.subject_terms.is_empty() {
        predicates.push(format!("subject~{}", reason.subject_terms.join("|")));
    }
    if !reason.label_names.is_empty() {
        predicates.push(format!("label_any={}", reason.label_names.join("|")));
    }
    if let Some(days) = reason.older_than_days {
        predicates.push(format!("older_than_days={days}"));
    }
    if let Some(has_attachments) = reason.has_attachments {
        predicates.push(format!("has_attachments={has_attachments}"));
    }
    if let Some(has_list_unsubscribe) = reason.has_list_unsubscribe {
        predicates.push(format!("has_list_unsubscribe={has_list_unsubscribe}"));
    }
    if !reason.list_id_terms.is_empty() {
        predicates.push(format!("list_id~{}", reason.list_id_terms.join("|")));
    }
    if !reason.precedence_values.is_empty() {
        predicates.push(format!("precedence={}", reason.precedence_values.join("|")));
    }
    predicates.join(", ")
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
    use super::render_run_detail;
    use crate::store::automation::{
        AutomationActionKind, AutomationActionSnapshot, AutomationApplyStatus,
        AutomationMatchReason, AutomationRunCandidateRecord, AutomationRunDetail,
        AutomationRunEventRecord, AutomationRunRecord, AutomationRunStatus,
    };

    #[test]
    fn render_run_detail_includes_failed_candidate_errors_and_events() {
        let detail = AutomationRunDetail {
            run: AutomationRunRecord {
                run_id: 7,
                account_id: String::from("gmail:operator@example.com"),
                rule_file_path: String::from(".mailroom/automation.toml"),
                rule_file_hash: String::from("hash"),
                selected_rule_ids: vec![String::from("rule-1")],
                status: AutomationRunStatus::ApplyFailed,
                candidate_count: 1,
                created_at_epoch_s: 100,
                applied_at_epoch_s: Some(101),
            },
            candidates: vec![AutomationRunCandidateRecord {
                candidate_id: 11,
                run_id: 7,
                account_id: String::from("gmail:operator@example.com"),
                rule_id: String::from("rule-1"),
                thread_id: String::from("thread-1"),
                message_id: String::from("message-1"),
                internal_date_epoch_ms: 1_700_000_000_000,
                subject: String::from("Digest"),
                from_header: String::from("Digest <digest@example.com>"),
                from_address: Some(String::from("digest@example.com")),
                snippet: String::from("Snippet"),
                label_names: vec![String::from("INBOX")],
                attachment_count: 0,
                has_list_unsubscribe: true,
                list_id_header: None,
                list_unsubscribe_header: None,
                list_unsubscribe_post_header: None,
                precedence_header: None,
                auto_submitted_header: None,
                action: AutomationActionSnapshot {
                    kind: AutomationActionKind::Archive,
                    add_label_ids: Vec::new(),
                    add_label_names: Vec::new(),
                    remove_label_ids: vec![String::from("INBOX")],
                    remove_label_names: vec![String::from("INBOX")],
                },
                reason: AutomationMatchReason::default(),
                apply_status: Some(AutomationApplyStatus::Failed),
                applied_at_epoch_s: Some(102),
                apply_error: Some(String::from("failed\tbecause\nremote draft missing")),
                created_at_epoch_s: 101,
            }],
            events: vec![AutomationRunEventRecord {
                event_id: 42,
                run_id: 7,
                account_id: String::from("gmail:operator@example.com"),
                event_kind: String::from("apply_finished"),
                payload_json: String::from("{\"failed_candidate_count\":1}"),
                created_at_epoch_s: 102,
            }],
        };

        let rendered = render_run_detail("apply", &detail);
        assert!(rendered.contains("candidate_id\trule_id\tthread_id\taction\tapply_status\tapply_error\thas_unsubscribe\tsubject"));
        assert!(rendered.contains("failed because remote draft missing"));
        assert!(rendered.contains("events_format=tsv"));
        assert!(rendered.contains("apply_finished"));
    }
}
