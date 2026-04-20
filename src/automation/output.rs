use super::model::{
    AutomationApplyReport, AutomationRulesValidateReport, AutomationRunPreviewReport,
    AutomationShowReport,
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
        format!("candidate_count={}", detail.candidates.len()),
        format!("event_count={}", detail.events.len()),
        String::from("results_format=tsv"),
        String::from(
            "candidate_id\trule_id\tthread_id\taction\tapply_status\thas_unsubscribe\tsubject",
        ),
    ];
    lines.extend(detail.candidates.iter().map(|candidate| {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            candidate.candidate_id,
            sanitize(&candidate.rule_id),
            sanitize(&candidate.thread_id),
            candidate.action.kind,
            candidate
                .apply_status
                .map(|status| status.to_string())
                .unwrap_or_default(),
            candidate.has_list_unsubscribe,
            sanitize(&candidate.subject),
        )
    }));
    lines.join("\n") + "\n"
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
