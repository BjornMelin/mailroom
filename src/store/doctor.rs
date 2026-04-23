use super::connection::{StorePragmas, read_pragmas};
use super::{automation, mailbox, pending_migrations, workflows};
use crate::config::ConfigReport;
use anyhow::Result;
use serde::Serialize;
use std::fmt::Display;
use std::io::{self, Write};

fn write_kv<W: Write, V: Display>(writer: &mut W, key: &str, value: V) -> Result<()> {
    writeln!(writer, "{key}={value}")?;
    Ok(())
}

fn write_kv_opt<W: Write, T: Display>(
    writer: &mut W,
    key: &str,
    value: Option<T>,
    none_text: &str,
) -> Result<()> {
    match value {
        Some(v) => writeln!(writer, "{key}={v}")?,
        None => writeln!(writer, "{key}={none_text}")?,
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreDoctorReport {
    pub config: ConfigReport,
    pub database_exists: bool,
    pub database_path: std::path::PathBuf,
    pub known_migrations: usize,
    pub schema_version: Option<i64>,
    pub pending_migrations: Option<usize>,
    pub pragmas: Option<StorePragmas>,
    pub mailbox: Option<mailbox::MailboxDoctorReport>,
    pub workflows: Option<workflows::WorkflowDoctorReport>,
    pub automation: Option<automation::AutomationDoctorReport>,
}

impl StoreDoctorReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            self.write_human(&mut stdout)?;
        }

        Ok(())
    }

    fn write_human<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_kv(writer, "database_path", self.database_path.display())?;
        write_kv(writer, "database_exists", self.database_exists)?;
        write_kv(writer, "known_migrations", self.known_migrations)?;
        write_kv(
            writer,
            "user_config",
            self.config
                .locations
                .user_config_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| String::from("<unavailable>")),
        )?;
        write_kv(
            writer,
            "user_config_exists",
            self.config.locations.user_config_exists,
        )?;
        write_kv(
            writer,
            "repo_config",
            self.config.locations.repo_config_path.display(),
        )?;
        write_kv(
            writer,
            "repo_config_exists",
            self.config.locations.repo_config_exists,
        )?;
        write_kv_opt(
            writer,
            "schema_version",
            self.schema_version,
            "<uninitialized>",
        )?;
        write_kv_opt(
            writer,
            "pending_migrations",
            self.pending_migrations,
            "<uninitialized>",
        )?;
        if let Some(pragmas) = &self.pragmas {
            write_pragmas(writer, pragmas)?;
        }
        if let Some(mailbox) = &self.mailbox {
            write_kv(writer, "mailbox_message_count", mailbox.message_count)?;
            write_kv(writer, "mailbox_label_count", mailbox.label_count)?;
            write_kv(
                writer,
                "mailbox_indexed_message_count",
                mailbox.indexed_message_count,
            )?;
            write_kv(writer, "mailbox_attachment_count", mailbox.attachment_count)?;
            write_kv(
                writer,
                "mailbox_vaulted_attachment_count",
                mailbox.vaulted_attachment_count,
            )?;
            write_kv(
                writer,
                "mailbox_attachment_export_count",
                mailbox.attachment_export_count,
            )?;
            match &mailbox.sync_state {
                Some(sync_state) => {
                    write_kv(writer, "mailbox_sync_status", sync_state.last_sync_status)?;
                    write_kv(writer, "mailbox_sync_mode", sync_state.last_sync_mode)?;
                    write_kv(writer, "mailbox_sync_epoch_s", sync_state.last_sync_epoch_s)?;
                    write_kv_opt(
                        writer,
                        "mailbox_last_full_sync_success_epoch_s",
                        sync_state.last_full_sync_success_epoch_s,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_last_incremental_sync_success_epoch_s",
                        sync_state.last_incremental_sync_success_epoch_s,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_cursor_history_id",
                        sync_state.cursor_history_id.as_deref(),
                        "<none>",
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_enabled",
                        sync_state.pipeline_enabled,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_list_queue_high_water",
                        sync_state.pipeline_list_queue_high_water,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_write_queue_high_water",
                        sync_state.pipeline_write_queue_high_water,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_write_batch_count",
                        sync_state.pipeline_write_batch_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_writer_wait_ms",
                        sync_state.pipeline_writer_wait_ms,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_fetch_batch_count",
                        sync_state.pipeline_fetch_batch_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_fetch_batch_avg_ms",
                        sync_state.pipeline_fetch_batch_avg_ms,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_fetch_batch_max_ms",
                        sync_state.pipeline_fetch_batch_max_ms,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_writer_tx_count",
                        sync_state.pipeline_writer_tx_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_writer_tx_avg_ms",
                        sync_state.pipeline_writer_tx_avg_ms,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_writer_tx_max_ms",
                        sync_state.pipeline_writer_tx_max_ms,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_reorder_buffer_high_water",
                        sync_state.pipeline_reorder_buffer_high_water,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_staged_message_count",
                        sync_state.pipeline_staged_message_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_staged_delete_count",
                        sync_state.pipeline_staged_delete_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pipeline_staged_attachment_count",
                        sync_state.pipeline_staged_attachment_count,
                    )?;
                }
                None => write_kv(writer, "mailbox_sync_status", "<never-run>")?,
            }
            match &mailbox.full_sync_checkpoint {
                Some(checkpoint) => {
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_status",
                        checkpoint.status,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_bootstrap_query",
                        &checkpoint.bootstrap_query,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_pages_fetched",
                        checkpoint.pages_fetched,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_messages_upserted",
                        checkpoint.messages_upserted,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_staged_message_count",
                        checkpoint.staged_message_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_next_page_token_present",
                        checkpoint.next_page_token.is_some(),
                    )?;
                    write_kv(
                        writer,
                        "mailbox_full_sync_checkpoint_updated_at_epoch_s",
                        checkpoint.updated_at_epoch_s,
                    )?;
                }
                None => write_kv(writer, "mailbox_full_sync_checkpoint_status", "<none>")?,
            }
            match &mailbox.sync_pacing_state {
                Some(pacing_state) => {
                    write_kv(
                        writer,
                        "mailbox_sync_pacing_learned_quota_units_per_minute",
                        pacing_state.learned_quota_units_per_minute,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pacing_learned_message_fetch_concurrency",
                        pacing_state.learned_message_fetch_concurrency,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pacing_clean_run_streak",
                        pacing_state.clean_run_streak,
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_pacing_last_pressure_kind",
                        pacing_state.last_pressure_kind,
                        "<none>",
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_pacing_updated_at_epoch_s",
                        pacing_state.updated_at_epoch_s,
                    )?;
                }
                None => write_kv(
                    writer,
                    "mailbox_sync_pacing_learned_quota_units_per_minute",
                    "<none>",
                )?,
            }
            match &mailbox.sync_run_summary {
                Some(summary) => {
                    write_kv(writer, "mailbox_sync_run_summary_mode", summary.sync_mode)?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_comparability_kind",
                        summary.comparability_kind,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_comparability_key",
                        &summary.comparability_key,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_comparability_label",
                        &summary.comparability_label,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_latest_run_id",
                        summary.latest_run_id,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_latest_status",
                        summary.latest_status,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_latest_finished_at_epoch_s",
                        summary.latest_finished_at_epoch_s,
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_best_clean_run_id",
                        summary.best_clean_run_id,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_best_clean_quota_units_per_minute",
                        summary.best_clean_quota_units_per_minute,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_best_clean_message_fetch_concurrency",
                        summary.best_clean_message_fetch_concurrency,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_best_clean_messages_per_second",
                        summary.best_clean_messages_per_second,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_best_clean_duration_ms",
                        summary.best_clean_duration_ms,
                        "<none>",
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_recent_success_count",
                        summary.recent_success_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_recent_failure_count",
                        summary.recent_failure_count,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_recent_failure_streak",
                        summary.recent_failure_streak,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_recent_clean_success_streak",
                        summary.recent_clean_success_streak,
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_regression_detected",
                        summary.regression_detected,
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_regression_kind",
                        summary.regression_kind,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_regression_run_id",
                        summary.regression_run_id,
                        "<none>",
                    )?;
                    write_kv_opt(
                        writer,
                        "mailbox_sync_run_summary_regression_message",
                        summary.regression_message.as_deref(),
                        "<none>",
                    )?;
                    write_kv(
                        writer,
                        "mailbox_sync_run_summary_updated_at_epoch_s",
                        summary.updated_at_epoch_s,
                    )?;
                }
                None => write_kv(writer, "mailbox_sync_run_summary_latest_run_id", "<none>")?,
            }
        }
        if let Some(workflows) = &self.workflows {
            write_kv(writer, "workflow_count", workflows.workflow_count)?;
            write_kv(writer, "workflow_open_count", workflows.open_workflow_count)?;
            write_kv(
                writer,
                "workflow_draft_count",
                workflows.draft_workflow_count,
            )?;
            write_kv(writer, "workflow_event_count", workflows.event_count)?;
            write_kv(
                writer,
                "workflow_draft_revision_count",
                workflows.draft_revision_count,
            )?;
        }
        if let Some(automation) = &self.automation {
            write_kv(writer, "automation_run_count", automation.run_count)?;
            write_kv(
                writer,
                "automation_previewed_run_count",
                automation.previewed_run_count,
            )?;
            write_kv(
                writer,
                "automation_applied_run_count",
                automation.applied_run_count,
            )?;
            write_kv(
                writer,
                "automation_apply_failed_run_count",
                automation.apply_failed_run_count,
            )?;
            write_kv(
                writer,
                "automation_candidate_count",
                automation.candidate_count,
            )?;
        }

        Ok(())
    }
}

pub fn inspect(config_report: ConfigReport) -> Result<StoreDoctorReport> {
    let database_path = config_report.config.store.database_path.clone();
    let known_migrations = super::migrations::known_migration_count();

    if !database_path.exists() {
        return Ok(StoreDoctorReport {
            config: config_report,
            database_exists: false,
            database_path,
            known_migrations,
            schema_version: None,
            pending_migrations: None,
            pragmas: None,
            mailbox: None,
            workflows: None,
            automation: None,
        });
    }

    let connection = super::connection::open_read_only_for_diagnostics(
        &database_path,
        config_report.config.store.busy_timeout_ms,
    )?;
    let pragmas = read_pragmas(&connection)?;
    let pending_migrations = pending_migrations(known_migrations, pragmas.user_version)?;
    let mailbox =
        mailbox::inspect_mailbox(&database_path, config_report.config.store.busy_timeout_ms)?;
    let workflows =
        workflows::inspect_workflows(&database_path, config_report.config.store.busy_timeout_ms)?;
    let automation =
        automation::inspect_automation(&database_path, config_report.config.store.busy_timeout_ms)?;

    Ok(StoreDoctorReport {
        config: config_report,
        database_exists: true,
        database_path,
        known_migrations,
        schema_version: Some(pragmas.user_version),
        pending_migrations: Some(pending_migrations),
        pragmas: Some(pragmas),
        mailbox,
        workflows,
        automation,
    })
}

pub(crate) fn print_pragmas(pragmas: &StorePragmas) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_pragmas(&mut stdout, pragmas)
}

fn write_pragmas<W: Write>(writer: &mut W, pragmas: &StorePragmas) -> Result<()> {
    write_kv(writer, "application_id", pragmas.application_id)?;
    write_kv(writer, "user_version", pragmas.user_version)?;
    write_kv(writer, "foreign_keys", pragmas.foreign_keys)?;
    write_kv(writer, "trusted_schema", pragmas.trusted_schema)?;
    write_kv(writer, "journal_mode", &pragmas.journal_mode)?;
    write_kv(writer, "synchronous", pragmas.synchronous)?;
    write_kv(writer, "busy_timeout_ms", pragmas.busy_timeout_ms)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::resolve;
    use crate::store::mailbox::{
        MailboxDoctorReport, SyncMode, SyncPacingPressureKind, SyncPacingStateRecord,
        SyncRunComparabilityKind, SyncRunRegressionKind, SyncRunSummaryRecord, SyncStatus,
    };
    use crate::workspace::WorkspacePaths;
    use tempfile::TempDir;

    #[test]
    fn write_human_restores_sync_run_summary_fields() {
        let repo_root = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        let config = resolve(&paths).unwrap();
        let report = StoreDoctorReport {
            config,
            database_exists: true,
            database_path: repo_root.path().join(".mailroom/store.sqlite3"),
            known_migrations: 16,
            schema_version: Some(16),
            pending_migrations: Some(0),
            pragmas: None,
            mailbox: Some(MailboxDoctorReport {
                sync_state: None,
                full_sync_checkpoint: None,
                sync_pacing_state: Some(SyncPacingStateRecord {
                    account_id: String::from("gmail:operator@example.com"),
                    learned_quota_units_per_minute: 12_000,
                    learned_message_fetch_concurrency: 4,
                    clean_run_streak: 3,
                    last_pressure_kind: Some(SyncPacingPressureKind::Quota),
                    updated_at_epoch_s: 530,
                }),
                sync_run_summary: Some(SyncRunSummaryRecord {
                    account_id: String::from("gmail:operator@example.com"),
                    sync_mode: SyncMode::Incremental,
                    comparability_kind: SyncRunComparabilityKind::IncrementalWorkloadTier,
                    comparability_key: String::from("large"),
                    comparability_label: String::from("large"),
                    latest_run_id: 42,
                    latest_status: SyncStatus::Ok,
                    latest_finished_at_epoch_s: 530,
                    best_clean_run_id: Some(41),
                    best_clean_quota_units_per_minute: Some(12_000),
                    best_clean_message_fetch_concurrency: Some(4),
                    best_clean_messages_per_second: Some(600.0),
                    best_clean_duration_ms: Some(1_000),
                    recent_success_count: 5,
                    recent_failure_count: 1,
                    recent_failure_streak: 0,
                    recent_clean_success_streak: 3,
                    regression_detected: true,
                    regression_kind: Some(SyncRunRegressionKind::ThroughputDrop),
                    regression_run_id: Some(42),
                    regression_message: Some(String::from("throughput dropped")),
                    updated_at_epoch_s: 531,
                }),
                message_count: 0,
                label_count: 0,
                indexed_message_count: 0,
                attachment_count: 0,
                vaulted_attachment_count: 0,
                attachment_export_count: 0,
            }),
            workflows: None,
            automation: None,
        };

        let mut output = Vec::new();
        report.write_human(&mut output).unwrap();
        let output = String::from_utf8(output).unwrap();

        assert!(output.contains("mailbox_sync_pacing_updated_at_epoch_s=530"));
        assert!(output.contains("mailbox_sync_run_summary_mode=incremental"));
        assert!(
            output
                .contains("mailbox_sync_run_summary_comparability_kind=incremental_workload_tier")
        );
        assert!(output.contains("mailbox_sync_run_summary_latest_run_id=42"));
        assert!(output.contains("mailbox_sync_run_summary_best_clean_run_id=41"));
        assert!(output.contains("mailbox_sync_run_summary_regression_kind=throughput_drop"));
        assert!(output.contains("mailbox_sync_run_summary_regression_message=throughput dropped"));
        assert!(output.contains("mailbox_sync_run_summary_updated_at_epoch_s=531"));
    }
}
