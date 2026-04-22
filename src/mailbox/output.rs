use crate::mailbox::{SearchReport, SyncHistoryReport, SyncPerfExplainReport, SyncRunReport};
use anyhow::Result;

impl SyncRunReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            print!("{}", self.render_plain());
        }

        Ok(())
    }

    fn render_plain(&self) -> String {
        [
            format!("run_id={}", self.run_id),
            format!("mode={}", self.mode),
            format!("comparability_kind={}", self.comparability_kind),
            format!("comparability_key={}", self.comparability_key),
            format!("comparability_label={}", self.comparability_label),
            format!(
                "startup_seed_run_id={}",
                self.startup_seed_run_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ),
            format!("fallback_from_history={}", self.fallback_from_history),
            format!("resumed_from_checkpoint={}", self.resumed_from_checkpoint),
            format!("bootstrap_query={}", self.bootstrap_query),
            format!("cursor_history_id={}", self.cursor_history_id),
            format!("pages_fetched={}", self.pages_fetched),
            format!("messages_listed={}", self.messages_listed),
            format!("messages_upserted={}", self.messages_upserted),
            format!("messages_deleted={}", self.messages_deleted),
            format!("labels_synced={}", self.labels_synced),
            format!("checkpoint_reused_pages={}", self.checkpoint_reused_pages),
            format!(
                "checkpoint_reused_messages_upserted={}",
                self.checkpoint_reused_messages_upserted
            ),
            format!("pipeline_enabled={}", self.pipeline_enabled),
            format!(
                "pipeline_list_queue_high_water={}",
                self.pipeline_list_queue_high_water
            ),
            format!(
                "pipeline_write_queue_high_water={}",
                self.pipeline_write_queue_high_water
            ),
            format!(
                "pipeline_write_batch_count={}",
                self.pipeline_write_batch_count
            ),
            format!("pipeline_writer_wait_ms={}", self.pipeline_writer_wait_ms),
            format!(
                "pipeline_fetch_batch_count={}",
                self.pipeline_fetch_batch_count
            ),
            format!(
                "pipeline_fetch_batch_avg_ms={}",
                self.pipeline_fetch_batch_avg_ms
            ),
            format!(
                "pipeline_fetch_batch_max_ms={}",
                self.pipeline_fetch_batch_max_ms
            ),
            format!("pipeline_writer_tx_count={}", self.pipeline_writer_tx_count),
            format!(
                "pipeline_writer_tx_avg_ms={}",
                self.pipeline_writer_tx_avg_ms
            ),
            format!(
                "pipeline_writer_tx_max_ms={}",
                self.pipeline_writer_tx_max_ms
            ),
            format!(
                "pipeline_reorder_buffer_high_water={}",
                self.pipeline_reorder_buffer_high_water
            ),
            format!(
                "pipeline_staged_message_count={}",
                self.pipeline_staged_message_count
            ),
            format!(
                "pipeline_staged_delete_count={}",
                self.pipeline_staged_delete_count
            ),
            format!(
                "pipeline_staged_attachment_count={}",
                self.pipeline_staged_attachment_count
            ),
            format!("store_message_count={}", self.store_message_count),
            format!("store_label_count={}", self.store_label_count),
            format!(
                "store_indexed_message_count={}",
                self.store_indexed_message_count
            ),
            format!("adaptive_pacing_enabled={}", self.adaptive_pacing_enabled),
            format!(
                "quota_units_budget_per_minute={}",
                self.quota_units_budget_per_minute
            ),
            format!(
                "message_fetch_concurrency={}",
                self.message_fetch_concurrency
            ),
            format!(
                "quota_units_cap_per_minute={}",
                self.quota_units_cap_per_minute
            ),
            format!(
                "message_fetch_concurrency_cap={}",
                self.message_fetch_concurrency_cap
            ),
            format!(
                "starting_quota_units_per_minute={}",
                self.starting_quota_units_per_minute
            ),
            format!(
                "starting_message_fetch_concurrency={}",
                self.starting_message_fetch_concurrency
            ),
            format!(
                "effective_quota_units_per_minute={}",
                self.effective_quota_units_per_minute
            ),
            format!(
                "effective_message_fetch_concurrency={}",
                self.effective_message_fetch_concurrency
            ),
            format!("adaptive_downshift_count={}", self.adaptive_downshift_count),
            format!(
                "estimated_quota_units_reserved={}",
                self.estimated_quota_units_reserved
            ),
            format!("http_attempt_count={}", self.http_attempt_count),
            format!("retry_count={}", self.retry_count),
            format!(
                "quota_pressure_retry_count={}",
                self.quota_pressure_retry_count
            ),
            format!(
                "concurrency_pressure_retry_count={}",
                self.concurrency_pressure_retry_count
            ),
            format!("backend_retry_count={}", self.backend_retry_count),
            format!("throttle_wait_count={}", self.throttle_wait_count),
            format!("throttle_wait_ms={}", self.throttle_wait_ms),
            format!("retry_after_wait_ms={}", self.retry_after_wait_ms),
            format!("duration_ms={}", self.duration_ms),
            format!("pages_per_second={:.3}", self.pages_per_second),
            format!("messages_per_second={:.3}", self.messages_per_second),
            format!("regression_detected={}", self.regression_detected),
            format!(
                "regression_kind={}",
                self.regression_kind
                    .map(|kind| kind.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ),
        ]
        .join("\n")
            + "\n"
    }
}

impl SyncHistoryReport {
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
            format!("account_id={}", self.account_id),
            format!("limit={}", self.limit),
            format!("run_count={}", self.runs.len()),
        ];

        match &self.summary {
            Some(summary) => {
                lines.push(format!("summary_sync_mode={}", summary.sync_mode));
                lines.push(format!(
                    "summary_comparability_kind={}",
                    summary.comparability_kind
                ));
                lines.push(format!(
                    "summary_comparability_key={}",
                    summary.comparability_key
                ));
                lines.push(format!(
                    "summary_comparability_label={}",
                    summary.comparability_label
                ));
                lines.push(format!("summary_latest_run_id={}", summary.latest_run_id));
                lines.push(format!("summary_latest_status={}", summary.latest_status));
                lines.push(format!(
                    "summary_best_clean_quota_units_per_minute={}",
                    summary
                        .best_clean_quota_units_per_minute
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| String::from("<none>"))
                ));
                lines.push(format!(
                    "summary_best_clean_message_fetch_concurrency={}",
                    summary
                        .best_clean_message_fetch_concurrency
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| String::from("<none>"))
                ));
                lines.push(format!(
                    "summary_regression_kind={}",
                    summary
                        .regression_kind
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| String::from("<none>"))
                ));
            }
            None => lines.extend([
                String::from("summary_sync_mode=<none>"),
                String::from("summary_comparability_kind=<none>"),
                String::from("summary_comparability_key=<none>"),
                String::from("summary_comparability_label=<none>"),
                String::from("summary_latest_run_id=<none>"),
                String::from("summary_latest_status=<none>"),
                String::from("summary_best_clean_quota_units_per_minute=<none>"),
                String::from("summary_best_clean_message_fetch_concurrency=<none>"),
                String::from("summary_regression_kind=<none>"),
            ]),
        }

        lines.push(String::from(
            "runs_format=tsv\trun_id\tfinished_at_epoch_s\tsync_mode\tcomparability_key\tstatus\tmessages_listed\tmessages_per_second\teffective_quota_units_per_minute\teffective_message_fetch_concurrency\tretry_count",
        ));
        lines.extend(self.runs.iter().map(|run| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{}\t{}\t{}",
                run.run_id,
                run.finished_at_epoch_s,
                run.sync_mode,
                sanitize_tsv_field(&run.comparability_key),
                run.status,
                run.messages_listed,
                run.messages_per_second,
                run.effective_quota_units_per_minute,
                run.effective_message_fetch_concurrency,
                run.retry_count,
            )
        }));
        lines.join("\n") + "\n"
    }
}

impl SyncPerfExplainReport {
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
            format!("account_id={}", self.account_id),
            format!("limit={}", self.limit),
            format!("run_count={}", self.runs.len()),
        ];

        if let Some(summary) = &self.summary {
            lines.push(format!("summary_sync_mode={}", summary.sync_mode));
            lines.push(format!(
                "summary_comparability_kind={}",
                summary.comparability_kind
            ));
            lines.push(format!(
                "summary_comparability_key={}",
                summary.comparability_key
            ));
            lines.push(format!(
                "summary_comparability_label={}",
                summary.comparability_label
            ));
            lines.push(format!("summary_latest_run_id={}", summary.latest_run_id));
            lines.push(format!(
                "summary_best_clean_run_id={}",
                summary
                    .best_clean_run_id
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ));
            lines.push(format!(
                "summary_regression_kind={}",
                summary
                    .regression_kind
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ));
            lines.push(format!(
                "summary_regression_message={}",
                summary
                    .regression_message
                    .clone()
                    .unwrap_or_else(|| String::from("<none>"))
            ));
        } else {
            lines.extend([
                String::from("summary_sync_mode=<none>"),
                String::from("summary_comparability_kind=<none>"),
                String::from("summary_comparability_key=<none>"),
                String::from("summary_comparability_label=<none>"),
                String::from("summary_latest_run_id=<none>"),
                String::from("summary_best_clean_run_id=<none>"),
                String::from("summary_regression_kind=<none>"),
                String::from("summary_regression_message=<none>"),
            ]);
        }

        if let Some(latest_run) = &self.latest_run {
            lines.push(format!("latest_run_id={}", latest_run.run_id));
            lines.push(format!(
                "latest_comparability_label={}",
                latest_run.comparability_label
            ));
        } else {
            lines.extend([
                String::from("latest_run_id=<none>"),
                String::from("latest_comparability_label=<none>"),
            ]);
        }
        if let Some(baseline_run) = &self.baseline_run {
            lines.push(format!("baseline_run_id={}", baseline_run.run_id));
            lines.push(format!(
                "baseline_comparability_label={}",
                baseline_run.comparability_label
            ));
        } else {
            lines.extend([
                String::from("baseline_run_id=<none>"),
                String::from("baseline_comparability_label=<none>"),
            ]);
        }
        lines.push(format!(
            "comparable_to_baseline={}",
            self.comparable_to_baseline
        ));
        if let Some(drift) = &self.drift {
            lines.push(format!(
                "drift_messages_per_second_delta={}",
                drift
                    .messages_per_second_delta
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| String::from("<none>"))
            ));
            lines.push(format!(
                "drift_duration_ms_delta={}",
                drift
                    .duration_ms_delta
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ));
            lines.push(format!(
                "drift_retry_count_delta={}",
                drift
                    .retry_count_delta
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ));
            lines.push(format!(
                "drift_quota_units_delta={}",
                drift
                    .quota_units_delta
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ));
            lines.push(format!(
                "drift_message_fetch_concurrency_delta={}",
                drift
                    .message_fetch_concurrency_delta
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>"))
            ));
        } else {
            lines.extend([
                String::from("drift_messages_per_second_delta=<none>"),
                String::from("drift_duration_ms_delta=<none>"),
                String::from("drift_retry_count_delta=<none>"),
                String::from("drift_quota_units_delta=<none>"),
                String::from("drift_message_fetch_concurrency_delta=<none>"),
            ]);
        }

        lines.push(String::from(
            "runs_format=tsv\trun_id\tfinished_at_epoch_s\tsync_mode\tcomparability_key\tstatus\tmessages_listed\tmessages_per_second\tretry_count",
        ));
        lines.extend(self.runs.iter().map(|run| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{}",
                run.run_id,
                run.finished_at_epoch_s,
                run.sync_mode,
                sanitize_tsv_field(&run.comparability_key),
                run.status,
                run.messages_listed,
                run.messages_per_second,
                run.retry_count,
            )
        }));

        lines.join("\n") + "\n"
    }
}

impl SearchReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            crate::cli_output::print_json_success(self)?;
        } else {
            print!("{}", self.render_plain());
        }

        Ok(())
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![format!("terms={}", sanitize_plain_field(&self.terms))];
        if let Some(label) = &self.label {
            lines.push(format!("label={}", sanitize_plain_field(label)));
        }
        if let Some(from_address) = &self.from_address {
            lines.push(format!("from={}", sanitize_plain_field(from_address)));
        }
        if let Some(after_epoch_ms) = self.after_epoch_ms {
            lines.push(format!("after_epoch_ms={after_epoch_ms}"));
        }
        if let Some(before_epoch_ms) = self.before_epoch_ms {
            lines.push(format!("before_epoch_ms={before_epoch_ms}"));
        }
        lines.push(format!("limit={}", self.limit));
        lines.push(format!("result_count={}", self.results.len()));
        lines.push(String::from(
            "results_format=tsv\tmessage_id\tinternal_date_epoch_ms\tfrom_header\tsubject",
        ));
        lines.extend(self.results.iter().map(|result| {
            format!(
                "{}\t{}\t{}\t{}",
                sanitize_tsv_field(&result.message_id),
                result.internal_date_epoch_ms,
                sanitize_tsv_field(&result.from_header),
                sanitize_tsv_field(&result.subject),
            )
        }));
        lines.join("\n") + "\n"
    }
}

fn sanitize_tsv_field(value: &str) -> String {
    sanitize_plain_field(value)
}

fn sanitize_plain_field(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\t' | '\r' | '\n' => ' ',
            character => character,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::SearchReport;
    use crate::store;

    #[test]
    fn render_plain_search_report_uses_tsv_result_rows() {
        let report = SearchReport {
            terms: String::from("alpha"),
            label: Some(String::from("INBOX")),
            from_address: Some(String::from("alice@example.com")),
            after_epoch_ms: Some(10),
            before_epoch_ms: Some(20),
            limit: 5,
            results: vec![store::mailbox::SearchResult {
                message_id: String::from("m-1"),
                thread_id: String::from("t-1"),
                internal_date_epoch_ms: 123,
                subject: String::from("Alpha launch checklist"),
                from_header: String::from("Alice Example <alice@example.com>"),
                from_address: Some(String::from("alice@example.com")),
                recipient_headers: String::from("ops@example.com"),
                snippet: String::from("snippet"),
                label_names: vec![String::from("INBOX")],
                thread_message_count: 1,
                rank: 0.5,
            }],
        };

        let rendered = report.render_plain();

        assert!(rendered.contains(
            "results_format=tsv\tmessage_id\tinternal_date_epoch_ms\tfrom_header\tsubject"
        ));
        assert!(
            rendered
                .contains("m-1\t123\tAlice Example <alice@example.com>\tAlpha launch checklist")
        );
    }

    #[test]
    fn render_plain_search_report_sanitizes_tsv_control_characters() {
        let report = SearchReport {
            terms: String::from("al\tpha\n"),
            label: Some(String::from("INBOX\r")),
            from_address: Some(String::from("alice@example.com\n")),
            after_epoch_ms: None,
            before_epoch_ms: None,
            limit: 1,
            results: vec![store::mailbox::SearchResult {
                message_id: String::from("m-\t1"),
                thread_id: String::from("t-1"),
                internal_date_epoch_ms: 123,
                subject: String::from("Alpha\nlaunch"),
                from_header: String::from("Alice\r\nExample <alice@example.com>"),
                from_address: Some(String::from("alice@example.com")),
                recipient_headers: String::from("ops@example.com"),
                snippet: String::from("snippet"),
                label_names: vec![String::from("INBOX")],
                thread_message_count: 1,
                rank: 0.5,
            }],
        };

        let rendered = report.render_plain();

        assert!(rendered.contains("terms=al pha "));
        assert!(rendered.contains("label=INBOX "));
        assert!(rendered.contains("from=alice@example.com "));
        assert_eq!(rendered.lines().count(), 7);
        assert!(rendered.contains("m- 1\t123\tAlice  Example <alice@example.com>\tAlpha launch"));
    }
}
