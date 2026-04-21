use crate::auth::{self, AuthStatusReport};
use crate::config::ConfigReport;
use crate::store::mailbox::{self, LabelUsageRecord, MailboxCoverageReport, MailboxDoctorReport};
use crate::store::{self, StoreDoctorReport};
use anyhow::Result;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;

const TOP_USER_LABEL_LIMIT: usize = 10;
const EMPTY_USER_LABEL_LIMIT: usize = 25;
const DEFAULT_OPERATIONAL_WINDOW_DAYS: u32 = 90;
const DEEP_AUDIT_WINDOW_DAYS: u32 = 365;

#[derive(Debug, Clone, Serialize)]
pub struct LabelAuditReport {
    pub account_id: Option<String>,
    pub local_cache_only: bool,
    pub total_label_count: usize,
    pub system_label_count: usize,
    pub user_label_count: usize,
    pub used_user_label_count: usize,
    pub empty_user_label_count: usize,
    pub normalized_overlap_count: usize,
    pub numbered_overlap_count: usize,
    pub top_user_labels: Vec<LabelUsageSummary>,
    pub empty_user_labels: Vec<LabelUsageSummary>,
    pub normalized_overlap_groups: Vec<LabelOverlapGroup>,
    pub numbered_overlap_groups: Vec<LabelOverlapGroup>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationAuditReport {
    pub account_id: Option<String>,
    pub authenticated: bool,
    pub rules_file_path: PathBuf,
    pub rules_file_exists: bool,
    pub bootstrap_query: Option<String>,
    pub bootstrap_recent_days: Option<u32>,
    pub mailbox: Option<MailboxCoverageReport>,
    pub store: VerificationStoreSummary,
    pub label_summary: VerificationLabelSummary,
    pub readiness: VerificationReadiness,
    pub warnings: Vec<String>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LabelUsageSummary {
    pub label_id: String,
    pub name: String,
    pub label_type: String,
    pub gmail_messages_total: Option<i64>,
    pub gmail_threads_total: Option<i64>,
    pub local_message_count: i64,
    pub local_thread_count: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LabelOverlapGroup {
    pub normalized_name: String,
    pub labels: Vec<LabelUsageSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationStoreSummary {
    pub database_exists: bool,
    pub schema_version: Option<i64>,
    pub message_count: i64,
    pub indexed_message_count: i64,
    pub attachment_count: i64,
    pub vaulted_attachment_count: i64,
    pub attachment_export_count: i64,
    pub workflow_count: i64,
    pub automation_run_count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationLabelSummary {
    pub total_label_count: usize,
    pub empty_user_label_count: usize,
    pub normalized_overlap_count: usize,
    pub numbered_overlap_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationReadiness {
    pub manual_mutation_ready: bool,
    pub sender_rule_tuning_ready: bool,
    pub list_header_rule_tuning_ready: bool,
    pub draft_send_canary_ready: bool,
    pub deep_audit_sync_recommended: bool,
}

pub fn labels(config_report: &ConfigReport) -> Result<LabelAuditReport> {
    let store_report = store::inspect(config_report.clone())?;
    let audit_auth = inspect_auth_status_best_effort(config_report);
    let account_id = resolve_audit_account_id(audit_auth.status.as_ref(), &store_report);

    let labels = match &account_id {
        Some(account_id) => mailbox::list_label_usage(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
        )?,
        None => Vec::new(),
    };

    let mut report = build_label_audit_report(account_id, labels);
    if let Some(warning) = audit_auth.warning {
        report.recommendations.push(warning);
    }
    Ok(report)
}

pub fn verification(config_report: &ConfigReport) -> Result<VerificationAuditReport> {
    let store_report = store::inspect(config_report.clone())?;
    let audit_auth = inspect_auth_status_best_effort(config_report);
    let account_id = resolve_audit_account_id(audit_auth.status.as_ref(), &store_report);

    let scoped_mailbox = match &account_id {
        Some(account_id) => mailbox::inspect_mailbox_account(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
        )?,
        None => None,
    };

    let label_report = match &account_id {
        Some(account_id) => build_label_audit_report(
            Some(account_id.clone()),
            mailbox::list_label_usage(
                &config_report.config.store.database_path,
                config_report.config.store.busy_timeout_ms,
                account_id,
            )?,
        ),
        None => build_label_audit_report(None, Vec::new()),
    };

    let mailbox = match &account_id {
        Some(account_id) => mailbox::get_mailbox_coverage(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
        )?,
        None => None,
    };

    Ok(build_verification_report(
        config_report,
        audit_auth.status.as_ref(),
        audit_auth.warning,
        &store_report,
        scoped_mailbox,
        label_report,
        mailbox,
    ))
}

impl LabelAuditReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| {
            if json {
                crate::cli_output::write_json_success(stdout, self)
            } else {
                stdout.write_all(self.render_plain().as_bytes())?;
                Ok(())
            }
        })
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            format!(
                "account_id={}",
                self.account_id.as_deref().unwrap_or("<none>")
            ),
            format!("local_cache_only={}", self.local_cache_only),
            format!("total_label_count={}", self.total_label_count),
            format!("system_label_count={}", self.system_label_count),
            format!("user_label_count={}", self.user_label_count),
            format!("used_user_label_count={}", self.used_user_label_count),
            format!("empty_user_label_count={}", self.empty_user_label_count),
            format!("normalized_overlap_count={}", self.normalized_overlap_count),
            format!("numbered_overlap_count={}", self.numbered_overlap_count),
        ];

        if !self.top_user_labels.is_empty() {
            lines.push(String::from("top_user_labels_format=tsv"));
            lines.push(String::from(
                "name\tlabel_id\tlocal_message_count\tlocal_thread_count\tgmail_messages_total\tgmail_threads_total",
            ));
            lines.extend(self.top_user_labels.iter().map(render_label_summary));
        }

        if !self.empty_user_labels.is_empty() {
            lines.push(String::from("empty_user_labels_format=tsv"));
            lines.push(String::from(
                "name\tlabel_id\tlocal_message_count\tlocal_thread_count\tgmail_messages_total\tgmail_threads_total",
            ));
            lines.extend(self.empty_user_labels.iter().map(render_label_summary));
        }

        if !self.normalized_overlap_groups.is_empty() {
            lines.push(String::from("normalized_overlap_groups_format=tsv"));
            lines.push(String::from(
                "normalized_name\tlabel_count\tlocal_thread_count\tlabels",
            ));
            lines.extend(
                self.normalized_overlap_groups
                    .iter()
                    .map(render_overlap_group),
            );
        }

        if !self.numbered_overlap_groups.is_empty() {
            lines.push(String::from("numbered_overlap_groups_format=tsv"));
            lines.push(String::from(
                "normalized_name\tlabel_count\tlocal_thread_count\tlabels",
            ));
            lines.extend(
                self.numbered_overlap_groups
                    .iter()
                    .map(render_overlap_group),
            );
        }

        for recommendation in &self.recommendations {
            lines.push(format!("recommendation={}", sanitize(recommendation)));
        }

        lines.join("\n") + "\n"
    }
}

impl VerificationAuditReport {
    pub fn print(&self, json: bool) -> Result<()> {
        route_output_to_stdout(json, |json, stdout| {
            if json {
                crate::cli_output::write_json_success(stdout, self)
            } else {
                stdout.write_all(self.render_plain().as_bytes())?;
                Ok(())
            }
        })
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![
            format!(
                "account_id={}",
                self.account_id.as_deref().unwrap_or("<none>")
            ),
            format!("authenticated={}", self.authenticated),
            format!("rules_file_path={}", self.rules_file_path.display()),
            format!("rules_file_exists={}", self.rules_file_exists),
            format!(
                "bootstrap_query={}",
                sanitize(self.bootstrap_query.as_deref().unwrap_or("<none>"))
            ),
            format!(
                "bootstrap_recent_days={}",
                self.bootstrap_recent_days
                    .map(|days| days.to_string())
                    .unwrap_or_else(|| String::from("<unknown>"))
            ),
            format!("database_exists={}", self.store.database_exists),
            format!(
                "schema_version={}",
                self.store
                    .schema_version
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<uninitialized>"))
            ),
            format!("mailbox_message_count={}", self.store.message_count),
            format!(
                "mailbox_indexed_message_count={}",
                self.store.indexed_message_count
            ),
            format!("mailbox_attachment_count={}", self.store.attachment_count),
            format!(
                "mailbox_vaulted_attachment_count={}",
                self.store.vaulted_attachment_count
            ),
            format!(
                "mailbox_attachment_export_count={}",
                self.store.attachment_export_count
            ),
            format!("workflow_count={}", self.store.workflow_count),
            format!("automation_run_count={}", self.store.automation_run_count),
            format!(
                "label_empty_user_count={}",
                self.label_summary.empty_user_label_count
            ),
            format!(
                "label_normalized_overlap_count={}",
                self.label_summary.normalized_overlap_count
            ),
            format!(
                "label_numbered_overlap_count={}",
                self.label_summary.numbered_overlap_count
            ),
        ];

        if let Some(mailbox) = &self.mailbox {
            lines.push(format!("mailbox_thread_count={}", mailbox.thread_count));
            lines.push(format!(
                "messages_with_attachments={}",
                mailbox.messages_with_attachments
            ));
            lines.push(format!(
                "messages_with_list_unsubscribe={}",
                mailbox.messages_with_list_unsubscribe
            ));
            lines.push(format!(
                "messages_with_list_id={}",
                mailbox.messages_with_list_id
            ));
            lines.push(format!(
                "messages_with_precedence={}",
                mailbox.messages_with_precedence
            ));
            lines.push(format!(
                "messages_with_auto_submitted={}",
                mailbox.messages_with_auto_submitted
            ));
        }

        lines.push(format!(
            "manual_mutation_ready={}",
            self.readiness.manual_mutation_ready
        ));
        lines.push(format!(
            "sender_rule_tuning_ready={}",
            self.readiness.sender_rule_tuning_ready
        ));
        lines.push(format!(
            "list_header_rule_tuning_ready={}",
            self.readiness.list_header_rule_tuning_ready
        ));
        lines.push(format!(
            "draft_send_canary_ready={}",
            self.readiness.draft_send_canary_ready
        ));
        lines.push(format!(
            "deep_audit_sync_recommended={}",
            self.readiness.deep_audit_sync_recommended
        ));

        for warning in &self.warnings {
            lines.push(format!("warning={}", sanitize(warning)));
        }
        for next_step in &self.next_steps {
            lines.push(format!("next_step={}", sanitize(next_step)));
        }

        lines.join("\n") + "\n"
    }
}

fn build_label_audit_report(
    account_id: Option<String>,
    labels: Vec<LabelUsageRecord>,
) -> LabelAuditReport {
    let system_label_count = labels
        .iter()
        .filter(|label| label.label_type.eq_ignore_ascii_case("system"))
        .count();
    let user_labels = labels
        .iter()
        .filter(|label| label.label_type.eq_ignore_ascii_case("user"))
        .cloned()
        .collect::<Vec<_>>();
    let used_user_label_count = user_labels
        .iter()
        .filter(|label| label.local_message_count > 0)
        .count();
    let empty_user_label_count = user_labels
        .iter()
        .filter(|label| label.local_message_count == 0)
        .count();
    let mut empty_user_label_records = user_labels
        .iter()
        .filter(|label| label.local_message_count == 0)
        .cloned()
        .collect::<Vec<_>>();
    empty_user_label_records.sort_by(|left, right| left.name.cmp(&right.name));
    let empty_user_labels = empty_user_label_records
        .into_iter()
        .take(EMPTY_USER_LABEL_LIMIT)
        .map(LabelUsageSummary::from)
        .collect::<Vec<_>>();
    let normalized_overlap_groups = collect_overlap_groups(&user_labels, normalize_label_name);
    let numbered_overlap_groups =
        collect_overlap_groups(&user_labels, normalize_label_name_without_prefix);
    let top_user_labels = top_user_labels(&user_labels);

    let mut recommendations = Vec::new();
    if account_id.is_none() {
        recommendations.push(String::from(
            "No active or previously synced mailbox account was found; run auth status and sync run before relying on local audits.",
        ));
    }
    if !numbered_overlap_groups.is_empty() {
        recommendations.push(String::from(
            "Canonicalize numbered and legacy label duplicates before high-volume automation apply runs.",
        ));
    }
    if !normalized_overlap_groups.is_empty() {
        recommendations.push(String::from(
            "Review similarly named user labels for duplicate ownership or near-duplicate taxonomy.",
        ));
    }
    if user_labels.is_empty() && account_id.is_some() {
        recommendations.push(String::from(
            "No user labels are present in the local cache yet; sync labels before designing routing rules.",
        ));
    }
    if !empty_user_labels.is_empty() {
        recommendations.push(String::from(
            "Review empty user labels before building broad automation rules; they are good cleanup or consolidation candidates.",
        ));
    }
    if recommendations.is_empty() {
        recommendations.push(String::from(
            "Label taxonomy looks stable enough for first-wave sender and age-threshold rule tuning.",
        ));
    }

    LabelAuditReport {
        account_id,
        local_cache_only: true,
        total_label_count: labels.len(),
        system_label_count,
        user_label_count: user_labels.len(),
        used_user_label_count,
        empty_user_label_count,
        normalized_overlap_count: normalized_overlap_groups.len(),
        numbered_overlap_count: numbered_overlap_groups.len(),
        top_user_labels,
        empty_user_labels,
        normalized_overlap_groups,
        numbered_overlap_groups,
        recommendations,
    }
}

fn build_verification_report(
    config_report: &ConfigReport,
    auth_status: Option<&AuthStatusReport>,
    auth_warning: Option<String>,
    store_report: &StoreDoctorReport,
    scoped_mailbox: Option<MailboxDoctorReport>,
    label_report: LabelAuditReport,
    mailbox: Option<MailboxCoverageReport>,
) -> VerificationAuditReport {
    let bootstrap_query = scoped_mailbox
        .as_ref()
        .and_then(|mailbox| mailbox.sync_state.as_ref())
        .map(|sync_state| sync_state.bootstrap_query.clone());
    let bootstrap_recent_days = bootstrap_query
        .as_deref()
        .and_then(extract_recent_days_from_bootstrap_query);
    let has_successful_sync = scoped_mailbox
        .as_ref()
        .and_then(|mailbox| mailbox.sync_state.as_ref())
        .is_some_and(|sync_state| sync_state.last_sync_status == mailbox::SyncStatus::Ok);
    let has_live_auth = auth_status
        .is_some_and(|status| status.credential_exists && status.active_account.is_some());

    let readiness = VerificationReadiness {
        manual_mutation_ready: has_live_auth
            && store_report.database_exists
            && has_successful_sync
            && scoped_mailbox
                .as_ref()
                .is_some_and(|mailbox| mailbox.message_count > 0),
        sender_rule_tuning_ready: has_successful_sync
            && scoped_mailbox
                .as_ref()
                .is_some_and(|mailbox| mailbox.message_count > 0)
            && label_report.user_label_count > 0,
        list_header_rule_tuning_ready: has_successful_sync
            && mailbox
                .as_ref()
                .is_some_and(|mailbox| mailbox.messages_with_list_unsubscribe > 0),
        draft_send_canary_ready: has_live_auth
            && has_successful_sync
            && scoped_mailbox
                .as_ref()
                .is_some_and(|mailbox| mailbox.message_count > 0),
        deep_audit_sync_recommended: bootstrap_recent_days
            .is_none_or(|days| days < DEEP_AUDIT_WINDOW_DAYS),
    };

    let mut warnings = Vec::new();
    if let Some(auth_warning) = auth_warning {
        warnings.push(auth_warning);
    }
    if !store_report.database_exists {
        warnings.push(String::from(
            "Local SQLite store is missing; run workspace init, store init, and sync run before verification.",
        ));
    }
    match auth_status {
        Some(auth_status) => {
            if !auth_status.credential_exists {
                warnings.push(String::from(
                    "No Gmail credentials are stored locally; live mutation smoke tests are not ready.",
                ));
            }
            if auth_status.active_account.is_none() {
                warnings.push(String::from(
                    "No active Gmail account is recorded locally; live mutation smoke tests are not ready.",
                ));
            }
        }
        None => warnings.push(String::from(
            "Live Gmail auth status is unavailable; verification is running from cached store data only.",
        )),
    }
    if let Some(mailbox_report) = &scoped_mailbox {
        if let Some(sync_state) = &mailbox_report.sync_state {
            if sync_state.last_sync_status != mailbox::SyncStatus::Ok {
                warnings.push(String::from(
                    "Latest mailbox sync did not finish cleanly; fix sync health before automation rollout.",
                ));
            }
        } else {
            warnings.push(String::from(
                "Mailbox sync has never run; local audits reflect an empty cache.",
            ));
        }
    }
    if let Some(mailbox) = &mailbox
        && mailbox.messages_with_list_unsubscribe == 0
    {
        warnings.push(String::from(
            "List-Unsubscribe headers are absent in the current cache; do a deeper fresh full sync before trusting newsletter/list-header rules.",
        ));
    }
    if readiness.deep_audit_sync_recommended {
        warnings.push(format!(
            "Current bootstrap window is narrower than the recommended {} day audit pass; keep the {} day window for normal operations, but do one deeper sync before final ruleset rollout.",
            DEEP_AUDIT_WINDOW_DAYS, DEFAULT_OPERATIONAL_WINDOW_DAYS
        ));
    }
    if label_report.numbered_overlap_count > 0 {
        warnings.push(String::from(
            "Numbered and legacy label overlaps still exist; clean them up before broad automation applies.",
        ));
    }
    if !config_report
        .config
        .workspace
        .runtime_root
        .join("automation.toml")
        .exists()
    {
        warnings.push(String::from(
            "No active .mailroom/automation.toml ruleset is present yet; use the verification runbook before real bulk actions.",
        ));
    }

    let mut next_steps = vec![
        String::from(
            "Run `cargo run -- sync run --full --recent-days 365 --json` once to build a deeper audit corpus before final rule tuning.",
        ),
        String::from(
            "Run `cargo run -- audit labels --json` and retire numbered-vs-legacy label duplicates before real bulk applies.",
        ),
        String::from(
            "Use self-addressed canary threads for draft/send validation before any real reply workflow rollout.",
        ),
        String::from(
            "Seed first-wave rules with exact senders plus age thresholds, then preview micro-batches before execute.",
        ),
        String::from(
            "Keep first live automation actions to archive plus label only; defer trash and unsubscribe execution.",
        ),
    ];
    if !readiness.list_header_rule_tuning_ready {
        next_steps.insert(
            3,
            String::from(
                "Do not rely on `has_list_unsubscribe` or `list_id_contains` rules until the deeper sync shows nonzero list-header coverage.",
            ),
        );
    }

    VerificationAuditReport {
        account_id: label_report.account_id.clone(),
        authenticated: has_live_auth,
        rules_file_path: config_report
            .config
            .workspace
            .runtime_root
            .join("automation.toml"),
        rules_file_exists: config_report
            .config
            .workspace
            .runtime_root
            .join("automation.toml")
            .exists(),
        bootstrap_query,
        bootstrap_recent_days,
        mailbox,
        store: VerificationStoreSummary {
            database_exists: store_report.database_exists,
            schema_version: store_report.schema_version,
            message_count: scoped_mailbox
                .as_ref()
                .map(|mailbox| mailbox.message_count)
                .unwrap_or(0),
            indexed_message_count: scoped_mailbox
                .as_ref()
                .map(|mailbox| mailbox.indexed_message_count)
                .unwrap_or(0),
            attachment_count: scoped_mailbox
                .as_ref()
                .map(|mailbox| mailbox.attachment_count)
                .unwrap_or(0),
            vaulted_attachment_count: scoped_mailbox
                .as_ref()
                .map(|mailbox| mailbox.vaulted_attachment_count)
                .unwrap_or(0),
            attachment_export_count: scoped_mailbox
                .as_ref()
                .map(|mailbox| mailbox.attachment_export_count)
                .unwrap_or(0),
            workflow_count: store_report
                .workflows
                .as_ref()
                .map(|workflows| workflows.workflow_count)
                .unwrap_or(0),
            automation_run_count: store_report
                .automation
                .as_ref()
                .map(|automation| automation.run_count)
                .unwrap_or(0),
        },
        label_summary: VerificationLabelSummary {
            total_label_count: label_report.total_label_count,
            empty_user_label_count: label_report.empty_user_label_count,
            normalized_overlap_count: label_report.normalized_overlap_count,
            numbered_overlap_count: label_report.numbered_overlap_count,
        },
        readiness,
        warnings,
        next_steps,
    }
}

fn resolve_audit_account_id(
    auth_status: Option<&AuthStatusReport>,
    store_report: &StoreDoctorReport,
) -> Option<String> {
    auth_status
        .and_then(|status| {
            status
                .active_account
                .as_ref()
                .map(|account| account.account_id.clone())
        })
        .or_else(|| {
            store_report
                .mailbox
                .as_ref()
                .and_then(|mailbox| mailbox.sync_state.as_ref())
                .map(|sync_state| sync_state.account_id.clone())
        })
}

struct AuditAuthStatus {
    status: Option<AuthStatusReport>,
    warning: Option<String>,
}

fn inspect_auth_status_best_effort(config_report: &ConfigReport) -> AuditAuthStatus {
    match auth::status(config_report) {
        Ok(status) => AuditAuthStatus {
            status: Some(status),
            warning: None,
        },
        Err(error) => AuditAuthStatus {
            status: None,
            warning: Some(format!(
                "Gmail auth inspection failed; continuing with cached-store audit data only until OAuth config is fixed: {error}"
            )),
        },
    }
}

fn collect_overlap_groups<F>(labels: &[LabelUsageRecord], mut key_fn: F) -> Vec<LabelOverlapGroup>
where
    F: FnMut(&str) -> String,
{
    let mut groups = BTreeMap::<String, Vec<LabelUsageSummary>>::new();
    for label in labels {
        let key = key_fn(&label.name);
        if key.is_empty() {
            continue;
        }
        groups
            .entry(key)
            .or_default()
            .push(LabelUsageSummary::from(label.clone()));
    }

    groups
        .into_iter()
        .filter_map(|(normalized_name, mut labels)| {
            labels.sort_by(|left, right| left.name.cmp(&right.name));
            let unique_names = labels
                .iter()
                .map(|label| label.name.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if unique_names.len() < 2 {
                return None;
            }
            Some(LabelOverlapGroup {
                normalized_name,
                labels,
            })
        })
        .collect()
}

fn top_user_labels(user_labels: &[LabelUsageRecord]) -> Vec<LabelUsageSummary> {
    let mut labels = user_labels
        .iter()
        .filter(|label| label.local_thread_count > 0)
        .cloned()
        .collect::<Vec<_>>();
    labels.sort_by(|left, right| {
        right
            .local_thread_count
            .cmp(&left.local_thread_count)
            .then_with(|| right.local_message_count.cmp(&left.local_message_count))
            .then_with(|| left.name.cmp(&right.name))
    });
    labels
        .into_iter()
        .take(TOP_USER_LABEL_LIMIT)
        .map(LabelUsageSummary::from)
        .collect()
}

fn render_label_summary(label: &LabelUsageSummary) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        sanitize(&label.name),
        sanitize(&label.label_id),
        label.local_message_count,
        label.local_thread_count,
        label
            .gmail_messages_total
            .map(|value| value.to_string())
            .unwrap_or_default(),
        label
            .gmail_threads_total
            .map(|value| value.to_string())
            .unwrap_or_default(),
    )
}

fn render_overlap_group(group: &LabelOverlapGroup) -> String {
    let local_thread_count = group
        .labels
        .iter()
        .map(|label| label.local_thread_count)
        .sum::<i64>();
    let labels = group
        .labels
        .iter()
        .map(|label| label.name.clone())
        .collect::<Vec<_>>()
        .join(" | ");
    format!(
        "{}\t{}\t{}\t{}",
        sanitize(&group.normalized_name),
        group.labels.len(),
        local_thread_count,
        sanitize(&labels),
    )
}

fn normalize_label_name(value: &str) -> String {
    value
        .split_whitespace()
        .map(|segment| segment.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_label_name_without_prefix(value: &str) -> String {
    normalize_label_name(strip_numeric_label_prefix(value))
}

fn strip_numeric_label_prefix(value: &str) -> &str {
    let trimmed = value.trim_start();
    let digit_count = trimmed
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .count();
    if digit_count == 0 {
        return trimmed;
    }
    let digits_end = trimmed
        .char_indices()
        .nth(digit_count)
        .map(|(index, _)| index)
        .unwrap_or(trimmed.len());
    let rest = trimmed[digits_end..].trim_start();
    let rest = if let Some(separator) = rest.chars().next() {
        if matches!(separator, '.' | '-' | ':') {
            rest[separator.len_utf8()..].trim_start()
        } else {
            rest
        }
    } else {
        rest
    };
    rest.trim_start()
}

fn extract_recent_days_from_bootstrap_query(query: &str) -> Option<u32> {
    let marker = "newer_than:";
    let start = query.find(marker)? + marker.len();
    let suffix = &query[start..];
    let digits = suffix
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
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

fn route_output_to_stdout<F>(json: bool, mut write_fn: F) -> Result<()>
where
    F: FnMut(bool, &mut io::StdoutLock<'_>) -> Result<()>,
{
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_fn(json, &mut stdout)
}

impl From<LabelUsageRecord> for LabelUsageSummary {
    fn from(value: LabelUsageRecord) -> Self {
        Self {
            label_id: value.label_id,
            name: value.name,
            label_type: value.label_type,
            gmail_messages_total: value.messages_total,
            gmail_threads_total: value.threads_total,
            local_message_count: value.local_message_count,
            local_thread_count: value.local_thread_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_label_audit_report, extract_recent_days_from_bootstrap_query, labels,
        normalize_label_name, normalize_label_name_without_prefix, verification,
    };
    use crate::config::resolve;
    use crate::gmail::GmailLabel;
    use crate::store::accounts;
    use crate::store::init;
    use crate::store::mailbox::{
        GmailAutomationHeaders, GmailMessageUpsertInput, LabelUsageRecord, SyncMode,
        SyncStateUpdate, SyncStatus, replace_labels, upsert_messages, upsert_sync_state,
    };
    use crate::workspace::WorkspacePaths;
    use std::fs;
    use tempfile::{Builder, TempDir};

    #[test]
    fn normalize_label_name_without_prefix_collapses_numbered_labels() {
        assert_eq!(normalize_label_name("0. To Reply"), "0. to reply");
        assert_eq!(
            normalize_label_name_without_prefix("0. To Reply"),
            "to reply"
        );
        assert_eq!(normalize_label_name_without_prefix("10 - Jobs"), "jobs");
        assert_eq!(
            normalize_label_name_without_prefix("Inbox/Alerts"),
            "inbox/alerts"
        );
    }

    #[test]
    fn build_label_audit_report_detects_numbered_and_empty_labels() {
        let report = build_label_audit_report(
            Some(String::from("gmail:operator@example.com")),
            vec![
                label("Label_1", "0. To Reply", "user", 3, 3),
                label("Label_2", "To Reply", "user", 4, 4),
                label("Label_3", "FYI", "user", 0, 0),
                label("INBOX", "INBOX", "system", 10, 8),
            ],
        );

        assert_eq!(report.total_label_count, 4);
        assert_eq!(report.user_label_count, 3);
        assert_eq!(report.empty_user_label_count, 1);
        assert_eq!(report.numbered_overlap_count, 1);
        assert_eq!(report.normalized_overlap_count, 0);
    }

    #[test]
    fn extract_recent_days_reads_newer_than_queries() {
        assert_eq!(
            extract_recent_days_from_bootstrap_query("newer_than:365d -label:SPAM -label:TRASH"),
            Some(365)
        );
        assert_eq!(extract_recent_days_from_bootstrap_query("in:inbox"), None);
    }

    #[test]
    fn verification_requires_a_successful_sync_for_operational_readiness() {
        let repo_root = unique_temp_dir("mailroom-audit-verification-failed-sync");
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        seed_account(&config_report, "operator@example.com", 100, 1, 1, "scope:a");
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &[gmail_label("INBOX", "INBOX", "system")],
            100,
        )
        .unwrap();
        upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &[message("gmail:operator@example.com", "m-1", "t-1", "101")],
            100,
        )
        .unwrap();
        upsert_sync_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(String::from("101")),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Incremental,
                last_sync_status: SyncStatus::Failed,
                last_error: Some(String::from("quota exhausted")),
                last_sync_epoch_s: 101,
                last_full_sync_success_epoch_s: Some(90),
                last_incremental_sync_success_epoch_s: Some(90),
                pipeline_enabled: false,
                pipeline_list_queue_high_water: 0,
                pipeline_write_queue_high_water: 0,
                pipeline_write_batch_count: 0,
                pipeline_writer_wait_ms: 0,
            },
        )
        .unwrap();

        let report = verification(&config_report).unwrap();

        assert_eq!(
            report.account_id.as_deref(),
            Some("gmail:operator@example.com")
        );
        assert_eq!(report.store.message_count, 1);
        assert!(!report.readiness.manual_mutation_ready);
        assert!(!report.readiness.sender_rule_tuning_ready);
        assert!(!report.readiness.draft_send_canary_ready);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("Latest mailbox sync did not finish cleanly"))
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("No Gmail credentials are stored locally"))
        );
    }

    #[test]
    fn verification_scopes_store_summary_to_the_resolved_account() {
        let repo_root = unique_temp_dir("mailroom-audit-verification-account-scope");
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        seed_account(&config_report, "operator@example.com", 100, 1, 1, "scope:a");
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &[gmail_label("INBOX", "INBOX", "system")],
            100,
        )
        .unwrap();
        upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &[message("gmail:operator@example.com", "m-1", "t-1", "101")],
            100,
        )
        .unwrap();
        upsert_sync_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(String::from("101")),
                bootstrap_query: String::from("newer_than:365d"),
                last_sync_mode: SyncMode::Full,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 101,
                last_full_sync_success_epoch_s: Some(101),
                last_incremental_sync_success_epoch_s: None,
                pipeline_enabled: false,
                pipeline_list_queue_high_water: 0,
                pipeline_write_queue_high_water: 0,
                pipeline_write_batch_count: 0,
                pipeline_writer_wait_ms: 0,
            },
        )
        .unwrap();

        seed_account(&config_report, "other@example.com", 200, 0, 0, "scope:b");

        let report = verification(&config_report).unwrap();

        assert_eq!(
            report.account_id.as_deref(),
            Some("gmail:other@example.com")
        );
        assert!(report.bootstrap_query.is_none());
        assert_eq!(report.store.message_count, 0);
        assert_eq!(report.store.indexed_message_count, 0);
        assert_eq!(report.store.attachment_count, 0);
        assert!(!report.readiness.manual_mutation_ready);
        assert!(!report.readiness.draft_send_canary_ready);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("Mailbox sync has never run"))
        );
    }

    #[test]
    fn labels_continue_with_cached_store_when_auth_status_fails() {
        let repo_root = unique_temp_dir("mailroom-audit-labels-malformed-auth");
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        write_inline_config_and_malformed_imported_oauth(repo_root.path());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        seed_account(&config_report, "operator@example.com", 100, 1, 1, "scope:a");
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &[
                gmail_label("INBOX", "INBOX", "system"),
                gmail_label("Label_1", "Project", "user"),
            ],
            100,
        )
        .unwrap();
        upsert_sync_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(String::from("101")),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Full,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 101,
                last_full_sync_success_epoch_s: Some(101),
                last_incremental_sync_success_epoch_s: None,
                pipeline_enabled: false,
                pipeline_list_queue_high_water: 0,
                pipeline_write_queue_high_water: 0,
                pipeline_write_batch_count: 0,
                pipeline_writer_wait_ms: 0,
            },
        )
        .unwrap();

        let report = labels(&config_report).unwrap();

        assert_eq!(
            report.account_id.as_deref(),
            Some("gmail:operator@example.com")
        );
        assert_eq!(report.total_label_count, 2);
        assert!(
            report
                .recommendations
                .iter()
                .any(|warning| warning.contains("Gmail auth inspection failed"))
        );
    }

    #[test]
    fn verification_uses_cached_store_when_auth_status_fails() {
        let repo_root = unique_temp_dir("mailroom-audit-verification-malformed-auth");
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        write_inline_config_and_malformed_imported_oauth(repo_root.path());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        seed_account(&config_report, "operator@example.com", 100, 1, 1, "scope:a");
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &[gmail_label("INBOX", "INBOX", "system")],
            100,
        )
        .unwrap();
        upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &[message("gmail:operator@example.com", "m-1", "t-1", "101")],
            100,
        )
        .unwrap();
        upsert_sync_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(String::from("101")),
                bootstrap_query: String::from("newer_than:365d"),
                last_sync_mode: SyncMode::Full,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 101,
                last_full_sync_success_epoch_s: Some(101),
                last_incremental_sync_success_epoch_s: None,
                pipeline_enabled: false,
                pipeline_list_queue_high_water: 0,
                pipeline_write_queue_high_water: 0,
                pipeline_write_batch_count: 0,
                pipeline_writer_wait_ms: 0,
            },
        )
        .unwrap();

        let report = verification(&config_report).unwrap();

        assert_eq!(
            report.account_id.as_deref(),
            Some("gmail:operator@example.com")
        );
        assert!(!report.authenticated);
        assert_eq!(report.store.message_count, 1);
        assert!(!report.readiness.manual_mutation_ready);
        assert!(!report.readiness.draft_send_canary_ready);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("Gmail auth inspection failed"))
        );
    }

    #[test]
    fn verification_requires_credentials_for_live_mutation_readiness() {
        let repo_root = unique_temp_dir("mailroom-audit-verification-missing-credentials");
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        seed_account(&config_report, "operator@example.com", 100, 1, 1, "scope:a");
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &[gmail_label("INBOX", "INBOX", "system")],
            100,
        )
        .unwrap();
        upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &[message("gmail:operator@example.com", "m-1", "t-1", "101")],
            100,
        )
        .unwrap();
        upsert_sync_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(String::from("101")),
                bootstrap_query: String::from("newer_than:365d"),
                last_sync_mode: SyncMode::Full,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 101,
                last_full_sync_success_epoch_s: Some(101),
                last_incremental_sync_success_epoch_s: None,
                pipeline_enabled: false,
                pipeline_list_queue_high_water: 0,
                pipeline_write_queue_high_water: 0,
                pipeline_write_batch_count: 0,
                pipeline_writer_wait_ms: 0,
            },
        )
        .unwrap();

        let report = verification(&config_report).unwrap();

        assert!(!report.authenticated);
        assert!(!report.readiness.manual_mutation_ready);
        assert!(!report.readiness.draft_send_canary_ready);
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("No Gmail credentials are stored locally"))
        );
    }

    fn label(
        label_id: &str,
        name: &str,
        label_type: &str,
        local_message_count: i64,
        local_thread_count: i64,
    ) -> LabelUsageRecord {
        LabelUsageRecord {
            label_id: label_id.to_owned(),
            name: name.to_owned(),
            label_type: label_type.to_owned(),
            messages_total: None,
            threads_total: None,
            local_message_count,
            local_thread_count,
        }
    }

    fn seed_account(
        config_report: &crate::config::ConfigReport,
        email_address: &str,
        refreshed_at_epoch_s: i64,
        messages_total: i64,
        threads_total: i64,
        access_scope: &str,
    ) {
        accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: email_address.to_owned(),
                history_id: refreshed_at_epoch_s.to_string(),
                messages_total,
                threads_total,
                access_scope: access_scope.to_owned(),
                refreshed_at_epoch_s,
            },
        )
        .unwrap();
    }

    fn gmail_label(id: &str, name: &str, label_type: &str) -> GmailLabel {
        GmailLabel {
            id: id.to_owned(),
            name: name.to_owned(),
            label_type: label_type.to_owned(),
            message_list_visibility: None,
            label_list_visibility: None,
            messages_total: None,
            messages_unread: None,
            threads_total: None,
            threads_unread: None,
        }
    }

    fn message(
        account_id: &str,
        message_id: &str,
        thread_id: &str,
        history_id: &str,
    ) -> GmailMessageUpsertInput {
        GmailMessageUpsertInput {
            account_id: account_id.to_owned(),
            message_id: message_id.to_owned(),
            thread_id: thread_id.to_owned(),
            history_id: history_id.to_owned(),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Audit verification seed"),
            subject: String::from("Seed"),
            from_header: String::from("Alice <alice@example.com>"),
            from_address: Some(String::from("alice@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: Vec::new(),
        }
    }

    fn unique_temp_dir(prefix: &str) -> TempDir {
        Builder::new().prefix(prefix).tempdir().unwrap()
    }

    fn write_inline_config_and_malformed_imported_oauth(repo_root: &std::path::Path) {
        let repo_config_path = repo_root.join(".mailroom/config.toml");
        let oauth_client_path = repo_root.join(".mailroom/auth/gmail-oauth-client.json");
        fs::create_dir_all(repo_config_path.parent().unwrap()).unwrap();
        fs::create_dir_all(oauth_client_path.parent().unwrap()).unwrap();
        fs::write(
            &repo_config_path,
            r#"
[gmail]
client_id = "inline-client.apps.googleusercontent.com"
client_secret = "inline-secret"
"#,
        )
        .unwrap();
        fs::write(&oauth_client_path, "{not-json").unwrap();
    }
}
