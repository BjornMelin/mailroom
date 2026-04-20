use super::model::{
    AutomationApplyReport, AutomationRule, AutomationRuleAction, AutomationRulesValidateReport,
    AutomationRunPreviewReport, AutomationRunRequest, AutomationShowReport,
};
use super::rules::{ResolvedAutomationRules, resolve_rule_selection, validate_rule_file};
use crate::config::ConfigReport;
use crate::gmail::GmailClient;
use crate::store;
use crate::store::automation::{
    AppendAutomationRunEventInput, AutomationActionKind, AutomationActionSnapshot,
    AutomationApplyStatus, AutomationMatchReason, AutomationRunCandidateRecord,
    AutomationRunStatus, AutomationThreadCandidate, CandidateApplyResultInput,
    CreateAutomationRunInput, FinalizeAutomationRunInput, NewAutomationRunCandidate,
};
use crate::time::current_epoch_seconds;
use anyhow::Result;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::{JoinSet, spawn_blocking};

const AUTOMATION_APPLY_CONCURRENCY: usize = 4;

#[derive(Debug, Error)]
pub enum AutomationServiceError {
    #[error(
        "no active Gmail account found; run `mailroom auth login` and `mailroom sync run` first"
    )]
    NoActiveAccount,
    #[error("automation run limit must be greater than zero")]
    InvalidLimit,
    #[error("re-run with --execute to apply automation changes")]
    ExecuteRequired,
    #[error("automation run {run_id} was not found")]
    RunNotFound { run_id: i64 },
    #[error("automation rules file is missing: {path}")]
    RuleFileMissing { path: PathBuf },
    #[error("failed to read automation rules file {path}: {source}")]
    RuleFileRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse automation rules file {path}: {source}")]
    RuleFileParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{message}")]
    RuleValidation { message: String },
    #[error("failed to join blocking automation task: {source}")]
    BlockingTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("failed to initialize the local store: {source}")]
    StoreInit {
        #[source]
        source: anyhow::Error,
    },
    #[error("failed to read local mailbox state: {source}")]
    MailboxRead {
        #[source]
        source: store::mailbox::MailboxReadError,
    },
    #[error("failed to read automation review state: {source}")]
    AutomationRead {
        #[source]
        source: store::automation::AutomationStoreReadError,
    },
    #[error("failed to persist automation review state: {source}")]
    AutomationWrite {
        #[source]
        source: store::automation::AutomationStoreWriteError,
    },
    #[error("gmail action failed: {source}")]
    Gmail {
        #[from]
        source: crate::gmail::GmailClientError,
    },
}

pub async fn validate_rules(config_report: &ConfigReport) -> Result<AutomationRulesValidateReport> {
    ensure_runtime_dirs_task(configured_paths(config_report)?).await?;
    Ok(validate_rule_file(config_report)?)
}

pub async fn run_preview(
    config_report: &ConfigReport,
    request: AutomationRunRequest,
) -> Result<AutomationRunPreviewReport> {
    if request.limit == 0 {
        return Err(AutomationServiceError::InvalidLimit.into());
    }

    ensure_runtime_dirs_task(configured_paths(config_report)?).await?;
    init_store_task(config_report).await?;
    let account_id = resolve_automation_account_id_task(config_report).await?;
    let resolved_rules = resolve_rule_selection(config_report, &request.rule_ids)?;
    let thread_candidates = list_latest_thread_candidates_task(config_report, &account_id).await?;
    let planned_rules =
        resolve_rule_actions_task(config_report, &account_id, &resolved_rules).await?;
    let now_epoch_ms = current_epoch_seconds()?.saturating_mul(1_000);

    let preview_candidates = build_run_candidates(
        &thread_candidates,
        &planned_rules,
        now_epoch_ms,
        request.limit,
    );
    let created_at_epoch_s = current_epoch_seconds()?;
    let detail = create_automation_run_task(
        config_report,
        &CreateAutomationRunInput {
            account_id,
            rule_file_path: resolved_rules.path.display().to_string(),
            rule_file_hash: resolved_rules.rule_file_hash,
            selected_rule_ids: planned_rules
                .iter()
                .map(|plan| plan.rule.id.clone())
                .collect(),
            created_at_epoch_s,
            candidates: preview_candidates,
        },
    )
    .await?;

    Ok(AutomationRunPreviewReport { detail })
}

pub async fn show_run(config_report: &ConfigReport, run_id: i64) -> Result<AutomationShowReport> {
    init_store_task(config_report).await?;
    let detail = load_run_detail_task(config_report, run_id).await?;
    Ok(AutomationShowReport { detail })
}

pub async fn apply_run(
    config_report: &ConfigReport,
    run_id: i64,
    execute: bool,
) -> Result<AutomationApplyReport> {
    if !execute {
        return Err(AutomationServiceError::ExecuteRequired.into());
    }

    ensure_runtime_dirs_task(configured_paths(config_report)?).await?;
    init_store_task(config_report).await?;
    let detail = load_run_detail_task(config_report, run_id).await?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
    gmail_client.get_profile_with_access_scope().await?;
    let pending_candidates = detail
        .candidates
        .iter()
        .filter(|candidate| candidate.apply_status != Some(AutomationApplyStatus::Succeeded))
        .cloned()
        .collect::<Vec<_>>();
    let started_at_epoch_s = current_epoch_seconds()?;

    append_run_event_task(
        config_report,
        &AppendAutomationRunEventInput {
            run_id,
            account_id: detail.run.account_id.clone(),
            event_kind: String::from("apply_started"),
            payload_json: serde_json::to_string(&json!({
                "candidate_count": pending_candidates.len(),
            }))?,
            created_at_epoch_s: started_at_epoch_s,
        },
    )
    .await?;

    let outcomes = apply_candidates(
        config_report,
        &detail.run.account_id,
        gmail_client,
        pending_candidates,
    )
    .await?;
    let mut applied_candidate_count = 0usize;
    let mut failed_candidate_count = 0usize;
    for outcome in &outcomes {
        if outcome.status == AutomationApplyStatus::Succeeded {
            applied_candidate_count += 1;
        } else {
            failed_candidate_count += 1;
        }
        record_candidate_apply_result_task(
            config_report,
            &CandidateApplyResultInput {
                run_id,
                candidate_id: outcome.candidate_id,
                status: outcome.status,
                applied_at_epoch_s: outcome.applied_at_epoch_s,
                apply_error: outcome.apply_error.clone(),
            },
        )
        .await?;
    }

    let finalized_status = if failed_candidate_count == 0 {
        AutomationRunStatus::Applied
    } else {
        AutomationRunStatus::ApplyFailed
    };
    let finished_at_epoch_s = current_epoch_seconds()?;
    finalize_run_task(
        config_report,
        &FinalizeAutomationRunInput {
            run_id,
            status: finalized_status,
            applied_at_epoch_s: finished_at_epoch_s,
        },
    )
    .await?;
    append_run_event_task(
        config_report,
        &AppendAutomationRunEventInput {
            run_id,
            account_id: detail.run.account_id.clone(),
            event_kind: String::from("apply_finished"),
            payload_json: serde_json::to_string(&json!({
                "applied_candidate_count": applied_candidate_count,
                "failed_candidate_count": failed_candidate_count,
                "status": finalized_status,
            }))?,
            created_at_epoch_s: finished_at_epoch_s,
        },
    )
    .await?;

    let sync_report = if applied_candidate_count == 0 {
        None
    } else {
        best_effort_sync_report(
            crate::mailbox::sync_run(
                config_report,
                false,
                crate::mailbox::DEFAULT_BOOTSTRAP_RECENT_DAYS,
            )
            .await,
            "automation apply sync reconciliation failed",
        )
    };

    let detail = load_run_detail_task(config_report, run_id).await?;
    Ok(AutomationApplyReport {
        detail,
        execute,
        applied_candidate_count,
        failed_candidate_count,
        sync_report,
    })
}

#[derive(Debug, Clone)]
struct PlannedRule {
    rule: AutomationRule,
    action: AutomationActionSnapshot,
}

#[derive(Debug, Clone)]
struct ApplyOutcome {
    candidate_id: i64,
    status: AutomationApplyStatus,
    applied_at_epoch_s: i64,
    apply_error: Option<String>,
}

fn build_run_candidates(
    thread_candidates: &[AutomationThreadCandidate],
    planned_rules: &[PlannedRule],
    now_epoch_ms: i64,
    limit: usize,
) -> Vec<NewAutomationRunCandidate> {
    struct RankedCandidate {
        mailbox_index: usize,
        rule_priority: i64,
        candidate: NewAutomationRunCandidate,
    }

    let mut selected = thread_candidates
        .iter()
        .enumerate()
        .filter_map(|(mailbox_index, thread_candidate)| {
            planned_rules.iter().find_map(|planned_rule| {
                let reason = match_rule(&planned_rule.rule, thread_candidate, now_epoch_ms)?;
                Some(RankedCandidate {
                    mailbox_index,
                    rule_priority: planned_rule.rule.priority,
                    candidate: NewAutomationRunCandidate {
                        rule_id: planned_rule.rule.id.clone(),
                        thread_id: thread_candidate.thread_id.clone(),
                        message_id: thread_candidate.message_id.clone(),
                        internal_date_epoch_ms: thread_candidate.internal_date_epoch_ms,
                        subject: thread_candidate.subject.clone(),
                        from_header: thread_candidate.from_header.clone(),
                        from_address: thread_candidate.from_address.clone(),
                        snippet: thread_candidate.snippet.clone(),
                        label_names: thread_candidate.label_names.clone(),
                        attachment_count: thread_candidate.attachment_count,
                        has_list_unsubscribe: has_list_unsubscribe(thread_candidate),
                        list_id_header: thread_candidate.list_id_header.clone(),
                        list_unsubscribe_header: thread_candidate.list_unsubscribe_header.clone(),
                        list_unsubscribe_post_header: thread_candidate
                            .list_unsubscribe_post_header
                            .clone(),
                        precedence_header: thread_candidate.precedence_header.clone(),
                        auto_submitted_header: thread_candidate.auto_submitted_header.clone(),
                        action: planned_rule.action.clone(),
                        reason,
                    },
                })
            })
        })
        .collect::<Vec<_>>();

    selected.sort_by(|left, right| {
        right
            .rule_priority
            .cmp(&left.rule_priority)
            .then_with(|| left.mailbox_index.cmp(&right.mailbox_index))
    });
    selected.truncate(limit);
    selected.into_iter().map(|entry| entry.candidate).collect()
}

fn match_rule(
    rule: &AutomationRule,
    candidate: &AutomationThreadCandidate,
    now_epoch_ms: i64,
) -> Option<AutomationMatchReason> {
    let candidate_from_address = candidate
        .from_address
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase());
    if let Some(expected_from) = &rule.matcher.from_address
        && candidate_from_address.as_deref() != Some(expected_from.as_str())
    {
        return None;
    }

    let subject_matches = match_any_contains(&candidate.subject, &rule.matcher.subject_contains)?;
    let label_matches = match_any_exact(&candidate.label_names, &rule.matcher.label_any)?;
    if let Some(older_than_days) = rule.matcher.older_than_days {
        let cutoff = now_epoch_ms.saturating_sub(i64::from(older_than_days) * 86_400_000);
        if candidate.internal_date_epoch_ms > cutoff {
            return None;
        }
    }

    let candidate_has_attachments = candidate.attachment_count > 0;
    if let Some(required) = rule.matcher.has_attachments
        && candidate_has_attachments != required
    {
        return None;
    }

    let candidate_has_list_unsubscribe = has_list_unsubscribe(candidate);
    if let Some(required) = rule.matcher.has_list_unsubscribe
        && candidate_has_list_unsubscribe != required
    {
        return None;
    }

    let list_id_matches = match_optional_contains(
        candidate.list_id_header.as_deref(),
        &rule.matcher.list_id_contains,
    )?;
    let precedence_matches = match_optional_contains_case_insensitive(
        candidate.precedence_header.as_deref(),
        &rule.matcher.precedence,
    )?;

    Some(AutomationMatchReason {
        from_address: rule.matcher.from_address.clone(),
        subject_terms: subject_matches,
        label_names: label_matches,
        older_than_days: rule.matcher.older_than_days,
        has_attachments: rule.matcher.has_attachments,
        has_list_unsubscribe: rule.matcher.has_list_unsubscribe,
        list_id_terms: list_id_matches,
        precedence_values: precedence_matches,
    })
}

fn match_any_contains(candidate: &str, required_terms: &[String]) -> Option<Vec<String>> {
    if required_terms.is_empty() {
        return Some(Vec::new());
    }
    let candidate_lower = candidate.to_ascii_lowercase();
    let matches = required_terms
        .iter()
        .filter(|term| candidate_lower.contains(&term.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    (!matches.is_empty()).then_some(matches)
}

fn match_optional_contains(
    candidate: Option<&str>,
    required_terms: &[String],
) -> Option<Vec<String>> {
    if required_terms.is_empty() {
        return Some(Vec::new());
    }
    let candidate = candidate?.to_ascii_lowercase();
    let matches = required_terms
        .iter()
        .filter(|term| candidate.contains(&term.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    (!matches.is_empty()).then_some(matches)
}

fn match_optional_contains_case_insensitive(
    candidate: Option<&str>,
    required_terms: &[String],
) -> Option<Vec<String>> {
    match_optional_contains(candidate, required_terms)
}

fn match_any_exact(candidate_values: &[String], required_values: &[String]) -> Option<Vec<String>> {
    if required_values.is_empty() {
        return Some(Vec::new());
    }
    let candidate_lower = candidate_values
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let matches = required_values
        .iter()
        .filter(|value| candidate_lower.contains(&value.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    (!matches.is_empty()).then_some(matches)
}

fn has_list_unsubscribe(candidate: &AutomationThreadCandidate) -> bool {
    candidate
        .list_unsubscribe_header
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

fn best_effort_sync_report(
    sync_result: anyhow::Result<crate::mailbox::SyncRunReport>,
    warning_context: &str,
) -> Option<crate::mailbox::SyncRunReport> {
    match sync_result {
        Ok(report) => Some(report),
        Err(error) => {
            eprintln!("warning: {warning_context}: {error:#}");
            None
        }
    }
}

async fn apply_candidates(
    config_report: &ConfigReport,
    account_id: &str,
    gmail_client: GmailClient,
    candidates: Vec<AutomationRunCandidateRecord>,
) -> Result<Vec<ApplyOutcome>> {
    let semaphore = std::sync::Arc::new(Semaphore::new(AUTOMATION_APPLY_CONCURRENCY));
    let mut join_set = JoinSet::new();
    let account_id = account_id.to_owned();

    for candidate in candidates {
        let config_report = config_report.clone();
        let account_id = account_id.clone();
        let gmail_client = gmail_client.clone();
        let semaphore = semaphore.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire_owned().await.expect("semaphore closed");
            let applied_at_epoch_s = current_epoch_seconds().unwrap_or(0);
            let result =
                apply_candidate_action(&config_report, &account_id, &gmail_client, &candidate)
                    .await;
            match result {
                Ok(()) => ApplyOutcome {
                    candidate_id: candidate.candidate_id,
                    status: AutomationApplyStatus::Succeeded,
                    applied_at_epoch_s,
                    apply_error: None,
                },
                Err(error) => ApplyOutcome {
                    candidate_id: candidate.candidate_id,
                    status: AutomationApplyStatus::Failed,
                    applied_at_epoch_s,
                    apply_error: Some(error.to_string()),
                },
            }
        });
    }

    let mut outcomes = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(outcome) => outcomes.push(outcome),
            Err(source) => return Err(AutomationServiceError::BlockingTask { source }.into()),
        }
    }
    outcomes.sort_by_key(|outcome| outcome.candidate_id);
    Ok(outcomes)
}

async fn apply_candidate_action(
    config_report: &ConfigReport,
    account_id: &str,
    gmail_client: &GmailClient,
    candidate: &AutomationRunCandidateRecord,
) -> Result<()> {
    let cleanup_action = match candidate.action.kind {
        AutomationActionKind::Archive => Some(store::workflows::CleanupAction::Archive),
        AutomationActionKind::Trash => Some(store::workflows::CleanupAction::Trash),
        AutomationActionKind::Label => None,
    };
    if let Some(cleanup_action) = cleanup_action
        && crate::workflows::cleanup_tracked_thread_for_automation(
            config_report,
            gmail_client,
            account_id,
            &candidate.thread_id,
            cleanup_action,
            Vec::new(),
            Vec::new(),
        )
        .await?
    {
        return Ok(());
    }

    match candidate.action.kind {
        AutomationActionKind::Archive | AutomationActionKind::Label => {
            gmail_client
                .modify_thread_labels(
                    &candidate.thread_id,
                    &candidate.action.add_label_ids,
                    &candidate.action.remove_label_ids,
                )
                .await?;
        }
        AutomationActionKind::Trash => {
            gmail_client.trash_thread(&candidate.thread_id).await?;
        }
    }
    Ok(())
}

async fn resolve_rule_actions_task(
    config_report: &ConfigReport,
    account_id: &str,
    resolved_rules: &ResolvedAutomationRules,
) -> Result<Vec<PlannedRule>> {
    let label_names = required_rule_label_names(&resolved_rules.rules);
    let resolved_labels = if label_names.is_empty() {
        BTreeMap::new()
    } else {
        resolve_label_ids_task(config_report, account_id, &label_names).await?
    };

    let mut planned = Vec::new();
    for rule in &resolved_rules.rules {
        planned.push(PlannedRule {
            rule: rule.clone(),
            action: plan_rule_action(rule, &resolved_labels)?,
        });
    }
    Ok(planned)
}

fn required_rule_label_names(rules: &[AutomationRule]) -> Vec<String> {
    let mut labels = BTreeSet::new();
    for rule in rules {
        match &rule.action {
            AutomationRuleAction::Archive => {
                labels.insert(String::from("INBOX"));
            }
            AutomationRuleAction::Trash => {}
            AutomationRuleAction::Label { add, remove } => {
                labels.extend(add.iter().cloned());
                labels.extend(remove.iter().cloned());
            }
        }
    }
    labels.into_iter().collect()
}

fn plan_rule_action(
    rule: &AutomationRule,
    resolved_labels: &BTreeMap<String, String>,
) -> Result<AutomationActionSnapshot> {
    match &rule.action {
        AutomationRuleAction::Archive => {
            let (remove_label_ids, remove_label_names) =
                resolve_required_labels(&rule.id, &[String::from("INBOX")], resolved_labels)?;
            Ok(AutomationActionSnapshot {
                kind: AutomationActionKind::Archive,
                add_label_ids: Vec::new(),
                add_label_names: Vec::new(),
                remove_label_ids,
                remove_label_names,
            })
        }
        AutomationRuleAction::Trash => Ok(AutomationActionSnapshot {
            kind: AutomationActionKind::Trash,
            add_label_ids: Vec::new(),
            add_label_names: Vec::new(),
            remove_label_ids: Vec::new(),
            remove_label_names: Vec::new(),
        }),
        AutomationRuleAction::Label { add, remove } => {
            let (add_label_ids, add_label_names) =
                resolve_required_labels(&rule.id, add, resolved_labels)?;
            let (remove_label_ids, remove_label_names) =
                resolve_required_labels(&rule.id, remove, resolved_labels)?;
            Ok(AutomationActionSnapshot {
                kind: AutomationActionKind::Label,
                add_label_ids,
                add_label_names,
                remove_label_ids,
                remove_label_names,
            })
        }
    }
}

fn resolve_required_labels(
    rule_id: &str,
    required_names: &[String],
    resolved_labels: &BTreeMap<String, String>,
) -> Result<(Vec<String>, Vec<String>)> {
    if required_names.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut missing = Vec::new();
    for name in required_names {
        if let Some(label_id) = resolved_labels.get(&name.to_ascii_lowercase()) {
            ids.push(label_id.clone());
            names.push(name.clone());
        } else {
            missing.push(name.clone());
        }
    }

    if !missing.is_empty() {
        return Err(AutomationServiceError::RuleValidation {
            message: format!(
                "automation rule `{rule_id}` references labels not present in the local cache: {}. Run `mailroom sync run` or create the labels in Gmail first.",
                missing.join(", ")
            ),
        }
        .into());
    }

    Ok((ids, names))
}

async fn resolve_label_ids_task(
    config_report: &ConfigReport,
    account_id: &str,
    label_names: &[String],
) -> Result<BTreeMap<String, String>> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = account_id.to_owned();
    let label_names = label_names.to_vec();
    let resolved = spawn_blocking(move || {
        store::mailbox::resolve_label_ids_by_names(
            &database_path,
            busy_timeout_ms,
            &account_id,
            &label_names,
        )
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::MailboxRead { source })?;

    Ok(resolved
        .into_iter()
        .map(|(label_id, label_name)| (label_name.to_ascii_lowercase(), label_id))
        .collect())
}

async fn list_latest_thread_candidates_task(
    config_report: &ConfigReport,
    account_id: &str,
) -> Result<Vec<AutomationThreadCandidate>> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let account_id = account_id.to_owned();
    spawn_blocking(move || {
        store::automation::list_latest_thread_candidates(
            &database_path,
            busy_timeout_ms,
            &account_id,
        )
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::AutomationRead { source })
    .map_err(Into::into)
}

async fn create_automation_run_task(
    config_report: &ConfigReport,
    input: &CreateAutomationRunInput,
) -> Result<crate::store::automation::AutomationRunDetail> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let input = input.clone();
    spawn_blocking(move || {
        store::automation::create_automation_run(&database_path, busy_timeout_ms, &input)
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })
    .map_err(Into::into)
}

async fn load_run_detail_task(
    config_report: &ConfigReport,
    run_id: i64,
) -> Result<crate::store::automation::AutomationRunDetail> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let detail = spawn_blocking(move || {
        store::automation::get_automation_run_detail(&database_path, busy_timeout_ms, run_id)
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::AutomationRead { source })?;
    detail.ok_or_else(|| AutomationServiceError::RunNotFound { run_id }.into())
}

async fn record_candidate_apply_result_task(
    config_report: &ConfigReport,
    input: &CandidateApplyResultInput,
) -> Result<()> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let input = input.clone();
    spawn_blocking(move || {
        store::automation::record_candidate_apply_result(&database_path, busy_timeout_ms, &input)
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
}

async fn finalize_run_task(
    config_report: &ConfigReport,
    input: &FinalizeAutomationRunInput,
) -> Result<()> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let input = input.clone();
    spawn_blocking(move || {
        store::automation::finalize_automation_run(&database_path, busy_timeout_ms, &input)
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
}

async fn append_run_event_task(
    config_report: &ConfigReport,
    input: &AppendAutomationRunEventInput,
) -> Result<()> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let input = input.clone();
    spawn_blocking(move || {
        store::automation::append_automation_run_event(&database_path, busy_timeout_ms, &input)
    })
    .await
    .map_err(|source| AutomationServiceError::BlockingTask { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
}

async fn init_store_task(config_report: &ConfigReport) -> Result<()> {
    let config_report = config_report.clone();
    spawn_blocking(move || store::init(&config_report))
        .await
        .map_err(|source| AutomationServiceError::BlockingTask { source })?
        .map_err(|source| AutomationServiceError::StoreInit { source })?;
    Ok(())
}

async fn ensure_runtime_dirs_task(workspace_paths: crate::workspace::WorkspacePaths) -> Result<()> {
    spawn_blocking(move || workspace_paths.ensure_runtime_dirs())
        .await
        .map_err(|source| AutomationServiceError::BlockingTask { source })?
        .map(|_| ())?;
    Ok(())
}

async fn resolve_automation_account_id_task(config_report: &ConfigReport) -> Result<String> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    spawn_blocking(move || resolve_automation_account_id(&database_path, busy_timeout_ms))
        .await
        .map_err(|source| AutomationServiceError::BlockingTask { source })?
}

fn resolve_automation_account_id(
    database_path: &std::path::Path,
    busy_timeout_ms: u64,
) -> Result<String> {
    if let Some(active_account) = store::accounts::get_active(database_path, busy_timeout_ms)? {
        return Ok(active_account.account_id);
    }
    if let Some(mailbox) = store::mailbox::inspect_mailbox(database_path, busy_timeout_ms)?
        && let Some(sync_state) = mailbox.sync_state
    {
        return Ok(sync_state.account_id);
    }
    Err(AutomationServiceError::NoActiveAccount.into())
}

fn configured_paths(config_report: &ConfigReport) -> Result<crate::workspace::WorkspacePaths> {
    crate::configured_paths(config_report)
}

#[cfg(test)]
mod tests {
    use super::{PlannedRule, apply_run, build_run_candidates};
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::automation::model::{AutomationMatchRule, AutomationRule};
    use crate::config::{ConfigReport, resolve};
    use crate::gmail::GmailLabel;
    use crate::store;
    use crate::store::accounts;
    use crate::store::automation::{
        AutomationActionKind, AutomationActionSnapshot, AutomationApplyStatus,
        AutomationMatchReason, AutomationRunStatus, AutomationThreadCandidate,
        CreateAutomationRunInput, NewAutomationRunCandidate,
    };
    use crate::store::mailbox::{GmailMessageUpsertInput, replace_labels, upsert_messages};
    use crate::store::workflows::{
        CleanupAction, ReplyMode, TriageBucket, UpsertDraftRevisionInput, get_workflow_detail,
        set_remote_draft_state, set_triage_state, upsert_draft_revision,
    };
    use crate::workspace::WorkspacePaths;
    use secrecy::SecretString;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn build_run_candidates_applies_limit_after_priority_sorting() {
        let planned_rules = vec![
            planned_rule("high-priority", 200, "vip", AutomationActionKind::Archive),
            planned_rule("low-priority", 100, "digest", AutomationActionKind::Archive),
        ];
        let thread_candidates = vec![
            thread_candidate("thread-new", "message-new", 200, "Daily digest"),
            thread_candidate("thread-old", "message-old", 100, "VIP follow-up"),
        ];

        let selected = build_run_candidates(&thread_candidates, &planned_rules, 0, 1);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].rule_id, "high-priority");
        assert_eq!(selected[0].thread_id, "thread-old");
    }

    #[tokio::test]
    async fn apply_run_requires_auth_before_marking_candidates_failed() {
        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, None);
        store::init(&config_report).unwrap();
        let account = seed_account(&config_report);
        let detail = store::automation::create_automation_run(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &CreateAutomationRunInput {
                account_id: account.account_id.clone(),
                rule_file_path: String::from(".mailroom/automation.toml"),
                rule_file_hash: String::from("hash"),
                selected_rule_ids: vec![String::from("archive-digest")],
                created_at_epoch_s: 100,
                candidates: vec![sample_candidate("archive-digest", "thread-1")],
            },
        )
        .unwrap();

        let error = apply_run(&config_report, detail.run.run_id, true)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "mailroom is not authenticated; run `mailroom auth login` first"
        );

        let detail = store::automation::get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            detail.run.run_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(detail.run.status, AutomationRunStatus::Previewed);
        assert_eq!(detail.candidates[0].apply_status, None);
        assert_eq!(detail.events.len(), 1);
    }

    #[tokio::test]
    async fn apply_run_reuses_workflow_cleanup_for_tracked_threads() {
        let mock_server = MockServer::start().await;
        mount_profile(&mock_server).await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/trash"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "710"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, Some(&mock_server));
        seed_credentials(&config_report);
        seed_local_thread_snapshot(&config_report, "thread-1", "m-1", 100, "Tracked thread");
        let account = seed_account(&config_report);
        seed_tracked_drafting_workflow(&config_report, &account.account_id, "thread-1");
        let detail = store::automation::create_automation_run(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &CreateAutomationRunInput {
                account_id: account.account_id.clone(),
                rule_file_path: String::from(".mailroom/automation.toml"),
                rule_file_hash: String::from("hash"),
                selected_rule_ids: vec![String::from("trash-thread")],
                created_at_epoch_s: 100,
                candidates: vec![NewAutomationRunCandidate {
                    rule_id: String::from("trash-thread"),
                    thread_id: String::from("thread-1"),
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Tracked thread"),
                    from_header: String::from("Alice <alice@example.com>"),
                    from_address: Some(String::from("alice@example.com")),
                    snippet: String::from("Tracked thread"),
                    label_names: vec![String::from("INBOX")],
                    attachment_count: 0,
                    has_list_unsubscribe: false,
                    list_id_header: None,
                    list_unsubscribe_header: None,
                    list_unsubscribe_post_header: None,
                    precedence_header: None,
                    auto_submitted_header: None,
                    action: AutomationActionSnapshot {
                        kind: AutomationActionKind::Trash,
                        add_label_ids: Vec::new(),
                        add_label_names: Vec::new(),
                        remove_label_ids: Vec::new(),
                        remove_label_names: Vec::new(),
                    },
                    reason: AutomationMatchReason {
                        from_address: Some(String::from("alice@example.com")),
                        subject_terms: vec![String::from("tracked")],
                        label_names: vec![String::from("INBOX")],
                        older_than_days: None,
                        has_attachments: None,
                        has_list_unsubscribe: None,
                        list_id_terms: Vec::new(),
                        precedence_values: Vec::new(),
                    },
                }],
            },
        )
        .unwrap();

        let report = apply_run(&config_report, detail.run.run_id, true)
            .await
            .unwrap();

        assert_eq!(report.applied_candidate_count, 1);
        assert_eq!(report.failed_candidate_count, 0);

        let workflow_detail = get_workflow_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &account.account_id,
            "thread-1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            workflow_detail.workflow.current_stage,
            store::workflows::WorkflowStage::Closed
        );
        assert_eq!(
            workflow_detail.workflow.last_cleanup_action,
            Some(CleanupAction::Trash)
        );
        assert_eq!(workflow_detail.workflow.gmail_draft_id, None);
        assert!(workflow_detail.current_draft.is_none());

        let refreshed_run = store::automation::get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            detail.run.run_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(refreshed_run.run.status, AutomationRunStatus::Applied);
        assert_eq!(
            refreshed_run.candidates[0].apply_status,
            Some(AutomationApplyStatus::Succeeded)
        );

        let requests = mock_server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/trash"
        }));
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/gmail/v1/users/me/drafts/draft-1"
        }));
    }

    fn planned_rule(
        id: &str,
        priority: i64,
        subject_term: &str,
        action_kind: AutomationActionKind,
    ) -> PlannedRule {
        PlannedRule {
            rule: AutomationRule {
                id: String::from(id),
                description: None,
                enabled: true,
                priority,
                matcher: AutomationMatchRule {
                    subject_contains: vec![String::from(subject_term)],
                    ..AutomationMatchRule::default()
                },
                action: match action_kind {
                    AutomationActionKind::Archive => {
                        crate::automation::model::AutomationRuleAction::Archive
                    }
                    AutomationActionKind::Trash => {
                        crate::automation::model::AutomationRuleAction::Trash
                    }
                    AutomationActionKind::Label => {
                        crate::automation::model::AutomationRuleAction::Label {
                            add: vec![String::from("Review")],
                            remove: Vec::new(),
                        }
                    }
                },
            },
            action: AutomationActionSnapshot {
                kind: action_kind,
                add_label_ids: Vec::new(),
                add_label_names: Vec::new(),
                remove_label_ids: vec![String::from("INBOX")],
                remove_label_names: vec![String::from("INBOX")],
            },
        }
    }

    fn thread_candidate(
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
        subject: &str,
    ) -> AutomationThreadCandidate {
        AutomationThreadCandidate {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: String::from(thread_id),
            message_id: String::from(message_id),
            internal_date_epoch_ms,
            subject: String::from(subject),
            from_header: String::from("Sender <sender@example.com>"),
            from_address: Some(String::from("sender@example.com")),
            snippet: String::from("Snippet"),
            label_names: vec![String::from("INBOX")],
            attachment_count: 0,
            list_id_header: None,
            list_unsubscribe_header: None,
            list_unsubscribe_post_header: None,
            precedence_header: None,
            auto_submitted_header: None,
        }
    }

    fn sample_candidate(rule_id: &str, thread_id: &str) -> NewAutomationRunCandidate {
        NewAutomationRunCandidate {
            rule_id: String::from(rule_id),
            thread_id: String::from(thread_id),
            message_id: String::from("message-1"),
            internal_date_epoch_ms: 1_700_000_000_000,
            subject: String::from("Daily digest"),
            from_header: String::from("Digest <digest@example.com>"),
            from_address: Some(String::from("digest@example.com")),
            snippet: String::from("Digest snippet"),
            label_names: vec![String::from("INBOX")],
            attachment_count: 0,
            has_list_unsubscribe: true,
            list_id_header: Some(String::from("<digest.example.com>")),
            list_unsubscribe_header: Some(String::from("<mailto:unsubscribe@example.com>")),
            list_unsubscribe_post_header: None,
            precedence_header: Some(String::from("bulk")),
            auto_submitted_header: None,
            action: AutomationActionSnapshot {
                kind: AutomationActionKind::Archive,
                add_label_ids: Vec::new(),
                add_label_names: Vec::new(),
                remove_label_ids: vec![String::from("INBOX")],
                remove_label_names: vec![String::from("INBOX")],
            },
            reason: AutomationMatchReason {
                from_address: Some(String::from("digest@example.com")),
                subject_terms: vec![String::from("digest")],
                label_names: vec![String::from("INBOX")],
                older_than_days: Some(7),
                has_attachments: Some(false),
                has_list_unsubscribe: Some(true),
                list_id_terms: vec![String::from("digest")],
                precedence_values: vec![String::from("bulk")],
            },
        }
    }

    fn config_report_for(temp_dir: &TempDir, mock_server: Option<&MockServer>) -> ConfigReport {
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let mut config_report = resolve(&paths).unwrap();
        if let Some(mock_server) = mock_server {
            config_report.config.gmail.api_base_url = format!("{}/gmail/v1", mock_server.uri());
            config_report.config.gmail.auth_url = format!("{}/oauth2/auth", mock_server.uri());
            config_report.config.gmail.token_url = format!("{}/oauth2/token", mock_server.uri());
            config_report.config.gmail.open_browser = false;
            config_report.config.gmail.client_id = Some(String::from("client-id"));
            config_report.config.gmail.client_secret = Some(String::from("client-secret"));
        }
        config_report
    }

    fn seed_credentials(config_report: &ConfigReport) {
        let credential_store = FileCredentialStore::new(
            config_report
                .config
                .gmail
                .credential_path(&config_report.config.workspace),
        );
        credential_store
            .save(&StoredCredentials {
                account_id: String::from("gmail:operator@example.com"),
                access_token: SecretString::from(String::from("access-token")),
                refresh_token: Some(SecretString::from(String::from("refresh-token"))),
                expires_at_epoch_s: Some(u64::MAX),
                scopes: vec![String::from("scope:a")],
            })
            .unwrap();
    }

    fn seed_account(config_report: &ConfigReport) -> accounts::AccountRecord {
        accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: 1,
                threads_total: 1,
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap()
    }

    fn seed_local_thread_snapshot(
        config_report: &ConfigReport,
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
        subject: &str,
    ) {
        store::init(config_report).unwrap();
        let account = seed_account(config_report);
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &account.account_id,
            &[GmailLabel {
                id: String::from("INBOX"),
                name: String::from("INBOX"),
                label_type: String::from("system"),
                message_list_visibility: None,
                label_list_visibility: None,
                messages_total: None,
                messages_unread: None,
                threads_total: None,
                threads_unread: None,
            }],
            100,
        )
        .unwrap();
        upsert_messages(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &[GmailMessageUpsertInput {
                account_id: account.account_id,
                message_id: String::from(message_id),
                thread_id: String::from(thread_id),
                history_id: String::from("101"),
                internal_date_epoch_ms,
                snippet: String::from(subject),
                subject: String::from(subject),
                from_header: String::from("Alice <alice@example.com>"),
                from_address: Some(String::from("alice@example.com")),
                recipient_headers: String::from("operator@example.com"),
                to_header: String::from("operator@example.com"),
                cc_header: String::new(),
                bcc_header: String::new(),
                reply_to_header: String::new(),
                size_estimate: 123,
                automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
                attachments: Vec::new(),
            }],
            100,
        )
        .unwrap();
    }

    fn seed_tracked_drafting_workflow(
        config_report: &ConfigReport,
        account_id: &str,
        thread_id: &str,
    ) {
        set_triage_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &store::workflows::SetTriageStateInput {
                account_id: account_id.to_owned(),
                thread_id: String::from(thread_id),
                triage_bucket: TriageBucket::NeedsReplySoon,
                note: None,
                snapshot: store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-1"),
                    internal_date_epoch_ms: 100,
                    subject: String::from("Tracked thread"),
                    from_header: String::from("Alice <alice@example.com>"),
                    snippet: String::from("Tracked thread"),
                },
                updated_at_epoch_s: 101,
            },
        )
        .unwrap();
        upsert_draft_revision(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &UpsertDraftRevisionInput {
                account_id: account_id.to_owned(),
                thread_id: String::from(thread_id),
                reply_mode: ReplyMode::Reply,
                source_message_id: String::from("m-1"),
                subject: String::from("Re: Tracked thread"),
                to_addresses: vec![String::from("alice@example.com")],
                cc_addresses: Vec::new(),
                bcc_addresses: Vec::new(),
                body_text: String::from("Draft body"),
                attachments: Vec::new(),
                snapshot: store::workflows::WorkflowMessageSnapshot {
                    message_id: String::from("m-2"),
                    internal_date_epoch_ms: 101,
                    subject: String::from("Re: Tracked thread"),
                    from_header: String::from("Operator <operator@example.com>"),
                    snippet: String::from("Draft body"),
                },
                updated_at_epoch_s: 102,
            },
        )
        .unwrap();
        set_remote_draft_state(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &store::workflows::RemoteDraftStateInput {
                account_id: account_id.to_owned(),
                thread_id: String::from(thread_id),
                gmail_draft_id: Some(String::from("draft-1")),
                gmail_draft_message_id: Some(String::from("draft-message-1")),
                gmail_draft_thread_id: Some(String::from(thread_id)),
                updated_at_epoch_s: 103,
            },
        )
        .unwrap();
    }

    async fn mount_profile(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/profile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "emailAddress": "operator@example.com",
                "messagesTotal": 1,
                "threadsTotal": 1,
                "historyId": "12345"
            })))
            .mount(mock_server)
            .await;
    }
}
