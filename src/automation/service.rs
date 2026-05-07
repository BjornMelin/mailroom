use super::model::{
    AutomationApplyReport, AutomationPruneReport, AutomationPruneRequest, AutomationPruneStatus,
    AutomationRolloutCandidateSummary, AutomationRolloutReport, AutomationRolloutRequest,
    AutomationRule, AutomationRuleAction, AutomationRulesSuggestReport,
    AutomationRulesSuggestRequest, AutomationRulesValidateReport, AutomationRunPreviewReport,
    AutomationRunRequest, AutomationShowReport,
};
use super::rules::{ResolvedAutomationRules, resolve_rule_selection, validate_rule_file};
use super::suggestions::suggest_rules_from_candidates;
use crate::config::ConfigReport;
use crate::gmail::GmailClient;
use crate::store;
use crate::store::automation::{
    AppendAutomationRunEventInput, AutomationActionKind, AutomationActionSnapshot,
    AutomationApplyStatus, AutomationMatchReason, AutomationRunCandidateRecord,
    AutomationRunStatus, AutomationThreadCandidate, CandidateApplyResultInput,
    CreateAutomationRunInput, FinalizeAutomationRunInput, NewAutomationRunCandidate,
    PruneAutomationRunsInput,
};
use crate::time::current_epoch_seconds;
use anyhow::Result;
use fs2::FileExt;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::PathBuf;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::{JoinSet, spawn_blocking};

const AUTOMATION_APPLY_CONCURRENCY: usize = 4;
const SECONDS_PER_DAY: i64 = 86_400;

#[derive(Debug, Error)]
pub enum AutomationServiceError {
    #[error(
        "no active Gmail account found; run `mailroom auth login` and `mailroom sync run` first"
    )]
    NoActiveAccount,
    #[error("automation run limit must be greater than zero")]
    InvalidLimit,
    #[error("automation rollout limit must be greater than zero")]
    InvalidRolloutLimit,
    #[error("automation prune --older-than-days must be greater than zero")]
    InvalidPruneWindow,
    #[error("automation rules suggest --limit must be greater than zero")]
    InvalidSuggestionLimit,
    #[error("automation rules suggest --min-thread-count must be greater than zero")]
    InvalidSuggestionMinThreadCount,
    #[error("automation rules suggest --older-than-days must be greater than zero")]
    InvalidSuggestionOlderThanDays,
    #[error("automation rules suggest --sample-limit must be greater than zero")]
    InvalidSuggestionSampleLimit,
    #[error("re-run with --execute to apply automation changes")]
    ExecuteRequired,
    #[error(
        "automation run {run_id} was previewed for {expected_account_id}, but the authenticated Gmail account is {actual_account_id}; re-run preview under the authenticated account"
    )]
    RunAccountMismatch {
        run_id: i64,
        expected_account_id: String,
        actual_account_id: String,
    },
    #[error("automation run {run_id} was not found")]
    RunNotFound { run_id: i64 },
    #[error("automation apply is already in progress for run {run_id}")]
    ApplyAlreadyInProgress { run_id: i64 },
    #[error("failed to acquire automation apply lock at {path}: {source}")]
    ApplyLock {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
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
    #[error("failed to render suggested automation rule TOML: {source}")]
    RuleTomlSerialize {
        #[source]
        source: toml::ser::Error,
    },
    #[error("failed to join automation task: {source}")]
    TaskPanic {
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
    Ok(validate_rule_file(config_report).await?)
}

pub async fn suggest_rules(
    config_report: &ConfigReport,
    request: AutomationRulesSuggestRequest,
) -> Result<AutomationRulesSuggestReport> {
    validate_suggest_request(&request)?;
    ensure_runtime_dirs_task(configured_paths(config_report)?).await?;
    init_store_task(config_report).await?;
    let account_id = resolve_automation_account_id_task(config_report).await?;
    let thread_candidates = list_latest_thread_candidates_task(config_report, &account_id).await?;
    let now_epoch_ms = current_epoch_seconds()?.saturating_mul(1_000);
    Ok(suggest_rules_from_candidates(
        config_report,
        account_id,
        &thread_candidates,
        &request,
        now_epoch_ms,
    )?)
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
    let resolved_rules = resolve_rule_selection(config_report, &request.rule_ids).await?;
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

pub async fn rollout(
    config_report: &ConfigReport,
    request: AutomationRolloutRequest,
) -> Result<AutomationRolloutReport> {
    if request.limit == 0 {
        return Err(AutomationServiceError::InvalidRolloutLimit.into());
    }

    ensure_runtime_dirs_task(configured_paths(config_report)?).await?;
    init_store_task(config_report).await?;

    let verification = verification_audit_task(config_report).await?;
    let mut blockers = Vec::new();
    let mut warnings = verification.warnings.clone();
    let mut selected_rule_ids = Vec::new();
    let mut blocked_rule_ids = Vec::new();
    let mut candidates = Vec::new();

    let rules = match validate_rule_file(config_report).await {
        Ok(report) => Some(report),
        Err(error) => {
            blockers.push(format!("automation rules are not ready: {error}"));
            None
        }
    };

    if rules.is_some() {
        match resolve_rollout_candidates(config_report, &request).await {
            Ok(selection) => {
                selected_rule_ids = selection.selected_rule_ids;
                blocked_rule_ids = selection.blocked_rule_ids;
                candidates = selection.candidates;
            }
            Err(error) if is_rollout_blocker(&error) => {
                blockers.push(error.to_string());
            }
            Err(error) => return Err(error),
        }
    }

    if !blocked_rule_ids.is_empty() {
        blockers.push(format!(
            "first-wave automation rollout excludes trash rules; remove or disable: {}",
            blocked_rule_ids.join(", ")
        ));
        candidates.clear();
    }
    if candidates.is_empty() && blockers.is_empty() {
        warnings.push(String::from(
            "Selected rules did not match any local thread candidates; inspect the synced cache and rule predicates before applying.",
        ));
    }

    let command_plan = rollout_command_plan(&selected_rule_ids, request.limit);
    let mut next_steps = verification.next_steps.clone();
    if blockers.is_empty() {
        next_steps.push(String::from(
            "Persist a review snapshot with the matching automation run command, inspect it with automation show, then apply only a reviewed micro-batch.",
        ));
    } else {
        next_steps.push(String::from(
            "Clear rollout blockers before creating a persistent automation run.",
        ));
    }

    Ok(AutomationRolloutReport {
        verification,
        rules,
        selected_rule_count: selected_rule_ids.len(),
        selected_rule_ids,
        candidate_count: candidates.len(),
        candidates,
        blocked_rule_ids,
        blockers,
        warnings,
        next_steps,
        command_plan,
    })
}

pub async fn show_run(config_report: &ConfigReport, run_id: i64) -> Result<AutomationShowReport> {
    init_store_task(config_report).await?;
    let detail = load_run_detail_task(config_report, run_id).await?;
    Ok(AutomationShowReport { detail })
}

pub async fn prune_runs(
    config_report: &ConfigReport,
    request: AutomationPruneRequest,
) -> Result<AutomationPruneReport> {
    if request.older_than_days == 0 {
        return Err(AutomationServiceError::InvalidPruneWindow.into());
    }

    ensure_runtime_dirs_task(configured_paths(config_report)?).await?;
    init_store_task(config_report).await?;
    let account_id = resolve_automation_account_id_task(config_report).await?;
    let statuses = normalize_prune_statuses(request.statuses);
    let cutoff_epoch_s = current_epoch_seconds()?
        .saturating_sub(i64::from(request.older_than_days) * SECONDS_PER_DAY);
    let store_report = prune_automation_runs_task(
        config_report,
        &PruneAutomationRunsInput {
            account_id: account_id.clone(),
            cutoff_epoch_s,
            statuses: statuses
                .iter()
                .copied()
                .map(prune_status_to_run_status)
                .collect(),
            execute: request.execute,
        },
    )
    .await?;

    let mut warnings = Vec::new();
    if !request.execute {
        warnings.push(String::from(
            "Dry run only; rerun with --execute to delete matched local automation snapshots.",
        ));
    }
    let next_steps = if request.execute {
        vec![String::from(
            "Run `cargo run -- doctor --json` if you want to inspect updated local automation counts.",
        )]
    } else {
        vec![String::from(
            "Rerun the same prune command with --execute after reviewing the matched counts.",
        )]
    };

    Ok(AutomationPruneReport {
        account_id,
        execute: request.execute,
        older_than_days: request.older_than_days,
        cutoff_epoch_s,
        statuses: statuses
            .iter()
            .map(|status| status.as_str().to_owned())
            .collect(),
        matched_run_count: store_report.matched_run_count,
        matched_candidate_count: store_report.matched_candidate_count,
        matched_event_count: store_report.matched_event_count,
        deleted_run_count: store_report.deleted_run_count,
        warnings,
        next_steps,
    })
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
    let _apply_lock = acquire_apply_run_lock_task(config_report, run_id).await?;
    let detail = load_run_detail_task(config_report, run_id).await?;
    let gmail_client = crate::gmail_client_for_config(config_report)?;
    let (profile, _) = gmail_client.get_profile_with_access_scope().await?;
    let authenticated_account_id = gmail_account_id_for_email(&profile.email_address);
    if authenticated_account_id != detail.run.account_id {
        return Err(AutomationServiceError::RunAccountMismatch {
            run_id,
            expected_account_id: detail.run.account_id.clone(),
            actual_account_id: authenticated_account_id,
        }
        .into());
    }
    let pending_candidates = detail
        .candidates
        .iter()
        .filter(|candidate| candidate.apply_status != Some(AutomationApplyStatus::Succeeded))
        .cloned()
        .collect::<Vec<_>>();
    match detail.run.status {
        AutomationRunStatus::Previewed => {
            let started_at_epoch_s = current_epoch_seconds()?;
            claim_run_for_apply_task(config_report, run_id, started_at_epoch_s).await?;
            append_run_start_event_task(
                config_report,
                &detail.run.account_id,
                run_id,
                pending_candidates.len(),
                started_at_epoch_s,
            )
            .await?;
        }
        AutomationRunStatus::Applying => {
            let started_at_epoch_s = detail
                .run
                .applied_at_epoch_s
                .unwrap_or(current_epoch_seconds()?);
            if !detail
                .events
                .iter()
                .any(|event| event.event_kind == "apply_started")
            {
                append_run_start_event_task(
                    config_report,
                    &detail.run.account_id,
                    run_id,
                    pending_candidates.len(),
                    started_at_epoch_s,
                )
                .await?;
            }
        }
        AutomationRunStatus::Applied => {
            return Err(AutomationServiceError::RuleValidation {
                message: format!(
                    "automation run {run_id} has already been finalized with status {}",
                    detail.run.status
                ),
            }
            .into());
        }
        AutomationRunStatus::ApplyFailed => {
            let started_at_epoch_s = detail
                .run
                .applied_at_epoch_s
                .unwrap_or(current_epoch_seconds()?);
            claim_run_for_apply_task(config_report, run_id, started_at_epoch_s).await?;
            append_run_start_event_task(
                config_report,
                &detail.run.account_id,
                run_id,
                pending_candidates.len(),
                started_at_epoch_s,
            )
            .await?;
        }
    };

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

#[derive(Debug)]
struct AutomationApplyLock {
    file: File,
}

impl Drop for AutomationApplyLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
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

#[derive(Debug)]
struct RolloutSelection {
    selected_rule_ids: Vec<String>,
    blocked_rule_ids: Vec<String>,
    candidates: Vec<AutomationRolloutCandidateSummary>,
}

async fn resolve_rollout_candidates(
    config_report: &ConfigReport,
    request: &AutomationRolloutRequest,
) -> Result<RolloutSelection> {
    let account_id = resolve_automation_account_id_task(config_report).await?;
    let resolved_rules = resolve_rule_selection(config_report, &request.rule_ids).await?;
    let selected_rule_ids = resolved_rules
        .rules
        .iter()
        .map(|rule| rule.id.clone())
        .collect::<Vec<_>>();
    let blocked_rule_ids = resolved_rules
        .rules
        .iter()
        .filter(|rule| rule.action_kind() == AutomationActionKind::Trash)
        .map(|rule| rule.id.clone())
        .collect::<Vec<_>>();
    if !blocked_rule_ids.is_empty() {
        return Ok(RolloutSelection {
            selected_rule_ids,
            blocked_rule_ids,
            candidates: Vec::new(),
        });
    }

    let planned_rules =
        resolve_rule_actions_task(config_report, &account_id, &resolved_rules).await?;
    let thread_candidates = list_latest_thread_candidates_task(config_report, &account_id).await?;
    let now_epoch_ms = current_epoch_seconds()?.saturating_mul(1_000);
    let candidates = build_run_candidates(
        &thread_candidates,
        &planned_rules,
        now_epoch_ms,
        request.limit,
    )
    .iter()
    .map(rollout_candidate_summary)
    .collect();

    Ok(RolloutSelection {
        selected_rule_ids,
        blocked_rule_ids,
        candidates,
    })
}

fn is_rollout_blocker(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<AutomationServiceError>(),
        Some(
            AutomationServiceError::NoActiveAccount
                | AutomationServiceError::RuleFileMissing { .. }
                | AutomationServiceError::RuleFileRead { .. }
                | AutomationServiceError::RuleFileParse { .. }
                | AutomationServiceError::RuleValidation { .. }
        )
    )
}

fn rollout_candidate_summary(
    candidate: &NewAutomationRunCandidate,
) -> AutomationRolloutCandidateSummary {
    AutomationRolloutCandidateSummary {
        rule_id: candidate.rule_id.clone(),
        thread_id: candidate.thread_id.clone(),
        message_id: candidate.message_id.clone(),
        action_kind: candidate.action.kind.as_str().to_owned(),
        subject: candidate.subject.clone(),
        from_address: candidate.from_address.clone(),
        label_names: candidate.label_names.clone(),
        has_list_unsubscribe: candidate.has_list_unsubscribe,
        matched_predicates: match_reason_tokens(&candidate.reason),
    }
}

fn match_reason_tokens(reason: &AutomationMatchReason) -> Vec<String> {
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
    predicates
}

fn rollout_command_plan(selected_rule_ids: &[String], limit: usize) -> Vec<String> {
    let selected_rules = selected_rule_ids
        .iter()
        .map(|rule_id| format!(" --rule {rule_id}"))
        .collect::<String>();
    vec![
        String::from("cargo run -- automation rules validate --json"),
        format!("cargo run -- automation run{selected_rules} --limit {limit} --json"),
        String::from("cargo run -- automation show <run-id> --json"),
        String::from("cargo run -- automation apply <run-id> --execute --json"),
        String::from("cargo run -- audit verification --json"),
    ]
}

fn normalize_prune_statuses(statuses: Vec<AutomationPruneStatus>) -> Vec<AutomationPruneStatus> {
    if statuses.is_empty() {
        vec![AutomationPruneStatus::Previewed]
    } else {
        let mut normalized = Vec::new();
        for status in statuses {
            if !normalized.contains(&status) {
                normalized.push(status);
            }
        }
        normalized
    }
}

fn validate_suggest_request(
    request: &AutomationRulesSuggestRequest,
) -> Result<(), AutomationServiceError> {
    if request.limit == 0 {
        return Err(AutomationServiceError::InvalidSuggestionLimit);
    }
    if request.min_thread_count == 0 {
        return Err(AutomationServiceError::InvalidSuggestionMinThreadCount);
    }
    if request.older_than_days == 0 {
        return Err(AutomationServiceError::InvalidSuggestionOlderThanDays);
    }
    if request.sample_limit == 0 {
        return Err(AutomationServiceError::InvalidSuggestionSampleLimit);
    }
    Ok(())
}

fn prune_status_to_run_status(status: AutomationPruneStatus) -> AutomationRunStatus {
    match status {
        AutomationPruneStatus::Previewed => AutomationRunStatus::Previewed,
        AutomationPruneStatus::Applied => AutomationRunStatus::Applied,
        AutomationPruneStatus::ApplyFailed => AutomationRunStatus::ApplyFailed,
    }
}

async fn verification_audit_task(
    config_report: &ConfigReport,
) -> Result<crate::audit::VerificationAuditReport> {
    let config_report = config_report.clone();
    spawn_blocking(move || crate::audit::verification(&config_report))
        .await
        .map_err(|source| AutomationServiceError::TaskPanic { source })?
}

async fn acquire_apply_run_lock_task(
    config_report: &ConfigReport,
    run_id: i64,
) -> Result<AutomationApplyLock, AutomationServiceError> {
    let lock_path = automation_apply_lock_path(config_report);
    spawn_blocking(move || acquire_apply_run_lock_blocking(lock_path, run_id))
        .await
        .map_err(|source| AutomationServiceError::TaskPanic { source })?
}

fn acquire_apply_run_lock_blocking(
    lock_path: PathBuf,
    run_id: i64,
) -> Result<AutomationApplyLock, AutomationServiceError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| AutomationServiceError::ApplyLock {
            path: lock_path.clone(),
            source,
        })?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(AutomationApplyLock { file }),
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            Err(AutomationServiceError::ApplyAlreadyInProgress { run_id })
        }
        Err(source) => Err(AutomationServiceError::ApplyLock {
            path: lock_path,
            source,
        }),
    }
}

fn automation_apply_lock_path(config_report: &ConfigReport) -> PathBuf {
    config_report
        .config
        .workspace
        .state_dir
        .join("automation-apply.lock")
}

async fn claim_run_for_apply_task(
    config_report: &ConfigReport,
    run_id: i64,
    started_at_epoch_s: i64,
) -> Result<()> {
    let config_report = config_report.clone();
    spawn_blocking(move || {
        store::automation::claim_automation_run_for_apply(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            run_id,
            started_at_epoch_s,
        )
    })
    .await
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
}

async fn append_run_start_event_task(
    config_report: &ConfigReport,
    account_id: &str,
    run_id: i64,
    candidate_count: usize,
    created_at_epoch_s: i64,
) -> Result<()> {
    let config_report = config_report.clone();
    let account_id = account_id.to_owned();
    spawn_blocking(move || {
        store::automation::append_automation_run_event(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &AppendAutomationRunEventInput {
                run_id,
                account_id,
                event_kind: String::from("apply_started"),
                payload_json: serde_json::to_string(&json!({
                    "candidate_count": candidate_count,
                }))?,
                created_at_epoch_s,
            },
        )
    })
    .await
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
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
    let precedence_matches = match_optional_contains(
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
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed");
        join_set.spawn(async move {
            let _permit = permit;
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
            Err(source) => return Err(AutomationServiceError::TaskPanic { source }.into()),
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
        AutomationActionKind::Label => Some(store::workflows::CleanupAction::Label),
    };
    if let Some(cleanup_action) = cleanup_action
        && crate::workflows::cleanup_tracked_thread_for_automation(
            config_report,
            gmail_client,
            account_id,
            &candidate.thread_id,
            cleanup_action,
            candidate.action.add_label_names.clone(),
            candidate.action.remove_label_names.clone(),
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
}

async fn prune_automation_runs_task(
    config_report: &ConfigReport,
    input: &PruneAutomationRunsInput,
) -> Result<store::automation::AutomationPruneStoreReport> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let input = input.clone();
    spawn_blocking(move || {
        store::automation::prune_automation_runs(&database_path, busy_timeout_ms, &input)
    })
    .await
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })
    .map_err(Into::into)
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
    .map_err(|source| AutomationServiceError::TaskPanic { source })?
    .map_err(|source| AutomationServiceError::AutomationWrite { source })?;
    Ok(())
}

async fn init_store_task(config_report: &ConfigReport) -> Result<()> {
    let config_report = config_report.clone();
    spawn_blocking(move || store::init(&config_report))
        .await
        .map_err(|source| AutomationServiceError::TaskPanic { source })?
        .map_err(|source| AutomationServiceError::StoreInit { source })?;
    Ok(())
}

async fn ensure_runtime_dirs_task(workspace_paths: crate::workspace::WorkspacePaths) -> Result<()> {
    spawn_blocking(move || workspace_paths.ensure_runtime_dirs())
        .await
        .map_err(|source| AutomationServiceError::TaskPanic { source })?
        .map(|_| ())?;
    Ok(())
}

async fn resolve_automation_account_id_task(config_report: &ConfigReport) -> Result<String> {
    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    spawn_blocking(move || resolve_automation_account_id(&database_path, busy_timeout_ms))
        .await
        .map_err(|source| AutomationServiceError::TaskPanic { source })?
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

fn gmail_account_id_for_email(email_address: &str) -> String {
    format!("gmail:{}", email_address.trim().to_ascii_lowercase())
}

fn configured_paths(config_report: &ConfigReport) -> Result<crate::workspace::WorkspacePaths> {
    crate::configured_paths(config_report)
}

#[cfg(test)]
mod tests {
    use super::{
        AutomationServiceError, PlannedRule, acquire_apply_run_lock_blocking, apply_run,
        automation_apply_lock_path, build_run_candidates, gmail_account_id_for_email, rollout,
    };
    use crate::auth::file_store::{CredentialStore, FileCredentialStore, StoredCredentials};
    use crate::automation::model::{AutomationMatchRule, AutomationRolloutRequest, AutomationRule};
    use crate::config::{ConfigReport, resolve};
    use crate::gmail::GmailLabel;
    use crate::store;
    use crate::store::accounts;
    use crate::store::automation::{
        AutomationActionKind, AutomationActionSnapshot, AutomationApplyStatus,
        AutomationMatchReason, AutomationRunStatus, AutomationThreadCandidate,
        CreateAutomationRunInput, FinalizeAutomationRunInput, NewAutomationRunCandidate,
    };
    use crate::store::mailbox::{GmailMessageUpsertInput, replace_labels, upsert_messages};
    use crate::store::workflows::{
        CleanupAction, ReplyMode, TriageBucket, UpsertDraftRevisionInput, get_workflow_detail,
        set_remote_draft_state, set_triage_state, upsert_draft_revision,
    };
    use crate::workspace::WorkspacePaths;
    use secrecy::SecretString;
    use std::fs;
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
    async fn rollout_reports_missing_rules_without_persisting_run() {
        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, None);
        store::init(&config_report).unwrap();
        seed_account(&config_report);

        let report = rollout(
            &config_report,
            AutomationRolloutRequest {
                rule_ids: Vec::new(),
                limit: 10,
            },
        )
        .await
        .unwrap();

        assert!(report.rules.is_none());
        assert_eq!(report.candidate_count, 0);
        assert!(report.blockers.iter().any(|blocker| {
            blocker.contains("automation rules are not ready")
                && blocker.contains("automation rules file is missing")
        }));
        let doctor = store::automation::inspect_automation(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
        )
        .unwrap()
        .unwrap();
        assert_eq!(doctor.run_count, 0);
    }

    #[tokio::test]
    async fn rollout_blocks_trash_rules_for_first_wave() {
        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, None);
        store::init(&config_report).unwrap();
        seed_account(&config_report);
        write_rules(
            &config_report,
            r#"
[[rules]]
id = "trash-digest"
priority = 100

[rules.match]
from_address = "digest@example.com"

[rules.action]
kind = "trash"
"#,
        );

        let report = rollout(
            &config_report,
            AutomationRolloutRequest {
                rule_ids: Vec::new(),
                limit: 10,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.selected_rule_ids, vec![String::from("trash-digest")]);
        assert_eq!(report.blocked_rule_ids, vec![String::from("trash-digest")]);
        assert_eq!(report.candidate_count, 0);
        assert!(report.blockers.iter().any(|blocker| {
            blocker.contains("first-wave automation rollout excludes trash rules")
        }));
        let doctor = store::automation::inspect_automation(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
        )
        .unwrap()
        .unwrap();
        assert_eq!(doctor.run_count, 0);
    }

    #[tokio::test]
    async fn rollout_previews_candidates_without_persisting_run() {
        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, None);
        seed_local_thread_snapshot(&config_report, "thread-1", "m-1", 100, "Daily digest");
        write_rules(
            &config_report,
            r#"
[[rules]]
id = "archive-digest"
priority = 100

[rules.match]
from_address = "alice@example.com"
label_any = ["INBOX"]

[rules.action]
kind = "archive"
"#,
        );

        let report = rollout(
            &config_report,
            AutomationRolloutRequest {
                rule_ids: Vec::new(),
                limit: 10,
            },
        )
        .await
        .unwrap();

        assert!(report.blockers.is_empty());
        assert_eq!(
            report.selected_rule_ids,
            vec![String::from("archive-digest")]
        );
        assert_eq!(report.candidate_count, 1);
        assert_eq!(report.candidates[0].thread_id, "thread-1");
        assert_eq!(report.candidates[0].action_kind, "archive");
        let doctor = store::automation::inspect_automation(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
        )
        .unwrap()
        .unwrap();
        assert_eq!(doctor.run_count, 0);
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
    async fn apply_run_rejects_authenticated_account_mismatch_before_recording_apply_events() {
        let mock_server = MockServer::start().await;
        mount_profile_for_email(&mock_server, "Other.Operator@Example.com").await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, Some(&mock_server));
        store::init(&config_report).unwrap();
        seed_credentials_for_email(&config_report, "other.operator@example.com");
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
            format!(
                "automation run {} was previewed for {}, but the authenticated Gmail account is gmail:other.operator@example.com; re-run preview under the authenticated account",
                detail.run.run_id, account.account_id
            )
        );

        let refreshed_run = store::automation::get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            detail.run.run_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(refreshed_run.run.status, AutomationRunStatus::Previewed);
        assert_eq!(refreshed_run.candidates[0].apply_status, None);
        assert_eq!(refreshed_run.events.len(), 1);

        let requests = mock_server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method.as_str(), "GET");
        assert_eq!(requests[0].url.path(), "/gmail/v1/users/me/profile");
    }

    #[tokio::test]
    async fn apply_run_resumes_work_when_run_is_already_applying() {
        let mock_server = MockServer::start().await;
        mount_profile_for_email(&mock_server, "operator@example.com").await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "710"
            })))
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, Some(&mock_server));
        store::init(&config_report).unwrap();
        seed_credentials(&config_report);
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
        store::automation::finalize_automation_run(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &FinalizeAutomationRunInput {
                run_id: detail.run.run_id,
                status: AutomationRunStatus::Applying,
                applied_at_epoch_s: 101,
            },
        )
        .unwrap();

        let report = apply_run(&config_report, detail.run.run_id, true)
            .await
            .unwrap();

        assert_eq!(report.detail.run.status, AutomationRunStatus::Applied);
        assert_eq!(report.applied_candidate_count, 1);
        assert_eq!(report.failed_candidate_count, 0);

        let requests = mock_server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.method.as_str() == "POST"
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
        }));

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
    }

    #[tokio::test]
    async fn apply_run_allows_rerunning_failed_runs() {
        let mock_server = MockServer::start().await;
        mount_profile_for_email(&mock_server, "operator@example.com").await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(500).set_body_string("temporary failure"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let temp_dir = TempDir::new().unwrap();
        let config_report = config_report_for(&temp_dir, Some(&mock_server));
        store::init(&config_report).unwrap();
        seed_credentials(&config_report);
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

        let first_report = apply_run(&config_report, detail.run.run_id, true)
            .await
            .unwrap();
        assert_eq!(
            first_report.detail.run.status,
            AutomationRunStatus::ApplyFailed
        );
        assert_eq!(first_report.applied_candidate_count, 0);
        assert_eq!(first_report.failed_candidate_count, 1);

        let after_first_run = store::automation::get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            detail.run.run_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(after_first_run.run.status, AutomationRunStatus::ApplyFailed);
        assert_eq!(
            after_first_run.candidates[0].apply_status,
            Some(AutomationApplyStatus::Failed)
        );

        mock_server.reset().await;
        mount_profile_for_email(&mock_server, "operator@example.com").await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "thread-1",
                "historyId": "711"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let second_report = apply_run(&config_report, detail.run.run_id, true)
            .await
            .unwrap();
        assert_eq!(
            second_report.detail.run.status,
            AutomationRunStatus::Applied
        );
        assert_eq!(second_report.applied_candidate_count, 1);
        assert_eq!(second_report.failed_candidate_count, 0);

        let after_second_run = store::automation::get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            detail.run.run_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(after_second_run.run.status, AutomationRunStatus::Applied);
        assert_eq!(
            after_second_run.candidates[0].apply_status,
            Some(AutomationApplyStatus::Succeeded)
        );
    }

    #[tokio::test]
    async fn apply_run_returns_conflict_when_apply_lock_is_held() {
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
        let _lock = acquire_apply_run_lock_blocking(
            automation_apply_lock_path(&config_report),
            detail.run.run_id,
        )
        .unwrap();

        let error = apply_run(&config_report, detail.run.run_id, true)
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<AutomationServiceError>(),
            Some(AutomationServiceError::ApplyAlreadyInProgress { run_id })
                if *run_id == detail.run.run_id
        ));

        let refreshed_run = store::automation::get_automation_run_detail(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            detail.run.run_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(refreshed_run.run.status, AutomationRunStatus::Previewed);
        assert_eq!(refreshed_run.candidates[0].apply_status, None);
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

    #[tokio::test]
    async fn apply_run_reuses_workflow_cleanup_for_tracked_label_actions() {
        let mock_server = MockServer::start().await;
        mount_profile(&mock_server).await;
        Mock::given(method("DELETE"))
            .and(path("/gmail/v1/users/me/drafts/draft-1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&mock_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/gmail/v1/users/me/threads/thread-1/modify"))
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
        seed_mailbox_label(
            &config_report,
            &account.account_id,
            "Label_review",
            "Review",
        );
        seed_tracked_drafting_workflow(&config_report, &account.account_id, "thread-1");
        let detail = store::automation::create_automation_run(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &CreateAutomationRunInput {
                account_id: account.account_id.clone(),
                rule_file_path: String::from(".mailroom/automation.toml"),
                rule_file_hash: String::from("hash"),
                selected_rule_ids: vec![String::from("label-thread")],
                created_at_epoch_s: 100,
                candidates: vec![NewAutomationRunCandidate {
                    rule_id: String::from("label-thread"),
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
                        kind: AutomationActionKind::Label,
                        add_label_ids: vec![String::from("Label_review")],
                        add_label_names: vec![String::from("Review")],
                        remove_label_ids: vec![String::from("INBOX")],
                        remove_label_names: vec![String::from("INBOX")],
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
            Some(CleanupAction::Label)
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
                && request.url.path() == "/gmail/v1/users/me/threads/thread-1/modify"
                && request.body == br#"{"addLabelIds":["Label_review"],"removeLabelIds":["INBOX"]}"#
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

    fn write_rules(config_report: &ConfigReport, contents: &str) {
        fs::write(
            config_report
                .config
                .workspace
                .runtime_root
                .join("automation.toml"),
            contents,
        )
        .unwrap();
    }

    fn seed_credentials(config_report: &ConfigReport) {
        seed_credentials_for_email(config_report, "operator@example.com");
    }

    fn seed_credentials_for_email(config_report: &ConfigReport, email_address: &str) {
        let credential_store = FileCredentialStore::new(
            config_report
                .config
                .gmail
                .credential_path(&config_report.config.workspace),
        );
        credential_store
            .save(&StoredCredentials {
                account_id: gmail_account_id_for_email(email_address),
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

    fn seed_mailbox_label(
        config_report: &ConfigReport,
        account_id: &str,
        label_id: &str,
        name: &str,
    ) {
        store::mailbox::replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            account_id,
            &[
                GmailLabel {
                    id: String::from("INBOX"),
                    name: String::from("INBOX"),
                    message_list_visibility: None,
                    label_list_visibility: None,
                    label_type: String::from("system"),
                    messages_total: None,
                    messages_unread: None,
                    threads_total: None,
                    threads_unread: None,
                },
                GmailLabel {
                    id: String::from(label_id),
                    name: String::from(name),
                    message_list_visibility: None,
                    label_list_visibility: None,
                    label_type: String::from("user"),
                    messages_total: None,
                    messages_unread: None,
                    threads_total: None,
                    threads_unread: None,
                },
            ],
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
        mount_profile_for_email(mock_server, "operator@example.com").await;
    }

    async fn mount_profile_for_email(mock_server: &MockServer, email_address: &str) {
        Mock::given(method("GET"))
            .and(path("/gmail/v1/users/me/profile"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "emailAddress": email_address,
                "messagesTotal": 1,
                "threadsTotal": 1,
                "historyId": "12345"
            })))
            .mount(mock_server)
            .await;
    }
}
