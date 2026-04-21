use super::{
    SyncMode, SyncRunComparability, SyncRunComparabilityKind, SyncRunHistoryRecord,
    SyncRunOutcomeInput, SyncRunRegressionKind, SyncStatus,
};

pub(crate) const SYNC_RUN_HISTORY_RETENTION_PER_ACCOUNT: i64 = 1_000;
pub(crate) const SYNC_RUN_SUMMARY_RECENT_WINDOW: usize = 10;
pub(crate) const SYNC_RUN_REGRESSION_SUCCESS_WINDOW: usize = 5;
pub(crate) const SYNC_RUN_RETRY_BASELINE_WINDOW: usize = 3;
pub(crate) const SYNC_RUN_REGRESSION_MIN_MESSAGES: i64 = 100;
pub(crate) const SYNC_RUN_THROUGHPUT_DROP_SEVERE_RATIO: f64 = 0.7;
pub(crate) const SYNC_RUN_THROUGHPUT_DROP_WEAK_RATIO: f64 = 0.85;
pub(crate) const SYNC_RUN_DURATION_SPIKE_SEVERE_RATIO: f64 = 1.5;
pub(crate) const SYNC_RUN_DURATION_SPIKE_WEAK_RATIO: f64 = 1.25;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DetectedSyncRunRegression {
    pub(crate) kind: SyncRunRegressionKind,
    pub(crate) run_id: i64,
    pub(crate) message: String,
}

pub(crate) fn sync_run_comparability_label(kind: SyncRunComparabilityKind, key: &str) -> String {
    match kind {
        SyncRunComparabilityKind::FullRecentDays => format!("full recent_days={key}"),
        SyncRunComparabilityKind::FullQuery => format!("full query={key}"),
        SyncRunComparabilityKind::IncrementalWorkloadTier => {
            format!("incremental workload={key}")
        }
    }
}

pub(crate) fn comparability_for_full_bootstrap_query(
    bootstrap_query: &str,
) -> SyncRunComparability {
    if let Some(days) = extract_recent_days(bootstrap_query) {
        let key = days.to_string();
        return SyncRunComparability {
            kind: SyncRunComparabilityKind::FullRecentDays,
            label: sync_run_comparability_label(SyncRunComparabilityKind::FullRecentDays, &key),
            key,
        };
    }

    SyncRunComparability {
        kind: SyncRunComparabilityKind::FullQuery,
        key: bootstrap_query.to_owned(),
        label: sync_run_comparability_label(SyncRunComparabilityKind::FullQuery, bootstrap_query),
    }
}

pub(crate) fn comparability_for_incremental_workload(
    messages_listed: i64,
    messages_deleted: i64,
) -> SyncRunComparability {
    let total_work = messages_listed.saturating_add(messages_deleted);
    let key = match total_work {
        0 => "zero_work",
        1..=24 => "tiny",
        25..=99 => "small",
        100..=499 => "medium",
        _ => "large",
    };
    SyncRunComparability {
        kind: SyncRunComparabilityKind::IncrementalWorkloadTier,
        key: key.to_owned(),
        label: sync_run_comparability_label(SyncRunComparabilityKind::IncrementalWorkloadTier, key),
    }
}

pub(crate) fn comparability_for_outcome(outcome: &SyncRunOutcomeInput) -> SyncRunComparability {
    match outcome.sync_mode {
        SyncMode::Full => comparability_for_full_bootstrap_query(&outcome.bootstrap_query),
        SyncMode::Incremental => comparability_for_incremental_workload(
            outcome.messages_listed,
            outcome.messages_deleted,
        ),
    }
}

pub(crate) fn is_clean_success(row: &SyncRunHistoryRecord) -> bool {
    row.status == SyncStatus::Ok
        && row.messages_listed > 0
        && row.retry_count == 0
        && row.quota_pressure_retry_count == 0
        && row.concurrency_pressure_retry_count == 0
        && row.backend_retry_count == 0
}

pub(crate) fn compare_best_clean_run(
    left: &SyncRunHistoryRecord,
    right: &SyncRunHistoryRecord,
) -> std::cmp::Ordering {
    left.messages_per_second
        .partial_cmp(&right.messages_per_second)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| {
            right
                .estimated_quota_units_reserved
                .cmp(&left.estimated_quota_units_reserved)
        })
        .then_with(|| {
            right
                .effective_message_fetch_concurrency
                .cmp(&left.effective_message_fetch_concurrency)
        })
        .then_with(|| left.run_id.cmp(&right.run_id))
}

pub(crate) fn detect_sync_run_regression(
    history: &[SyncRunHistoryRecord],
) -> Option<DetectedSyncRunRegression> {
    let latest = history.first()?;
    if latest.status == SyncStatus::Failed {
        let failure_streak = history
            .iter()
            .take_while(|row| row.status == SyncStatus::Failed)
            .count();
        if failure_streak >= 2 {
            return Some(DetectedSyncRunRegression {
                kind: SyncRunRegressionKind::FailureStreak,
                run_id: latest.run_id,
                message: format!(
                    "{} consecutive {} sync failures",
                    failure_streak, latest.sync_mode
                ),
            });
        }
        return None;
    }

    if (latest.quota_pressure_retry_count > 0 || latest.concurrency_pressure_retry_count > 0)
        && history
            .iter()
            .skip(1)
            .filter(|row| row.status == SyncStatus::Ok)
            .take(SYNC_RUN_RETRY_BASELINE_WINDOW)
            .all(|row| {
                row.quota_pressure_retry_count == 0 && row.concurrency_pressure_retry_count == 0
            })
    {
        return Some(DetectedSyncRunRegression {
            kind: SyncRunRegressionKind::RetryPressure,
            run_id: latest.run_id,
            message: format!(
                "retry pressure appeared after {} clean successful {} runs",
                SYNC_RUN_RETRY_BASELINE_WINDOW, latest.sync_mode
            ),
        });
    }

    if latest.messages_listed < SYNC_RUN_REGRESSION_MIN_MESSAGES {
        return None;
    }

    let throughput_baseline = baseline_rows(history, 0);
    if throughput_baseline.len() < SYNC_RUN_REGRESSION_SUCCESS_WINDOW {
        return None;
    }

    let avg_messages_per_second = throughput_baseline
        .iter()
        .map(|row| row.messages_per_second)
        .sum::<f64>()
        / throughput_baseline.len() as f64;
    if avg_messages_per_second > 0.0 {
        let throughput_ratio = latest.messages_per_second / avg_messages_per_second;
        if throughput_ratio < SYNC_RUN_THROUGHPUT_DROP_SEVERE_RATIO
            || (throughput_ratio < SYNC_RUN_THROUGHPUT_DROP_WEAK_RATIO
                && prior_weak_throughput_drop(history))
        {
            return Some(DetectedSyncRunRegression {
                kind: SyncRunRegressionKind::ThroughputDrop,
                run_id: latest.run_id,
                message: format!(
                    "messages_per_second dropped from {:.3} baseline to {:.3}",
                    avg_messages_per_second, latest.messages_per_second
                ),
            });
        }
    }

    let avg_duration_ms = throughput_baseline
        .iter()
        .map(|row| row.duration_ms as f64)
        .sum::<f64>()
        / throughput_baseline.len() as f64;
    let duration_ratio = latest.duration_ms as f64 / avg_duration_ms;
    if duration_ratio > SYNC_RUN_DURATION_SPIKE_SEVERE_RATIO
        || (duration_ratio > SYNC_RUN_DURATION_SPIKE_WEAK_RATIO
            && prior_weak_duration_spike(history))
    {
        return Some(DetectedSyncRunRegression {
            kind: SyncRunRegressionKind::DurationSpike,
            run_id: latest.run_id,
            message: format!(
                "duration_ms rose from {:.0} baseline to {}",
                avg_duration_ms, latest.duration_ms
            ),
        });
    }

    None
}

fn prior_weak_throughput_drop(history: &[SyncRunHistoryRecord]) -> bool {
    weak_comparable_performance(history, 1, |latest, baseline| {
        let avg_messages_per_second = baseline
            .iter()
            .map(|row| row.messages_per_second)
            .sum::<f64>()
            / baseline.len() as f64;
        avg_messages_per_second > 0.0
            && latest.messages_per_second / avg_messages_per_second
                < SYNC_RUN_THROUGHPUT_DROP_WEAK_RATIO
    })
}

fn prior_weak_duration_spike(history: &[SyncRunHistoryRecord]) -> bool {
    weak_comparable_performance(history, 1, |latest, baseline| {
        let avg_duration_ms = baseline
            .iter()
            .map(|row| row.duration_ms as f64)
            .sum::<f64>()
            / baseline.len() as f64;
        avg_duration_ms > 0.0
            && latest.duration_ms as f64 / avg_duration_ms > SYNC_RUN_DURATION_SPIKE_WEAK_RATIO
    })
}

fn weak_comparable_performance<F>(
    history: &[SyncRunHistoryRecord],
    latest_index: usize,
    predicate: F,
) -> bool
where
    F: Fn(&SyncRunHistoryRecord, &[&SyncRunHistoryRecord]) -> bool,
{
    let Some(latest) = history.get(latest_index) else {
        return false;
    };
    if latest.status != SyncStatus::Ok || latest.messages_listed < SYNC_RUN_REGRESSION_MIN_MESSAGES
    {
        return false;
    }
    let baseline = baseline_rows(history, latest_index);
    baseline.len() >= SYNC_RUN_REGRESSION_SUCCESS_WINDOW && predicate(latest, &baseline)
}

fn baseline_rows(
    history: &[SyncRunHistoryRecord],
    latest_index: usize,
) -> Vec<&SyncRunHistoryRecord> {
    history
        .iter()
        .skip(latest_index + 1)
        .filter(|row| {
            row.status == SyncStatus::Ok && row.messages_listed >= SYNC_RUN_REGRESSION_MIN_MESSAGES
        })
        .take(SYNC_RUN_REGRESSION_SUCCESS_WINDOW)
        .collect()
}

fn extract_recent_days(bootstrap_query: &str) -> Option<u32> {
    let needle = "newer_than:";
    let start = bootstrap_query.find(needle)?;
    let suffix = &bootstrap_query[start + needle.len()..];
    let digits = suffix
        .chars()
        .take_while(|char| char.is_ascii_digit())
        .collect::<String>();
    let unit = suffix.chars().nth(digits.len())?;
    (unit == 'd' && !digits.is_empty())
        .then(|| digits.parse().ok())
        .flatten()
}
