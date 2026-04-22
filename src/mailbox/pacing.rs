use crate::gmail::{
    GmailClient, GmailQuotaMetricsSnapshot, MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE,
};
use crate::mailbox::{DEFAULT_MESSAGE_FETCH_CONCURRENCY, DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE};
use crate::store;
use anyhow::Result;

const DEFAULT_ADAPTIVE_QUOTA_FLOOR_UNITS_PER_MINUTE: u32 = 3_000;
const QUOTA_UPSHIFT_STEP_UNITS_PER_MINUTE: u32 = 500;
const CLEAN_STREAK_FOR_QUOTA_UPSHIFT: i64 = 2;
const CLEAN_STREAK_FOR_CONCURRENCY_UPSHIFT: i64 = 3;
const MAX_LEARNED_QUOTA_UNITS_PER_MINUTE: u32 = 12_000;
const MAX_LEARNED_MESSAGE_FETCH_CONCURRENCY: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NormalizedPersistedPacingState {
    learned_quota_units_per_minute: u32,
    learned_message_fetch_concurrency: usize,
    clean_run_streak: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RetryDelta {
    quota_pressure_retry_count: u64,
    concurrency_pressure_retry_count: u64,
    backend_retry_count: u64,
}

impl RetryDelta {
    fn from_snapshots(
        previous: Option<GmailQuotaMetricsSnapshot>,
        current: GmailQuotaMetricsSnapshot,
    ) -> Self {
        let previous = previous.unwrap_or(GmailQuotaMetricsSnapshot {
            reserved_units: 0,
            http_attempts: 0,
            retry_count: 0,
            quota_pressure_retry_count: 0,
            concurrency_pressure_retry_count: 0,
            backend_retry_count: 0,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
        });

        Self {
            quota_pressure_retry_count: current
                .quota_pressure_retry_count
                .saturating_sub(previous.quota_pressure_retry_count),
            concurrency_pressure_retry_count: current
                .concurrency_pressure_retry_count
                .saturating_sub(previous.concurrency_pressure_retry_count),
            backend_retry_count: current
                .backend_retry_count
                .saturating_sub(previous.backend_retry_count),
        }
    }

    fn has_quota_pressure(self) -> bool {
        self.quota_pressure_retry_count > 0
    }

    fn has_concurrency_pressure(self) -> bool {
        self.concurrency_pressure_retry_count > 0
    }

    fn has_backend_retry(self) -> bool {
        self.backend_retry_count > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdaptiveSyncPacingSeed {
    pub(crate) run_id: i64,
    pub(crate) quota_units_per_minute: u32,
    pub(crate) message_fetch_concurrency: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct AdaptiveSyncPacing {
    adaptive_pacing_enabled: bool,
    quota_units_cap_per_minute: u32,
    message_fetch_concurrency_cap: usize,
    starting_quota_units_per_minute: u32,
    starting_message_fetch_concurrency: usize,
    effective_quota_units_per_minute: u32,
    effective_message_fetch_concurrency: usize,
    adaptive_downshift_count: u64,
    persisted_learned_quota_units_per_minute: u32,
    persisted_learned_message_fetch_concurrency: usize,
    persisted_clean_run_streak: i64,
    startup_seed_run_id: Option<i64>,
    saw_quota_pressure: bool,
    saw_concurrency_pressure: bool,
    saw_backend_retry: bool,
    last_snapshot: Option<GmailQuotaMetricsSnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AdaptiveSyncPacingReport {
    pub(crate) adaptive_pacing_enabled: bool,
    pub(crate) quota_units_cap_per_minute: u32,
    pub(crate) message_fetch_concurrency_cap: usize,
    pub(crate) starting_quota_units_per_minute: u32,
    pub(crate) starting_message_fetch_concurrency: usize,
    pub(crate) effective_quota_units_per_minute: u32,
    pub(crate) effective_message_fetch_concurrency: usize,
    pub(crate) adaptive_downshift_count: u64,
}

impl AdaptiveSyncPacing {
    pub(crate) fn new(
        persisted: Option<&store::mailbox::SyncPacingStateRecord>,
        quota_units_cap_per_minute: u32,
        message_fetch_concurrency_cap: usize,
    ) -> Self {
        let normalized = normalize_persisted_pacing_state(persisted);
        let persisted_learned_quota_units_per_minute = normalized.learned_quota_units_per_minute;
        let persisted_learned_message_fetch_concurrency =
            normalized.learned_message_fetch_concurrency;
        let starting_quota_units_per_minute = persisted_learned_quota_units_per_minute
            .min(quota_units_cap_per_minute)
            .max(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE);
        let starting_message_fetch_concurrency = persisted_learned_message_fetch_concurrency
            .min(message_fetch_concurrency_cap)
            .max(1);

        Self {
            adaptive_pacing_enabled: true,
            quota_units_cap_per_minute,
            message_fetch_concurrency_cap,
            starting_quota_units_per_minute,
            starting_message_fetch_concurrency,
            effective_quota_units_per_minute: starting_quota_units_per_minute,
            effective_message_fetch_concurrency: starting_message_fetch_concurrency,
            adaptive_downshift_count: 0,
            persisted_learned_quota_units_per_minute,
            persisted_learned_message_fetch_concurrency,
            persisted_clean_run_streak: normalized.clean_run_streak,
            startup_seed_run_id: None,
            saw_quota_pressure: false,
            saw_concurrency_pressure: false,
            saw_backend_retry: false,
            last_snapshot: None,
        }
    }

    pub(crate) const fn starting_quota_units_per_minute(&self) -> u32 {
        self.starting_quota_units_per_minute
    }

    pub(crate) const fn current_message_fetch_concurrency(&self) -> usize {
        self.effective_message_fetch_concurrency
    }

    pub(crate) const fn startup_seed_run_id(&self) -> Option<i64> {
        self.startup_seed_run_id
    }

    pub(crate) fn apply_startup_seed(
        &mut self,
        seed: AdaptiveSyncPacingSeed,
        gmail_client: Option<&GmailClient>,
    ) -> Result<bool> {
        if self.adaptive_downshift_count > 0
            || self.saw_quota_pressure
            || self.saw_concurrency_pressure
            || self.saw_backend_retry
        {
            return Ok(false);
        }

        let quota_units_per_minute = seed
            .quota_units_per_minute
            .min(self.quota_units_cap_per_minute)
            .max(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE);
        let message_fetch_concurrency = seed
            .message_fetch_concurrency
            .min(self.message_fetch_concurrency_cap)
            .max(1);

        if quota_units_per_minute == self.starting_quota_units_per_minute
            && message_fetch_concurrency == self.starting_message_fetch_concurrency
        {
            return Ok(false);
        }

        if let Some(gmail_client) = gmail_client {
            gmail_client.update_quota_budget(quota_units_per_minute)?;
        }
        self.starting_quota_units_per_minute = quota_units_per_minute;
        self.starting_message_fetch_concurrency = message_fetch_concurrency;
        self.effective_quota_units_per_minute = quota_units_per_minute;
        self.effective_message_fetch_concurrency = message_fetch_concurrency;
        self.startup_seed_run_id = Some(seed.run_id);

        Ok(true)
    }

    pub(crate) fn observe_metrics_snapshot(
        &mut self,
        snapshot: GmailQuotaMetricsSnapshot,
        gmail_client: Option<&GmailClient>,
    ) -> Result<()> {
        let delta = RetryDelta::from_snapshots(self.last_snapshot, snapshot);
        self.last_snapshot = Some(snapshot);

        if !self.adaptive_pacing_enabled {
            return Ok(());
        }

        self.saw_quota_pressure |= delta.has_quota_pressure();
        self.saw_concurrency_pressure |= delta.has_concurrency_pressure();
        self.saw_backend_retry |= delta.has_backend_retry();

        let mut downshifted = false;
        if delta.has_quota_pressure() {
            let new_quota = downshifted_quota_units_per_minute(
                self.effective_quota_units_per_minute,
                quota_floor_units_per_minute(self.quota_units_cap_per_minute),
            );
            if new_quota < self.effective_quota_units_per_minute {
                if let Some(gmail_client) = gmail_client {
                    gmail_client.update_quota_budget(new_quota)?;
                }
                self.effective_quota_units_per_minute = new_quota;
                downshifted = true;
            }
        }

        if delta.has_concurrency_pressure() && self.effective_message_fetch_concurrency > 1 {
            self.effective_message_fetch_concurrency -= 1;
            downshifted = true;
        }

        if downshifted {
            self.adaptive_downshift_count += 1;
        }

        Ok(())
    }

    pub(crate) fn finalize_success(
        &self,
        account_id: &str,
        updated_at_epoch_s: i64,
    ) -> store::mailbox::SyncPacingStateUpdate {
        self.finalize(account_id, updated_at_epoch_s, true)
    }

    pub(crate) fn finalize_failure(
        &self,
        account_id: &str,
        updated_at_epoch_s: i64,
    ) -> store::mailbox::SyncPacingStateUpdate {
        self.finalize(account_id, updated_at_epoch_s, false)
    }

    pub(crate) const fn report(&self) -> AdaptiveSyncPacingReport {
        AdaptiveSyncPacingReport {
            adaptive_pacing_enabled: self.adaptive_pacing_enabled,
            quota_units_cap_per_minute: self.quota_units_cap_per_minute,
            message_fetch_concurrency_cap: self.message_fetch_concurrency_cap,
            starting_quota_units_per_minute: self.starting_quota_units_per_minute,
            starting_message_fetch_concurrency: self.starting_message_fetch_concurrency,
            effective_quota_units_per_minute: self.effective_quota_units_per_minute,
            effective_message_fetch_concurrency: self.effective_message_fetch_concurrency,
            adaptive_downshift_count: self.adaptive_downshift_count,
        }
    }

    fn finalize(
        &self,
        account_id: &str,
        updated_at_epoch_s: i64,
        sync_succeeded: bool,
    ) -> store::mailbox::SyncPacingStateUpdate {
        let mut learned_quota_units_per_minute = self.persisted_learned_quota_units_per_minute;
        let mut learned_message_fetch_concurrency =
            self.persisted_learned_message_fetch_concurrency;
        let clean_run_streak;

        if !sync_succeeded || self.saw_quota_pressure || self.saw_concurrency_pressure {
            clean_run_streak = 0;
            if self.saw_quota_pressure {
                learned_quota_units_per_minute = self.effective_quota_units_per_minute;
            }
            if self.saw_concurrency_pressure {
                learned_message_fetch_concurrency = self.effective_message_fetch_concurrency;
            }
        } else if self.saw_backend_retry {
            clean_run_streak = 0;
        } else {
            clean_run_streak = self.persisted_clean_run_streak.saturating_add(1);
            if clean_run_streak >= CLEAN_STREAK_FOR_QUOTA_UPSHIFT
                && learned_quota_units_per_minute < MAX_LEARNED_QUOTA_UNITS_PER_MINUTE
            {
                learned_quota_units_per_minute = (learned_quota_units_per_minute
                    + QUOTA_UPSHIFT_STEP_UNITS_PER_MINUTE)
                    .min(MAX_LEARNED_QUOTA_UNITS_PER_MINUTE);
            }
            if clean_run_streak >= CLEAN_STREAK_FOR_CONCURRENCY_UPSHIFT
                && learned_message_fetch_concurrency < MAX_LEARNED_MESSAGE_FETCH_CONCURRENCY
            {
                learned_message_fetch_concurrency += 1;
            }
        }

        store::mailbox::SyncPacingStateUpdate {
            account_id: account_id.to_owned(),
            learned_quota_units_per_minute: i64::from(learned_quota_units_per_minute),
            learned_message_fetch_concurrency: i64::try_from(learned_message_fetch_concurrency)
                .unwrap_or(i64::MAX),
            clean_run_streak,
            last_pressure_kind: pressure_kind(
                self.saw_quota_pressure,
                self.saw_concurrency_pressure,
            ),
            updated_at_epoch_s,
        }
    }
}

fn normalize_persisted_pacing_state(
    persisted: Option<&store::mailbox::SyncPacingStateRecord>,
) -> NormalizedPersistedPacingState {
    let Some(persisted) = persisted else {
        return NormalizedPersistedPacingState {
            learned_quota_units_per_minute: DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE,
            learned_message_fetch_concurrency: DEFAULT_MESSAGE_FETCH_CONCURRENCY,
            clean_run_streak: 0,
        };
    };

    NormalizedPersistedPacingState {
        learned_quota_units_per_minute: normalize_learned_quota_units_per_minute(
            persisted.learned_quota_units_per_minute,
        ),
        learned_message_fetch_concurrency: normalize_learned_message_fetch_concurrency(
            persisted.learned_message_fetch_concurrency,
        ),
        clean_run_streak: persisted.clean_run_streak.max(0),
    }
}

fn normalize_learned_quota_units_per_minute(raw: i64) -> u32 {
    let clamped = raw.clamp(
        i64::from(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE),
        i64::from(MAX_LEARNED_QUOTA_UNITS_PER_MINUTE),
    );
    u32::try_from(clamped).unwrap_or(MAX_LEARNED_QUOTA_UNITS_PER_MINUTE)
}

fn normalize_learned_message_fetch_concurrency(raw: i64) -> usize {
    let clamped = raw.clamp(
        1,
        i64::try_from(MAX_LEARNED_MESSAGE_FETCH_CONCURRENCY).unwrap_or(1),
    );
    usize::try_from(clamped).unwrap_or(MAX_LEARNED_MESSAGE_FETCH_CONCURRENCY)
}

fn pressure_kind(
    saw_quota_pressure: bool,
    saw_concurrency_pressure: bool,
) -> Option<store::mailbox::SyncPacingPressureKind> {
    match (saw_quota_pressure, saw_concurrency_pressure) {
        (true, true) => Some(store::mailbox::SyncPacingPressureKind::Mixed),
        (true, false) => Some(store::mailbox::SyncPacingPressureKind::Quota),
        (false, true) => Some(store::mailbox::SyncPacingPressureKind::Concurrency),
        (false, false) => None,
    }
}

fn quota_floor_units_per_minute(quota_units_cap_per_minute: u32) -> u32 {
    if quota_units_cap_per_minute < DEFAULT_ADAPTIVE_QUOTA_FLOOR_UNITS_PER_MINUTE {
        MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE
    } else {
        DEFAULT_ADAPTIVE_QUOTA_FLOOR_UNITS_PER_MINUTE
    }
}

fn downshifted_quota_units_per_minute(current: u32, floor: u32) -> u32 {
    let scaled = (u64::from(current) * 80) / 100;
    let rounded = ((scaled / 500) * 500).max(u64::from(floor));
    u32::try_from(rounded).unwrap_or(floor)
}

#[cfg(test)]
mod tests {
    use super::AdaptiveSyncPacing;
    use crate::gmail::GmailQuotaMetricsSnapshot;

    fn snapshot(
        quota_pressure_retry_count: u64,
        concurrency_pressure_retry_count: u64,
        backend_retry_count: u64,
    ) -> GmailQuotaMetricsSnapshot {
        GmailQuotaMetricsSnapshot {
            reserved_units: 0,
            http_attempts: 0,
            retry_count: quota_pressure_retry_count
                + concurrency_pressure_retry_count
                + backend_retry_count,
            quota_pressure_retry_count,
            concurrency_pressure_retry_count,
            backend_retry_count,
            throttle_wait_count: 0,
            throttle_wait_ms: 0,
            retry_after_wait_ms: 0,
        }
    }

    #[test]
    fn startup_clamps_persisted_state_to_caps() {
        let pacing = AdaptiveSyncPacing::new(None, 9_000, 3);

        assert_eq!(pacing.starting_quota_units_per_minute(), 9_000);
        assert_eq!(pacing.current_message_fetch_concurrency(), 3);
    }

    #[test]
    fn clean_success_upshifts_quota_after_two_runs() {
        let pacing_state = crate::store::mailbox::SyncPacingStateRecord {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 9_000,
            learned_message_fetch_concurrency: 3,
            clean_run_streak: 1,
            last_pressure_kind: None,
            updated_at_epoch_s: 100,
        };
        let pacing = AdaptiveSyncPacing::new(Some(&pacing_state), 12_000, 4);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(update.clean_run_streak, 2);
        assert_eq!(update.learned_quota_units_per_minute, 9_500);
        assert_eq!(update.learned_message_fetch_concurrency, 3);
    }

    #[test]
    fn quota_pressure_downshifts_effective_budget_and_persists_it() {
        let mut pacing = AdaptiveSyncPacing::new(None, 12_000, 4);
        pacing
            .observe_metrics_snapshot(snapshot(1, 0, 0), None)
            .unwrap();

        assert_eq!(pacing.report().effective_quota_units_per_minute, 9_500);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(update.clean_run_streak, 0);
        assert_eq!(update.learned_quota_units_per_minute, 9_500);
        assert_eq!(
            update.last_pressure_kind,
            Some(crate::store::mailbox::SyncPacingPressureKind::Quota)
        );
    }

    #[test]
    fn concurrency_pressure_reduces_effective_concurrency_and_persists_it() {
        let mut pacing = AdaptiveSyncPacing::new(None, 12_000, 4);
        pacing
            .observe_metrics_snapshot(snapshot(0, 1, 0), None)
            .unwrap();

        assert_eq!(pacing.report().effective_message_fetch_concurrency, 3);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(update.learned_message_fetch_concurrency, 3);
        assert_eq!(
            update.last_pressure_kind,
            Some(crate::store::mailbox::SyncPacingPressureKind::Concurrency)
        );
    }

    #[test]
    fn startup_normalizes_corrupt_persisted_state() {
        let pacing_state = crate::store::mailbox::SyncPacingStateRecord {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 0,
            learned_message_fetch_concurrency: 0,
            clean_run_streak: -3,
            last_pressure_kind: None,
            updated_at_epoch_s: 100,
        };
        let pacing = AdaptiveSyncPacing::new(Some(&pacing_state), 12_000, 4);

        assert_eq!(
            pacing.starting_quota_units_per_minute(),
            crate::gmail::MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE
        );
        assert_eq!(pacing.current_message_fetch_concurrency(), 1);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(
            update.learned_quota_units_per_minute,
            i64::from(crate::gmail::MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE)
        );
        assert_eq!(update.learned_message_fetch_concurrency, 1);
        assert_eq!(update.clean_run_streak, 1);
    }

    #[test]
    fn mixed_pressure_downshifts_quota_and_concurrency_in_one_window() {
        let mut pacing = AdaptiveSyncPacing::new(None, 12_000, 4);
        pacing
            .observe_metrics_snapshot(snapshot(1, 1, 0), None)
            .unwrap();

        let report = pacing.report();
        assert_eq!(report.effective_quota_units_per_minute, 9_500);
        assert_eq!(report.effective_message_fetch_concurrency, 3);
        assert_eq!(report.adaptive_downshift_count, 1);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(
            update.last_pressure_kind,
            Some(crate::store::mailbox::SyncPacingPressureKind::Mixed)
        );
    }

    #[test]
    fn backend_retries_reset_clean_streak_without_lowering_learned_state() {
        let pacing_state = crate::store::mailbox::SyncPacingStateRecord {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 9_500,
            learned_message_fetch_concurrency: 3,
            clean_run_streak: 2,
            last_pressure_kind: None,
            updated_at_epoch_s: 100,
        };
        let mut pacing = AdaptiveSyncPacing::new(Some(&pacing_state), 12_000, 4);
        pacing
            .observe_metrics_snapshot(snapshot(0, 0, 1), None)
            .unwrap();

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(update.clean_run_streak, 0);
        assert_eq!(update.learned_quota_units_per_minute, 9_500);
        assert_eq!(update.learned_message_fetch_concurrency, 3);
        assert!(update.last_pressure_kind.is_none());
    }

    #[test]
    fn lower_cli_caps_do_not_permanently_lower_learned_state_without_pressure() {
        let pacing_state = crate::store::mailbox::SyncPacingStateRecord {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: 12_000,
            learned_message_fetch_concurrency: 4,
            clean_run_streak: 1,
            last_pressure_kind: None,
            updated_at_epoch_s: 100,
        };
        let pacing = AdaptiveSyncPacing::new(Some(&pacing_state), 9_000, 3);

        assert_eq!(pacing.starting_quota_units_per_minute(), 9_000);
        assert_eq!(pacing.current_message_fetch_concurrency(), 3);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(update.clean_run_streak, 2);
        assert_eq!(update.learned_quota_units_per_minute, 12_000);
        assert_eq!(update.learned_message_fetch_concurrency, 4);
    }

    #[test]
    fn startup_normalization_uses_learned_state_maxima_instead_of_defaults() {
        let pacing_state = crate::store::mailbox::SyncPacingStateRecord {
            account_id: String::from("gmail:operator@example.com"),
            learned_quota_units_per_minute: i64::MAX,
            learned_message_fetch_concurrency: i64::MAX,
            clean_run_streak: 0,
            last_pressure_kind: None,
            updated_at_epoch_s: 100,
        };
        let pacing = AdaptiveSyncPacing::new(Some(&pacing_state), 12_000, 4);

        assert_eq!(pacing.starting_quota_units_per_minute(), 12_000);
        assert_eq!(pacing.current_message_fetch_concurrency(), 4);

        let update = pacing.finalize_success("gmail:operator@example.com", 200);
        assert_eq!(update.learned_quota_units_per_minute, 12_000);
        assert_eq!(update.learned_message_fetch_concurrency, 4);
    }
}
