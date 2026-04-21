use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub(crate) const MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE: u32 = 5;
const DEFAULT_QUOTA_BURST_UNITS: u32 = 25;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GmailRequestCost {
    LabelRead,
    HistoryList,
    MessageList,
    MessageGet,
    AttachmentGet,
    ThreadGet,
    ProfileGet,
    DraftWrite,
    ThreadModify,
    DraftDelete,
}

impl GmailRequestCost {
    pub(crate) const fn units(self) -> u32 {
        match self {
            Self::LabelRead => 1,
            Self::HistoryList => 2,
            Self::MessageList => 5,
            Self::MessageGet => 5,
            Self::AttachmentGet => 5,
            Self::ThreadGet => 10,
            Self::ProfileGet => 1,
            Self::DraftWrite => 10,
            Self::ThreadModify => 10,
            Self::DraftDelete => 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GmailQuotaMetricsSnapshot {
    pub(crate) reserved_units: u64,
    pub(crate) http_attempts: u64,
    pub(crate) retry_count: u64,
    pub(crate) throttle_wait_count: u64,
    pub(crate) throttle_wait_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct GmailQuotaPolicy {
    limiter: Arc<DefaultDirectRateLimiter>,
    metrics: Arc<GmailQuotaMetrics>,
    units_per_minute: u32,
}

impl GmailQuotaPolicy {
    pub(crate) fn new(units_per_minute: u32) -> Option<Self> {
        if units_per_minute < MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE {
            return None;
        }

        let replenishment_period = Duration::from_nanos(
            (60 * 1_000_000_000u64)
                .checked_div(u64::from(units_per_minute))
                .unwrap_or(1),
        );
        // Keep the burst ceiling aligned with the configured budget so low-budget
        // operators do not get an implicit larger initial bucket.
        let burst_units = units_per_minute.min(DEFAULT_QUOTA_BURST_UNITS);
        let quota =
            Quota::with_period(replenishment_period)?.allow_burst(NonZeroU32::new(burst_units)?);

        Some(Self {
            limiter: Arc::new(RateLimiter::direct(quota)),
            metrics: Arc::new(GmailQuotaMetrics::default()),
            units_per_minute,
        })
    }

    pub(crate) fn units_per_minute(&self) -> u32 {
        self.units_per_minute
    }

    pub(crate) async fn acquire(&self, request_cost: GmailRequestCost) -> Result<(), u32> {
        let requested_units = request_cost.units();
        let cells = NonZeroU32::new(requested_units).expect("gmail request cost must be nonzero");
        let started_at = Instant::now();
        self.limiter
            .until_n_ready(cells)
            .await
            .map_err(|_| requested_units)?;
        let waited = started_at.elapsed();
        self.metrics
            .record_reserved_units(u64::from(requested_units));
        self.metrics.record_throttle_wait(waited);
        Ok(())
    }

    pub(crate) fn record_http_attempt(&self) {
        self.metrics.record_http_attempt();
    }

    pub(crate) fn record_retry(&self) {
        self.metrics.record_retry();
    }

    pub(crate) fn snapshot(&self) -> GmailQuotaMetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[derive(Debug, Default)]
struct GmailQuotaMetrics {
    reserved_units: AtomicU64,
    http_attempts: AtomicU64,
    retry_count: AtomicU64,
    throttle_wait_count: AtomicU64,
    throttle_wait_ms: AtomicU64,
}

impl GmailQuotaMetrics {
    fn record_reserved_units(&self, units: u64) {
        self.reserved_units.fetch_add(units, Ordering::Relaxed);
    }

    fn record_http_attempt(&self) {
        self.http_attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_retry(&self) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_throttle_wait(&self, waited: Duration) {
        if waited.is_zero() {
            return;
        }

        self.throttle_wait_count.fetch_add(1, Ordering::Relaxed);
        self.throttle_wait_ms.fetch_add(
            waited.as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> GmailQuotaMetricsSnapshot {
        GmailQuotaMetricsSnapshot {
            reserved_units: self.reserved_units.load(Ordering::Relaxed),
            http_attempts: self.http_attempts.load(Ordering::Relaxed),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            throttle_wait_count: self.throttle_wait_count.load(Ordering::Relaxed),
            throttle_wait_ms: self.throttle_wait_ms.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GmailQuotaPolicy, GmailRequestCost, MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE};
    use std::time::Duration;

    #[test]
    fn rejects_quota_below_single_read_request_cost() {
        assert!(GmailQuotaPolicy::new(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE - 1).is_none());
    }

    #[tokio::test]
    async fn records_reserved_units_and_attempts() {
        let policy = GmailQuotaPolicy::new(120).unwrap();

        policy.acquire(GmailRequestCost::MessageGet).await.unwrap();
        policy.record_http_attempt();

        let snapshot = policy.snapshot();
        assert_eq!(snapshot.reserved_units, 5);
        assert_eq!(snapshot.http_attempts, 1);
    }

    #[tokio::test]
    async fn throttles_when_capacity_is_exhausted() {
        let policy = GmailQuotaPolicy::new(60).unwrap();

        for _ in 0..5 {
            policy.acquire(GmailRequestCost::MessageGet).await.unwrap();
        }

        let result = tokio::time::timeout(
            Duration::from_millis(250),
            policy.acquire(GmailRequestCost::MessageGet),
        )
        .await;

        assert!(
            result.is_err(),
            "quota limiter should have delayed the next request"
        );
    }

    #[tokio::test]
    async fn low_budget_does_not_allow_a_second_message_read_immediately() {
        let policy = GmailQuotaPolicy::new(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE).unwrap();

        policy.acquire(GmailRequestCost::MessageGet).await.unwrap();

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            policy.acquire(GmailRequestCost::MessageGet),
        )
        .await;

        assert!(
            result.is_err(),
            "configured low-budget burst should not allow a second immediate message read"
        );
    }
}
