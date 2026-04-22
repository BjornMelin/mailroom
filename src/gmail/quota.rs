use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Mailroom keeps quota pacing local to the Gmail client instead of depending on
// a generic limiter crate. The sync path needs Gmail-specific weighted request
// costs, live per-run reconfiguration, and integrated retry/throttle metrics
// that feed operator reports and adaptive pacing.
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
pub(crate) enum GmailRetryClassification {
    QuotaPressure,
    ConcurrencyPressure,
    Backend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GmailQuotaMetricsSnapshot {
    pub(crate) reserved_units: u64,
    pub(crate) http_attempts: u64,
    pub(crate) retry_count: u64,
    pub(crate) quota_pressure_retry_count: u64,
    pub(crate) concurrency_pressure_retry_count: u64,
    pub(crate) backend_retry_count: u64,
    pub(crate) throttle_wait_count: u64,
    pub(crate) throttle_wait_ms: u64,
    pub(crate) retry_after_wait_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct GmailQuotaPolicy {
    state: Arc<Mutex<GmailQuotaState>>,
    metrics: Arc<GmailQuotaMetrics>,
    units_per_minute: Arc<AtomicU32>,
}

impl GmailQuotaPolicy {
    pub(crate) fn new(units_per_minute: u32) -> Option<Self> {
        if units_per_minute < MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE {
            return None;
        }

        Some(Self {
            state: Arc::new(Mutex::new(GmailQuotaState::new(units_per_minute))),
            metrics: Arc::new(GmailQuotaMetrics::default()),
            units_per_minute: Arc::new(AtomicU32::new(units_per_minute)),
        })
    }

    pub(crate) fn units_per_minute(&self) -> u32 {
        self.units_per_minute.load(Ordering::Relaxed)
    }

    pub(crate) fn reconfigure(&self, units_per_minute: u32) -> Option<()> {
        if units_per_minute < MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE {
            return None;
        }

        {
            let mut state = self.state.lock().expect("gmail quota state poisoned");
            state.reconfigure(units_per_minute);
        }
        self.units_per_minute
            .store(units_per_minute, Ordering::Relaxed);
        Some(())
    }

    pub(crate) async fn acquire(&self, request_cost: GmailRequestCost) -> Result<(), u32> {
        let requested_units = request_cost.units();
        let started_at = Instant::now();

        loop {
            let wait_duration = {
                let mut state = self.state.lock().expect("gmail quota state poisoned");
                state.try_consume(requested_units)?
            };

            match wait_duration {
                None => {
                    self.metrics
                        .record_reserved_units(u64::from(requested_units));
                    return Ok(());
                }
                Some(wait_duration) => {
                    tokio::time::sleep(wait_duration).await;
                    self.metrics.record_throttle_wait(started_at.elapsed());
                }
            }
        }
    }

    pub(crate) fn record_http_attempt(&self) {
        self.metrics.record_http_attempt();
    }

    pub(crate) fn record_retry(&self, classification: GmailRetryClassification) {
        self.metrics.record_retry(classification);
    }

    pub(crate) fn record_retry_after_wait(&self, waited: Duration) {
        self.metrics.record_retry_after_wait(waited);
    }

    pub(crate) fn snapshot(&self) -> GmailQuotaMetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[derive(Debug)]
struct GmailQuotaState {
    available_units: u32,
    burst_units: u32,
    replenish_per_unit: Duration,
    last_refill_at: Instant,
}

impl GmailQuotaState {
    fn new(units_per_minute: u32) -> Self {
        let burst_units = burst_units_for(units_per_minute);
        Self {
            available_units: burst_units,
            burst_units,
            replenish_per_unit: replenish_period_for(units_per_minute),
            last_refill_at: Instant::now(),
        }
    }

    fn reconfigure(&mut self, units_per_minute: u32) {
        let now = Instant::now();
        self.refill(now);
        self.burst_units = burst_units_for(units_per_minute);
        self.available_units = self.available_units.min(self.burst_units);
        self.replenish_per_unit = replenish_period_for(units_per_minute);
        self.last_refill_at = now;
    }

    fn try_consume(&mut self, requested_units: u32) -> Result<Option<Duration>, u32> {
        if requested_units > self.burst_units {
            return Err(requested_units);
        }

        let now = Instant::now();
        self.refill(now);
        if self.available_units >= requested_units {
            self.available_units -= requested_units;
            return Ok(None);
        }

        let missing_units = requested_units - self.available_units;
        let remainder = now.saturating_duration_since(self.last_refill_at);
        let wait_for_first = if remainder.is_zero() {
            self.replenish_per_unit
        } else {
            self.replenish_per_unit.saturating_sub(remainder)
        };
        let additional_wait =
            duration_mul_u32(self.replenish_per_unit, missing_units.saturating_sub(1));
        Ok(Some(wait_for_first.saturating_add(additional_wait)))
    }

    fn refill(&mut self, now: Instant) {
        if self.available_units == self.burst_units {
            self.last_refill_at = now;
            return;
        }

        let elapsed = now.saturating_duration_since(self.last_refill_at);
        if elapsed < self.replenish_per_unit {
            return;
        }

        let added_units = (elapsed.as_nanos() / self.replenish_per_unit.as_nanos())
            .min(u128::from(u32::MAX)) as u32;
        if added_units == 0 {
            return;
        }

        self.available_units = self
            .available_units
            .saturating_add(added_units)
            .min(self.burst_units);
        self.last_refill_at = self
            .last_refill_at
            .checked_add(duration_mul_u32(self.replenish_per_unit, added_units))
            .unwrap_or(now);
    }
}

#[derive(Debug, Default)]
struct GmailQuotaMetrics {
    reserved_units: AtomicU64,
    http_attempts: AtomicU64,
    retry_count: AtomicU64,
    quota_pressure_retry_count: AtomicU64,
    concurrency_pressure_retry_count: AtomicU64,
    backend_retry_count: AtomicU64,
    throttle_wait_count: AtomicU64,
    throttle_wait_ms: AtomicU64,
    retry_after_wait_ms: AtomicU64,
}

impl GmailQuotaMetrics {
    fn record_reserved_units(&self, units: u64) {
        self.reserved_units.fetch_add(units, Ordering::Relaxed);
    }

    fn record_http_attempt(&self) {
        self.http_attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_retry(&self, classification: GmailRetryClassification) {
        self.retry_count.fetch_add(1, Ordering::Relaxed);
        match classification {
            GmailRetryClassification::QuotaPressure => {
                self.quota_pressure_retry_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            GmailRetryClassification::ConcurrencyPressure => {
                self.concurrency_pressure_retry_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            GmailRetryClassification::Backend => {
                self.backend_retry_count.fetch_add(1, Ordering::Relaxed);
            }
        }
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

    fn record_retry_after_wait(&self, waited: Duration) {
        if waited.is_zero() {
            return;
        }

        self.retry_after_wait_ms.fetch_add(
            waited.as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> GmailQuotaMetricsSnapshot {
        GmailQuotaMetricsSnapshot {
            reserved_units: self.reserved_units.load(Ordering::Relaxed),
            http_attempts: self.http_attempts.load(Ordering::Relaxed),
            retry_count: self.retry_count.load(Ordering::Relaxed),
            quota_pressure_retry_count: self.quota_pressure_retry_count.load(Ordering::Relaxed),
            concurrency_pressure_retry_count: self
                .concurrency_pressure_retry_count
                .load(Ordering::Relaxed),
            backend_retry_count: self.backend_retry_count.load(Ordering::Relaxed),
            throttle_wait_count: self.throttle_wait_count.load(Ordering::Relaxed),
            throttle_wait_ms: self.throttle_wait_ms.load(Ordering::Relaxed),
            retry_after_wait_ms: self.retry_after_wait_ms.load(Ordering::Relaxed),
        }
    }
}

fn burst_units_for(units_per_minute: u32) -> u32 {
    units_per_minute.min(DEFAULT_QUOTA_BURST_UNITS)
}

fn replenish_period_for(units_per_minute: u32) -> Duration {
    Duration::from_nanos(
        (60 * 1_000_000_000u64)
            .checked_div(u64::from(units_per_minute))
            .unwrap_or(1),
    )
}

fn duration_mul_u32(duration: Duration, multiplier: u32) -> Duration {
    if multiplier == 0 {
        return Duration::ZERO;
    }

    let nanos = duration
        .as_nanos()
        .saturating_mul(u128::from(multiplier))
        .min(u128::from(u64::MAX));
    Duration::from_nanos(nanos as u64)
}

#[cfg(test)]
mod tests {
    use super::{
        GmailQuotaPolicy, GmailRequestCost, GmailRetryClassification,
        MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE,
    };
    use std::time::Duration;

    #[test]
    fn rejects_quota_below_single_read_request_cost() {
        assert!(GmailQuotaPolicy::new(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE - 1).is_none());
    }

    #[tokio::test]
    async fn records_reserved_units_attempts_and_retry_breakdown() {
        let policy = GmailQuotaPolicy::new(120).unwrap();

        policy.acquire(GmailRequestCost::MessageGet).await.unwrap();
        policy.record_http_attempt();
        policy.record_retry(GmailRetryClassification::QuotaPressure);
        policy.record_retry(GmailRetryClassification::ConcurrencyPressure);
        policy.record_retry(GmailRetryClassification::Backend);
        policy.record_retry_after_wait(Duration::from_secs(2));

        let snapshot = policy.snapshot();
        assert_eq!(snapshot.reserved_units, 5);
        assert_eq!(snapshot.http_attempts, 1);
        assert_eq!(snapshot.retry_count, 3);
        assert_eq!(snapshot.quota_pressure_retry_count, 1);
        assert_eq!(snapshot.concurrency_pressure_retry_count, 1);
        assert_eq!(snapshot.backend_retry_count, 1);
        assert_eq!(snapshot.retry_after_wait_ms, 2_000);
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

    #[tokio::test]
    async fn reconfigure_reduces_immediate_capacity() {
        let policy = GmailQuotaPolicy::new(60).unwrap();

        for _ in 0..5 {
            policy.acquire(GmailRequestCost::MessageGet).await.unwrap();
        }
        policy.reconfigure(MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE);

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            policy.acquire(GmailRequestCost::MessageGet),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            policy.units_per_minute(),
            MIN_READ_REQUEST_QUOTA_UNITS_PER_MINUTE
        );
    }
}
