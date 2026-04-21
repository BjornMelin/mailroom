use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub(crate) const PIPELINE_LIST_QUEUE_CAPACITY: usize = 2;
pub(crate) const PIPELINE_PAGE_PROCESSING_CONCURRENCY: usize = 2;
pub(crate) const PIPELINE_WRITE_QUEUE_CAPACITY: usize = 2;
pub(crate) const PIPELINE_WRITE_BATCH_MESSAGE_TARGET: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ListedPage {
    pub(crate) page_seq: usize,
    pub(crate) message_ids: Vec<String>,
    pub(crate) next_page_token: Option<String>,
    pub(crate) listed_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PipelineStatsReport {
    pub(crate) pipeline_enabled: bool,
    pub(crate) list_queue_high_water: usize,
    pub(crate) write_queue_high_water: usize,
    pub(crate) write_batch_count: usize,
    pub(crate) writer_wait_ms: u64,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PipelineStats {
    inner: Arc<PipelineStatsInner>,
}

#[derive(Debug, Default)]
struct PipelineStatsInner {
    list_queue_depth: AtomicUsize,
    list_queue_high_water: AtomicUsize,
    write_queue_depth: AtomicUsize,
    write_queue_high_water: AtomicUsize,
    write_batch_count: AtomicUsize,
    writer_wait_ms: AtomicU64,
}

impl PipelineStats {
    pub(crate) fn on_list_enqueued(&self) {
        update_depth_and_high_water(
            &self.inner.list_queue_depth,
            &self.inner.list_queue_high_water,
            1,
        );
    }

    pub(crate) fn on_list_dequeued(&self) {
        self.inner.list_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn on_write_enqueued(&self) {
        update_depth_and_high_water(
            &self.inner.write_queue_depth,
            &self.inner.write_queue_high_water,
            1,
        );
    }

    pub(crate) fn on_write_dequeued(&self) {
        self.inner.write_queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn on_write_batch_committed(&self) {
        self.inner.write_batch_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_writer_wait(&self, waited: Duration) {
        let millis = waited.as_millis().min(u128::from(u64::MAX)) as u64;
        self.inner
            .writer_wait_ms
            .fetch_add(millis, Ordering::Relaxed);
    }

    pub(crate) fn report(&self) -> PipelineStatsReport {
        PipelineStatsReport {
            pipeline_enabled: true,
            list_queue_high_water: self.inner.list_queue_high_water.load(Ordering::Relaxed),
            write_queue_high_water: self.inner.write_queue_high_water.load(Ordering::Relaxed),
            write_batch_count: self.inner.write_batch_count.load(Ordering::Relaxed),
            writer_wait_ms: self.inner.writer_wait_ms.load(Ordering::Relaxed),
        }
    }
}

fn update_depth_and_high_water(depth: &AtomicUsize, high_water: &AtomicUsize, increment: usize) {
    let next_depth = depth.fetch_add(increment, Ordering::Relaxed) + increment;
    let mut observed_high_water = high_water.load(Ordering::Relaxed);
    while next_depth > observed_high_water {
        match high_water.compare_exchange(
            observed_high_water,
            next_depth,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(actual) => observed_high_water = actual,
        }
    }
}
