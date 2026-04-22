use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

pub(crate) const PIPELINE_LIST_QUEUE_CAPACITY: usize = 2;
pub(crate) const PIPELINE_PAGE_PROCESSING_MAX_CONCURRENCY: usize = 2;
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
    pub(crate) fetch_batch_count: usize,
    pub(crate) fetch_batch_avg_ms: u64,
    pub(crate) fetch_batch_max_ms: u64,
    pub(crate) writer_tx_count: usize,
    pub(crate) writer_tx_avg_ms: u64,
    pub(crate) writer_tx_max_ms: u64,
    pub(crate) reorder_buffer_high_water: usize,
    pub(crate) staged_message_count: usize,
    pub(crate) staged_delete_count: usize,
    pub(crate) staged_attachment_count: usize,
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
    fetch_batch_count: AtomicUsize,
    fetch_batch_total_ms: AtomicU64,
    fetch_batch_max_ms: AtomicU64,
    writer_tx_count: AtomicUsize,
    writer_tx_total_ms: AtomicU64,
    writer_tx_max_ms: AtomicU64,
    reorder_buffer_high_water: AtomicUsize,
    staged_message_count: AtomicUsize,
    staged_delete_count: AtomicUsize,
    staged_attachment_count: AtomicUsize,
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
        saturating_decrement(&self.inner.list_queue_depth);
    }

    pub(crate) fn on_write_enqueued(&self) {
        update_depth_and_high_water(
            &self.inner.write_queue_depth,
            &self.inner.write_queue_high_water,
            1,
        );
    }

    pub(crate) fn on_write_dequeued(&self) {
        saturating_decrement(&self.inner.write_queue_depth);
    }

    pub(crate) fn on_write_batch_committed(&self) {
        self.inner.write_batch_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_fetch_batch(&self, elapsed: Duration) {
        let millis = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        self.inner.fetch_batch_count.fetch_add(1, Ordering::Relaxed);
        self.inner
            .fetch_batch_total_ms
            .fetch_add(millis, Ordering::Relaxed);
        update_max(&self.inner.fetch_batch_max_ms, millis);
    }

    pub(crate) fn record_writer_wait(&self, waited: Duration) {
        let millis = waited.as_millis().min(u128::from(u64::MAX)) as u64;
        self.inner
            .writer_wait_ms
            .fetch_add(millis, Ordering::Relaxed);
    }

    pub(crate) fn record_writer_transaction(&self, elapsed: Duration) {
        let millis = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        self.inner.writer_tx_count.fetch_add(1, Ordering::Relaxed);
        self.inner
            .writer_tx_total_ms
            .fetch_add(millis, Ordering::Relaxed);
        update_max(&self.inner.writer_tx_max_ms, millis);
    }

    pub(crate) fn observe_reorder_buffer_depth(&self, depth: usize) {
        let mut observed_high_water = self.inner.reorder_buffer_high_water.load(Ordering::Relaxed);
        while depth > observed_high_water {
            match self.inner.reorder_buffer_high_water.compare_exchange(
                observed_high_water,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed_high_water = actual,
            }
        }
    }

    pub(crate) fn record_staged_messages(&self, count: usize) {
        self.inner
            .staged_message_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_staged_deletes(&self, count: usize) {
        self.inner
            .staged_delete_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_staged_attachments(&self, count: usize) {
        self.inner
            .staged_attachment_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn report(&self) -> PipelineStatsReport {
        let fetch_batch_count = self.inner.fetch_batch_count.load(Ordering::Relaxed);
        let fetch_batch_total_ms = self.inner.fetch_batch_total_ms.load(Ordering::Relaxed);
        let writer_tx_count = self.inner.writer_tx_count.load(Ordering::Relaxed);
        let writer_tx_total_ms = self.inner.writer_tx_total_ms.load(Ordering::Relaxed);
        PipelineStatsReport {
            pipeline_enabled: true,
            list_queue_high_water: self.inner.list_queue_high_water.load(Ordering::Relaxed),
            write_queue_high_water: self.inner.write_queue_high_water.load(Ordering::Relaxed),
            write_batch_count: self.inner.write_batch_count.load(Ordering::Relaxed),
            writer_wait_ms: self.inner.writer_wait_ms.load(Ordering::Relaxed),
            fetch_batch_count,
            fetch_batch_avg_ms: average_u64(fetch_batch_total_ms, fetch_batch_count),
            fetch_batch_max_ms: self.inner.fetch_batch_max_ms.load(Ordering::Relaxed),
            writer_tx_count,
            writer_tx_avg_ms: average_u64(writer_tx_total_ms, writer_tx_count),
            writer_tx_max_ms: self.inner.writer_tx_max_ms.load(Ordering::Relaxed),
            reorder_buffer_high_water: self.inner.reorder_buffer_high_water.load(Ordering::Relaxed),
            staged_message_count: self.inner.staged_message_count.load(Ordering::Relaxed),
            staged_delete_count: self.inner.staged_delete_count.load(Ordering::Relaxed),
            staged_attachment_count: self.inner.staged_attachment_count.load(Ordering::Relaxed),
        }
    }
}

pub(crate) fn page_processing_concurrency_for_fetch(fetch_concurrency: usize) -> usize {
    fetch_concurrency
        .max(1)
        .div_ceil(2)
        .clamp(1, PIPELINE_PAGE_PROCESSING_MAX_CONCURRENCY)
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

fn update_max(maximum: &AtomicU64, candidate: u64) {
    let mut observed = maximum.load(Ordering::Relaxed);
    while candidate > observed {
        match maximum.compare_exchange(observed, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => observed = actual,
        }
    }
}

fn saturating_decrement(depth: &AtomicUsize) {
    let _ = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

fn average_u64(total: u64, count: usize) -> u64 {
    match u64::try_from(count).ok().filter(|count| *count > 0) {
        Some(count) => total / count,
        None => 0,
    }
}
