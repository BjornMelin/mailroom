use crate::store;
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub terms: String,
    pub label: Option<String>,
    pub from_address: Option<String>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct SyncRunOptions {
    pub force_full: bool,
    pub recent_days: u32,
    pub quota_units_per_minute: u32,
    pub message_fetch_concurrency: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncRunReport {
    pub mode: store::mailbox::SyncMode,
    pub fallback_from_history: bool,
    pub resumed_from_checkpoint: bool,
    pub bootstrap_query: String,
    pub cursor_history_id: String,
    pub pages_fetched: usize,
    pub messages_listed: usize,
    pub messages_upserted: usize,
    pub messages_deleted: usize,
    pub labels_synced: usize,
    pub checkpoint_reused_pages: usize,
    pub checkpoint_reused_messages_upserted: usize,
    pub pipeline_enabled: bool,
    pub pipeline_list_queue_high_water: usize,
    pub pipeline_write_queue_high_water: usize,
    pub pipeline_write_batch_count: usize,
    pub pipeline_writer_wait_ms: u64,
    pub store_message_count: i64,
    pub store_label_count: i64,
    pub store_indexed_message_count: i64,
    pub adaptive_pacing_enabled: bool,
    pub quota_units_budget_per_minute: u32,
    pub message_fetch_concurrency: usize,
    pub quota_units_cap_per_minute: u32,
    pub message_fetch_concurrency_cap: usize,
    pub starting_quota_units_per_minute: u32,
    pub starting_message_fetch_concurrency: usize,
    pub effective_quota_units_per_minute: u32,
    pub effective_message_fetch_concurrency: usize,
    pub adaptive_downshift_count: u64,
    pub estimated_quota_units_reserved: u64,
    pub http_attempt_count: u64,
    pub retry_count: u64,
    pub quota_pressure_retry_count: u64,
    pub concurrency_pressure_retry_count: u64,
    pub backend_retry_count: u64,
    pub throttle_wait_count: u64,
    pub throttle_wait_ms: u64,
    pub retry_after_wait_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchReport {
    pub terms: String,
    pub label: Option<String>,
    pub from_address: Option<String>,
    pub after_epoch_ms: Option<i64>,
    pub before_epoch_ms: Option<i64>,
    pub limit: usize,
    pub results: Vec<store::mailbox::SearchResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct FinalizeSyncInput {
    pub(crate) mode: store::mailbox::SyncMode,
    pub(crate) fallback_from_history: bool,
    pub(crate) resumed_from_checkpoint: bool,
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) pages_fetched: usize,
    pub(crate) messages_listed: usize,
    pub(crate) messages_upserted: usize,
    pub(crate) messages_deleted: usize,
    pub(crate) labels_synced: usize,
    pub(crate) checkpoint_reused_pages: usize,
    pub(crate) checkpoint_reused_messages_upserted: usize,
    pub(crate) pipeline_enabled: bool,
    pub(crate) pipeline_list_queue_high_water: usize,
    pub(crate) pipeline_write_queue_high_water: usize,
    pub(crate) pipeline_write_batch_count: usize,
    pub(crate) pipeline_writer_wait_ms: u64,
}
