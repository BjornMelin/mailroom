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
    pub bootstrap_query: String,
    pub cursor_history_id: String,
    pub pages_fetched: usize,
    pub messages_listed: usize,
    pub messages_upserted: usize,
    pub messages_deleted: usize,
    pub labels_synced: usize,
    pub store_message_count: i64,
    pub store_label_count: i64,
    pub store_indexed_message_count: i64,
    pub quota_units_budget_per_minute: u32,
    pub message_fetch_concurrency: usize,
    pub estimated_quota_units_reserved: u64,
    pub http_attempt_count: u64,
    pub retry_count: u64,
    pub throttle_wait_count: u64,
    pub throttle_wait_ms: u64,
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
    pub(crate) cursor_history_id: Option<String>,
    pub(crate) pages_fetched: usize,
    pub(crate) messages_listed: usize,
    pub(crate) messages_upserted: usize,
    pub(crate) messages_deleted: usize,
    pub(crate) labels_synced: usize,
}
