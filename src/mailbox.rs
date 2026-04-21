#[path = "mailbox/model.rs"]
mod model;
#[path = "mailbox/output.rs"]
mod output;
#[path = "mailbox/pacing.rs"]
mod pacing;
#[path = "mailbox/pipeline.rs"]
mod pipeline;
#[path = "mailbox/search.rs"]
mod search;
#[path = "mailbox/sync.rs"]
mod sync;
#[path = "mailbox/telemetry.rs"]
mod telemetry;
#[cfg(test)]
#[path = "mailbox/tests.rs"]
mod tests;
#[path = "mailbox/util.rs"]
mod util;

pub use model::{
    SearchReport, SearchRequest, SyncHistoryReport, SyncPerfExplainReport, SyncRunOptions,
    SyncRunReport,
};
pub use search::search;
pub use sync::{sync_history, sync_perf_explain, sync_run, sync_run_with_options};

pub const DEFAULT_BOOTSTRAP_RECENT_DAYS: u32 = 90;
pub const DEFAULT_SEARCH_LIMIT: usize = 25;
pub const DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE: u32 = 12_000;
pub const DEFAULT_MESSAGE_FETCH_CONCURRENCY: usize = 4;

pub(crate) const FULL_SYNC_PAGE_SIZE: u32 = 500;

#[cfg(test)]
pub(crate) use util::{newest_history_id, parse_start_of_day_epoch_ms};
