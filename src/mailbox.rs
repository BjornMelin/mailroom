#[path = "mailbox/model.rs"]
mod model;
#[path = "mailbox/output.rs"]
mod output;
#[path = "mailbox/search.rs"]
mod search;
#[path = "mailbox/sync.rs"]
mod sync;
#[cfg(test)]
#[path = "mailbox/tests.rs"]
mod tests;
#[path = "mailbox/util.rs"]
mod util;

pub use model::{SearchReport, SearchRequest, SyncRunReport};
pub use search::search;
pub use sync::sync_run;

pub const DEFAULT_BOOTSTRAP_RECENT_DAYS: u32 = 90;
pub const DEFAULT_SEARCH_LIMIT: usize = 25;

pub(crate) const FULL_SYNC_PAGE_SIZE: u32 = 100;
pub(crate) const MESSAGE_FETCH_CONCURRENCY: usize = 8;

#[cfg(test)]
pub(crate) use util::{newest_history_id, parse_start_of_day_epoch_ms};
