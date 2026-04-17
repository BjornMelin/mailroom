use crate::config::ConfigReport;
use crate::mailbox::util::parse_start_of_day_epoch_ms;
use crate::mailbox::{SearchReport, SearchRequest};
use crate::store;
use anyhow::{Result, anyhow};
use tokio::task::spawn_blocking;

pub async fn search(config_report: &ConfigReport, request: SearchRequest) -> Result<SearchReport> {
    store::init(config_report)?;

    let after_epoch_ms = request
        .after
        .as_deref()
        .map(parse_start_of_day_epoch_ms)
        .transpose()?;
    let before_epoch_ms = request
        .before
        .as_deref()
        .map(parse_start_of_day_epoch_ms)
        .transpose()?;

    let database_path = config_report.config.store.database_path.clone();
    let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
    let terms = request.terms.trim().to_owned();
    if terms.is_empty() {
        return Err(anyhow!("search terms cannot be empty"));
    }
    let limit = request.limit;
    if limit == 0 {
        return Err(anyhow!("search limit must be greater than zero"));
    }
    let report_terms = terms.clone();
    let account_id = resolve_search_account_id(config_report)?;
    let label = request.label.clone();
    let from_address = request.from_address.clone();
    let results = spawn_blocking(move || {
        store::mailbox::search_messages(
            &database_path,
            busy_timeout_ms,
            &store::mailbox::SearchQuery {
                account_id,
                terms,
                label,
                from_address,
                after_epoch_ms,
                before_epoch_ms,
                limit,
            },
        )
    })
    .await??;

    Ok(SearchReport {
        terms: report_terms,
        label: request.label,
        from_address: request.from_address,
        after_epoch_ms,
        before_epoch_ms,
        limit: request.limit,
        results,
    })
}

fn resolve_search_account_id(config_report: &ConfigReport) -> Result<String> {
    if let Some(active_account) = store::accounts::get_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )? {
        return Ok(active_account.account_id);
    }

    if let Some(mailbox) = store::mailbox::inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )? && let Some(sync_state) = mailbox.sync_state
    {
        return Ok(sync_state.account_id);
    }

    Err(anyhow!(
        "no active Gmail account found; run `mailroom auth login` first"
    ))
}
