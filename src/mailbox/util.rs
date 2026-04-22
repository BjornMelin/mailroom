use crate::gmail::{GmailClientError, GmailLabel, GmailMessageMetadata};
use anyhow::{Result, anyhow};
use reqwest::StatusCode;
use std::collections::BTreeMap;

pub(crate) fn recipient_headers(message: &GmailMessageMetadata) -> String {
    [
        message.to_header.as_str(),
        message.cc_header.as_str(),
        message.bcc_header.as_str(),
        message.reply_to_header.as_str(),
    ]
    .iter()
    .filter_map(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
    .collect::<Vec<_>>()
    .join(" ")
}

pub(crate) fn labels_by_id(labels: &[GmailLabel]) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|label| (label.id.clone(), label.name.clone()))
        .collect()
}

pub(crate) fn message_is_excluded(label_ids: &[String]) -> bool {
    label_ids
        .iter()
        .any(|label_id| matches!(label_id.as_str(), "SPAM" | "TRASH"))
}

pub(crate) fn bootstrap_query(recent_days: u32) -> String {
    format!("in:anywhere -in:spam -in:trash newer_than:{recent_days}d")
}

pub(crate) fn newest_history_id(current: Option<String>, candidate: &str) -> Option<String> {
    match current {
        Some(current) if history_id_is_newer(&current, candidate) => Some(candidate.to_owned()),
        Some(current) => Some(current),
        None => Some(candidate.to_owned()),
    }
}

fn history_id_is_newer(current: &str, candidate: &str) -> bool {
    match (current.parse::<u128>(), candidate.parse::<u128>()) {
        (Ok(current), Ok(candidate)) => candidate > current,
        _ => candidate > current,
    }
}

pub(crate) fn is_stale_history_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<GmailClientError>()
        .is_some_and(|error| {
            matches!(
                error,
                GmailClientError::Api { path, status, .. }
                    if *status == StatusCode::NOT_FOUND && path == "users/me/history"
            )
        })
}

pub(crate) fn is_invalid_resume_page_token_error(error: &GmailClientError) -> bool {
    if let GmailClientError::Api { path, status, body } = error {
        if *status != StatusCode::BAD_REQUEST || path != "users/me/messages" {
            return false;
        }
        let body = body.to_ascii_lowercase();
        return body.contains("pagetoken")
            || body.contains("page token")
            || body.contains("page_token");
    }
    false
}

pub(crate) fn parse_start_of_day_epoch_ms(value: &str) -> Result<i64> {
    let (year, month, day) = parse_yyyy_mm_dd(value)?;
    days_from_civil(year, month, day)
        .checked_mul(86_400_000)
        .ok_or_else(|| anyhow!("date `{value}` is out of range"))
}

fn parse_yyyy_mm_dd(value: &str) -> Result<(i64, u32, u32)> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .ok_or_else(|| anyhow!("date `{value}` must be in YYYY-MM-DD format"))?;
    let month = parts
        .next()
        .ok_or_else(|| anyhow!("date `{value}` must be in YYYY-MM-DD format"))?;
    let day = parts
        .next()
        .ok_or_else(|| anyhow!("date `{value}` must be in YYYY-MM-DD format"))?;
    if parts.next().is_some() || year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return Err(anyhow!("date `{value}` must be in YYYY-MM-DD format"));
    }

    let year = year.parse::<i64>()?;
    if !(0..=9999).contains(&year) {
        return Err(anyhow!("date `{value}` has an invalid year"));
    }

    let month = month.parse::<u32>()?;
    let day = day.parse::<u32>()?;
    if !(1..=12).contains(&month) {
        return Err(anyhow!("date `{value}` has an invalid month"));
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => unreachable!(),
    };
    if day == 0 || day > max_day {
        return Err(anyhow!("date `{value}` has an invalid day"));
    }
    Ok((year, month, day))
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let month = i64::from(month);
    let day = i64::from(day);
    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}
