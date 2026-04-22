use super::{WorkflowResult, join_blocking};
use crate::gmail::GmailThreadMessage;
use crate::store;
use crate::workflows::WorkflowServiceError;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mail_builder::MessageBuilder;
use std::fs;
use tokio::task::spawn_blocking;

#[derive(Debug)]
pub(super) struct ReplyRecipients {
    pub(super) to_addresses: Vec<String>,
    pub(super) cc_addresses: Vec<String>,
}

pub(super) fn build_reply_recipients(
    account_email: &str,
    latest_message: &GmailThreadMessage,
    reply_mode: store::workflows::ReplyMode,
) -> WorkflowResult<ReplyRecipients> {
    let primary = first_non_self_reply_recipient(account_email, latest_message)
        .ok_or(WorkflowServiceError::ReplyRecipientUndetermined)?;

    let mut to_addresses = vec![primary];
    let mut cc_addresses = Vec::new();

    if reply_mode == store::workflows::ReplyMode::ReplyAll {
        for address in split_address_list(&latest_message.to_header) {
            push_unique_address(&mut to_addresses, &address, account_email);
        }
        for address in split_address_list(&latest_message.cc_header) {
            if address.eq_ignore_ascii_case(account_email)
                || to_addresses
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(&address))
            {
                continue;
            }
            push_unique_address(&mut cc_addresses, &address, account_email);
        }
    }

    if to_addresses.is_empty() {
        return Err(WorkflowServiceError::ReplyDraftWithoutRecipients);
    }
    Ok(ReplyRecipients {
        to_addresses,
        cc_addresses,
    })
}

pub(super) fn first_non_self_reply_recipient(
    account_email: &str,
    latest_message: &GmailThreadMessage,
) -> Option<String> {
    first_address(&latest_message.reply_to_header)
        .into_iter()
        .chain(latest_message.from_address.clone())
        .chain(first_address(&latest_message.from_header))
        .chain(split_address_list(&latest_message.to_header))
        .chain(split_address_list(&latest_message.cc_header))
        .find(|address| !address.eq_ignore_ascii_case(account_email))
}

pub(super) fn normalize_reply_subject(subject: &str) -> String {
    let trimmed = subject.trim();
    if trimmed.is_empty() {
        return String::from("Re:");
    }
    if trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("re:"))
    {
        trimmed.to_owned()
    } else {
        format!("Re: {trimmed}")
    }
}

pub(super) async fn build_raw_message(
    account_email: &str,
    source_message: &GmailThreadMessage,
    draft_revision: &store::workflows::DraftRevisionRecord,
    attachments: &[store::workflows::AttachmentInput],
) -> WorkflowResult<String> {
    let message_id = source_message
        .message_id_header
        .as_ref()
        .and_then(|header| normalize_message_id(header));
    let references_header = source_message.references_header.clone();
    let attachments: Vec<(store::workflows::AttachmentInput, Vec<u8>)> = if attachments.is_empty() {
        Vec::new()
    } else {
        let attachments = attachments.to_vec();
        join_blocking(
            spawn_blocking(move || {
                attachments
                    .into_iter()
                    .map(|attachment| {
                        let bytes = fs::read(&attachment.path).map_err(|source| {
                            WorkflowServiceError::AttachmentRead {
                                path: attachment.path.clone(),
                                source,
                            }
                        })?;
                        Ok::<_, WorkflowServiceError>((attachment, bytes))
                    })
                    .collect::<WorkflowResult<Vec<_>>>()
            }),
            "workflow.attachments.read",
        )
        .await?
    };

    let mut builder = MessageBuilder::new()
        .from(account_email.to_owned())
        .to(draft_revision.to_addresses.clone())
        .subject(draft_revision.subject.clone())
        .text_body(draft_revision.body_text.clone());

    if !draft_revision.cc_addresses.is_empty() {
        builder = builder.cc(draft_revision.cc_addresses.clone());
    }
    if !draft_revision.bcc_addresses.is_empty() {
        builder = builder.bcc(draft_revision.bcc_addresses.clone());
    }
    if let Some(message_id) = message_id {
        let references = build_references(&references_header, &message_id);
        builder = builder.in_reply_to(message_id);
        if !references.is_empty() {
            builder = builder.references(references);
        }
    }

    for (attachment, bytes) in attachments {
        builder = builder.attachment(attachment.mime_type, attachment.file_name, bytes);
    }

    let mut output = Vec::new();
    builder
        .write_to(&mut output)
        .map_err(|source| WorkflowServiceError::MessageBuild {
            source: source.into(),
        })?;
    Ok(URL_SAFE_NO_PAD.encode(output))
}

pub(super) fn build_references(existing: &str, message_id: &str) -> Vec<String> {
    let mut ids = parse_message_id_header(existing);
    if !ids.iter().any(|existing_id| existing_id == message_id) {
        ids.push(message_id.to_owned());
    }
    ids
}

pub(super) fn parse_message_id_header(value: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut current = String::new();
    let mut in_brackets = false;

    for character in value.chars() {
        match character {
            '<' => {
                current.clear();
                in_brackets = true;
            }
            '>' if in_brackets => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    ids.push(trimmed.to_owned());
                }
                current.clear();
                in_brackets = false;
            }
            _ if in_brackets => current.push(character),
            _ => {}
        }
    }

    ids
}

pub(super) fn normalize_message_id(value: &str) -> Option<String> {
    parse_message_id_header(value).into_iter().next()
}

pub(super) fn split_address_list(header: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut angle_depth = 0usize;

    for character in header.chars() {
        match character {
            '"' => {
                in_quotes = !in_quotes;
                current.push(character);
            }
            '<' if !in_quotes => {
                angle_depth += 1;
                current.push(character);
            }
            '>' if !in_quotes && angle_depth > 0 => {
                angle_depth -= 1;
                current.push(character);
            }
            ',' if !in_quotes && angle_depth == 0 => {
                if let Some(value) = normalize_address_candidate(&current) {
                    values.push(value);
                }
                current.clear();
            }
            _ => current.push(character),
        }
    }
    if let Some(value) = normalize_address_candidate(&current) {
        values.push(value);
    }
    values
}

pub(super) fn first_address(header: &str) -> Option<String> {
    split_address_list(header).into_iter().next()
}

pub(super) fn normalize_address_candidate(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let candidate = if let Some((_, remainder)) = value.split_once('<') {
        remainder.split_once('>')?.0.trim()
    } else {
        value.trim_matches('"')
    };
    if candidate.is_empty()
        || candidate.contains(char::is_whitespace)
        || candidate.matches('@').count() != 1
    {
        return None;
    }
    Some(candidate.to_ascii_lowercase())
}

pub(super) fn push_unique_address(target: &mut Vec<String>, address: &str, account_email: &str) {
    if address.eq_ignore_ascii_case(account_email)
        || target
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(address))
    {
        return;
    }
    target.push(address.to_owned());
}

pub(super) fn parse_day_to_epoch_s(value: &str) -> WorkflowResult<i64> {
    let epoch_ms = parse_start_of_day_epoch_ms(value)?;
    Ok(epoch_ms / 1000)
}

pub(super) fn parse_start_of_day_epoch_ms(value: &str) -> WorkflowResult<i64> {
    let mut parts = value.split('-');
    let year = parts
        .next()
        .ok_or_else(|| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let month = parts
        .next()
        .ok_or_else(|| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let day = parts
        .next()
        .ok_or_else(|| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    if parts.next().is_some() || year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return Err(WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        });
    }
    let year = year
        .parse::<i64>()
        .map_err(|_| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let month = month
        .parse::<u32>()
        .map_err(|_| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let day = day
        .parse::<u32>()
        .map_err(|_| WorkflowServiceError::InvalidDateFormat {
            value: value.to_owned(),
        })?;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => {
            return Err(WorkflowServiceError::InvalidDateMonth {
                value: value.to_owned(),
            });
        }
    };
    if day == 0 || day > max_day {
        return Err(WorkflowServiceError::InvalidDateDay {
            value: value.to_owned(),
        });
    }

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
    Ok((era * 146_097 + day_of_era - 719_468) * 86_400_000)
}

pub(super) fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}
