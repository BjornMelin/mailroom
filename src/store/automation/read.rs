use super::{
    AutomationActionKind, AutomationActionSnapshot, AutomationApplyStatus, AutomationDoctorReport,
    AutomationRunCandidateRecord, AutomationRunDetail, AutomationRunEventRecord,
    AutomationRunRecord, AutomationRunStatus, AutomationStoreReadError, AutomationThreadCandidate,
    is_missing_automation_table_error,
};
use crate::store::connection;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use std::str::FromStr;

const LABEL_SEPARATOR: char = '\u{001F}';

pub(crate) fn inspect_automation(
    database_path: &Path,
    busy_timeout_ms: u64,
) -> Result<Option<AutomationDoctorReport>, AutomationStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreReadError::open_database(database_path, source))?;

    let report = connection
        .query_row(
            "SELECT
                 COUNT(*),
                 COALESCE(SUM(CASE WHEN status = 'previewed' THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN status = 'apply_failed' THEN 1 ELSE 0 END), 0),
                 COALESCE((SELECT COUNT(*) FROM automation_run_candidates), 0)
             FROM automation_runs",
            [],
            |row| {
                Ok(AutomationDoctorReport {
                    run_count: row.get(0)?,
                    previewed_run_count: row.get(1)?,
                    applied_run_count: row.get(2)?,
                    apply_failed_run_count: row.get(3)?,
                    candidate_count: row.get(4)?,
                })
            },
        )
        .optional();

    match report {
        Ok(report) => Ok(report),
        Err(error) if is_missing_automation_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn list_latest_thread_candidates(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
) -> Result<Vec<AutomationThreadCandidate>, AutomationStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreReadError::open_database(database_path, source))?;
    let mut statement = match connection.prepare(
        "WITH latest_messages AS (
             SELECT
                 gm.message_rowid,
                 gm.account_id,
                 gm.thread_id,
                 gm.message_id,
                 gm.internal_date_epoch_ms,
                 gm.subject,
                 gm.from_header,
                 gm.from_address,
                 gm.snippet,
                 gm.list_id_header,
                 gm.list_unsubscribe_header,
                 gm.list_unsubscribe_post_header,
                 gm.precedence_header,
                 gm.auto_submitted_header,
                 ROW_NUMBER() OVER (
                     PARTITION BY gm.account_id, gm.thread_id
                     ORDER BY gm.internal_date_epoch_ms DESC, gm.message_rowid DESC
                 ) AS thread_rank
             FROM gmail_messages gm
             WHERE gm.account_id = ?1
         ),
         label_names AS (
             SELECT
                 gml.message_rowid,
                 GROUP_CONCAT(gl.name, char(31)) AS label_names
             FROM gmail_message_labels gml
             INNER JOIN gmail_messages gm
               ON gm.message_rowid = gml.message_rowid
             INNER JOIN gmail_labels gl
               ON gl.account_id = gm.account_id
              AND gl.label_id = gml.label_id
             WHERE gm.account_id = ?1
             GROUP BY gml.message_rowid
         ),
         attachment_counts AS (
             SELECT
                 message_rowid,
                 COUNT(*) AS attachment_count
             FROM gmail_message_attachments
             WHERE account_id = ?1
             GROUP BY message_rowid
         )
         SELECT
             latest.account_id,
             latest.thread_id,
             latest.message_id,
             latest.internal_date_epoch_ms,
             latest.subject,
             latest.from_header,
             latest.from_address,
             latest.snippet,
             COALESCE(label_names.label_names, ''),
             COALESCE(attachment_counts.attachment_count, 0),
             latest.list_id_header,
             latest.list_unsubscribe_header,
             latest.list_unsubscribe_post_header,
             latest.precedence_header,
             latest.auto_submitted_header
         FROM latest_messages latest
         LEFT JOIN label_names
           ON label_names.message_rowid = latest.message_rowid
         LEFT JOIN attachment_counts
           ON attachment_counts.message_rowid = latest.message_rowid
         WHERE latest.thread_rank = 1
         ORDER BY latest.internal_date_epoch_ms DESC, latest.message_rowid DESC",
    ) {
        Ok(statement) => statement,
        Err(error) if is_missing_automation_thread_candidate_columns_error(&error) => {
            return Ok(Vec::new());
        }
        Err(error)
            if matches!(
                &error,
                rusqlite::Error::SqliteFailure(_, Some(message))
                    if message.contains("no such table: gmail_messages")
                        || message.contains("no such table: gmail_message_labels")
                        || message.contains("no such table: gmail_labels")
                        || message.contains("no such table: gmail_message_attachments")
            ) =>
        {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error.into()),
    };

    let rows = statement
        .query_map([account_id], |row| {
            let label_names = split_grouped_labels(&row.get::<_, String>(8)?);
            Ok(AutomationThreadCandidate {
                account_id: row.get(0)?,
                thread_id: row.get(1)?,
                message_id: row.get(2)?,
                internal_date_epoch_ms: row.get(3)?,
                subject: row.get(4)?,
                from_header: row.get(5)?,
                from_address: row.get(6)?,
                snippet: row.get(7)?,
                label_names,
                attachment_count: row.get(9)?,
                list_id_header: row.get(10)?,
                list_unsubscribe_header: row.get(11)?,
                list_unsubscribe_post_header: row.get(12)?,
                precedence_header: row.get(13)?,
                auto_submitted_header: row.get(14)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn is_missing_automation_thread_candidate_columns_error(error: &rusqlite::Error) -> bool {
    let message = error.to_string();
    message.contains("no such column")
        && [
            "list_id_header",
            "list_unsubscribe_header",
            "list_unsubscribe_post_header",
            "precedence_header",
            "auto_submitted_header",
        ]
        .iter()
        .any(|column| message.contains(column))
}

pub(crate) fn get_automation_run_detail(
    database_path: &Path,
    busy_timeout_ms: u64,
    run_id: i64,
) -> Result<Option<AutomationRunDetail>, AutomationStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreReadError::open_database(database_path, source))?;

    let run = match read_run_record(&connection, run_id) {
        Ok(run) => run,
        Err(error) if is_missing_automation_table_error(&error) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let Some(run) = run else {
        return Ok(None);
    };

    let candidates = read_run_candidates(&connection, run_id)?;
    let events = read_run_events(&connection, run_id)?;

    Ok(Some(AutomationRunDetail {
        run,
        candidates,
        events,
    }))
}

fn read_run_record(
    connection: &Connection,
    run_id: i64,
) -> Result<Option<AutomationRunRecord>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT
                 run_id,
                 account_id,
                 rule_file_path,
                 rule_file_hash,
                 selected_rule_ids_json,
                 status,
                 candidate_count,
                 created_at_epoch_s,
                 applied_at_epoch_s
             FROM automation_runs
             WHERE run_id = ?1",
            [run_id],
            |row| {
                let selected_rule_ids_json: String = row.get(4)?;
                let selected_rule_ids = serde_json::from_str::<Vec<String>>(
                    &selected_rule_ids_json,
                )
                .map_err(|source| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(source),
                    )
                })?;
                let status: String = row.get(5)?;
                let status = AutomationRunStatus::from_str(&status).map_err(|source| {
                    rusqlite::Error::FromSqlConversionFailure(
                        5,
                        rusqlite::types::Type::Text,
                        Box::new(source),
                    )
                })?;
                Ok(AutomationRunRecord {
                    run_id: row.get(0)?,
                    account_id: row.get(1)?,
                    rule_file_path: row.get(2)?,
                    rule_file_hash: row.get(3)?,
                    selected_rule_ids,
                    status,
                    candidate_count: row.get(6)?,
                    created_at_epoch_s: row.get(7)?,
                    applied_at_epoch_s: row.get(8)?,
                })
            },
        )
        .optional()
}

fn read_run_candidates(
    connection: &Connection,
    run_id: i64,
) -> Result<Vec<AutomationRunCandidateRecord>, AutomationStoreReadError> {
    let mut statement = connection.prepare(
        "SELECT
             candidate_id,
             run_id,
             account_id,
             rule_id,
             thread_id,
             message_id,
             internal_date_epoch_ms,
             subject,
             from_header,
             from_address,
             snippet,
             label_names_json,
             attachment_count,
             has_list_unsubscribe,
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             action_kind,
             add_label_ids_json,
             add_label_names_json,
             remove_label_ids_json,
             remove_label_names_json,
             reason_json,
             apply_status,
             applied_at_epoch_s,
             apply_error,
             created_at_epoch_s
         FROM automation_run_candidates
         WHERE run_id = ?1
         ORDER BY internal_date_epoch_ms DESC, candidate_id ASC",
    )?;

    let rows = statement
        .query_map([run_id], |row| {
            let action_kind =
                AutomationActionKind::from_str(&row.get::<_, String>(19)?).map_err(|source| {
                    rusqlite::Error::FromSqlConversionFailure(
                        19,
                        rusqlite::types::Type::Text,
                        Box::new(source),
                    )
                })?;
            let apply_status = row
                .get::<_, Option<String>>(25)?
                .map(|value| {
                    AutomationApplyStatus::from_str(&value).map_err(|source| {
                        rusqlite::Error::FromSqlConversionFailure(
                            25,
                            rusqlite::types::Type::Text,
                            Box::new(source),
                        )
                    })
                })
                .transpose()?;
            Ok(AutomationRunCandidateRecord {
                candidate_id: row.get(0)?,
                run_id: row.get(1)?,
                account_id: row.get(2)?,
                rule_id: row.get(3)?,
                thread_id: row.get(4)?,
                message_id: row.get(5)?,
                internal_date_epoch_ms: row.get(6)?,
                subject: row.get(7)?,
                from_header: row.get(8)?,
                from_address: row.get(9)?,
                snippet: row.get(10)?,
                label_names: parse_json_vec_row(row.get(11)?, 11)?,
                attachment_count: row.get(12)?,
                has_list_unsubscribe: row.get::<_, i64>(13)? != 0,
                list_id_header: row.get(14)?,
                list_unsubscribe_header: row.get(15)?,
                list_unsubscribe_post_header: row.get(16)?,
                precedence_header: row.get(17)?,
                auto_submitted_header: row.get(18)?,
                action: AutomationActionSnapshot {
                    kind: action_kind,
                    add_label_ids: parse_json_vec_row(row.get(20)?, 20)?,
                    add_label_names: parse_json_vec_row(row.get(21)?, 21)?,
                    remove_label_ids: parse_json_vec_row(row.get(22)?, 22)?,
                    remove_label_names: parse_json_vec_row(row.get(23)?, 23)?,
                },
                reason: parse_reason_row(row.get(24)?, 24)?,
                apply_status,
                applied_at_epoch_s: row.get(26)?,
                apply_error: row.get(27)?,
                created_at_epoch_s: row.get(28)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    Ok(rows)
}

fn read_run_events(
    connection: &Connection,
    run_id: i64,
) -> Result<Vec<AutomationRunEventRecord>, AutomationStoreReadError> {
    let mut statement = connection.prepare(
        "SELECT event_id, run_id, account_id, event_kind, payload_json, created_at_epoch_s
         FROM automation_run_events
         WHERE run_id = ?1
         ORDER BY created_at_epoch_s ASC, event_id ASC",
    )?;
    let rows = statement
        .query_map(params![run_id], |row| {
            Ok(AutomationRunEventRecord {
                event_id: row.get(0)?,
                run_id: row.get(1)?,
                account_id: row.get(2)?,
                event_kind: row.get(3)?,
                payload_json: row.get(4)?,
                created_at_epoch_s: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn split_grouped_labels(value: &str) -> Vec<String> {
    value
        .split(LABEL_SEPARATOR)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_json_vec_row(value: String, index: usize) -> Result<Vec<String>, rusqlite::Error> {
    serde_json::from_str(&value).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(source),
        )
    })
}

fn parse_reason_row(
    value: String,
    index: usize,
) -> Result<super::AutomationMatchReason, rusqlite::Error> {
    serde_json::from_str(&value).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(source),
        )
    })
}
