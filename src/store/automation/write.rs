use super::{
    AppendAutomationRunEventInput, AutomationPruneStoreReport, AutomationRunDetail,
    AutomationRunStatus, AutomationStoreWriteError, CandidateApplyResultInput,
    CreateAutomationRunInput, FinalizeAutomationRunInput, PruneAutomationRunsInput,
};
use crate::store::connection;
use anyhow::Result;
use rusqlite::{OptionalExtension, params, params_from_iter, types::Value};
use std::path::Path;

pub(crate) fn create_automation_run(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &CreateAutomationRunInput,
) -> Result<AutomationRunDetail, AutomationStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;

    let selected_rule_ids_json = serde_json::to_string(&input.selected_rule_ids)?;
    transaction.execute(
        "INSERT INTO automation_runs (
             account_id,
             rule_file_path,
             rule_file_hash,
             selected_rule_ids_json,
             status,
             candidate_count,
             created_at_epoch_s,
             applied_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
        params![
            &input.account_id,
            &input.rule_file_path,
            &input.rule_file_hash,
            selected_rule_ids_json,
            AutomationRunStatus::Previewed.as_str(),
            i64::try_from(input.candidates.len()).unwrap_or(i64::MAX),
            input.created_at_epoch_s,
        ],
    )?;
    let run_id = transaction.last_insert_rowid();

    let mut insert_candidate = transaction.prepare_cached(
        "INSERT INTO automation_run_candidates (
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
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, NULL, NULL, NULL, ?25)",
    )?;

    for candidate in &input.candidates {
        insert_candidate.execute(params![
            run_id,
            &input.account_id,
            &candidate.rule_id,
            &candidate.thread_id,
            &candidate.message_id,
            candidate.internal_date_epoch_ms,
            &candidate.subject,
            &candidate.from_header,
            &candidate.from_address,
            &candidate.snippet,
            serde_json::to_string(&candidate.label_names)?,
            candidate.attachment_count,
            if candidate.has_list_unsubscribe {
                1_i64
            } else {
                0_i64
            },
            &candidate.list_id_header,
            &candidate.list_unsubscribe_header,
            &candidate.list_unsubscribe_post_header,
            &candidate.precedence_header,
            &candidate.auto_submitted_header,
            candidate.action.kind.as_str(),
            serde_json::to_string(&candidate.action.add_label_ids)?,
            serde_json::to_string(&candidate.action.add_label_names)?,
            serde_json::to_string(&candidate.action.remove_label_ids)?,
            serde_json::to_string(&candidate.action.remove_label_names)?,
            serde_json::to_string(&candidate.reason)?,
            input.created_at_epoch_s,
        ])?;
    }

    transaction.execute(
        "INSERT INTO automation_run_events (
             run_id,
             account_id,
             event_kind,
             payload_json,
             created_at_epoch_s
         )
         VALUES (?1, ?2, 'preview_created', ?3, ?4)",
        params![
            run_id,
            &input.account_id,
            serde_json::to_string(&serde_json::json!({
                "candidate_count": input.candidates.len(),
                "selected_rule_ids": input.selected_rule_ids,
            }))?,
            input.created_at_epoch_s,
        ],
    )?;

    drop(insert_candidate);
    transaction.commit()?;
    let detail = super::read::get_automation_run_detail(database_path, busy_timeout_ms, run_id)?
        .ok_or(AutomationStoreWriteError::MissingRun { run_id })?;
    Ok(detail)
}

pub(crate) fn record_candidate_apply_result(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &CandidateApplyResultInput,
) -> Result<(), AutomationStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let rows_updated = transaction.execute(
        "UPDATE automation_run_candidates
         SET apply_status = ?3,
             applied_at_epoch_s = ?4,
             apply_error = ?5
         WHERE run_id = ?1
           AND candidate_id = ?2",
        params![
            input.run_id,
            input.candidate_id,
            input.status.as_str(),
            input.applied_at_epoch_s,
            &input.apply_error,
        ],
    )?;
    if rows_updated == 0 {
        return Err(AutomationStoreWriteError::MissingCandidate {
            run_id: input.run_id,
            candidate_id: input.candidate_id,
        });
    }
    if rows_updated != 1 {
        return Err(AutomationStoreWriteError::RowCountMismatch {
            operation: "record_candidate_apply_result",
            expected: 1,
            actual: rows_updated,
        });
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn finalize_automation_run(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &FinalizeAutomationRunInput,
) -> Result<(), AutomationStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let rows_updated = transaction.execute(
        "UPDATE automation_runs
         SET status = ?2,
             applied_at_epoch_s = ?3
         WHERE run_id = ?1",
        params![
            input.run_id,
            input.status.as_str(),
            input.applied_at_epoch_s
        ],
    )?;
    if rows_updated == 0 {
        return Err(AutomationStoreWriteError::MissingRun {
            run_id: input.run_id,
        });
    }
    if rows_updated != 1 {
        return Err(AutomationStoreWriteError::RowCountMismatch {
            operation: "finalize_automation_run",
            expected: 1,
            actual: rows_updated,
        });
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn claim_automation_run_for_apply(
    database_path: &Path,
    busy_timeout_ms: u64,
    run_id: i64,
    applied_at_epoch_s: i64,
) -> Result<(), AutomationStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let rows_updated = transaction.execute(
        "UPDATE automation_runs
         SET status = ?2,
             applied_at_epoch_s = ?3
         WHERE run_id = ?1
           AND status IN (?4, ?5)",
        params![
            run_id,
            AutomationRunStatus::Applying.as_str(),
            applied_at_epoch_s,
            AutomationRunStatus::Previewed.as_str(),
            AutomationRunStatus::ApplyFailed.as_str()
        ],
    )?;
    if rows_updated == 0 {
        let run_exists = transaction
            .query_row(
                "SELECT 1
                 FROM automation_runs
                 WHERE run_id = ?1",
                [run_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if run_exists {
            return Err(AutomationStoreWriteError::RowCountMismatch {
                operation: "claim_automation_run_for_apply",
                expected: 1,
                actual: 0,
            });
        }
        return Err(AutomationStoreWriteError::MissingRun { run_id });
    }
    if rows_updated != 1 {
        return Err(AutomationStoreWriteError::RowCountMismatch {
            operation: "claim_automation_run_for_apply",
            expected: 1,
            actual: rows_updated,
        });
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn prune_automation_runs(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &PruneAutomationRunsInput,
) -> Result<AutomationPruneStoreReport, AutomationStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;

    if input.statuses.is_empty() {
        transaction.commit()?;
        return Ok(AutomationPruneStoreReport {
            matched_run_count: 0,
            matched_candidate_count: 0,
            matched_event_count: 0,
            deleted_run_count: 0,
        });
    }

    let status_placeholders = vec!["?"; input.statuses.len()].join(", ");
    let predicate =
        format!("account_id = ? AND created_at_epoch_s < ? AND status IN ({status_placeholders})");
    let params = prune_params(input);

    let count_sql = format!(
        "WITH matched_runs AS (
             SELECT run_id
             FROM automation_runs
             WHERE {predicate}
         )
         SELECT
             (SELECT COUNT(*) FROM matched_runs),
             (SELECT COUNT(*) FROM automation_run_candidates
              WHERE run_id IN (SELECT run_id FROM matched_runs)),
             (SELECT COUNT(*) FROM automation_run_events
              WHERE run_id IN (SELECT run_id FROM matched_runs))"
    );
    let mut report = transaction.query_row(&count_sql, params_from_iter(params.iter()), |row| {
        Ok(AutomationPruneStoreReport {
            matched_run_count: row.get(0)?,
            matched_candidate_count: row.get(1)?,
            matched_event_count: row.get(2)?,
            deleted_run_count: 0,
        })
    })?;

    if input.execute {
        let delete_sql = format!("DELETE FROM automation_runs WHERE {predicate}");
        let deleted_run_count =
            transaction.execute(&delete_sql, params_from_iter(params.iter()))?;
        report.deleted_run_count = i64::try_from(deleted_run_count).unwrap_or(i64::MAX);
    }

    transaction.commit()?;
    Ok(report)
}

fn prune_params(input: &PruneAutomationRunsInput) -> Vec<Value> {
    let mut values = Vec::with_capacity(input.statuses.len() + 2);
    values.push(Value::Text(input.account_id.clone()));
    values.push(Value::Integer(input.cutoff_epoch_s));
    values.extend(
        input
            .statuses
            .iter()
            .map(|status| Value::Text(status.as_str().to_owned())),
    );
    values
}

pub(crate) fn append_automation_run_event(
    database_path: &Path,
    busy_timeout_ms: u64,
    input: &AppendAutomationRunEventInput,
) -> Result<(), AutomationStoreWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| AutomationStoreWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let run_account_id = transaction
        .query_row(
            "SELECT account_id FROM automation_runs WHERE run_id = ?1",
            [input.run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or(AutomationStoreWriteError::MissingRun {
            run_id: input.run_id,
        })?;
    if run_account_id != input.account_id {
        return Err(AutomationStoreWriteError::RunAccountMismatch {
            run_id: input.run_id,
            expected_account_id: run_account_id,
            actual_account_id: input.account_id.clone(),
        });
    }
    transaction.execute(
        "INSERT INTO automation_run_events (
             run_id,
             account_id,
             event_kind,
             payload_json,
             created_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            input.run_id,
            &input.account_id,
            &input.event_kind,
            &input.payload_json,
            input.created_at_epoch_s,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}
