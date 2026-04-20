use super::is_missing_workflow_table_error;
use super::{
    CleanupAction, DraftAttachmentRecord, DraftRevisionDetail, DraftRevisionRecord, ReplyMode,
    TriageBucket, WorkflowDetail, WorkflowDoctorReport, WorkflowEventRecord, WorkflowListFilter,
    WorkflowRecord, WorkflowStage, WorkflowStoreReadError,
};
use crate::store::connection;
use rusqlite::types::{ToSql, Type, Value};
use rusqlite::{Connection, OptionalExtension, params_from_iter};
use std::path::Path;
use std::str::FromStr;

pub(crate) fn list_workflows(
    database_path: &Path,
    busy_timeout_ms: u64,
    filter: &WorkflowListFilter,
) -> Result<Vec<WorkflowRecord>, WorkflowStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreReadError::open_database(database_path, source))?;
    list_workflows_with_connection(&connection, filter)
}

pub(crate) fn get_workflow_detail(
    database_path: &Path,
    busy_timeout_ms: u64,
    account_id: &str,
    thread_id: &str,
) -> Result<Option<WorkflowDetail>, WorkflowStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreReadError::open_database(database_path, source))?;
    load_workflow_detail(&connection, account_id, thread_id)
}

pub(crate) fn inspect_workflows(
    database_path: &Path,
    busy_timeout_ms: u64,
) -> Result<Option<WorkflowDoctorReport>, WorkflowStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreReadError::open_database(database_path, source))?;
    let workflow_count = count_query(&connection, "SELECT COUNT(*) FROM thread_workflows")?;
    let open_workflow_count = count_query(
        &connection,
        "SELECT COUNT(*) FROM thread_workflows WHERE current_stage <> 'closed'",
    )?;
    let draft_workflow_count = count_query(
        &connection,
        "SELECT COUNT(*) FROM thread_workflows WHERE current_stage IN ('drafting', 'ready_to_send')",
    )?;
    let event_count = count_query(&connection, "SELECT COUNT(*) FROM thread_workflow_events")?;
    let draft_revision_count =
        count_query(&connection, "SELECT COUNT(*) FROM thread_draft_revisions")?;

    Ok(Some(WorkflowDoctorReport {
        workflow_count,
        open_workflow_count,
        draft_workflow_count,
        event_count,
        draft_revision_count,
    }))
}

pub(crate) fn lookup_workflow_account_id(
    database_path: &Path,
    busy_timeout_ms: u64,
    thread_id: Option<&str>,
) -> Result<Option<String>, WorkflowStoreReadError> {
    if !database_path.try_exists()? {
        return Ok(None);
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)
        .map_err(|source| WorkflowStoreReadError::open_database(database_path, source))?;
    match lookup_workflow_account_id_with_connection(&connection, thread_id) {
        Ok(account_id) => Ok(account_id),
        Err(error) if is_missing_workflow_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn load_workflow(
    connection: &Connection,
    account_id: &str,
    thread_id: &str,
) -> Result<Option<WorkflowRecord>, WorkflowStoreReadError> {
    let record = connection
        .query_row(
            "SELECT
                 workflow_id,
                 account_id,
                 thread_id,
                 current_stage,
                 triage_bucket,
                 note,
                 snoozed_until_epoch_s,
                 follow_up_due_epoch_s,
                 latest_message_id,
                 latest_message_internal_date_epoch_ms,
                 latest_message_subject,
                 latest_message_from_header,
                 latest_message_snippet,
                 current_draft_revision_id,
                 gmail_draft_id,
                 gmail_draft_message_id,
                 gmail_draft_thread_id,
                 last_remote_sync_epoch_s,
                 last_sent_message_id,
                 last_cleanup_action,
                 created_at_epoch_s,
                 updated_at_epoch_s,
                 workflow_version
             FROM thread_workflows
             WHERE account_id = ?1
               AND thread_id = ?2",
            [account_id, thread_id],
            row_to_workflow,
        )
        .optional();

    match record {
        Ok(record) => Ok(record),
        Err(error) if is_missing_workflow_table_error(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn lookup_workflow_account_id_with_connection(
    connection: &Connection,
    thread_id: Option<&str>,
) -> Result<Option<String>, rusqlite::Error> {
    let query = if thread_id.is_some() {
        "SELECT DISTINCT account_id FROM thread_workflows WHERE thread_id = ?1 ORDER BY account_id LIMIT 2"
    } else {
        "SELECT DISTINCT account_id FROM thread_workflows ORDER BY account_id LIMIT 2"
    };
    let mut statement = connection.prepare(query)?;
    let account_ids = if let Some(thread_id) = thread_id {
        statement
            .query_map([thread_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    } else {
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(match account_ids.as_slice() {
        [account_id] => Some(account_id.clone()),
        _ => None,
    })
}

fn list_workflows_with_connection(
    connection: &Connection,
    filter: &WorkflowListFilter,
) -> Result<Vec<WorkflowRecord>, WorkflowStoreReadError> {
    let mut sql = String::from(
        "SELECT
             workflow_id,
             account_id,
             thread_id,
             current_stage,
             triage_bucket,
             note,
             snoozed_until_epoch_s,
             follow_up_due_epoch_s,
             latest_message_id,
             latest_message_internal_date_epoch_ms,
             latest_message_subject,
             latest_message_from_header,
             latest_message_snippet,
             current_draft_revision_id,
             gmail_draft_id,
             gmail_draft_message_id,
             gmail_draft_thread_id,
             last_remote_sync_epoch_s,
             last_sent_message_id,
             last_cleanup_action,
             created_at_epoch_s,
             updated_at_epoch_s,
             workflow_version
         FROM thread_workflows
         WHERE account_id = ?",
    );
    let mut values = vec![Value::from(filter.account_id.clone())];

    if let Some(stage) = filter.stage {
        sql.push_str(" AND current_stage = ?");
        values.push(Value::from(stage.as_str().to_owned()));
    }
    if let Some(bucket) = filter.triage_bucket {
        sql.push_str(" AND triage_bucket = ?");
        values.push(Value::from(bucket.as_str().to_owned()));
    }

    sql.push_str(
        " ORDER BY
             CASE current_stage
                 WHEN 'triage' THEN 0
                 WHEN 'follow_up' THEN 1
                 WHEN 'drafting' THEN 2
                 WHEN 'ready_to_send' THEN 3
                 WHEN 'sent' THEN 4
                 WHEN 'closed' THEN 5
                 ELSE 6
             END,
             CASE triage_bucket
                 WHEN 'urgent' THEN 0
                 WHEN 'needs_reply_soon' THEN 1
                 WHEN 'waiting' THEN 2
                 WHEN 'fyi' THEN 3
                 ELSE 4
             END,
             COALESCE(snoozed_until_epoch_s, 9223372036854775807) ASC,
             COALESCE(follow_up_due_epoch_s, 9223372036854775807) ASC,
             updated_at_epoch_s DESC",
    );

    let mut statement = match connection.prepare(&sql) {
        Ok(statement) => statement,
        Err(error) if is_missing_workflow_table_error(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };

    let rows = statement.query_map(
        params_from_iter(values.iter().map(|value| value as &dyn ToSql)),
        row_to_workflow,
    )?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn load_workflow_detail(
    connection: &Connection,
    account_id: &str,
    thread_id: &str,
) -> Result<Option<WorkflowDetail>, WorkflowStoreReadError> {
    let Some(workflow) = load_workflow(connection, account_id, thread_id)? else {
        return Ok(None);
    };

    let current_draft = match workflow.current_draft_revision_id {
        Some(draft_revision_id) => load_draft_revision_detail(connection, draft_revision_id)?,
        None => None,
    };
    let events = load_workflow_events(connection, workflow.workflow_id)?;

    Ok(Some(WorkflowDetail {
        workflow,
        current_draft,
        events,
    }))
}

fn load_draft_revision_detail(
    connection: &Connection,
    draft_revision_id: i64,
) -> Result<Option<DraftRevisionDetail>, WorkflowStoreReadError> {
    let revision = connection
        .query_row(
            "SELECT
                 draft_revision_id,
                 workflow_id,
                 account_id,
                 thread_id,
                 source_message_id,
                 reply_mode,
                 subject,
                 to_addresses_json,
                 cc_addresses_json,
                 bcc_addresses_json,
                 body_text,
                 created_at_epoch_s
             FROM thread_draft_revisions
             WHERE draft_revision_id = ?1",
            [draft_revision_id],
            row_to_draft_revision,
        )
        .optional()?;

    let Some(revision) = revision else {
        return Ok(None);
    };

    let attachments = load_draft_attachments(connection, draft_revision_id)?;

    Ok(Some(DraftRevisionDetail {
        revision,
        attachments,
    }))
}

fn load_draft_attachments(
    connection: &Connection,
    draft_revision_id: i64,
) -> Result<Vec<DraftAttachmentRecord>, WorkflowStoreReadError> {
    let mut statement = connection.prepare(
        "SELECT
             attachment_id,
             draft_revision_id,
             path,
             file_name,
             mime_type,
             size_bytes,
             created_at_epoch_s
         FROM thread_draft_attachments
         WHERE draft_revision_id = ?1
         ORDER BY created_at_epoch_s ASC, attachment_id ASC",
    )?;
    let rows = statement.query_map([draft_revision_id], |row| {
        Ok(DraftAttachmentRecord {
            attachment_id: row.get(0)?,
            draft_revision_id: row.get(1)?,
            path: row.get(2)?,
            file_name: row.get(3)?,
            mime_type: row.get(4)?,
            size_bytes: row.get(5)?,
            created_at_epoch_s: row.get(6)?,
        })
    })?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn load_workflow_events(
    connection: &Connection,
    workflow_id: i64,
) -> Result<Vec<WorkflowEventRecord>, WorkflowStoreReadError> {
    let mut statement = connection.prepare(
        "SELECT
             event_id,
             workflow_id,
             account_id,
             thread_id,
             event_kind,
             from_stage,
             to_stage,
             triage_bucket,
             note,
             payload_json,
             created_at_epoch_s
         FROM thread_workflow_events
         WHERE workflow_id = ?1
         ORDER BY created_at_epoch_s DESC, event_id DESC",
    )?;
    let rows = statement.query_map([workflow_id], row_to_event)?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn row_to_workflow(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkflowRecord> {
    Ok(WorkflowRecord {
        workflow_id: row.get(0)?,
        account_id: row.get(1)?,
        thread_id: row.get(2)?,
        current_stage: decode_stage(row.get(3)?, 3)?,
        triage_bucket: decode_optional_bucket(row.get(4)?, 4)?,
        note: row.get(5)?,
        snoozed_until_epoch_s: row.get(6)?,
        follow_up_due_epoch_s: row.get(7)?,
        latest_message_id: row.get(8)?,
        latest_message_internal_date_epoch_ms: row.get(9)?,
        latest_message_subject: row.get(10)?,
        latest_message_from_header: row.get(11)?,
        latest_message_snippet: row.get(12)?,
        current_draft_revision_id: row.get(13)?,
        gmail_draft_id: row.get(14)?,
        gmail_draft_message_id: row.get(15)?,
        gmail_draft_thread_id: row.get(16)?,
        last_remote_sync_epoch_s: row.get(17)?,
        last_sent_message_id: row.get(18)?,
        last_cleanup_action: decode_optional_cleanup_action(row.get(19)?, 19)?,
        created_at_epoch_s: row.get(20)?,
        updated_at_epoch_s: row.get(21)?,
        workflow_version: row.get(22)?,
    })
}

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkflowEventRecord> {
    Ok(WorkflowEventRecord {
        event_id: row.get(0)?,
        workflow_id: row.get(1)?,
        account_id: row.get(2)?,
        thread_id: row.get(3)?,
        event_kind: row.get(4)?,
        from_stage: decode_optional_stage(row.get(5)?, 5)?,
        to_stage: decode_optional_stage(row.get(6)?, 6)?,
        triage_bucket: decode_optional_bucket(row.get(7)?, 7)?,
        note: row.get(8)?,
        payload_json: row.get(9)?,
        created_at_epoch_s: row.get(10)?,
    })
}

pub(super) fn row_to_draft_revision(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<DraftRevisionRecord> {
    Ok(DraftRevisionRecord {
        draft_revision_id: row.get(0)?,
        workflow_id: row.get(1)?,
        account_id: row.get(2)?,
        thread_id: row.get(3)?,
        source_message_id: row.get(4)?,
        reply_mode: decode_reply_mode(row.get(5)?, 5)?,
        subject: row.get(6)?,
        to_addresses: decode_string_list(row.get(7)?, 7)?,
        cc_addresses: decode_string_list(row.get(8)?, 8)?,
        bcc_addresses: decode_string_list(row.get(9)?, 9)?,
        body_text: row.get(10)?,
        created_at_epoch_s: row.get(11)?,
    })
}

fn decode_stage(value: String, column_index: usize) -> rusqlite::Result<WorkflowStage> {
    WorkflowStage::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
    })
}

fn decode_optional_stage(
    value: Option<String>,
    column_index: usize,
) -> rusqlite::Result<Option<WorkflowStage>> {
    value
        .map(|value| decode_stage(value, column_index))
        .transpose()
}

fn decode_optional_bucket(
    value: Option<String>,
    column_index: usize,
) -> rusqlite::Result<Option<TriageBucket>> {
    value
        .map(|value| {
            TriageBucket::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
            })
        })
        .transpose()
}

fn decode_reply_mode(value: String, column_index: usize) -> rusqlite::Result<ReplyMode> {
    ReplyMode::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
    })
}

fn decode_optional_cleanup_action(
    value: Option<String>,
    column_index: usize,
) -> rusqlite::Result<Option<CleanupAction>> {
    value
        .map(|value| {
            CleanupAction::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
            })
        })
        .transpose()
}

fn decode_string_list(value: String, column_index: usize) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column_index, Type::Text, Box::new(error))
    })
}

fn count_query(connection: &Connection, sql: &str) -> Result<i64, WorkflowStoreReadError> {
    let count = match connection.query_row(sql, [], |row| row.get(0)) {
        Ok(count) => count,
        Err(error) if is_missing_workflow_table_error(&error) => 0,
        Err(error) => return Err(error.into()),
    };
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::lookup_workflow_account_id;
    use crate::config::resolve;
    use crate::store::init;
    use crate::workspace::WorkspacePaths;
    use rusqlite::params;
    use tempfile::TempDir;

    #[test]
    fn lookup_workflow_account_id_returns_none_for_empty_and_ambiguous_thread_matches() {
        let repo_root = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();

        let busy_timeout_ms = config_report.config.store.busy_timeout_ms;
        let database_path = &config_report.config.store.database_path;
        let thread_id = "thread-1";

        assert_eq!(
            lookup_workflow_account_id(database_path, busy_timeout_ms, Some(thread_id)).unwrap(),
            None
        );

        let mut connection =
            crate::store::connection::open_or_create(database_path, busy_timeout_ms).unwrap();
        let transaction = connection.transaction().unwrap();
        transaction
            .execute(
                "INSERT INTO accounts (
                     account_id,
                     provider,
                     email_address,
                     history_id,
                     messages_total,
                     threads_total,
                     access_scope,
                     is_active,
                     created_at_epoch_s,
                     updated_at_epoch_s,
                     last_profile_refresh_epoch_s
                 )
                 VALUES (?1, 'gmail', ?2, '1', 0, 0, 'scope:a', 0, 100, 100, 100)",
                params!["gmail:alice@example.com", "alice@example.com"],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO accounts (
                     account_id,
                     provider,
                     email_address,
                     history_id,
                     messages_total,
                     threads_total,
                     access_scope,
                     is_active,
                     created_at_epoch_s,
                     updated_at_epoch_s,
                     last_profile_refresh_epoch_s
                 )
                 VALUES (?1, 'gmail', ?2, '1', 0, 0, 'scope:a', 0, 100, 100, 100)",
                params!["gmail:bob@example.com", "bob@example.com"],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO thread_workflows (
                     workflow_id,
                     account_id,
                     thread_id,
                     current_stage,
                     created_at_epoch_s,
                     updated_at_epoch_s
                 )
                 VALUES (?1, ?2, ?3, 'triage', 100, 100)",
                params![1_i64, "gmail:alice@example.com", thread_id],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO thread_workflows (
                     workflow_id,
                     account_id,
                     thread_id,
                     current_stage,
                     created_at_epoch_s,
                     updated_at_epoch_s
                 )
                 VALUES (?1, ?2, ?3, 'triage', 100, 100)",
                params![2_i64, "gmail:bob@example.com", thread_id],
            )
            .unwrap();
        transaction.commit().unwrap();

        assert_eq!(
            lookup_workflow_account_id(database_path, busy_timeout_ms, Some(thread_id)).unwrap(),
            None
        );
    }
}
