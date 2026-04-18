use super::{LABEL_SEPARATOR, SearchQuery, SearchResult};
use crate::store::connection;
use anyhow::{Result, anyhow};
use rusqlite::{
    params_from_iter,
    types::{ToSql, Value},
};
use std::path::Path;

pub(crate) fn search_messages(
    database_path: &Path,
    busy_timeout_ms: u64,
    query: &SearchQuery,
) -> Result<Vec<SearchResult>> {
    if query.terms.trim().is_empty() {
        return Err(anyhow!("search terms must not be empty"));
    }
    if query.limit == 0 {
        return Err(anyhow!("search limit must be greater than zero"));
    }
    if !database_path.try_exists()? {
        return Ok(Vec::new());
    }

    let connection = connection::open_read_only_for_diagnostics(database_path, busy_timeout_ms)?;
    let mut sql = String::from(
        "WITH matched AS (
             SELECT
                 gm.message_rowid,
                 gm.account_id,
                 gm.message_id,
                 gm.thread_id,
                 gm.internal_date_epoch_ms,
                 gm.subject,
                 gm.from_header,
                 gm.from_address,
                 gm.recipient_headers,
                 gm.snippet,
                 bm25(gmail_message_search, 8.0, 5.0, 3.0, 1.5, 2.0) AS rank
             FROM gmail_message_search
             JOIN gmail_messages gm
               ON gm.message_rowid = gmail_message_search.rowid
             WHERE gm.account_id = ?",
    );

    let mut values = vec![Value::from(query.account_id.clone())];

    sql.push_str(" AND gmail_message_search MATCH ?");
    values.push(Value::from(build_plain_fts5_query(&query.terms)));

    if let Some(label) = &query.label {
        sql.push_str(
            " AND EXISTS (
                 SELECT 1
                 FROM gmail_message_labels gml_filter
                 JOIN gmail_labels gl_filter
                   ON gl_filter.account_id = gm.account_id
                  AND gl_filter.label_id = gml_filter.label_id
                 WHERE gml_filter.message_rowid = gm.message_rowid
                   AND lower(gl_filter.name) = lower(?)
             )",
        );
        values.push(Value::from(label.clone()));
    }

    if let Some(from_address) = &query.from_address {
        sql.push_str(" AND lower(COALESCE(gm.from_address, '')) = lower(?)");
        values.push(Value::from(from_address.clone()));
    }

    if let Some(after_epoch_ms) = query.after_epoch_ms {
        sql.push_str(" AND gm.internal_date_epoch_ms >= ?");
        values.push(Value::from(after_epoch_ms));
    }

    if let Some(before_epoch_ms) = query.before_epoch_ms {
        sql.push_str(" AND gm.internal_date_epoch_ms < ?");
        values.push(Value::from(before_epoch_ms));
    }

    sql.push_str(
        " ORDER BY rank ASC, gm.internal_date_epoch_ms DESC
          LIMIT ?
         )
         SELECT
             matched.message_id,
             matched.thread_id,
             matched.internal_date_epoch_ms,
             matched.subject,
             matched.from_header,
             matched.from_address,
             matched.recipient_headers,
             matched.snippet,
             COALESCE(
                 (
                     SELECT group_concat(gl.name, char(31))
                     FROM gmail_message_labels gml
                     JOIN gmail_labels gl
                       ON gl.account_id = matched.account_id
                      AND gl.label_id = gml.label_id
                     WHERE gml.message_rowid = matched.message_rowid
                 ),
                 ''
             ) AS label_names_joined,
             (
                 SELECT COUNT(*)
                 FROM gmail_messages gm_thread
                 WHERE gm_thread.account_id = matched.account_id
                   AND gm_thread.thread_id = matched.thread_id
             ) AS thread_message_count,
             matched.rank
         FROM matched
         ORDER BY matched.rank ASC, matched.internal_date_epoch_ms DESC",
    );
    values.push(Value::from(i64::try_from(query.limit)?));

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(
        params_from_iter(values.iter().map(|value| value as &dyn ToSql)),
        |row| {
            let joined_labels: String = row.get(8)?;
            let mut label_names: Vec<String> = if joined_labels.is_empty() {
                Vec::new()
            } else {
                joined_labels
                    .split(LABEL_SEPARATOR)
                    .map(str::to_owned)
                    .collect()
            };
            label_names.sort();
            label_names.dedup();

            Ok(SearchResult {
                message_id: row.get(0)?,
                thread_id: row.get(1)?,
                internal_date_epoch_ms: row.get(2)?,
                subject: row.get(3)?,
                from_header: row.get(4)?,
                from_address: row.get(5)?,
                recipient_headers: row.get(6)?,
                snippet: row.get(7)?,
                label_names,
                thread_message_count: row.get(9)?,
                rank: row.get(10)?,
            })
        },
    )?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

pub(super) fn build_plain_fts5_query(terms: &str) -> String {
    terms
        .split_whitespace()
        .map(quote_fts5_term)
        .collect::<Vec<_>>()
        .join(" ")
}

fn quote_fts5_term(term: &str) -> String {
    format!("\"{}\"", term.replace('"', "\"\""))
}
