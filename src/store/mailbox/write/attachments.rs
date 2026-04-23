use super::*;
#[cfg(test)]
pub(super) fn write_messages(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    updated_at_epoch_s: i64,
    preserved_attachment_vaults: Option<&BTreeMap<String, PreservedAttachmentVaultState>>,
) -> Result<usize> {
    if messages.is_empty() {
        return Ok(0);
    }
    ensure!(
        !account_id.is_empty(),
        "mailbox messages must have a non-empty account_id"
    );

    let mut upsert_message = transaction.prepare_cached(
        "INSERT INTO gmail_messages (
             account_id,
             message_id,
             thread_id,
             history_id,
             internal_date_epoch_ms,
             snippet,
             subject,
             from_header,
             from_address,
             recipient_headers,
             to_header,
             cc_header,
             bcc_header,
             reply_to_header,
             size_estimate,
             list_id_header,
             list_unsubscribe_header,
             list_unsubscribe_post_header,
             precedence_header,
             auto_submitted_header,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
         ON CONFLICT (account_id, message_id) DO UPDATE SET
             thread_id = excluded.thread_id,
             history_id = excluded.history_id,
             internal_date_epoch_ms = excluded.internal_date_epoch_ms,
             snippet = excluded.snippet,
             subject = excluded.subject,
             from_header = excluded.from_header,
             from_address = excluded.from_address,
             recipient_headers = excluded.recipient_headers,
             to_header = excluded.to_header,
             cc_header = excluded.cc_header,
             bcc_header = excluded.bcc_header,
             reply_to_header = excluded.reply_to_header,
             size_estimate = excluded.size_estimate,
             list_id_header = excluded.list_id_header,
             list_unsubscribe_header = excluded.list_unsubscribe_header,
             list_unsubscribe_post_header = excluded.list_unsubscribe_post_header,
             precedence_header = excluded.precedence_header,
             auto_submitted_header = excluded.auto_submitted_header,
             updated_at_epoch_s = excluded.updated_at_epoch_s
         RETURNING message_rowid",
    )?;
    let mut delete_message_labels =
        transaction.prepare_cached("DELETE FROM gmail_message_labels WHERE message_rowid = ?1")?;
    let mut delete_message_attachments = transaction
        .prepare_cached("DELETE FROM gmail_message_attachments WHERE message_rowid = ?1")?;
    let mut insert_message_label = transaction.prepare_cached(
        "INSERT INTO gmail_message_labels (message_rowid, label_id) VALUES (?1, ?2)",
    )?;
    let mut insert_attachment = transaction.prepare_cached(
        "INSERT INTO gmail_message_attachments (
             account_id,
             message_rowid,
             attachment_key,
             part_id,
             gmail_attachment_id,
             filename,
             mime_type,
             size_bytes,
             content_disposition,
             content_id,
             is_inline,
             vault_content_hash,
             vault_relative_path,
             vault_size_bytes,
             vault_fetched_at_epoch_s,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
    )?;
    let mut upsert_search = transaction.prepare_cached(
        "INSERT OR REPLACE INTO gmail_message_search (
             rowid,
             subject,
             from_header,
             recipient_headers,
             snippet,
             label_names
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;

    for message in messages {
        ensure!(
            message.account_id == account_id,
            "mailbox message account_id `{}` does not match batch account `{}`",
            message.account_id,
            account_id
        );
        let message_rowid: i64 = upsert_message.query_row(
            params![
                account_id,
                &message.message_id,
                &message.thread_id,
                &message.history_id,
                message.internal_date_epoch_ms,
                &message.snippet,
                &message.subject,
                &message.from_header,
                &message.from_address,
                &message.recipient_headers,
                &message.to_header,
                &message.cc_header,
                &message.bcc_header,
                &message.reply_to_header,
                message.size_estimate,
                &message.automation_headers.list_id_header,
                &message.automation_headers.list_unsubscribe_header,
                &message.automation_headers.list_unsubscribe_post_header,
                &message.automation_headers.precedence_header,
                &message.automation_headers.auto_submitted_header,
                updated_at_epoch_s,
            ],
            |row| row.get(0),
        )?;

        let message_attachment_vaults;
        let existing_attachment_vaults = match preserved_attachment_vaults {
            Some(preserved) => preserved,
            None => {
                message_attachment_vaults =
                    load_attachment_vault_state_for_message(transaction, message_rowid)?;
                &message_attachment_vaults
            }
        };
        delete_message_labels.execute([message_rowid])?;
        delete_message_attachments.execute([message_rowid])?;
        for label_id in unique_sorted_strings(&message.label_ids) {
            insert_message_label.execute(params![message_rowid, label_id])?;
        }
        for attachment in &message.attachments {
            let preserved = existing_attachment_vaults.get(&attachment.attachment_key);
            insert_attachment.execute(params![
                account_id,
                message_rowid,
                &attachment.attachment_key,
                &attachment.part_id,
                &attachment.gmail_attachment_id,
                &attachment.filename,
                &attachment.mime_type,
                attachment.size_bytes,
                &attachment.content_disposition,
                &attachment.content_id,
                if attachment.is_inline { 1_i64 } else { 0_i64 },
                preserved.map(|state| state.content_hash.as_str()),
                preserved.map(|state| state.relative_path.as_str()),
                preserved.map(|state| state.size_bytes),
                preserved.map(|state| state.fetched_at_epoch_s),
                updated_at_epoch_s,
            ])?;
        }

        upsert_search.execute(params![
            message_rowid,
            &message.subject,
            &message.from_header,
            &message.recipient_headers,
            &message.snippet,
            &message.label_names_text,
        ])?;
    }

    drop(upsert_search);
    drop(insert_attachment);
    drop(insert_message_label);
    drop(delete_message_attachments);
    drop(delete_message_labels);
    drop(upsert_message);
    Ok(messages.len())
}

pub(crate) fn set_attachment_vault_state(
    database_path: &Path,
    busy_timeout_ms: u64,
    update: &AttachmentVaultStateUpdate,
) -> Result<(), MailboxWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| MailboxWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    let rows_updated = transaction.execute(
        "UPDATE gmail_message_attachments
         SET vault_content_hash = ?3,
             vault_relative_path = ?4,
             vault_size_bytes = ?5,
             vault_fetched_at_epoch_s = ?6
         WHERE account_id = ?1
           AND attachment_key = ?2",
        params![
            &update.account_id,
            &update.attachment_key,
            &update.content_hash,
            &update.relative_path,
            update.size_bytes,
            update.fetched_at_epoch_s,
        ],
    )?;
    if rows_updated == 0 {
        return Err(MailboxWriteError::AttachmentNotFound {
            account_id: update.account_id.clone(),
            attachment_key: update.attachment_key.clone(),
        });
    }
    if rows_updated != 1 {
        return Err(MailboxWriteError::RowCountMismatch {
            operation: "set_attachment_vault_state",
            expected: 1,
            actual: rows_updated,
        });
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn record_attachment_export(
    database_path: &Path,
    busy_timeout_ms: u64,
    event: &AttachmentExportEventInput,
) -> Result<(), MailboxWriteError> {
    let mut connection = connection::open_or_create(database_path, busy_timeout_ms)
        .map_err(|source| MailboxWriteError::open_database(database_path, source))?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO attachment_export_events (
             account_id,
             attachment_key,
             message_id,
             thread_id,
             destination_path,
             content_hash,
             exported_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            &event.account_id,
            &event.attachment_key,
            &event.message_id,
            &event.thread_id,
            &event.destination_path,
            &event.content_hash,
            event.exported_at_epoch_s,
        ],
    )?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn reindex_message_search_for_account(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "INSERT OR REPLACE INTO gmail_message_search (
             rowid,
             subject,
             from_header,
             recipient_headers,
             snippet,
             label_names
         )
         SELECT
             gm.message_rowid,
             gm.subject,
             gm.from_header,
             gm.recipient_headers,
             gm.snippet,
             COALESCE(group_concat(gl.name, ' '), '')
         FROM gmail_messages gm
         LEFT JOIN gmail_message_labels gml
           ON gml.message_rowid = gm.message_rowid
         LEFT JOIN gmail_labels gl
           ON gl.account_id = gm.account_id
          AND gl.label_id = gml.label_id
         WHERE gm.account_id = ?1
         GROUP BY
             gm.message_rowid,
             gm.subject,
             gm.from_header,
             gm.recipient_headers,
             gm.snippet",
        [account_id],
    )?;
    Ok(())
}
