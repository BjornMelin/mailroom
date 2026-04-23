use super::*;
pub(super) fn reset_full_sync_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_page_messages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_pages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_attachments WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_message_labels WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_messages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_labels WHERE account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

pub(super) fn cleanup_incomplete_full_sync_stage_pages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    completed_page_count: i64,
) -> Result<()> {
    // For resumable full syncs, staged pages at or beyond the completed-page checkpoint
    // are considered in-progress and must be purged with any non-complete pages.
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_attachments
         WHERE account_id = ?1
           AND message_id IN (
               SELECT message_id
               FROM gmail_full_sync_stage_page_messages
               WHERE account_id = ?1
                 AND page_seq IN (
                     SELECT page_seq
                     FROM gmail_full_sync_stage_pages
                     WHERE account_id = ?1
                       AND (status != ?2 OR page_seq >= ?3)
                 )
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_message_labels
         WHERE account_id = ?1
           AND message_id IN (
               SELECT message_id
               FROM gmail_full_sync_stage_page_messages
               WHERE account_id = ?1
                 AND page_seq IN (
                     SELECT page_seq
                     FROM gmail_full_sync_stage_pages
                     WHERE account_id = ?1
                       AND (status != ?2 OR page_seq >= ?3)
                 )
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_messages
         WHERE account_id = ?1
           AND message_id IN (
               SELECT message_id
               FROM gmail_full_sync_stage_page_messages
               WHERE account_id = ?1
                 AND page_seq IN (
                     SELECT page_seq
                     FROM gmail_full_sync_stage_pages
                     WHERE account_id = ?1
                       AND (status != ?2 OR page_seq >= ?3)
                 )
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_page_messages
         WHERE account_id = ?1
           AND page_seq IN (
               SELECT page_seq
               FROM gmail_full_sync_stage_pages
               WHERE account_id = ?1
                 AND (status != ?2 OR page_seq >= ?3)
           )",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_pages
         WHERE account_id = ?1
           AND (status != ?2 OR page_seq >= ?3)",
        params![
            account_id,
            FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE,
            completed_page_count
        ],
    )?;
    Ok(())
}

pub(super) fn reset_incremental_sync_stage_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_attachments WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_message_labels WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_messages WHERE account_id = ?1",
        [account_id],
    )?;
    transaction.execute(
        "DELETE FROM gmail_incremental_sync_stage_delete_ids WHERE account_id = ?1",
        [account_id],
    )?;
    Ok(())
}

#[cfg(test)]
pub(super) fn stage_full_sync_labels_with_connection(
    connection: &mut Connection,
    account_id: &str,
    labels: &[GmailLabel],
) -> Result<()> {
    ensure!(
        !account_id.is_empty(),
        "mailbox staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    stage_full_sync_labels_in_transaction(&transaction, account_id, labels)?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn stage_full_sync_labels_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    labels: &[GmailLabel],
) -> Result<()> {
    transaction.execute(
        "DELETE FROM gmail_full_sync_stage_labels WHERE account_id = ?1",
        [account_id],
    )?;

    let mut insert = transaction.prepare_cached(
        "INSERT INTO gmail_full_sync_stage_labels (
             account_id,
             label_id,
             name,
             label_type,
             message_list_visibility,
             label_list_visibility,
             messages_total,
             messages_unread,
             threads_total,
             threads_unread
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT (account_id, label_id) DO UPDATE SET
             name = excluded.name,
             label_type = excluded.label_type,
             message_list_visibility = excluded.message_list_visibility,
             label_list_visibility = excluded.label_list_visibility,
             messages_total = excluded.messages_total,
             messages_unread = excluded.messages_unread,
             threads_total = excluded.threads_total,
             threads_unread = excluded.threads_unread",
    )?;

    for label in labels {
        insert.execute(params![
            account_id,
            &label.id,
            &label.name,
            &label.label_type,
            &label.message_list_visibility,
            &label.label_list_visibility,
            label.messages_total,
            label.messages_unread,
            label.threads_total,
            label.threads_unread,
        ])?;
    }

    drop(insert);
    Ok(())
}

#[cfg(test)]
pub(super) fn stage_full_sync_messages_with_connection(
    connection: &mut Connection,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }

    ensure!(
        !account_id.is_empty(),
        "mailbox staging requires a non-empty account_id"
    );

    let transaction = connection.transaction()?;
    stage_full_sync_messages_in_transaction(&transaction, account_id, None, messages)?;
    transaction.commit()?;
    Ok(())
}

pub(super) fn stage_full_sync_page_and_update_checkpoint_with_connection(
    connection: &mut Connection,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    update: &FullSyncCheckpointUpdate,
) -> Result<FullSyncCheckpointRecord> {
    let page_seq = update.pages_fetched.saturating_sub(1);
    stage_full_sync_page_chunk_and_maybe_update_checkpoint_with_connection(
        connection,
        account_id,
        &FullSyncStagePageInput {
            page_seq,
            listed_count: i64::try_from(messages.len()).unwrap_or(i64::MAX),
            next_page_token: update.next_page_token.clone(),
            updated_at_epoch_s: update.updated_at_epoch_s,
            page_complete: true,
        },
        messages,
        Some(update),
    )
}

pub(super) fn stage_full_sync_page_chunk_and_maybe_update_checkpoint_with_connection(
    connection: &mut Connection,
    account_id: &str,
    input: &FullSyncStagePageInput,
    messages: &[GmailMessageUpsertInput],
    checkpoint_update: Option<&FullSyncCheckpointUpdate>,
) -> Result<FullSyncCheckpointRecord> {
    let checkpoint_update = if input.page_complete {
        let update = checkpoint_update.ok_or_else(|| {
            anyhow!("full sync page completion requires a checkpoint update payload")
        })?;
        validate_checkpoint_fields(account_id, update)?;
        Some(update)
    } else {
        ensure!(
            checkpoint_update.is_none(),
            "partial full sync page chunks must not advance the checkpoint"
        );
        None
    };

    let transaction = connection.transaction()?;
    upsert_full_sync_stage_page_in_transaction(&transaction, account_id, input, 0)?;
    stage_full_sync_messages_in_transaction(
        &transaction,
        account_id,
        Some(input.page_seq),
        messages,
    )?;
    let staged_message_count = count_full_sync_stage_page_messages_in_transaction(
        &transaction,
        account_id,
        input.page_seq,
    )?;
    upsert_full_sync_stage_page_in_transaction(
        &transaction,
        account_id,
        input,
        staged_message_count,
    )?;
    let record = if let Some(update) = checkpoint_update {
        upsert_full_sync_checkpoint_in_transaction(&transaction, account_id, update)?
    } else {
        read_full_sync_checkpoint(&transaction, account_id)?
            .ok_or_else(|| anyhow!("full sync checkpoint disappeared while staging page chunk"))?
    };
    transaction.commit()?;
    Ok(record)
}

pub(super) fn stage_full_sync_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    page_seq: Option<i64>,
    messages: &[GmailMessageUpsertInput],
) -> Result<()> {
    stage_messages_in_transaction(
        transaction,
        account_id,
        messages,
        SyncStageTables {
            message_table: "gmail_full_sync_stage_messages",
            label_table: "gmail_full_sync_stage_message_labels",
            attachment_table: "gmail_full_sync_stage_attachments",
            page_message_table: Some("gmail_full_sync_stage_page_messages"),
        },
        page_seq,
    )
}

pub(super) struct SyncStageTables<'a> {
    pub(super) message_table: &'a str,
    pub(super) label_table: &'a str,
    pub(super) attachment_table: &'a str,
    pub(super) page_message_table: Option<&'a str>,
}

pub(super) fn stage_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    messages: &[GmailMessageUpsertInput],
    tables: SyncStageTables<'_>,
    page_seq: Option<i64>,
) -> Result<()> {
    let mut upsert_message = transaction.prepare_cached(
        &format!(
            "INSERT INTO {} (
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
             label_names_text
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
             label_names_text = excluded.label_names_text",
            tables.message_table
        ),
    )?;
    let mut delete_labels = transaction.prepare_cached(&format!(
        "DELETE FROM {}
         WHERE account_id = ?1
           AND message_id = ?2",
        tables.label_table
    ))?;
    let mut delete_attachments = transaction.prepare_cached(&format!(
        "DELETE FROM {}
         WHERE account_id = ?1
           AND message_id = ?2",
        tables.attachment_table
    ))?;
    let mut insert_label = transaction.prepare_cached(&format!(
        "INSERT INTO {} (
             account_id,
             message_id,
             label_id
         )
         VALUES (?1, ?2, ?3)",
        tables.label_table
    ))?;
    let mut insert_attachment = transaction.prepare_cached(&format!(
        "INSERT INTO {} (
             account_id,
             message_id,
             attachment_key,
             part_id,
             gmail_attachment_id,
             filename,
             mime_type,
             size_bytes,
             content_disposition,
             content_id,
             is_inline
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT (account_id, attachment_key) DO UPDATE SET
             message_id = excluded.message_id,
             part_id = excluded.part_id,
             gmail_attachment_id = excluded.gmail_attachment_id,
             filename = excluded.filename,
             mime_type = excluded.mime_type,
             size_bytes = excluded.size_bytes,
             content_disposition = excluded.content_disposition,
             content_id = excluded.content_id,
             is_inline = excluded.is_inline",
        tables.attachment_table
    ))?;
    let mut insert_page_message = match (page_seq, tables.page_message_table) {
        (Some(_), Some(page_message_table)) => Some(transaction.prepare_cached(&format!(
            "INSERT INTO {} (
                     account_id,
                     page_seq,
                     message_id
                 )
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (account_id, page_seq, message_id) DO NOTHING",
            page_message_table
        ))?),
        _ => None,
    };

    for message in messages {
        ensure!(
            message.account_id == account_id,
            "mailbox message account_id `{}` does not match batch account `{}`",
            message.account_id,
            account_id
        );

        upsert_message.execute(params![
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
            &message.label_names_text,
        ])?;
        delete_labels.execute(params![account_id, &message.message_id])?;
        delete_attachments.execute(params![account_id, &message.message_id])?;

        for label_id in unique_sorted_strings(&message.label_ids) {
            insert_label.execute(params![account_id, &message.message_id, label_id])?;
        }

        for attachment in &message.attachments {
            insert_attachment.execute(params![
                account_id,
                &message.message_id,
                &attachment.attachment_key,
                &attachment.part_id,
                &attachment.gmail_attachment_id,
                &attachment.filename,
                &attachment.mime_type,
                attachment.size_bytes,
                &attachment.content_disposition,
                &attachment.content_id,
                if attachment.is_inline { 1_i64 } else { 0_i64 },
            ])?;
        }

        if let (Some(page_seq), Some(insert_page_message)) =
            (page_seq, insert_page_message.as_mut())
        {
            insert_page_message.execute(params![account_id, page_seq, &message.message_id])?;
        }
    }

    drop(insert_page_message);
    drop(insert_attachment);
    drop(insert_label);
    drop(delete_attachments);
    drop(delete_labels);
    drop(upsert_message);
    Ok(())
}

pub(super) fn count_full_sync_stage_page_messages_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    page_seq: i64,
) -> Result<i64> {
    transaction
        .query_row(
            "SELECT COUNT(*)
             FROM gmail_full_sync_stage_page_messages
             WHERE account_id = ?1
               AND page_seq = ?2",
            params![account_id, page_seq],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(super) fn upsert_full_sync_stage_page_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    account_id: &str,
    input: &FullSyncStagePageInput,
    staged_message_count: i64,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO gmail_full_sync_stage_pages (
             account_id,
             page_seq,
             listed_count,
             staged_message_count,
             next_page_token,
             status,
             updated_at_epoch_s
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT (account_id, page_seq) DO UPDATE SET
             listed_count = excluded.listed_count,
             staged_message_count = excluded.staged_message_count,
             next_page_token = excluded.next_page_token,
             status = excluded.status,
             updated_at_epoch_s = excluded.updated_at_epoch_s",
        params![
            account_id,
            input.page_seq,
            input.listed_count,
            staged_message_count,
            &input.next_page_token,
            if input.page_complete {
                FULL_SYNC_STAGE_PAGE_STATUS_COMPLETE
            } else {
                FULL_SYNC_STAGE_PAGE_STATUS_PARTIAL
            },
            input.updated_at_epoch_s,
        ],
    )?;
    Ok(())
}
