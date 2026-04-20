DROP INDEX IF EXISTS attachment_export_events_attachment_key_idx;
DROP INDEX IF EXISTS attachment_export_events_account_exported_idx;
DROP TABLE IF EXISTS attachment_export_events;

DROP INDEX IF EXISTS gmail_message_attachments_vault_idx;
DROP INDEX IF EXISTS gmail_message_attachments_filename_idx;
DROP INDEX IF EXISTS gmail_message_attachments_message_rowid_idx;
DROP TABLE IF EXISTS gmail_message_attachments;
