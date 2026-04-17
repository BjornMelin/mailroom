CREATE TABLE gmail_labels (
    account_id TEXT NOT NULL,
    label_id TEXT NOT NULL,
    name TEXT NOT NULL,
    label_type TEXT NOT NULL,
    message_list_visibility TEXT,
    label_list_visibility TEXT,
    messages_total INTEGER,
    messages_unread INTEGER,
    threads_total INTEGER,
    threads_unread INTEGER,
    updated_at_epoch_s INTEGER NOT NULL,
    PRIMARY KEY (account_id, label_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_labels_account_name_idx
    ON gmail_labels (account_id, name);

CREATE TABLE gmail_messages (
    message_rowid INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    history_id TEXT NOT NULL,
    internal_date_epoch_ms INTEGER NOT NULL,
    snippet TEXT NOT NULL,
    subject TEXT NOT NULL,
    from_header TEXT NOT NULL,
    from_address TEXT,
    recipient_headers TEXT NOT NULL,
    to_header TEXT NOT NULL,
    cc_header TEXT NOT NULL,
    bcc_header TEXT NOT NULL,
    reply_to_header TEXT NOT NULL,
    size_estimate INTEGER NOT NULL,
    updated_at_epoch_s INTEGER NOT NULL,
    UNIQUE (account_id, message_id),
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_messages_account_internal_date_idx
    ON gmail_messages (account_id, internal_date_epoch_ms DESC);

CREATE INDEX gmail_messages_account_thread_idx
    ON gmail_messages (account_id, thread_id);

CREATE INDEX gmail_messages_account_from_address_idx
    ON gmail_messages (account_id, from_address);

CREATE TABLE gmail_message_labels (
    message_rowid INTEGER NOT NULL,
    label_id TEXT NOT NULL,
    PRIMARY KEY (message_rowid, label_id),
    FOREIGN KEY (message_rowid) REFERENCES gmail_messages (message_rowid) ON DELETE CASCADE
) STRICT;

CREATE INDEX gmail_message_labels_label_idx
    ON gmail_message_labels (label_id);

CREATE INDEX gmail_message_labels_message_rowid_idx
    ON gmail_message_labels (message_rowid);

CREATE TABLE gmail_sync_state (
    account_id TEXT PRIMARY KEY,
    cursor_history_id TEXT,
    bootstrap_query TEXT NOT NULL,
    last_sync_mode TEXT NOT NULL,
    last_sync_status TEXT NOT NULL,
    last_error TEXT,
    last_sync_epoch_s INTEGER NOT NULL,
    last_full_sync_success_epoch_s INTEGER,
    last_incremental_sync_success_epoch_s INTEGER,
    message_count INTEGER NOT NULL DEFAULT 0,
    label_count INTEGER NOT NULL DEFAULT 0,
    indexed_message_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE VIRTUAL TABLE gmail_message_search
USING fts5 (
    subject,
    from_header,
    recipient_headers,
    snippet,
    label_names,
    tokenize = "unicode61 remove_diacritics 1 tokenchars '-_@.'",
    content = '',
    contentless_delete = 1
);
