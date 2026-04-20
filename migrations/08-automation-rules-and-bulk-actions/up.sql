ALTER TABLE gmail_messages
    ADD COLUMN list_id_header TEXT;

ALTER TABLE gmail_messages
    ADD COLUMN list_unsubscribe_header TEXT;

ALTER TABLE gmail_messages
    ADD COLUMN list_unsubscribe_post_header TEXT;

ALTER TABLE gmail_messages
    ADD COLUMN precedence_header TEXT;

ALTER TABLE gmail_messages
    ADD COLUMN auto_submitted_header TEXT;

CREATE TABLE automation_runs (
    run_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL,
    rule_file_path TEXT NOT NULL,
    rule_file_hash TEXT NOT NULL,
    selected_rule_ids_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('previewed', 'applied', 'apply_failed')),
    candidate_count INTEGER NOT NULL,
    created_at_epoch_s INTEGER NOT NULL,
    applied_at_epoch_s INTEGER,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX automation_runs_account_created_idx
    ON automation_runs (account_id, created_at_epoch_s DESC);

CREATE TABLE automation_run_candidates (
    candidate_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    internal_date_epoch_ms INTEGER NOT NULL,
    subject TEXT NOT NULL,
    from_header TEXT NOT NULL,
    from_address TEXT,
    snippet TEXT NOT NULL,
    label_names_json TEXT NOT NULL,
    attachment_count INTEGER NOT NULL,
    has_list_unsubscribe INTEGER NOT NULL CHECK (has_list_unsubscribe IN (0, 1)),
    list_id_header TEXT,
    list_unsubscribe_header TEXT,
    list_unsubscribe_post_header TEXT,
    precedence_header TEXT,
    auto_submitted_header TEXT,
    action_kind TEXT NOT NULL CHECK (action_kind IN ('archive', 'label', 'trash')),
    add_label_ids_json TEXT NOT NULL,
    add_label_names_json TEXT NOT NULL,
    remove_label_ids_json TEXT NOT NULL,
    remove_label_names_json TEXT NOT NULL,
    reason_json TEXT NOT NULL,
    apply_status TEXT CHECK (apply_status IN ('succeeded', 'failed')),
    applied_at_epoch_s INTEGER,
    apply_error TEXT,
    created_at_epoch_s INTEGER NOT NULL,
    UNIQUE (run_id, thread_id),
    FOREIGN KEY (run_id) REFERENCES automation_runs (run_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX automation_run_candidates_run_idx
    ON automation_run_candidates (run_id, internal_date_epoch_ms DESC, candidate_id ASC);

CREATE INDEX automation_run_candidates_account_thread_idx
    ON automation_run_candidates (account_id, thread_id);

CREATE TABLE automation_run_events (
    event_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    account_id TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at_epoch_s INTEGER NOT NULL,
    FOREIGN KEY (run_id) REFERENCES automation_runs (run_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES accounts (account_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX automation_run_events_run_idx
    ON automation_run_events (run_id, created_at_epoch_s ASC, event_id ASC);
