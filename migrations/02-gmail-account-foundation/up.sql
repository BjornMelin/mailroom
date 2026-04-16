CREATE TABLE accounts (
    account_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    email_address TEXT NOT NULL,
    history_id TEXT NOT NULL,
    messages_total INTEGER NOT NULL,
    threads_total INTEGER NOT NULL,
    access_scope TEXT NOT NULL,
    is_active INTEGER NOT NULL CHECK (is_active IN (0, 1)),
    created_at_epoch_s INTEGER NOT NULL,
    updated_at_epoch_s INTEGER NOT NULL,
    last_profile_refresh_epoch_s INTEGER NOT NULL
) STRICT;

CREATE UNIQUE INDEX accounts_provider_email_idx
    ON accounts (provider, email_address);

CREATE UNIQUE INDEX accounts_single_active_idx
    ON accounts (is_active)
    WHERE is_active = 1;
