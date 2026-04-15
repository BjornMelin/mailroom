CREATE TABLE app_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

INSERT INTO app_metadata (key, value) VALUES
    ('app_name', 'mailroom'),
    ('schema_family', 'mailroom'),
    ('schema_kind', 'substrate');
