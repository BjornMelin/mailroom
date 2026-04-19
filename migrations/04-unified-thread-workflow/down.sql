DROP INDEX IF EXISTS thread_draft_attachments_revision_idx;
DROP TABLE IF EXISTS thread_draft_attachments;

DROP INDEX IF EXISTS thread_draft_revisions_workflow_idx;
DROP TABLE IF EXISTS thread_draft_revisions;

DROP INDEX IF EXISTS thread_workflow_events_account_thread_idx;
DROP INDEX IF EXISTS thread_workflow_events_workflow_idx;
DROP TABLE IF EXISTS thread_workflow_events;

DROP INDEX IF EXISTS thread_workflows_account_updated_idx;
DROP INDEX IF EXISTS thread_workflows_account_bucket_idx;
DROP INDEX IF EXISTS thread_workflows_account_stage_idx;
DROP TABLE IF EXISTS thread_workflows;
