mod read;
#[cfg(test)]
mod tests;
mod types;
mod write;

pub(crate) use read::{
    get_automation_run_detail, inspect_automation, list_latest_thread_candidates,
};
pub(crate) use types::{
    AppendAutomationRunEventInput, AutomationActionKind, AutomationActionSnapshot,
    AutomationApplyStatus, AutomationDoctorReport, AutomationMatchReason,
    AutomationRunCandidateRecord, AutomationRunDetail, AutomationRunEventRecord,
    AutomationRunRecord, AutomationRunStatus, AutomationStoreReadError, AutomationStoreWriteError,
    AutomationThreadCandidate, CandidateApplyResultInput, CreateAutomationRunInput,
    FinalizeAutomationRunInput, NewAutomationRunCandidate,
};
pub(crate) use write::{
    append_automation_run_event, claim_automation_run_for_apply, create_automation_run,
    finalize_automation_run, record_candidate_apply_result,
};

fn is_missing_automation_table_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("no such table: automation_runs")
                || message.contains("no such table: automation_run_candidates")
                || message.contains("no such table: automation_run_events")
    )
}
