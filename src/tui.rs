use crate::automation::{
    self, AutomationRolloutRequest, AutomationRulesSuggestRequest, AutomationRunRequest,
    DEFAULT_AUTOMATION_ROLLOUT_LIMIT, DEFAULT_AUTOMATION_RUN_LIMIT,
    DEFAULT_AUTOMATION_SUGGESTION_LIMIT, DEFAULT_AUTOMATION_SUGGESTION_MIN_THREAD_COUNT,
    DEFAULT_AUTOMATION_SUGGESTION_OLDER_THAN_DAYS, DEFAULT_AUTOMATION_SUGGESTION_SAMPLE_LIMIT,
};
use crate::config::ConfigReport;
use crate::doctor::DoctorReport;
use crate::mailbox::{self, SearchReport, SearchRequest};
use crate::store;
use crate::workflows::{WorkflowActionReport, WorkflowListReport, WorkflowShowReport};
use crate::{audit, workflows, workspace};
use anyhow::Result as AnyhowResult;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Tabs, Wrap};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use tokio::task::spawn_blocking;

const EVENT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const TUI_SEARCH_LIMIT: usize = 15;
const TUI_WORKFLOW_FALLBACK_WINDOW_LIMIT: usize = 50;
const VIEWS: [View; 5] = [
    View::Dashboard,
    View::Search,
    View::Workflows,
    View::Automation,
    View::Help,
];

pub async fn run(
    paths: &workspace::WorkspacePaths,
    config_report: ConfigReport,
    initial_search: Option<String>,
) -> AnyhowResult<()> {
    let snapshot = load_snapshot(paths, &config_report).await;
    let mut app = TuiApp::new(snapshot, initial_search);
    if app.has_search_input() {
        app.submit_search(&config_report).await;
        app.search_editing = false;
    }

    let mut terminal = ratatui::try_init()?;
    let _guard = TerminalGuard;

    loop {
        terminal.draw(|frame| render(frame, &mut app))?;

        if event::poll(EVENT_POLL_INTERVAL)? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            if handle_key(key, &mut app, paths, &config_report).await? {
                break;
            }
            if let Some(action) = app.pending_terminal_action.take() {
                ratatui::restore();
                let result = run_terminal_action(action, paths, &config_report).await;
                terminal = ratatui::try_init()?;
                app.handle_terminal_action_result(paths, &config_report, result)
                    .await;
            }
        }
    }

    Ok(())
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

#[derive(Debug)]
struct TuiApp {
    view: View,
    snapshot: Snapshot,
    search_input: String,
    search_editing: bool,
    search_report: Option<std::result::Result<SearchReport, String>>,
    workflow_selection: usize,
    workflow_scroll: usize,
    workflow_window_limit: usize,
    workflow_modal: Option<WorkflowModal>,
    workflow_detail_report: Option<std::result::Result<WorkflowShowReport, String>>,
    workflow_action_report: Option<std::result::Result<WorkflowActionReport, String>>,
    automation_modal: Option<AutomationModal>,
    automation_detail_report: Option<std::result::Result<automation::AutomationShowReport, String>>,
    automation_action_report: Option<std::result::Result<AutomationActionReport, String>>,
    automation_candidate_selection: usize,
    automation_candidate_scroll: usize,
    automation_candidate_window_limit: usize,
    pending_terminal_action: Option<TerminalAction>,
    status: String,
}

impl TuiApp {
    fn new(snapshot: Snapshot, initial_search: Option<String>) -> Self {
        let search_input = initial_search.unwrap_or_default();
        let has_search = !search_input.trim().is_empty();
        Self {
            view: if has_search {
                View::Search
            } else {
                View::Dashboard
            },
            snapshot,
            search_input,
            search_editing: false,
            search_report: None,
            workflow_selection: 0,
            workflow_scroll: 0,
            workflow_window_limit: TUI_WORKFLOW_FALLBACK_WINDOW_LIMIT,
            workflow_modal: None,
            workflow_detail_report: None,
            workflow_action_report: None,
            automation_modal: None,
            automation_detail_report: None,
            automation_action_report: None,
            automation_candidate_selection: 0,
            automation_candidate_scroll: 0,
            automation_candidate_window_limit: TUI_WORKFLOW_FALLBACK_WINDOW_LIMIT,
            pending_terminal_action: None,
            status: String::from("TUI ready: local workflow actions require explicit confirmation"),
        }
    }

    fn has_search_input(&self) -> bool {
        !self.search_input.trim().is_empty()
    }

    async fn submit_search(&mut self, config_report: &ConfigReport) {
        let terms = self.search_input.trim().to_owned();
        if terms.is_empty() {
            self.search_report = Some(Err(String::from("type search terms before pressing enter")));
            self.status = String::from("search skipped: empty query");
            return;
        }

        let request = SearchRequest {
            terms: terms.clone(),
            label: None,
            from_address: None,
            after: None,
            before: None,
            limit: TUI_SEARCH_LIMIT,
        };
        match mailbox::search_read_only(config_report, request).await {
            Ok(report) => {
                let count = report.results.len();
                self.search_report = Some(Ok(report));
                self.status = format!("search complete: {count} local hits for \"{terms}\"");
            }
            Err(error) => {
                self.search_report = Some(Err(error_chain(&error)));
                self.status = String::from("search failed");
            }
        }
    }

    async fn refresh(&mut self, paths: &workspace::WorkspacePaths, config_report: &ConfigReport) {
        let selected_thread_id = self.selected_thread_id();
        self.snapshot = load_snapshot(paths, config_report).await;
        self.restore_workflow_selection_by_thread_id(selected_thread_id.as_deref());
        self.status = String::from("reports refreshed");
    }

    fn next_view(&mut self) {
        let next = (self.view.index() + 1) % VIEWS.len();
        self.view = VIEWS[next];
        self.search_editing = false;
        self.workflow_modal = None;
        self.automation_modal = None;
    }

    fn previous_view(&mut self) {
        let previous = self.view.index().checked_sub(1).unwrap_or(VIEWS.len() - 1);
        self.view = VIEWS[previous];
        self.search_editing = false;
        self.workflow_modal = None;
        self.automation_modal = None;
    }

    fn workflow_rows(&self) -> &[store::workflows::WorkflowRecord] {
        self.snapshot
            .workflows
            .as_ref()
            .map(|report| report.workflows.as_slice())
            .unwrap_or(&[])
    }

    fn selected_workflow(&self) -> Option<&store::workflows::WorkflowRecord> {
        self.workflow_rows().get(self.workflow_selection)
    }

    fn selected_loaded_detail(&self) -> Option<&WorkflowShowReport> {
        let selected_thread_id = self.selected_workflow()?.thread_id.as_str();
        self.workflow_detail_report
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .filter(|report| report.detail.workflow.thread_id == selected_thread_id)
    }

    fn selected_thread_id(&self) -> Option<String> {
        self.selected_workflow()
            .map(|workflow| workflow.thread_id.clone())
    }

    fn visible_workflow_rows(
        &self,
    ) -> impl Iterator<Item = (usize, &store::workflows::WorkflowRecord)> {
        self.workflow_rows()
            .iter()
            .enumerate()
            .skip(self.workflow_scroll)
            .take(self.workflow_window_limit)
    }

    fn normalize_workflow_selection(&mut self) {
        let row_count = self.workflow_rows().len();
        if row_count == 0 {
            self.update_workflow_selection(0);
            self.workflow_scroll = 0;
        } else if self.workflow_selection >= row_count {
            self.update_workflow_selection(row_count - 1);
        }
        self.ensure_workflow_selection_visible();
    }

    fn ensure_workflow_selection_visible(&mut self) {
        let workflow_window_limit = self.workflow_window_limit.max(1);
        let row_count = self.workflow_rows().len();
        if row_count <= workflow_window_limit {
            self.workflow_scroll = 0;
            return;
        }

        if self.workflow_selection < self.workflow_scroll {
            self.workflow_scroll = self.workflow_selection;
        } else if self.workflow_selection >= self.workflow_scroll + workflow_window_limit {
            self.workflow_scroll = self.workflow_selection + 1 - workflow_window_limit;
        }
    }

    fn set_workflow_window_limit(&mut self, workflow_window_limit: usize) {
        self.workflow_window_limit = workflow_window_limit.max(1);
        self.ensure_workflow_selection_visible();
    }

    fn restore_workflow_selection_by_thread_id(&mut self, selected_thread_id: Option<&str>) {
        self.workflow_detail_report = None;
        self.workflow_action_report = None;
        if let Some(thread_id) = selected_thread_id {
            if let Some(index) = self
                .workflow_rows()
                .iter()
                .position(|workflow| workflow.thread_id == thread_id)
            {
                self.update_workflow_selection(index);
            } else {
                self.update_workflow_selection(0);
            }
        }
        self.normalize_workflow_selection();
    }

    fn update_workflow_selection(&mut self, workflow_selection: usize) {
        if self.workflow_selection != workflow_selection {
            self.workflow_selection = workflow_selection;
            self.workflow_detail_report = None;
            self.workflow_action_report = None;
        }
    }

    fn select_next_workflow(&mut self) {
        let row_count = self.workflow_rows().len();
        if row_count == 0 {
            self.update_workflow_selection(0);
            self.workflow_scroll = 0;
            return;
        }
        self.update_workflow_selection((self.workflow_selection + 1) % row_count);
        self.ensure_workflow_selection_visible();
    }

    fn select_previous_workflow(&mut self) {
        let row_count = self.workflow_rows().len();
        if row_count == 0 {
            self.update_workflow_selection(0);
            self.workflow_scroll = 0;
            return;
        }
        self.update_workflow_selection(
            self.workflow_selection
                .checked_sub(1)
                .unwrap_or(row_count - 1),
        );
        self.ensure_workflow_selection_visible();
    }

    fn open_workflow_modal(&mut self, modal: WorkflowModal) {
        if self.selected_workflow().is_none() {
            self.status = String::from("workflow action unavailable: no workflow row selected");
            return;
        }
        self.workflow_modal = Some(modal);
        self.status = String::from("workflow confirmation active; Enter confirms, Esc cancels");
    }

    fn open_triage_modal(&mut self) {
        let bucket = self
            .selected_workflow()
            .and_then(|workflow| workflow.triage_bucket)
            .unwrap_or(store::workflows::TriageBucket::NeedsReplySoon);
        self.open_workflow_modal(WorkflowModal::Triage { bucket });
    }

    fn open_promote_modal(&mut self) {
        let target = self
            .selected_workflow()
            .map(|workflow| match workflow.current_stage {
                store::workflows::WorkflowStage::ReadyToSend => {
                    store::workflows::WorkflowStage::ReadyToSend
                }
                _ => store::workflows::WorkflowStage::FollowUp,
            })
            .unwrap_or(store::workflows::WorkflowStage::FollowUp);
        self.open_workflow_modal(WorkflowModal::Promote { target });
    }

    fn open_snooze_modal(&mut self) {
        let until = self
            .selected_workflow()
            .and_then(|workflow| workflow.snoozed_until_epoch_s)
            .map(format_epoch_day_utc)
            .unwrap_or_default();
        self.open_workflow_modal(WorkflowModal::Snooze { until });
    }

    fn open_draft_start_modal(&mut self) {
        self.open_workflow_modal(WorkflowModal::DraftStart { reply_all: false });
    }

    async fn inspect_selected_workflow(&mut self, config_report: &ConfigReport) {
        let Some(thread_id) = self.selected_thread_id() else {
            self.workflow_detail_report = None;
            self.status = String::from("workflow inspect skipped: no workflow row selected");
            return;
        };

        match workflows::show_workflow(config_report, thread_id).await {
            Ok(report) => {
                let has_draft = report.detail.current_draft.is_some();
                self.workflow_detail_report = Some(Ok(report));
                self.status = if has_draft {
                    String::from("current draft detail loaded")
                } else {
                    String::from("workflow detail loaded: no current draft")
                };
            }
            Err(error) => {
                self.workflow_detail_report = Some(Err(error.to_string()));
                self.status = String::from("workflow detail load failed");
            }
        }
    }

    async fn open_draft_body_modal(&mut self, config_report: &ConfigReport) {
        self.inspect_selected_workflow(config_report).await;
        let Some(detail) = self.selected_loaded_detail() else {
            return;
        };
        let Some(draft) = &detail.detail.current_draft else {
            self.status = String::from("draft body unavailable: start a draft first");
            return;
        };
        self.open_workflow_modal(WorkflowModal::DraftBody {
            body_text: draft.revision.body_text.clone(),
        });
    }

    async fn open_draft_send_modal(&mut self, config_report: &ConfigReport) {
        self.inspect_selected_workflow(config_report).await;
        let Some(detail) = self.selected_loaded_detail() else {
            return;
        };
        let Some(_draft) = &detail.detail.current_draft else {
            self.status = String::from("draft send unavailable: start a draft first");
            return;
        };
        if detail.detail.workflow.gmail_draft_id.is_none() {
            self.status = String::from("draft send unavailable: no synced Gmail draft id");
            return;
        }
        self.open_workflow_modal(WorkflowModal::DraftSend {
            confirm_text: String::new(),
        });
    }

    fn open_cleanup_modal(&mut self, action: store::workflows::CleanupAction) {
        let active_field = if action == store::workflows::CleanupAction::Label {
            CleanupField::AddLabels
        } else {
            CleanupField::Confirm
        };
        self.open_workflow_modal(WorkflowModal::Cleanup {
            action,
            execute: false,
            add_labels: String::new(),
            remove_labels: String::new(),
            active_field,
            confirm_text: String::new(),
        });
    }

    async fn confirm_workflow_modal(
        &mut self,
        paths: &workspace::WorkspacePaths,
        config_report: &ConfigReport,
    ) {
        if let Some(WorkflowModal::Snooze { until }) = &self.workflow_modal
            && let Some(error) = snooze_until_validation_error(until)
        {
            self.workflow_action_report = Some(Err(error.to_owned()));
            self.status = error.to_owned();
            return;
        }
        if let Some(WorkflowModal::DraftSend { confirm_text }) = &self.workflow_modal
            && confirm_text != "SEND"
        {
            self.workflow_action_report = Some(Err(String::from(
                "draft send requires typing SEND before Enter",
            )));
            self.status = String::from("draft send blocked: type SEND before Enter");
            return;
        }
        if let Some(WorkflowModal::Cleanup {
            execute: true,
            confirm_text,
            ..
        }) = &self.workflow_modal
            && confirm_text != "APPLY"
        {
            self.workflow_action_report = Some(Err(String::from(
                "cleanup execute requires typing APPLY before Enter",
            )));
            self.status = String::from("cleanup execute blocked: type APPLY before Enter");
            return;
        }

        let Some(modal) = self.workflow_modal.take() else {
            return;
        };
        let Some(thread_id) = self.selected_thread_id() else {
            self.status = String::from("workflow action skipped: no workflow row selected");
            return;
        };
        let selected_thread_id = thread_id.clone();
        let result = match modal {
            WorkflowModal::Triage { bucket } => {
                workflows::set_triage(config_report, thread_id, bucket, None).await
            }
            WorkflowModal::Promote { target } => {
                workflows::promote_workflow(config_report, thread_id, target).await
            }
            WorkflowModal::Snooze { until } => {
                if until.trim().is_empty() {
                    workflows::clear_workflow_snooze(config_report, thread_id).await
                } else {
                    workflows::snooze_workflow(
                        config_report,
                        thread_id,
                        Some(until.trim().to_owned()),
                    )
                    .await
                }
            }
            WorkflowModal::DraftStart { reply_all } => {
                let reply_mode = if reply_all {
                    store::workflows::ReplyMode::ReplyAll
                } else {
                    store::workflows::ReplyMode::Reply
                };
                workflows::draft_start(config_report, thread_id, reply_mode).await
            }
            WorkflowModal::DraftBody { body_text } => {
                workflows::draft_body_set(config_report, thread_id, body_text).await
            }
            WorkflowModal::DraftSend { .. } => {
                workflows::draft_send(config_report, thread_id).await
            }
            WorkflowModal::Cleanup {
                action,
                execute,
                add_labels,
                remove_labels,
                ..
            } => match action {
                store::workflows::CleanupAction::Archive => {
                    workflows::cleanup_archive(config_report, thread_id, execute).await
                }
                store::workflows::CleanupAction::Trash => {
                    workflows::cleanup_trash(config_report, thread_id, execute).await
                }
                store::workflows::CleanupAction::Label => {
                    workflows::cleanup_label(
                        config_report,
                        thread_id,
                        execute,
                        split_label_names(&add_labels),
                        split_label_names(&remove_labels),
                    )
                    .await
                }
            },
        };

        match result {
            Ok(report) => {
                let action = report.action.to_string();
                self.workflow_action_report = Some(Ok(report));
                self.refresh_after_workflow_action(paths, config_report, Some(&selected_thread_id))
                    .await;
                self.status = format!("workflow action complete: {action}");
            }
            Err(error) => {
                self.workflow_action_report = Some(Err(error.to_string()));
                self.status = String::from("workflow action failed");
            }
        }
    }

    async fn refresh_after_workflow_action(
        &mut self,
        paths: &workspace::WorkspacePaths,
        config_report: &ConfigReport,
        selected_thread_id: Option<&str>,
    ) {
        self.snapshot = load_snapshot(paths, config_report).await;
        if self.search_report.is_some() && self.has_search_input() {
            self.submit_search(config_report).await;
        }
        if let Some(thread_id) = selected_thread_id
            && let Some(index) = self
                .workflow_rows()
                .iter()
                .position(|workflow| workflow.thread_id == thread_id)
        {
            self.workflow_selection = index;
            self.inspect_selected_workflow(config_report).await;
        }
        self.normalize_workflow_selection();
    }

    fn loaded_automation_detail(&self) -> Option<&automation::AutomationShowReport> {
        self.automation_detail_report
            .as_ref()
            .and_then(|result| result.as_ref().ok())
    }

    fn loaded_automation_run_id(&self) -> Option<i64> {
        self.loaded_automation_detail()
            .map(|report| report.detail.run.run_id)
    }

    fn visible_automation_candidates(
        &self,
    ) -> impl Iterator<Item = (usize, &store::automation::AutomationRunCandidateRecord)> {
        self.loaded_automation_detail()
            .map(|report| report.detail.candidates.as_slice())
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .skip(self.automation_candidate_scroll)
            .take(self.automation_candidate_window_limit)
    }

    fn selected_automation_candidate(
        &self,
    ) -> Option<&store::automation::AutomationRunCandidateRecord> {
        self.loaded_automation_detail().and_then(|report| {
            report
                .detail
                .candidates
                .get(self.automation_candidate_selection)
        })
    }

    fn normalize_automation_candidate_selection(&mut self) {
        let candidate_count = self
            .loaded_automation_detail()
            .map(|report| report.detail.candidates.len())
            .unwrap_or_default();
        if candidate_count == 0 {
            self.automation_candidate_selection = 0;
            self.automation_candidate_scroll = 0;
        } else if self.automation_candidate_selection >= candidate_count {
            self.automation_candidate_selection = candidate_count - 1;
        }
        self.ensure_automation_candidate_selection_visible();
    }

    fn ensure_automation_candidate_selection_visible(&mut self) {
        let window_limit = self.automation_candidate_window_limit.max(1);
        let candidate_count = self
            .loaded_automation_detail()
            .map(|report| report.detail.candidates.len())
            .unwrap_or_default();
        if candidate_count <= window_limit {
            self.automation_candidate_scroll = 0;
            return;
        }

        if self.automation_candidate_selection < self.automation_candidate_scroll {
            self.automation_candidate_scroll = self.automation_candidate_selection;
        } else if self.automation_candidate_selection
            >= self.automation_candidate_scroll + window_limit
        {
            self.automation_candidate_scroll =
                self.automation_candidate_selection + 1 - window_limit;
        }
    }

    fn set_automation_candidate_window_limit(&mut self, window_limit: usize) {
        self.automation_candidate_window_limit = window_limit.max(1);
        self.ensure_automation_candidate_selection_visible();
    }

    fn select_next_automation_candidate(&mut self) {
        let candidate_count = self
            .loaded_automation_detail()
            .map(|report| report.detail.candidates.len())
            .unwrap_or_default();
        if candidate_count == 0 {
            self.automation_candidate_selection = 0;
            self.automation_candidate_scroll = 0;
            return;
        }
        self.automation_candidate_selection =
            (self.automation_candidate_selection + 1) % candidate_count;
        self.ensure_automation_candidate_selection_visible();
    }

    fn select_previous_automation_candidate(&mut self) {
        let candidate_count = self
            .loaded_automation_detail()
            .map(|report| report.detail.candidates.len())
            .unwrap_or_default();
        if candidate_count == 0 {
            self.automation_candidate_selection = 0;
            self.automation_candidate_scroll = 0;
            return;
        }
        self.automation_candidate_selection = self
            .automation_candidate_selection
            .checked_sub(1)
            .unwrap_or(candidate_count - 1);
        self.ensure_automation_candidate_selection_visible();
    }

    async fn refresh_automation(&mut self, config_report: &ConfigReport) {
        self.snapshot.automation = automation::rollout_read_only(
            config_report,
            AutomationRolloutRequest {
                rule_ids: Vec::new(),
                limit: DEFAULT_AUTOMATION_ROLLOUT_LIMIT,
            },
        )
        .await
        .map_err(|error| error_chain(&error));
    }

    async fn validate_automation_rules(&mut self, config_report: &ConfigReport) {
        match automation::validate_rules(config_report).await {
            Ok(report) => {
                let enabled = report.enabled_rule_count;
                let total = report.rule_count;
                self.automation_action_report =
                    Some(Ok(AutomationActionReport::RulesValidate(report)));
                self.refresh_automation(config_report).await;
                self.status = format!("automation rules valid: {enabled}/{total} enabled");
            }
            Err(error) => {
                self.automation_action_report = Some(Err(error.to_string()));
                self.status = String::from("automation rules validation failed");
            }
        }
    }

    async fn suggest_automation_rules(&mut self, config_report: &ConfigReport) {
        let request = AutomationRulesSuggestRequest {
            limit: DEFAULT_AUTOMATION_SUGGESTION_LIMIT,
            min_thread_count: DEFAULT_AUTOMATION_SUGGESTION_MIN_THREAD_COUNT,
            older_than_days: DEFAULT_AUTOMATION_SUGGESTION_OLDER_THAN_DAYS,
            sample_limit: DEFAULT_AUTOMATION_SUGGESTION_SAMPLE_LIMIT,
        };
        match automation::suggest_rules(config_report, request).await {
            Ok(report) => {
                let count = report.suggestion_count;
                self.automation_action_report =
                    Some(Ok(AutomationActionReport::RulesSuggest(Box::new(report))));
                self.status = format!("automation suggestions ready: {count} disabled starters");
            }
            Err(error) => {
                self.automation_action_report = Some(Err(error.to_string()));
                self.status = String::from("automation rules suggestion failed");
            }
        }
    }

    fn open_automation_run_modal(&mut self) {
        self.automation_modal = Some(AutomationModal::RunPreview {
            limit_text: DEFAULT_AUTOMATION_RUN_LIMIT.to_string(),
        });
        self.status = String::from("automation run preview confirmation active");
    }

    fn open_automation_show_modal(&mut self) {
        let run_id_text = self
            .loaded_automation_run_id()
            .map(|run_id| run_id.to_string())
            .unwrap_or_default();
        self.automation_modal = Some(AutomationModal::ShowRun { run_id_text });
        self.status = String::from("automation run id input active");
    }

    fn open_automation_apply_modal(&mut self) {
        let Some(report) = self.loaded_automation_detail() else {
            self.status = String::from("automation apply unavailable: load a persisted run first");
            return;
        };
        self.automation_modal = Some(AutomationModal::ApplyRun {
            run_id: report.detail.run.run_id,
            confirm_text: String::new(),
        });
        self.status = String::from("automation apply confirmation active; type APPLY to execute");
    }

    fn queue_automation_rules_editor(&mut self) {
        self.pending_terminal_action = Some(TerminalAction::EditAutomationRules);
        self.status = String::from("opening $EDITOR for .mailroom/automation.toml");
    }

    async fn confirm_automation_modal(&mut self, config_report: &ConfigReport) {
        if let Some(AutomationModal::ApplyRun { confirm_text, .. }) = &self.automation_modal
            && confirm_text != "APPLY"
        {
            self.automation_action_report = Some(Err(String::from(
                "automation apply requires typing APPLY before Enter",
            )));
            self.status = String::from("automation apply blocked: type APPLY before Enter");
            return;
        }

        let Some(modal) = self.automation_modal.take() else {
            return;
        };
        match modal {
            AutomationModal::RunPreview { limit_text } => {
                let Some(limit) = parse_positive_usize(&limit_text) else {
                    self.automation_modal = Some(AutomationModal::RunPreview { limit_text });
                    self.automation_action_report = Some(Err(String::from(
                        "automation run limit must be a positive integer",
                    )));
                    self.status = String::from("automation run blocked: invalid limit");
                    return;
                };
                let request = AutomationRunRequest {
                    rule_ids: Vec::new(),
                    limit,
                };
                match automation::run_preview(config_report, request).await {
                    Ok(report) => {
                        let run_id = report.detail.run.run_id;
                        let candidate_count = report.detail.candidates.len();
                        self.automation_detail_report =
                            Some(Ok(automation::AutomationShowReport {
                                detail: report.detail.clone(),
                            }));
                        self.automation_action_report =
                            Some(Ok(AutomationActionReport::RunPreview(Box::new(report))));
                        self.automation_candidate_selection = 0;
                        self.normalize_automation_candidate_selection();
                        self.refresh_automation(config_report).await;
                        self.status = format!(
                            "automation run {run_id} persisted with {candidate_count} candidates"
                        );
                    }
                    Err(error) => {
                        self.automation_action_report = Some(Err(error.to_string()));
                        self.status = String::from("automation run creation failed");
                    }
                }
            }
            AutomationModal::ShowRun { run_id_text } => {
                let Some(run_id) = parse_positive_i64(&run_id_text) else {
                    self.automation_modal = Some(AutomationModal::ShowRun { run_id_text });
                    self.automation_action_report = Some(Err(String::from(
                        "automation run id must be a positive integer",
                    )));
                    self.status = String::from("automation show blocked: invalid run id");
                    return;
                };
                match automation::show_run(config_report, run_id).await {
                    Ok(report) => {
                        let candidate_count = report.detail.candidates.len();
                        self.automation_detail_report = Some(Ok(report));
                        self.automation_candidate_selection = 0;
                        self.normalize_automation_candidate_selection();
                        self.status = format!(
                            "automation run {run_id} loaded with {candidate_count} candidates"
                        );
                    }
                    Err(error) => {
                        self.automation_detail_report = Some(Err(error.to_string()));
                        self.status = String::from("automation run load failed");
                    }
                }
            }
            AutomationModal::ApplyRun { run_id, .. } => {
                match automation::apply_run(config_report, run_id, true).await {
                    Ok(report) => {
                        let applied = report.applied_candidate_count;
                        let failed = report.failed_candidate_count;
                        self.automation_detail_report =
                            Some(Ok(automation::AutomationShowReport {
                                detail: report.detail.clone(),
                            }));
                        self.automation_action_report =
                            Some(Ok(AutomationActionReport::Apply(Box::new(report))));
                        self.automation_candidate_selection = 0;
                        self.normalize_automation_candidate_selection();
                        self.refresh_automation(config_report).await;
                        self.status = format!(
                            "automation run {run_id} applied: {applied} succeeded, {failed} failed"
                        );
                    }
                    Err(error) => {
                        self.automation_action_report = Some(Err(error.to_string()));
                        self.status = String::from("automation apply failed");
                    }
                }
            }
        }
    }

    async fn handle_terminal_action_result(
        &mut self,
        _paths: &workspace::WorkspacePaths,
        config_report: &ConfigReport,
        result: std::result::Result<TerminalActionReport, String>,
    ) {
        match result {
            Ok(TerminalActionReport::AutomationRulesEdited { path }) => {
                match automation::validate_rules(config_report).await {
                    Ok(report) => {
                        let enabled = report.enabled_rule_count;
                        let total = report.rule_count;
                        self.automation_action_report =
                            Some(Ok(AutomationActionReport::RulesValidate(report)));
                        self.refresh_automation(config_report).await;
                        self.status =
                            format!("automation rules edited and valid: {enabled}/{total} enabled");
                    }
                    Err(error) => {
                        self.automation_action_report = Some(Err(format!(
                            "edited rules file {} did not validate: {error}",
                            path.display()
                        )));
                        self.status = String::from("automation rules edit returned invalid TOML");
                    }
                }
            }
            Err(error) => {
                self.automation_action_report = Some(Err(error));
                self.status = String::from("automation rules editor failed");
            }
        }
    }
}

#[derive(Debug, Clone)]
enum AutomationActionReport {
    RulesValidate(automation::AutomationRulesValidateReport),
    RulesSuggest(Box<automation::AutomationRulesSuggestReport>),
    RunPreview(Box<automation::AutomationRunPreviewReport>),
    Apply(Box<automation::AutomationApplyReport>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AutomationModal {
    RunPreview { limit_text: String },
    ShowRun { run_id_text: String },
    ApplyRun { run_id: i64, confirm_text: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalAction {
    EditAutomationRules,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TerminalActionReport {
    AutomationRulesEdited { path: PathBuf },
}

impl AutomationModal {
    fn title(&self) -> &'static str {
        match self {
            Self::RunPreview { .. } => "Create automation review snapshot",
            Self::ShowRun { .. } => "Load automation run",
            Self::ApplyRun { .. } => "Confirm automation apply",
        }
    }

    fn action_summary(&self) -> String {
        match self {
            Self::RunPreview { limit_text } => {
                format!(
                    "persist preview run from enabled rules, limit {}",
                    limit_text.trim()
                )
            }
            Self::ShowRun { run_id_text } => {
                format!(
                    "load persisted automation run {}",
                    blank_label_summary(run_id_text)
                )
            }
            Self::ApplyRun { run_id, .. } => {
                format!("apply persisted automation run {run_id}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkflowModal {
    Triage {
        bucket: store::workflows::TriageBucket,
    },
    Promote {
        target: store::workflows::WorkflowStage,
    },
    Snooze {
        until: String,
    },
    DraftStart {
        reply_all: bool,
    },
    DraftBody {
        body_text: String,
    },
    DraftSend {
        confirm_text: String,
    },
    Cleanup {
        action: store::workflows::CleanupAction,
        execute: bool,
        add_labels: String,
        remove_labels: String,
        active_field: CleanupField,
        confirm_text: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupField {
    AddLabels,
    RemoveLabels,
    Confirm,
}

impl WorkflowModal {
    fn title(&self) -> &'static str {
        match self {
            Self::Triage { .. } => "Confirm triage set",
            Self::Promote { .. } => "Confirm workflow promote",
            Self::Snooze { .. } => "Confirm workflow snooze",
            Self::DraftStart { .. } => "Confirm draft start",
            Self::DraftBody { .. } => "Confirm draft body edit",
            Self::DraftSend { .. } => "Confirm draft send",
            Self::Cleanup { execute: false, .. } => "Confirm cleanup preview",
            Self::Cleanup { execute: true, .. } => "Confirm cleanup execute",
        }
    }

    fn action_summary(&self) -> String {
        match self {
            Self::Triage { bucket } => format!("set triage bucket to {bucket}"),
            Self::Promote { target } => format!("promote workflow to {target}"),
            Self::Snooze { until } if until.trim().is_empty() => {
                String::from("clear workflow snooze")
            }
            Self::Snooze { until } => format!("snooze workflow until {}", until.trim()),
            Self::DraftStart { reply_all: true } => String::from("start reply-all Gmail draft"),
            Self::DraftStart { reply_all: false } => String::from("start reply Gmail draft"),
            Self::DraftBody { body_text } => {
                format!("replace draft body ({} chars)", body_text.chars().count())
            }
            Self::DraftSend { .. } => String::from("send current Gmail draft"),
            Self::Cleanup {
                action,
                execute,
                add_labels,
                remove_labels,
                ..
            } => {
                let mode = if *execute { "execute" } else { "preview" };
                if *action == store::workflows::CleanupAction::Label {
                    format!(
                        "{mode} label cleanup (add: {}; remove: {})",
                        blank_label_summary(add_labels),
                        blank_label_summary(remove_labels)
                    )
                } else {
                    format!("{mode} {action} cleanup")
                }
            }
        }
    }

    fn cycle_next(&mut self) {
        match self {
            Self::Triage { bucket } => *bucket = next_triage_bucket(*bucket),
            Self::Promote { target } => *target = next_tui_promote_target(*target),
            Self::Snooze { .. } => {}
            Self::DraftStart { reply_all } => *reply_all = !*reply_all,
            Self::DraftBody { .. } | Self::DraftSend { .. } => {}
            Self::Cleanup {
                action,
                execute,
                active_field,
                ..
            } => {
                if *action == store::workflows::CleanupAction::Label {
                    *active_field = if *execute {
                        active_field.next()
                    } else {
                        active_field.next_preview()
                    };
                }
            }
        }
    }

    fn cycle_previous(&mut self) {
        match self {
            Self::Triage { bucket } => *bucket = previous_triage_bucket(*bucket),
            Self::Promote { target } => *target = previous_tui_promote_target(*target),
            Self::Snooze { .. } => {}
            Self::DraftStart { reply_all } => *reply_all = !*reply_all,
            Self::DraftBody { .. } | Self::DraftSend { .. } => {}
            Self::Cleanup {
                action,
                execute,
                active_field,
                ..
            } => {
                if *action == store::workflows::CleanupAction::Label {
                    *active_field = if *execute {
                        active_field.previous()
                    } else {
                        active_field.previous_preview()
                    };
                }
            }
        }
    }

    fn toggle_cleanup_execute(&mut self) {
        if let Self::Cleanup {
            action,
            execute,
            active_field,
            confirm_text,
            ..
        } = self
        {
            *execute = !*execute;
            confirm_text.clear();
            *active_field = if *execute {
                CleanupField::Confirm
            } else if *action == store::workflows::CleanupAction::Label {
                CleanupField::AddLabels
            } else {
                CleanupField::Confirm
            };
        }
    }
}

impl CleanupField {
    const fn next_preview(self) -> Self {
        match self {
            Self::AddLabels | Self::Confirm => Self::RemoveLabels,
            Self::RemoveLabels => Self::AddLabels,
        }
    }

    const fn previous_preview(self) -> Self {
        match self {
            Self::AddLabels | Self::Confirm => Self::RemoveLabels,
            Self::RemoveLabels => Self::AddLabels,
        }
    }

    const fn next(self) -> Self {
        match self {
            Self::AddLabels => Self::RemoveLabels,
            Self::RemoveLabels => Self::Confirm,
            Self::Confirm => Self::AddLabels,
        }
    }

    const fn previous(self) -> Self {
        match self {
            Self::AddLabels => Self::Confirm,
            Self::RemoveLabels => Self::AddLabels,
            Self::Confirm => Self::RemoveLabels,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::AddLabels => "add",
            Self::RemoveLabels => "remove",
            Self::Confirm => "confirm",
        }
    }
}

fn accepts_plain_text_key(key: KeyEvent) -> bool {
    key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT
}

fn workflow_modal_uses_text_input(modal: &WorkflowModal) -> bool {
    match modal {
        WorkflowModal::Snooze { .. }
        | WorkflowModal::DraftBody { .. }
        | WorkflowModal::DraftSend { .. } => true,
        WorkflowModal::Cleanup {
            action: store::workflows::CleanupAction::Label,
            active_field: CleanupField::AddLabels | CleanupField::RemoveLabels,
            ..
        } => true,
        WorkflowModal::Cleanup {
            execute: true,
            active_field: CleanupField::Confirm,
            ..
        } => true,
        WorkflowModal::Triage { .. }
        | WorkflowModal::Promote { .. }
        | WorkflowModal::DraftStart { .. }
        | WorkflowModal::Cleanup { .. } => false,
    }
}

fn push_workflow_modal_text(modal: &mut WorkflowModal, value: char) {
    match modal {
        WorkflowModal::Snooze { until } => until.push(value),
        WorkflowModal::DraftBody { body_text } => body_text.push(value),
        WorkflowModal::DraftSend { confirm_text } => confirm_text.push(value),
        WorkflowModal::Cleanup {
            action,
            active_field,
            add_labels,
            remove_labels,
            confirm_text,
            ..
        } => match (*action, *active_field) {
            (store::workflows::CleanupAction::Label, CleanupField::AddLabels) => {
                add_labels.push(value);
            }
            (store::workflows::CleanupAction::Label, CleanupField::RemoveLabels) => {
                remove_labels.push(value);
            }
            (_, CleanupField::Confirm) => {
                confirm_text.push(value);
            }
            _ => {}
        },
        WorkflowModal::Triage { .. }
        | WorkflowModal::Promote { .. }
        | WorkflowModal::DraftStart { .. } => {}
    }
}

fn pop_workflow_modal_text(modal: &mut WorkflowModal) {
    match modal {
        WorkflowModal::Snooze { until } => {
            until.pop();
        }
        WorkflowModal::DraftBody { body_text } => {
            body_text.pop();
        }
        WorkflowModal::DraftSend { confirm_text } => {
            confirm_text.pop();
        }
        WorkflowModal::Cleanup {
            action,
            active_field,
            add_labels,
            remove_labels,
            confirm_text,
            ..
        } => match (*action, *active_field) {
            (store::workflows::CleanupAction::Label, CleanupField::AddLabels) => {
                add_labels.pop();
            }
            (store::workflows::CleanupAction::Label, CleanupField::RemoveLabels) => {
                remove_labels.pop();
            }
            (_, CleanupField::Confirm) => {
                confirm_text.pop();
            }
            _ => {}
        },
        WorkflowModal::Triage { .. }
        | WorkflowModal::Promote { .. }
        | WorkflowModal::DraftStart { .. } => {}
    }
}

fn automation_modal_uses_text_input(modal: &AutomationModal) -> bool {
    match modal {
        AutomationModal::RunPreview { .. }
        | AutomationModal::ShowRun { .. }
        | AutomationModal::ApplyRun { .. } => true,
    }
}

fn push_automation_modal_text(modal: &mut AutomationModal, value: char) {
    match modal {
        AutomationModal::RunPreview { limit_text }
        | AutomationModal::ShowRun {
            run_id_text: limit_text,
        }
        | AutomationModal::ApplyRun {
            confirm_text: limit_text,
            ..
        } => limit_text.push(value),
    }
}

fn pop_automation_modal_text(modal: &mut AutomationModal) {
    match modal {
        AutomationModal::RunPreview { limit_text }
        | AutomationModal::ShowRun {
            run_id_text: limit_text,
        }
        | AutomationModal::ApplyRun {
            confirm_text: limit_text,
            ..
        } => {
            limit_text.pop();
        }
    }
}

fn parse_positive_usize(value: &str) -> Option<usize> {
    value
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
}

fn parse_positive_i64(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok().filter(|value| *value > 0)
}

async fn run_terminal_action(
    action: TerminalAction,
    paths: &workspace::WorkspacePaths,
    config_report: &ConfigReport,
) -> std::result::Result<TerminalActionReport, String> {
    match action {
        TerminalAction::EditAutomationRules => {
            let paths = paths.clone();
            let runtime_root = config_report.config.workspace.runtime_root.clone();
            spawn_blocking(move || edit_automation_rules_blocking(&paths, &runtime_root))
                .await
                .map_err(|error| format!("rules editor task failed: {error}"))?
        }
    }
}

fn edit_automation_rules_blocking(
    paths: &workspace::WorkspacePaths,
    runtime_root: &Path,
) -> std::result::Result<TerminalActionReport, String> {
    paths
        .ensure_runtime_dirs()
        .map_err(|error| format!("failed to create runtime directories: {error}"))?;
    let rules_path = runtime_root.join("automation.toml");
    if !rules_path.exists() {
        seed_automation_rules_file(paths, &rules_path)?;
    }

    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| String::from("vi"));
    let shell = std::env::var("SHELL").unwrap_or_else(|_| String::from("sh"));
    let command = format!("{} {}", editor, shell_quote_path(&rules_path));
    let status = Command::new(shell)
        .arg("-lc")
        .arg(command)
        .status()
        .map_err(|error| {
            format!(
                "failed to launch editor for {}: {error}",
                rules_path.display()
            )
        })?;
    if !status.success() {
        return Err(format!(
            "editor exited unsuccessfully for {}: {status}",
            rules_path.display()
        ));
    }
    Ok(TerminalActionReport::AutomationRulesEdited { path: rules_path })
}

fn seed_automation_rules_file(
    paths: &workspace::WorkspacePaths,
    rules_path: &Path,
) -> std::result::Result<(), String> {
    let parent = rules_path
        .parent()
        .ok_or_else(|| format!("rules path has no parent: {}", rules_path.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    let template_path = paths
        .repo_root
        .join("config")
        .join("automation.example.toml");
    if template_path.exists() {
        std::fs::copy(&template_path, rules_path).map_err(|error| {
            format!(
                "failed to seed {} from {}: {error}",
                rules_path.display(),
                template_path.display()
            )
        })?;
    } else {
        std::fs::write(
            rules_path,
            "# Mailroom automation rules. Add [[rules]] entries here.\nrules = []\n",
        )
        .map_err(|error| format!("failed to write {}: {error}", rules_path.display()))?;
    }
    Ok(())
}

fn shell_quote_path(path: &Path) -> String {
    let value = path.display().to_string();
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[derive(Debug, Clone)]
struct Snapshot {
    doctor: std::result::Result<DoctorReport, String>,
    verification: std::result::Result<audit::VerificationAuditReport, String>,
    workflows: std::result::Result<WorkflowListReport, String>,
    automation: std::result::Result<automation::AutomationRolloutReport, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Dashboard,
    Search,
    Workflows,
    Automation,
    Help,
}

impl View {
    const fn index(self) -> usize {
        match self {
            Self::Dashboard => 0,
            Self::Search => 1,
            Self::Workflows => 2,
            Self::Automation => 3,
            Self::Help => 4,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Dashboard => "Dashboard",
            Self::Search => "Search",
            Self::Workflows => "Workflows",
            Self::Automation => "Automation",
            Self::Help => "Help",
        }
    }
}

async fn load_snapshot(
    paths: &workspace::WorkspacePaths,
    config_report: &ConfigReport,
) -> Snapshot {
    let report_paths = paths.clone();
    let report_config = config_report.clone();
    let report_task = spawn_blocking(move || {
        let doctor = DoctorReport::inspect(&report_paths, report_config.clone())
            .map_err(|error| error_chain(&error));
        let verification = audit::verification(&report_config).map_err(|error| error_chain(&error));
        (doctor, verification)
    });

    let (doctor, verification) = match report_task.await {
        Ok(reports) => reports,
        Err(error) => failed_diagnostic_reports(error),
    };

    let workflows = crate::workflows::list_workflows_read_only(config_report, None, None)
        .await
        .map_err(|error| error.to_string());
    let automation = automation::rollout_read_only(
        config_report,
        AutomationRolloutRequest {
            rule_ids: Vec::new(),
            limit: DEFAULT_AUTOMATION_ROLLOUT_LIMIT,
        },
    )
    .await
    .map_err(|error| error_chain(&error));

    Snapshot {
        doctor,
        verification,
        workflows,
        automation,
    }
}

fn failed_diagnostic_reports(
    error: impl std::fmt::Display,
) -> (
    std::result::Result<DoctorReport, String>,
    std::result::Result<audit::VerificationAuditReport, String>,
) {
    (
        Err(format!("diagnostic task failed: {error}")),
        Err(format!("verification task failed: {error}")),
    )
}

async fn handle_key(
    key: KeyEvent,
    app: &mut TuiApp,
    paths: &workspace::WorkspacePaths,
    config_report: &ConfigReport,
) -> AnyhowResult<bool> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Ok(true);
    }

    if app.workflow_modal.is_some() {
        return handle_workflow_modal_key(key, app, paths, config_report).await;
    }

    if app.automation_modal.is_some() {
        return handle_automation_modal_key(key, app, config_report).await;
    }

    if app.search_editing {
        return handle_search_key(key, app, config_report).await;
    }

    if app.view == View::Workflows {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                app.select_next_workflow();
                return Ok(false);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.select_previous_workflow();
                return Ok(false);
            }
            KeyCode::Char('t') => {
                app.open_triage_modal();
                return Ok(false);
            }
            KeyCode::Char('p') => {
                app.open_promote_modal();
                return Ok(false);
            }
            KeyCode::Char('z') => {
                app.open_snooze_modal();
                return Ok(false);
            }
            KeyCode::Char('i') => {
                app.inspect_selected_workflow(config_report).await;
                return Ok(false);
            }
            KeyCode::Char('d') => {
                app.open_draft_start_modal();
                return Ok(false);
            }
            KeyCode::Char('b') => {
                app.open_draft_body_modal(config_report).await;
                return Ok(false);
            }
            KeyCode::Char('s') => {
                app.open_draft_send_modal(config_report).await;
                return Ok(false);
            }
            KeyCode::Char('a') => {
                app.open_cleanup_modal(store::workflows::CleanupAction::Archive);
                return Ok(false);
            }
            KeyCode::Char('l') => {
                app.open_cleanup_modal(store::workflows::CleanupAction::Label);
                return Ok(false);
            }
            KeyCode::Char('x') => {
                app.open_cleanup_modal(store::workflows::CleanupAction::Trash);
                return Ok(false);
            }
            _ => {}
        }
    }

    if app.view == View::Automation {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                app.select_next_automation_candidate();
                return Ok(false);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.select_previous_automation_candidate();
                return Ok(false);
            }
            KeyCode::Char('v') => {
                app.validate_automation_rules(config_report).await;
                return Ok(false);
            }
            KeyCode::Char('g') => {
                app.suggest_automation_rules(config_report).await;
                return Ok(false);
            }
            KeyCode::Char('n') => {
                app.open_automation_run_modal();
                return Ok(false);
            }
            KeyCode::Char('o') => {
                app.open_automation_show_modal();
                return Ok(false);
            }
            KeyCode::Char('a') => {
                app.open_automation_apply_modal();
                return Ok(false);
            }
            KeyCode::Char('e') => {
                app.queue_automation_rules_editor();
                return Ok(false);
            }
            _ => {}
        }
    }

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => Ok(true),
        KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => {
            app.next_view();
            Ok(false)
        }
        KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
            app.previous_view();
            Ok(false)
        }
        KeyCode::Char('1') => {
            app.view = View::Dashboard;
            Ok(false)
        }
        KeyCode::Char('2') => {
            app.view = View::Search;
            Ok(false)
        }
        KeyCode::Char('3') => {
            app.view = View::Workflows;
            Ok(false)
        }
        KeyCode::Char('4') => {
            app.view = View::Automation;
            Ok(false)
        }
        KeyCode::Char('5') => {
            app.view = View::Help;
            Ok(false)
        }
        KeyCode::Char('/') => {
            app.view = View::Search;
            app.search_editing = true;
            app.status = String::from("search input active; enter runs a local read-only query");
            Ok(false)
        }
        KeyCode::Char('r') => {
            app.refresh(paths, config_report).await;
            Ok(false)
        }
        _ => Ok(false),
    }
}

async fn handle_workflow_modal_key(
    key: KeyEvent,
    app: &mut TuiApp,
    paths: &workspace::WorkspacePaths,
    config_report: &ConfigReport,
) -> AnyhowResult<bool> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('e') {
        if let Some(modal) = &mut app.workflow_modal {
            modal.toggle_cleanup_execute();
        }
        return Ok(false);
    }

    match key.code {
        KeyCode::Esc => {
            app.workflow_modal = None;
            app.status = String::from("workflow action canceled");
        }
        KeyCode::Enter => app.confirm_workflow_modal(paths, config_report).await,
        KeyCode::Tab | KeyCode::Right => {
            if let Some(modal) = &mut app.workflow_modal {
                modal.cycle_next();
            }
        }
        KeyCode::BackTab | KeyCode::Left => {
            if let Some(modal) = &mut app.workflow_modal {
                modal.cycle_previous();
            }
        }
        KeyCode::Backspace => {
            if let Some(modal) = &mut app.workflow_modal {
                pop_workflow_modal_text(modal);
            }
        }
        KeyCode::Char(value)
            if accepts_plain_text_key(key)
                && app
                    .workflow_modal
                    .as_ref()
                    .is_some_and(workflow_modal_uses_text_input) =>
        {
            if let Some(modal) = &mut app.workflow_modal {
                push_workflow_modal_text(modal, value);
            }
        }
        KeyCode::Char('q') => {
            app.workflow_modal = None;
            app.status = String::from("workflow action canceled");
        }
        KeyCode::Char('1') => match &mut app.workflow_modal {
            Some(WorkflowModal::Triage { bucket }) => {
                *bucket = store::workflows::TriageBucket::Urgent;
            }
            Some(WorkflowModal::Snooze { until }) => until.push('1'),
            _ => {}
        },
        KeyCode::Char('2') => match &mut app.workflow_modal {
            Some(WorkflowModal::Triage { bucket }) => {
                *bucket = store::workflows::TriageBucket::NeedsReplySoon;
            }
            Some(WorkflowModal::Snooze { until }) => until.push('2'),
            _ => {}
        },
        KeyCode::Char('3') => match &mut app.workflow_modal {
            Some(WorkflowModal::Triage { bucket }) => {
                *bucket = store::workflows::TriageBucket::Waiting;
            }
            Some(WorkflowModal::Snooze { until }) => until.push('3'),
            _ => {}
        },
        KeyCode::Char('4') => match &mut app.workflow_modal {
            Some(WorkflowModal::Triage { bucket }) => {
                *bucket = store::workflows::TriageBucket::Fyi;
            }
            Some(WorkflowModal::Snooze { until }) => until.push('4'),
            _ => {}
        },
        KeyCode::Char('f') => match &mut app.workflow_modal {
            Some(WorkflowModal::Promote { target }) => {
                *target = store::workflows::WorkflowStage::FollowUp;
            }
            Some(WorkflowModal::Snooze { until }) => until.push('f'),
            _ => {}
        },
        KeyCode::Char('r') => match &mut app.workflow_modal {
            Some(WorkflowModal::Promote { target }) => {
                *target = store::workflows::WorkflowStage::ReadyToSend;
            }
            Some(WorkflowModal::Snooze { until }) => until.push('r'),
            _ => {}
        },
        KeyCode::Char(value) => {
            if let Some(modal) = &mut app.workflow_modal
                && accepts_plain_text_key(key)
            {
                push_workflow_modal_text(modal, value);
            }
        }
        _ => {}
    }
    Ok(false)
}

async fn handle_automation_modal_key(
    key: KeyEvent,
    app: &mut TuiApp,
    config_report: &ConfigReport,
) -> AnyhowResult<bool> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.automation_modal = None;
            app.status = String::from("automation action canceled");
        }
        KeyCode::Enter => app.confirm_automation_modal(config_report).await,
        KeyCode::Backspace => {
            if let Some(modal) = &mut app.automation_modal {
                pop_automation_modal_text(modal);
            }
        }
        KeyCode::Char(value)
            if accepts_plain_text_key(key)
                && app
                    .automation_modal
                    .as_ref()
                    .is_some_and(automation_modal_uses_text_input) =>
        {
            if let Some(modal) = &mut app.automation_modal {
                push_automation_modal_text(modal, value);
            }
        }
        _ => {}
    }
    Ok(false)
}

async fn handle_search_key(
    key: KeyEvent,
    app: &mut TuiApp,
    config_report: &ConfigReport,
) -> AnyhowResult<bool> {
    match key.code {
        KeyCode::Esc => {
            app.search_editing = false;
            app.status = String::from("search input inactive");
            Ok(false)
        }
        KeyCode::Enter => {
            app.search_editing = false;
            app.submit_search(config_report).await;
            Ok(false)
        }
        KeyCode::Backspace => {
            app.search_input.pop();
            Ok(false)
        }
        KeyCode::Char(value) => {
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT {
                app.search_input.push(value);
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn render(frame: &mut Frame<'_>, app: &mut TuiApp) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(2),
        ])
        .split(area);

    render_header(frame, chunks[0], app);
    match app.view {
        View::Dashboard => render_dashboard(frame, chunks[1], app),
        View::Search => render_search(frame, chunks[1], app),
        View::Workflows => render_workflows(frame, chunks[1], app),
        View::Automation => render_automation(frame, chunks[1], app),
        View::Help => render_help(frame, chunks[1]),
    }
    render_footer(frame, chunks[2], app);
    if let Some(modal) = &app.workflow_modal {
        render_workflow_modal(frame, area, app, modal);
    }
    if let Some(modal) = &app.automation_modal {
        render_automation_modal(frame, area, app, modal);
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let titles = VIEWS
        .iter()
        .map(|view| Line::from(Span::styled(view.label(), Style::default().fg(Color::Cyan))))
        .collect::<Vec<_>>();
    let tabs = Tabs::new(titles)
        .select(app.view.index())
        .block(Block::default().borders(Borders::ALL).title("mailroom"))
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, area);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let footer = Paragraph::new(Text::from(vec![Line::from(vec![
        Span::raw(
            "q quit | tab view | 1-5 jump | / search | r refresh | workflows: j/k t/p/z d/b/s a/l/x | automation: v/g/n/o/a/e | ",
        ),
        Span::styled(&app.status, Style::default().fg(Color::Yellow)),
    ])]));
    frame.render_widget(footer, area);
}

fn render_dashboard(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let mut left = Vec::new();
    left.push(Line::from(Span::styled(
        "Local readiness",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    match &app.snapshot.doctor {
        Ok(doctor) => {
            left.push(metric(
                "repo",
                doctor.workspace.repo_root.display().to_string(),
            ));
            left.push(metric(
                "runtime",
                bool_word(doctor.workspace.runtime_root_exists),
            ));
            left.push(metric("database", bool_word(doctor.store.database_exists)));
            left.push(metric("auth", bool_word(doctor.auth.configured)));
            left.push(metric(
                "account",
                doctor
                    .auth
                    .active_account
                    .as_ref()
                    .map(|account| account.email_address.as_str())
                    .unwrap_or("<none>"),
            ));
        }
        Err(error) => left.push(error_line(error)),
    }
    if let Ok(report) = &app.snapshot.verification {
        left.push(Line::default());
        left.push(metric("messages", report.store.message_count.to_string()));
        left.push(metric(
            "indexed",
            report.store.indexed_message_count.to_string(),
        ));
        left.push(metric("workflows", report.store.workflow_count.to_string()));
        left.push(metric(
            "automation runs",
            report.store.automation_run_count.to_string(),
        ));
    }
    frame.render_widget(
        Paragraph::new(Text::from(left))
            .block(Block::default().borders(Borders::ALL).title("Dashboard"))
            .wrap(Wrap { trim: true }),
        columns[0],
    );

    let mut right = Vec::new();
    right.push(Line::from(Span::styled(
        "Readiness flags",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    match &app.snapshot.verification {
        Ok(report) => {
            right.push(metric(
                "manual mutation",
                bool_word(report.readiness.manual_mutation_ready),
            ));
            right.push(metric(
                "sender tuning",
                bool_word(report.readiness.sender_rule_tuning_ready),
            ));
            right.push(metric(
                "list-header tuning",
                bool_word(report.readiness.list_header_rule_tuning_ready),
            ));
            right.push(metric(
                "draft canary",
                bool_word(report.readiness.draft_send_canary_ready),
            ));
            right.push(metric(
                "deep audit sync",
                if report.readiness.deep_audit_sync_recommended {
                    "recommended"
                } else {
                    "not needed"
                },
            ));
            render_messages(&mut right, "Warnings", &report.warnings, 4);
            render_messages(&mut right, "Next steps", &report.next_steps, 4);
        }
        Err(error) => right.push(error_line(error)),
    }
    frame.render_widget(
        Paragraph::new(Text::from(right))
            .block(Block::default().borders(Borders::ALL).title("Verification"))
            .wrap(Wrap { trim: true }),
        columns[1],
    );
}

fn render_search(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(6)])
        .split(area);

    let title = if app.search_editing {
        "Search input (editing)"
    } else {
        "Search input"
    };
    let input_style = if app.search_editing {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    };
    frame.render_widget(
        Paragraph::new(app.search_input.as_str())
            .style(input_style)
            .block(Block::default().borders(Borders::ALL).title(title)),
        chunks[0],
    );

    match &app.search_report {
        Some(Ok(report)) => render_search_table(frame, chunks[1], report),
        Some(Err(error)) => render_text_panel(frame, chunks[1], "Search", vec![error_line(error)]),
        None => render_text_panel(
            frame,
            chunks[1],
            "Search",
            vec![
                Line::from("Press /, type local mailbox terms, then press Enter."),
                Line::from("This view reads the SQLite FTS index only; it does not call Gmail."),
            ],
        ),
    }
}

fn render_search_table(frame: &mut Frame<'_>, area: Rect, report: &SearchReport) {
    let rows = report.results.iter().map(|result| {
        Row::new(vec![
            Cell::from(truncate(&result.subject, 44)),
            Cell::from(truncate(&result.from_header, 30)),
            Cell::from(result.thread_message_count.to_string()),
            Cell::from(truncate(&result.label_names.join(","), 28)),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Percentage(42),
            Constraint::Percentage(28),
            Constraint::Length(7),
            Constraint::Percentage(30),
        ],
    )
    .header(
        Row::new(vec!["Subject", "From", "Thread", "Labels"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Search results ({})", report.results.len())),
    );
    frame.render_widget(table, area);
}

fn render_workflows(frame: &mut Frame<'_>, area: Rect, app: &mut TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(area);
    app.set_workflow_window_limit(workflow_table_row_capacity(chunks[0]));

    match &app.snapshot.workflows {
        Ok(report) => {
            let rows = app.visible_workflow_rows().map(|(index, workflow)| {
                let marker = if index == app.workflow_selection {
                    ">"
                } else {
                    " "
                };
                let mut row = Row::new(vec![
                    Cell::from(marker),
                    Cell::from(workflow.workflow_id.to_string()),
                    Cell::from(workflow.current_stage.to_string()),
                    Cell::from(
                        workflow
                            .triage_bucket
                            .map(|bucket| bucket.to_string())
                            .unwrap_or_else(|| String::from("-")),
                    ),
                    Cell::from(truncate(&workflow.latest_message_subject, 44)),
                    Cell::from(truncate(&workflow.latest_message_from_header, 28)),
                ]);
                if index == app.workflow_selection {
                    row = row.style(
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::Cyan)
                            .add_modifier(Modifier::BOLD),
                    );
                }
                row
            });
            let table = Table::new(
                rows,
                [
                    Constraint::Length(1),
                    Constraint::Length(6),
                    Constraint::Length(14),
                    Constraint::Length(18),
                    Constraint::Percentage(42),
                    Constraint::Percentage(26),
                ],
            )
            .header(
                Row::new(vec!["", "ID", "Stage", "Bucket", "Subject", "From"])
                    .style(Style::default().add_modifier(Modifier::BOLD)),
            )
            .block(Block::default().borders(Borders::ALL).title(format!(
                "Workflows ({}, showing {}-{}) - j/k select, t/p/z workflow, d/b/s draft, a/l/x cleanup",
                report.workflows.len(),
                app.workflow_scroll.saturating_add(1).min(report.workflows.len()),
                (app.workflow_scroll + app.workflow_window_limit).min(report.workflows.len()),
            )));
            frame.render_widget(table, chunks[0]);
            render_workflow_detail(frame, chunks[1], app);
        }
        Err(error) => render_text_panel(frame, area, "Workflows", vec![error_line(error)]),
    }
}

fn render_workflow_detail(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let mut lines = Vec::new();
    if let Some(workflow) = app.selected_workflow() {
        lines.push(Line::from(Span::styled(
            "Selected workflow",
            Style::default().add_modifier(Modifier::BOLD),
        )));
        lines.push(metric("thread", truncate(&workflow.thread_id, 36)));
        lines.push(metric("stage", workflow.current_stage.to_string()));
        lines.push(metric(
            "bucket",
            workflow
                .triage_bucket
                .map(|bucket| bucket.to_string())
                .unwrap_or_else(|| String::from("-")),
        ));
        lines.push(metric(
            "snoozed_until_epoch_s",
            workflow
                .snoozed_until_epoch_s
                .map(|value| value.to_string())
                .unwrap_or_else(|| String::from("-")),
        ));
        lines.push(metric(
            "subject",
            truncate(&workflow.latest_message_subject, 48),
        ));
        lines.push(metric(
            "from",
            truncate(&workflow.latest_message_from_header, 42),
        ));
        if !workflow.note.trim().is_empty() {
            lines.push(metric("note", truncate(&workflow.note, 72)));
        }
        if let Some(report) = app.selected_loaded_detail() {
            lines.push(Line::default());
            render_current_draft_summary(&mut lines, report.detail.current_draft.as_ref());
        } else if let Some(Err(error)) = &app.workflow_detail_report {
            lines.push(Line::default());
            lines.push(error_line(error));
        }
        lines.push(Line::default());
        lines.push(Line::from("Actions require confirmation:"));
        lines.push(Line::from(
            "t triage | p promote | z snooze/clear | i inspect draft",
        ));
        lines.push(Line::from("d start draft | b edit body | s send draft"));
        lines.push(Line::from("a archive | l label cleanup | x trash cleanup"));
    } else {
        lines.push(Line::from("No workflow row selected."));
        lines.push(Line::from(
            "Create workflow state with `triage set` from the CLI first.",
        ));
    }

    if let Some(result) = &app.workflow_action_report {
        lines.push(Line::default());
        lines.push(Line::from(Span::styled(
            "Latest action",
            Style::default().add_modifier(Modifier::BOLD),
        )));
        match result {
            Ok(report) => {
                lines.push(metric("action", report.action.to_string()));
                lines.push(metric("stage", report.workflow.current_stage.to_string()));
                lines.push(metric(
                    "bucket",
                    report
                        .workflow
                        .triage_bucket
                        .map(|bucket| bucket.to_string())
                        .unwrap_or_else(|| String::from("-")),
                ));
                if let Some(preview) = &report.cleanup_preview {
                    lines.push(metric("cleanup_action", preview.action.to_string()));
                    lines.push(metric("cleanup_execute", preview.execute.to_string()));
                    if !preview.add_label_names.is_empty() {
                        lines.push(metric("cleanup_add", preview.add_label_names.join(", ")));
                    }
                    if !preview.remove_label_names.is_empty() {
                        lines.push(metric(
                            "cleanup_remove",
                            preview.remove_label_names.join(", "),
                        ));
                    }
                }
                render_current_draft_summary(&mut lines, report.current_draft.as_ref());
                if let Some(sync_report) = &report.sync_report {
                    lines.push(metric("sync_mode", sync_report.mode.to_string()));
                    lines.push(metric(
                        "sync_messages_upserted",
                        sync_report.messages_upserted.to_string(),
                    ));
                    lines.push(metric(
                        "sync_messages_deleted",
                        sync_report.messages_deleted.to_string(),
                    ));
                }
            }
            Err(error) => lines.push(error_line(error)),
        }
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Workflow detail"),
            )
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_workflow_modal(frame: &mut Frame<'_>, area: Rect, app: &TuiApp, modal: &WorkflowModal) {
    let popup = centered_rect(72, workflow_modal_height(modal), area);
    frame.render_widget(Clear, popup);
    let mut lines = Vec::new();
    if let Some(workflow) = app.selected_workflow() {
        lines.push(metric("thread", truncate(&workflow.thread_id, 48)));
        lines.push(metric(
            "subject",
            truncate(&workflow.latest_message_subject, 58),
        ));
    }
    lines.push(metric("action", modal.action_summary()));
    match modal {
        WorkflowModal::Triage { .. } => {
            lines.push(Line::from(
                "Tab/Shift-Tab or 1-4 changes the triage bucket.",
            ));
            lines.push(Line::from(
                "1 urgent | 2 needs_reply_soon | 3 waiting | 4 fyi",
            ));
        }
        WorkflowModal::Promote { .. } => {
            lines.push(Line::from(
                "Tab/Shift-Tab changes the target. f follow_up | r ready_to_send",
            ));
            lines.push(Line::from("Closed promotion stays CLI-only in this issue."));
        }
        WorkflowModal::Snooze { until } => {
            lines.push(Line::from(
                "Type YYYY-MM-DD, or leave empty to clear snooze.",
            ));
            let until_value = if until.is_empty() {
                String::from("<clear>")
            } else {
                until.clone()
            };
            let until_style = if snooze_until_validation_error(until).is_some() {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };
            lines.push(Line::from(vec![
                Span::styled("until: ", Style::default().fg(Color::Cyan)),
                Span::styled(until_value, until_style),
            ]));
            if let Some(error) = snooze_until_validation_error(until) {
                lines.push(error_line(error));
            }
        }
        WorkflowModal::DraftStart { reply_all } => {
            lines.push(Line::from(
                "Tab/Shift-Tab toggles reply vs reply-all before creating a Gmail draft.",
            ));
            lines.push(metric(
                "reply_mode",
                if *reply_all { "reply_all" } else { "reply" },
            ));
        }
        WorkflowModal::DraftBody { body_text } => {
            lines.push(Line::from(
                "Type a plain-text body. Enter replaces the current Gmail draft body.",
            ));
            lines.push(metric("body_chars", body_text.chars().count().to_string()));
            lines.push(metric("body", truncate(body_text, 96)));
        }
        WorkflowModal::DraftSend { confirm_text } => {
            lines.push(Line::from(Span::styled(
                "This sends the current Gmail draft. Type SEND exactly, then Enter.",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )));
            if let Some(report) = app.selected_loaded_detail() {
                render_current_draft_summary(&mut lines, report.detail.current_draft.as_ref());
            } else {
                lines.push(Line::from(
                    "Press i to inspect the current draft before sending.",
                ));
            }
            lines.push(metric("confirm", confirm_text.clone()));
        }
        WorkflowModal::Cleanup {
            action,
            execute,
            add_labels,
            remove_labels,
            active_field,
            confirm_text,
        } => {
            lines.push(Line::from(
                "Ctrl-E toggles preview/execute. Execute mutates Gmail after confirmation.",
            ));
            lines.push(metric("mode", if *execute { "execute" } else { "preview" }));
            lines.push(metric("cleanup_action", action.to_string()));
            if *action == store::workflows::CleanupAction::Label {
                lines.push(Line::from(
                    "Tab/Shift-Tab switches add/remove/confirm fields; labels are comma-separated.",
                ));
                lines.push(metric("active_field", active_field.label()));
                lines.push(metric("add_labels", blank_label_summary(add_labels)));
                lines.push(metric("remove_labels", blank_label_summary(remove_labels)));
            }
            if *execute {
                lines.push(Line::from(Span::styled(
                    "Type APPLY exactly, then Enter to execute.",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )));
                lines.push(metric("confirm", confirm_text.clone()));
            }
        }
    }
    lines.push(Line::default());
    lines.push(Line::from("Enter confirm | Esc cancel | Ctrl-C quit"));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title(modal.title()))
            .wrap(Wrap { trim: true }),
        popup,
    );
}

fn workflow_modal_height(modal: &WorkflowModal) -> u16 {
    match modal {
        WorkflowModal::DraftSend { .. } => 18,
        WorkflowModal::Cleanup {
            action: store::workflows::CleanupAction::Label,
            ..
        } => 16,
        WorkflowModal::DraftBody { .. } => 13,
        WorkflowModal::Cleanup { execute: true, .. } => 14,
        _ => 11,
    }
}

fn render_automation_modal(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &TuiApp,
    modal: &AutomationModal,
) {
    let popup = centered_rect(76, automation_modal_height(modal), area);
    frame.render_widget(Clear, popup);
    let mut lines = Vec::new();
    lines.push(metric("action", modal.action_summary()));
    match modal {
        AutomationModal::RunPreview { limit_text } => {
            lines.push(Line::from(
                "Creates a persisted local review snapshot using automation run semantics.",
            ));
            lines.push(Line::from(
                "This does not mutate Gmail; apply remains a separate saved-run action.",
            ));
            lines.push(metric("limit", limit_text.clone()));
        }
        AutomationModal::ShowRun { run_id_text } => {
            lines.push(Line::from(
                "Loads a persisted run snapshot for candidate inspection.",
            ));
            lines.push(metric("run_id", run_id_text.clone()));
        }
        AutomationModal::ApplyRun {
            run_id,
            confirm_text,
        } => {
            lines.push(Line::from(Span::styled(
                "This mutates Gmail using the saved snapshot only. Type APPLY exactly, then Enter.",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            )));
            lines.push(metric("run_id", run_id.to_string()));
            if let Some(report) = app.loaded_automation_detail() {
                lines.push(metric(
                    "candidate_count",
                    report.detail.candidates.len().to_string(),
                ));
                lines.push(metric(
                    "action_mix",
                    automation_action_mix(&report.detail).join(", "),
                ));
            }
            if let Ok(rollout) = &app.snapshot.automation
                && !rollout.blocked_rule_ids.is_empty()
            {
                lines.push(metric("blocked_rules", rollout.blocked_rule_ids.join(", ")));
            }
            lines.push(Line::from(Span::styled(
                "Gmail warning: archive, label, or trash operations are applied to real threads.",
                Style::default().fg(Color::Red),
            )));
            lines.push(metric("confirm", confirm_text.clone()));
        }
    }
    lines.push(Line::default());
    lines.push(Line::from("Enter confirm | Esc/q cancel | Ctrl-C quit"));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title(modal.title()))
            .wrap(Wrap { trim: true }),
        popup,
    );
}

fn automation_modal_height(modal: &AutomationModal) -> u16 {
    match modal {
        AutomationModal::ApplyRun { .. } => 15,
        _ => 10,
    }
}

fn render_automation(frame: &mut Frame<'_>, area: Rect, app: &mut TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Min(6),
        ])
        .split(area);

    match app.snapshot.automation.clone() {
        Ok(report) => {
            let mut summary = vec![
                metric("selected rules", report.selected_rule_count.to_string()),
                metric("candidates", report.candidate_count.to_string()),
                metric("blocked rules", report.blocked_rule_ids.len().to_string()),
            ];
            render_messages(&mut summary, "Blockers", &report.blockers, 3);
            render_messages(&mut summary, "Warnings", &report.warnings, 3);
            frame.render_widget(
                Paragraph::new(Text::from(summary))
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Rollout readiness"),
                    )
                    .wrap(Wrap { trim: true }),
                chunks[0],
            );

            render_automation_action_panel(frame, chunks[1], app);
            render_automation_run_panel(frame, chunks[2], app, &report);
        }
        Err(error) => render_text_panel(frame, area, "Automation", vec![error_line(&error)]),
    }
}

fn render_automation_run_panel(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &mut TuiApp,
    rollout: &automation::AutomationRolloutReport,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(area);
    app.set_automation_candidate_window_limit(workflow_table_row_capacity(chunks[0]));

    if let Some(Ok(report)) = &app.automation_detail_report {
        let rows = app
            .visible_automation_candidates()
            .map(|(index, candidate)| {
                let marker = if index == app.automation_candidate_selection {
                    ">"
                } else {
                    " "
                };
                Row::new(vec![
                    Cell::from(marker),
                    Cell::from(candidate.candidate_id.to_string()),
                    Cell::from(truncate(&candidate.rule_id, 20)),
                    Cell::from(candidate.action.kind.to_string()),
                    Cell::from(truncate(&candidate.subject, 36)),
                ])
            });
        let table = Table::new(
            rows,
            [
                Constraint::Length(1),
                Constraint::Length(8),
                Constraint::Percentage(26),
                Constraint::Length(10),
                Constraint::Percentage(50),
            ],
        )
        .header(
            Row::new(vec!["", "ID", "Rule", "Action", "Subject"])
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(Block::default().borders(Borders::ALL).title(format!(
            "Saved run {} ({}, {} candidates)",
            report.detail.run.run_id, report.detail.run.status, report.detail.run.candidate_count
        )));
        frame.render_widget(table, chunks[0]);
        render_automation_candidate_detail(frame, chunks[1], app);
    } else {
        let rows = rollout.candidates.iter().take(50).map(|candidate| {
            Row::new(vec![
                Cell::from(truncate(&candidate.rule_id, 24)),
                Cell::from(candidate.action_kind.clone()),
                Cell::from(truncate(&candidate.subject, 42)),
                Cell::from(truncate(
                    candidate.from_address.as_deref().unwrap_or("-"),
                    28,
                )),
            ])
        });
        let table = Table::new(
            rows,
            [
                Constraint::Percentage(24),
                Constraint::Length(10),
                Constraint::Percentage(42),
                Constraint::Percentage(24),
            ],
        )
        .header(
            Row::new(vec!["Rule", "Action", "Subject", "From"])
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Rollout candidate preview"),
        );
        frame.render_widget(table, chunks[0]);
        render_text_panel(
            frame,
            chunks[1],
            "Automation keys",
            vec![
                Line::from("v validate rules | g suggest disabled starters"),
                Line::from("n create persisted preview run | o load run"),
                Line::from("j/k move saved-run candidates after load"),
                Line::from("a apply loaded run after typing APPLY"),
                Line::from("e edit rules in $EDITOR, then validate"),
            ],
        );
    }
}

fn render_automation_action_panel(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let mut lines = Vec::new();
    match &app.automation_action_report {
        Some(Ok(report)) => render_automation_action_report(&mut lines, report),
        Some(Err(error)) => lines.push(error_line(error)),
        None => {
            lines.push(Line::from(
                "Actions: v validate | g suggest | n run snapshot | o show run | a apply loaded run | e edit rules",
            ));
            lines.push(Line::from(
                "Apply always targets a persisted run snapshot; rollout preview is never applied directly.",
            ));
        }
    }
    render_text_panel(frame, area, "Automation action", lines);
}

fn render_automation_action_report(
    lines: &mut Vec<Line<'static>>,
    report: &AutomationActionReport,
) {
    match report {
        AutomationActionReport::RulesValidate(report) => {
            lines.push(Line::from("rules validation complete"));
            lines.push(metric("path", report.path.display().to_string()));
            lines.push(metric("rules", report.rule_count.to_string()));
            lines.push(metric("enabled", report.enabled_rule_count.to_string()));
            let ids = report
                .rules
                .iter()
                .take(4)
                .map(|rule| rule.id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            if !ids.is_empty() {
                lines.push(metric("sample", ids));
            }
        }
        AutomationActionReport::RulesSuggest(report) => {
            lines.push(Line::from("disabled starter suggestions ready"));
            lines.push(metric(
                "rules_path",
                report.rules_path.display().to_string(),
            ));
            lines.push(metric("suggestions", report.suggestion_count.to_string()));
            for suggestion in report.suggestions.iter().take(3) {
                lines.push(Line::from(format!(
                    "- {} [{}]: {} threads",
                    truncate(&suggestion.rule_id, 32),
                    suggestion.confidence,
                    suggestion.matched_thread_count
                )));
            }
        }
        AutomationActionReport::RunPreview(report) => {
            lines.push(Line::from("persisted preview run created"));
            lines.push(metric("run_id", report.detail.run.run_id.to_string()));
            lines.push(metric(
                "candidates",
                report.detail.candidates.len().to_string(),
            ));
            lines.push(metric(
                "actions",
                automation_action_mix(&report.detail).join(", "),
            ));
        }
        AutomationActionReport::Apply(report) => {
            lines.push(Line::from("persisted run apply complete"));
            lines.push(metric("run_id", report.detail.run.run_id.to_string()));
            lines.push(metric(
                "applied",
                report.applied_candidate_count.to_string(),
            ));
            lines.push(metric("failed", report.failed_candidate_count.to_string()));
        }
    }
}

fn render_automation_candidate_detail(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let mut lines = Vec::new();
    if let Some(candidate) = app.selected_automation_candidate() {
        lines.push(metric("candidate_id", candidate.candidate_id.to_string()));
        lines.push(metric("rule_id", candidate.rule_id.clone()));
        lines.push(metric("thread_id", candidate.thread_id.clone()));
        lines.push(metric("action", candidate.action.kind.to_string()));
        lines.push(metric(
            "apply_status",
            candidate
                .apply_status
                .map(|status| status.to_string())
                .unwrap_or_else(|| String::from("pending")),
        ));
        if let Some(error) = &candidate.apply_error {
            lines.push(error_line(error));
        }
        lines.push(metric(
            "from",
            candidate.from_address.clone().unwrap_or_default(),
        ));
        lines.push(metric("subject", truncate(&candidate.subject, 60)));
        lines.push(metric("labels", candidate.label_names.join(", ")));
        lines.push(metric(
            "matched",
            automation_match_summary(&candidate.reason),
        ));
    } else if let Some(Err(error)) = &app.automation_detail_report {
        lines.push(error_line(error));
    } else {
        lines.push(Line::from("No persisted run candidate loaded."));
    }
    render_text_panel(frame, area, "Candidate detail", lines);
}

fn automation_action_mix(detail: &store::automation::AutomationRunDetail) -> Vec<String> {
    let mut archive = 0usize;
    let mut label = 0usize;
    let mut trash = 0usize;
    for candidate in &detail.candidates {
        match candidate.action.kind {
            store::automation::AutomationActionKind::Archive => archive += 1,
            store::automation::AutomationActionKind::Label => label += 1,
            store::automation::AutomationActionKind::Trash => trash += 1,
        }
    }
    let mut parts = Vec::new();
    if archive > 0 {
        parts.push(format!("archive={archive}"));
    }
    if label > 0 {
        parts.push(format!("label={label}"));
    }
    if trash > 0 {
        parts.push(format!("trash={trash}"));
    }
    if parts.is_empty() {
        parts.push(String::from("none"));
    }
    parts
}

fn automation_match_summary(reason: &store::automation::AutomationMatchReason) -> String {
    let mut parts = Vec::new();
    if let Some(from_address) = &reason.from_address {
        parts.push(format!("from={from_address}"));
    }
    if !reason.subject_terms.is_empty() {
        parts.push(format!("subject={}", reason.subject_terms.join(",")));
    }
    if !reason.label_names.is_empty() {
        parts.push(format!("labels={}", reason.label_names.join(",")));
    }
    if let Some(days) = reason.older_than_days {
        parts.push(format!("older_than_days={days}"));
    }
    if let Some(value) = reason.has_attachments {
        parts.push(format!("has_attachments={value}"));
    }
    if let Some(value) = reason.has_list_unsubscribe {
        parts.push(format!("has_list_unsubscribe={value}"));
    }
    if !reason.list_id_terms.is_empty() {
        parts.push(format!("list_id={}", reason.list_id_terms.join(",")));
    }
    if !reason.precedence_values.is_empty() {
        parts.push(format!("precedence={}", reason.precedence_values.join(",")));
    }
    if parts.is_empty() {
        String::from("-")
    } else {
        truncate(&parts.join("; "), 96)
    }
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    render_text_panel(
        frame,
        area,
        "Help",
        vec![
            Line::from("Operator shell"),
            Line::from(""),
            Line::from("1 Dashboard: auth, store, mailbox, and readiness summary."),
            Line::from("2 Search: run local SQLite FTS queries against synced mail."),
            Line::from("3 Workflows: confirm triage/promote/snooze, draft, and cleanup actions."),
            Line::from("4 Automation: validate rules, persist runs, inspect candidates, apply."),
            Line::from("5 Help: key bindings and safety posture."),
            Line::from(""),
            Line::from("Workflow keys: i inspect draft | d start | b body | s send."),
            Line::from("Cleanup keys: a archive | l label | x trash; Ctrl-E toggles execute."),
            Line::from("Draft send requires typing SEND; cleanup execute requires APPLY."),
            Line::from(
                "Automation keys: v validate | g suggest | n run | o show | a apply | e edit.",
            ),
            Line::from("Automation apply requires a persisted run and typing APPLY."),
            Line::from("No view applies live rollout output or exports attachments."),
        ],
    );
}

fn render_text_panel(frame: &mut Frame<'_>, area: Rect, title: &str, lines: Vec<Line<'static>>) {
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_messages(lines: &mut Vec<Line<'static>>, title: &str, messages: &[String], limit: usize) {
    if messages.is_empty() {
        return;
    }

    lines.push(Line::default());
    lines.push(Line::from(Span::styled(
        title.to_owned(),
        Style::default().add_modifier(Modifier::BOLD),
    )));
    lines.extend(
        messages
            .iter()
            .take(limit)
            .map(|message| Line::from(format!("- {}", truncate(message, 96)))),
    );
}

fn metric(name: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{name}: "), Style::default().fg(Color::Cyan)),
        Span::raw(value.into()),
    ])
}

fn error_line(error: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!("error: {error}"),
        Style::default().fg(Color::Red),
    ))
}

fn render_current_draft_summary(
    lines: &mut Vec<Line<'static>>,
    draft: Option<&store::workflows::DraftRevisionDetail>,
) {
    lines.push(Line::from(Span::styled(
        "Current draft",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    let Some(draft) = draft else {
        lines.push(Line::from("No current draft loaded for this workflow."));
        return;
    };
    lines.push(metric(
        "draft_revision_id",
        draft.revision.draft_revision_id.to_string(),
    ));
    lines.push(metric("reply_mode", draft.revision.reply_mode.to_string()));
    lines.push(metric(
        "to",
        truncate(&draft.revision.to_addresses.join(", "), 52),
    ));
    if !draft.revision.cc_addresses.is_empty() {
        lines.push(metric(
            "cc",
            truncate(&draft.revision.cc_addresses.join(", "), 52),
        ));
    }
    lines.push(metric("subject", truncate(&draft.revision.subject, 52)));
    lines.push(metric(
        "body_chars",
        draft.revision.body_text.chars().count().to_string(),
    ));
    if !draft.revision.body_text.trim().is_empty() {
        lines.push(metric("body", truncate(&draft.revision.body_text, 72)));
    }
    lines.push(metric("attachments", draft.attachments.len().to_string()));
}

fn split_label_names(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|label| !label.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn blank_label_summary(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        String::from("<none>")
    } else {
        trimmed.to_owned()
    }
}

fn bool_word(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn workflow_table_row_capacity(area: Rect) -> usize {
    // Table data rows are the outer area minus block borders and the header row.
    usize::from(area.height.saturating_sub(3)).max(1)
}

fn snooze_until_validation_error(value: &str) -> Option<&'static str> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let mut parts = value.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Some("invalid snooze date: use YYYY-MM-DD or clear the field");
    };
    if year.len() != 4
        || month.len() != 2
        || day.len() != 2
        || !year.chars().all(|character| character.is_ascii_digit())
        || !month.chars().all(|character| character.is_ascii_digit())
        || !day.chars().all(|character| character.is_ascii_digit())
    {
        return Some("invalid snooze date: use YYYY-MM-DD or clear the field");
    }

    let year = year.parse::<i64>().ok()?;
    let month = month.parse::<u32>().ok()?;
    let day = day.parse::<u32>().ok()?;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return Some("invalid snooze date: month must be 01-12"),
    };
    if day == 0 || day > max_day {
        return Some("invalid snooze date: day is out of range");
    }
    None
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn format_epoch_day_utc(epoch_s: i64) -> String {
    let days_since_unix_epoch = epoch_s.div_euclid(86_400);
    let (year, month, day) = civil_from_unix_days(days_since_unix_epoch);
    format!("{year:04}-{month:02}-{day:02}")
}

fn civil_from_unix_days(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    let adjusted_days = days_since_unix_epoch + 719_468;
    let era = if adjusted_days >= 0 {
        adjusted_days
    } else {
        adjusted_days - 146_096
    } / 146_097;
    let day_of_era = adjusted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

fn next_triage_bucket(value: store::workflows::TriageBucket) -> store::workflows::TriageBucket {
    use store::workflows::TriageBucket;
    match value {
        TriageBucket::Urgent => TriageBucket::NeedsReplySoon,
        TriageBucket::NeedsReplySoon => TriageBucket::Waiting,
        TriageBucket::Waiting => TriageBucket::Fyi,
        TriageBucket::Fyi => TriageBucket::Urgent,
    }
}

fn previous_triage_bucket(value: store::workflows::TriageBucket) -> store::workflows::TriageBucket {
    use store::workflows::TriageBucket;
    match value {
        TriageBucket::Urgent => TriageBucket::Fyi,
        TriageBucket::NeedsReplySoon => TriageBucket::Urgent,
        TriageBucket::Waiting => TriageBucket::NeedsReplySoon,
        TriageBucket::Fyi => TriageBucket::Waiting,
    }
}

fn next_tui_promote_target(
    value: store::workflows::WorkflowStage,
) -> store::workflows::WorkflowStage {
    match value {
        store::workflows::WorkflowStage::FollowUp => store::workflows::WorkflowStage::ReadyToSend,
        _ => store::workflows::WorkflowStage::FollowUp,
    }
}

fn previous_tui_promote_target(
    value: store::workflows::WorkflowStage,
) -> store::workflows::WorkflowStage {
    next_tui_promote_target(value)
}

fn centered_rect(percent_x: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(height.min(area.height)),
            Constraint::Fill(1),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    if max_chars == 0 {
        return String::new();
    }
    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }

    let keep = max_chars.saturating_sub(3);
    let mut output = value.chars().take(keep).collect::<String>();
    output.push_str("...");
    output
}

fn error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

#[cfg(test)]
mod tests {
    use super::{
        AutomationModal, CleanupField, DEFAULT_AUTOMATION_RUN_LIMIT, Snapshot, TUI_SEARCH_LIMIT,
        TerminalAction, TuiApp, View, WorkflowModal, automation_match_summary,
        failed_diagnostic_reports, format_epoch_day_utc, handle_key, load_snapshot, render,
        snooze_until_validation_error, truncate, workflow_table_row_capacity,
    };
    use crate::config;
    use crate::mailbox::{self, SearchRequest};
    use crate::store::automation::{
        AutomationActionKind, AutomationActionSnapshot, AutomationMatchReason,
        AutomationRunCandidateRecord, AutomationRunDetail, AutomationRunRecord,
        AutomationRunStatus,
    };
    use crate::store::workflows::{
        CleanupAction, DraftAttachmentRecord, DraftRevisionDetail, DraftRevisionRecord, ReplyMode,
        TriageBucket, WorkflowDetail, WorkflowRecord, WorkflowStage,
    };
    use crate::workspace::WorkspacePaths;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use tempfile::TempDir;

    #[test]
    fn truncate_preserves_short_values() {
        assert_eq!(truncate("short", 8), "short");
    }

    #[test]
    fn truncate_marks_shortened_values() {
        assert_eq!(truncate("abcdefghijkl", 6), "abc...");
    }

    #[test]
    fn seeded_search_opens_search_view() {
        let app = TuiApp::new(empty_snapshot(), Some(String::from("invoice")));
        assert_eq!(app.view, View::Search);
        assert!(!app.search_editing);
    }

    #[test]
    fn view_navigation_wraps() {
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.previous_view();
        assert_eq!(app.view, View::Help);
        app.next_view();
        assert_eq!(app.view, View::Dashboard);
    }

    #[test]
    fn workflow_selection_wraps_over_available_rows() {
        let mut app = TuiApp::new(snapshot_with_workflows(2), None);
        app.view = View::Workflows;

        app.select_previous_workflow();
        assert_eq!(app.selected_thread_id().as_deref(), Some("thread-2"));

        app.select_next_workflow();
        assert_eq!(app.selected_thread_id().as_deref(), Some("thread-1"));
    }

    #[test]
    fn workflow_selection_change_clears_stale_reports() {
        let mut app = TuiApp::new(snapshot_with_workflows(2), None);
        app.view = View::Workflows;
        app.workflow_detail_report = Some(Err(String::from("old detail error")));
        app.workflow_action_report = Some(Err(String::from("old action error")));

        app.select_next_workflow();

        assert_eq!(app.selected_thread_id().as_deref(), Some("thread-2"));
        assert!(app.workflow_detail_report.is_none());
        assert!(app.workflow_action_report.is_none());
    }

    #[test]
    fn workflow_refresh_restores_selection_by_thread_id_after_reorder() {
        let mut app = TuiApp::new(snapshot_with_workflows(3), None);
        app.view = View::Workflows;
        app.select_next_workflow();
        let selected_thread_id = app.selected_thread_id();
        app.workflow_detail_report = Some(Err(String::from("old detail error")));
        app.workflow_action_report = Some(Err(String::from("old action error")));
        app.snapshot.workflows.as_mut().unwrap().workflows.reverse();

        app.restore_workflow_selection_by_thread_id(selected_thread_id.as_deref());

        assert_eq!(app.selected_thread_id().as_deref(), Some("thread-2"));
        assert!(app.workflow_detail_report.is_none());
        assert!(app.workflow_action_report.is_none());
    }

    #[test]
    fn workflow_selection_scrolls_to_keep_selected_row_inside_rendered_viewport() {
        let visible_rows = workflow_table_row_capacity(ratatui::layout::Rect::new(0, 0, 59, 19));
        let mut app = TuiApp::new(snapshot_with_workflows(visible_rows + 2), None);
        app.view = View::Workflows;
        app.set_workflow_window_limit(visible_rows);

        for _ in 0..visible_rows {
            app.select_next_workflow();
        }

        assert_eq!(app.workflow_selection, visible_rows);
        assert_eq!(app.workflow_scroll, 1);

        let output = render_app(&mut app);
        assert!(output.contains(&format!("thread-{}", visible_rows + 1)));
        assert!(app.workflow_scroll > 0);
    }

    #[test]
    fn workflow_promote_modal_cycles_only_local_targets() {
        let mut modal = WorkflowModal::Promote {
            target: WorkflowStage::FollowUp,
        };

        modal.cycle_next();
        assert!(matches!(
            modal,
            WorkflowModal::Promote {
                target: WorkflowStage::ReadyToSend
            }
        ));

        modal.cycle_next();
        assert!(matches!(
            modal,
            WorkflowModal::Promote {
                target: WorkflowStage::FollowUp
            }
        ));
    }

    #[test]
    fn workflow_snooze_clear_modal_describes_stage_preserving_clear() {
        let modal = WorkflowModal::Snooze {
            until: String::new(),
        };

        assert_eq!(modal.action_summary(), "clear workflow snooze");
    }

    #[test]
    fn workflow_snooze_validation_accepts_empty_clear_and_valid_dates() {
        assert_eq!(snooze_until_validation_error(""), None);
        assert_eq!(snooze_until_validation_error("2026-05-09"), None);
        assert_eq!(snooze_until_validation_error("2028-02-29"), None);
    }

    #[test]
    fn workflow_snooze_validation_rejects_invalid_dates() {
        assert_eq!(
            snooze_until_validation_error("tomorrow"),
            Some("invalid snooze date: use YYYY-MM-DD or clear the field")
        );
        assert_eq!(
            snooze_until_validation_error("2026-13-09"),
            Some("invalid snooze date: month must be 01-12")
        );
        assert_eq!(
            snooze_until_validation_error("2026-02-29"),
            Some("invalid snooze date: day is out of range")
        );
    }

    #[tokio::test]
    async fn workflow_key_opens_and_cancels_triage_modal() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;

        let should_quit = handle_key(key(KeyCode::Char('t')), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(!should_quit);
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Triage {
                bucket: TriageBucket::Urgent
            })
        ));

        handle_key(key(KeyCode::Esc), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(app.workflow_modal.is_none());
        assert_eq!(app.status, "workflow action canceled");
    }

    #[tokio::test]
    async fn workflow_modals_seed_from_selected_state() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut snapshot = snapshot_with_workflows(1);
        let workflow = snapshot
            .workflows
            .as_mut()
            .unwrap()
            .workflows
            .first_mut()
            .unwrap();
        workflow.current_stage = WorkflowStage::ReadyToSend;
        workflow.triage_bucket = Some(TriageBucket::Waiting);
        workflow.snoozed_until_epoch_s = Some(0);
        let mut app = TuiApp::new(snapshot, None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('t')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Triage {
                bucket: TriageBucket::Waiting
            })
        ));

        app.workflow_modal = None;
        handle_key(key(KeyCode::Char('p')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Promote {
                target: WorkflowStage::ReadyToSend
            })
        ));

        app.workflow_modal = None;
        handle_key(key(KeyCode::Char('z')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Snooze { ref until }) if until == "1970-01-01"
        ));
    }

    #[test]
    fn epoch_day_format_uses_utc_calendar_day() {
        assert_eq!(format_epoch_day_utc(0), "1970-01-01");
        assert_eq!(format_epoch_day_utc(86_400), "1970-01-02");
    }

    #[tokio::test]
    async fn workflow_snooze_modal_captures_text_input() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('z')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        for character in "2026-05-09".chars() {
            handle_key(
                key(KeyCode::Char(character)),
                &mut app,
                &paths,
                &config_report,
            )
            .await
            .unwrap();
        }

        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Snooze { ref until }) if until == "2026-05-09"
        ));
    }

    #[tokio::test]
    async fn workflow_snooze_modal_blocks_invalid_date_before_service_call() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;
        app.workflow_modal = Some(WorkflowModal::Snooze {
            until: String::from("tomorrow"),
        });

        handle_key(key(KeyCode::Enter), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Snooze { ref until }) if until == "tomorrow"
        ));
        assert_eq!(
            app.status,
            "invalid snooze date: use YYYY-MM-DD or clear the field"
        );
        let output = render_app_with_size(&mut app, 140, 30);
        assert!(output.contains("error: invalid snooze date"));
    }

    #[tokio::test]
    async fn workflow_draft_start_modal_toggles_reply_all_and_cancels() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('d')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::DraftStart { reply_all: false })
        ));

        handle_key(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::empty()),
            &mut app,
            &paths,
            &config_report,
        )
        .await
        .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::DraftStart { reply_all: true })
        ));

        handle_key(key(KeyCode::Esc), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(app.workflow_modal.is_none());
        assert_eq!(app.status, "workflow action canceled");
    }

    #[tokio::test]
    async fn workflow_draft_body_modal_captures_text_and_cancels_without_action() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;
        app.workflow_modal = Some(WorkflowModal::DraftBody {
            body_text: String::new(),
        });

        for character in "Thanks".chars() {
            handle_key(
                key(KeyCode::Char(character)),
                &mut app,
                &paths,
                &config_report,
            )
            .await
            .unwrap();
        }
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::DraftBody { ref body_text }) if body_text == "Thanks"
        ));

        handle_key(key(KeyCode::Esc), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(app.workflow_modal.is_none());
        assert!(app.workflow_action_report.is_none());
    }

    #[tokio::test]
    async fn workflow_draft_send_requires_send_confirmation() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;
        app.workflow_modal = Some(WorkflowModal::DraftSend {
            confirm_text: String::new(),
        });

        handle_key(key(KeyCode::Enter), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::DraftSend { ref confirm_text }) if confirm_text.is_empty()
        ));
        assert_eq!(app.status, "draft send blocked: type SEND before Enter");
    }

    #[tokio::test]
    async fn workflow_cleanup_label_modal_captures_labels_and_requires_apply_for_execute() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('l')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        for character in "Important".chars() {
            handle_key(
                key(KeyCode::Char(character)),
                &mut app,
                &paths,
                &config_report,
            )
            .await
            .unwrap();
        }
        handle_key(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::empty()),
            &mut app,
            &paths,
            &config_report,
        )
        .await
        .unwrap();
        for character in "INBOX".chars() {
            handle_key(
                key(KeyCode::Char(character)),
                &mut app,
                &paths,
                &config_report,
            )
            .await
            .unwrap();
        }
        handle_key(ctrl_key('e'), &mut app, &paths, &config_report)
            .await
            .unwrap();
        handle_key(key(KeyCode::Enter), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Cleanup {
                action: CleanupAction::Label,
                execute: true,
                ref add_labels,
                ref remove_labels,
                ref confirm_text,
                ..
            }) if add_labels == "Important" && remove_labels == "INBOX" && confirm_text.is_empty()
        ));
        assert_eq!(
            app.status,
            "cleanup execute blocked: type APPLY before Enter"
        );
    }

    #[tokio::test]
    async fn workflow_cleanup_archive_preview_does_not_capture_hidden_text() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('a')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        handle_key(key(KeyCode::Char('q')), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(app.workflow_modal.is_none());
        assert_eq!(app.status, "workflow action canceled");
    }

    #[tokio::test]
    async fn workflow_cleanup_label_preview_cycles_visible_fields_only() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('l')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        handle_key(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::empty()),
            &mut app,
            &paths,
            &config_report,
        )
        .await
        .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Cleanup {
                active_field: CleanupField::RemoveLabels,
                execute: false,
                ..
            })
        ));

        handle_key(
            KeyEvent::new(KeyCode::Tab, KeyModifiers::empty()),
            &mut app,
            &paths,
            &config_report,
        )
        .await
        .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Cleanup {
                active_field: CleanupField::AddLabels,
                execute: false,
                ..
            })
        ));

        handle_key(ctrl_key('e'), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Cleanup {
                active_field: CleanupField::Confirm,
                execute: true,
                ..
            })
        ));

        handle_key(ctrl_key('e'), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.workflow_modal,
            Some(WorkflowModal::Cleanup {
                active_field: CleanupField::AddLabels,
                execute: false,
                ..
            })
        ));
    }

    #[test]
    fn workflows_render_loaded_current_draft_detail() {
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;
        app.workflow_detail_report = Some(Ok(crate::workflows::WorkflowShowReport {
            detail: sample_workflow_detail(),
        }));

        let output = render_app_with_size(&mut app, 140, 30);

        assert!(output.contains("Current draft"));
        assert!(output.contains("draft_revision_id: 7"));
        assert!(output.contains("subject: Re: Subject 1"));
        assert!(output.contains("body_chars: 14"));
    }

    #[tokio::test]
    async fn workflow_action_requires_selected_row() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(snapshot_with_workflows(0), None);
        app.view = View::Workflows;

        handle_key(key(KeyCode::Char('p')), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(app.workflow_modal.is_none());
        assert_eq!(
            app.status,
            "workflow action unavailable: no workflow row selected"
        );
    }

    #[test]
    fn failed_diagnostic_reports_preserve_task_failure_context() {
        let (doctor, verification) = failed_diagnostic_reports("worker panicked");

        assert_eq!(
            doctor.unwrap_err(),
            "diagnostic task failed: worker panicked"
        );
        assert_eq!(
            verification.unwrap_err(),
            "verification task failed: worker panicked"
        );
    }

    #[test]
    fn dashboard_renders_snapshot_errors() {
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.snapshot.doctor = Err(String::from("doctor failed"));
        app.snapshot.verification = Err(String::from("verification failed"));

        let output = render_app(&mut app);

        assert!(output.contains("error: doctor failed"));
        assert!(output.contains("error: verification failed"));
    }

    #[test]
    fn search_renders_search_errors() {
        let mut app = TuiApp::new(empty_snapshot(), Some(String::from("invoice")));
        app.search_report = Some(Err(String::from("search failed")));

        let output = render_app(&mut app);

        assert!(output.contains("error: search failed"));
    }

    #[test]
    fn workflows_render_report_errors() {
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Workflows;
        app.snapshot.workflows = Err(String::from("workflow report failed"));

        let output = render_app(&mut app);

        assert!(output.contains("error: workflow report failed"));
    }

    #[test]
    fn workflows_render_selected_detail_and_confirmation_modal() {
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;
        app.workflow_modal = Some(WorkflowModal::Triage {
            bucket: TriageBucket::Urgent,
        });

        let output = render_app(&mut app);

        assert!(output.contains("Workflow detail"));
        assert!(output.contains("thread-1"));
        assert!(output.contains("Confirm triage set"));
        assert!(output.contains("set triage bucket to urgent"));
        assert!(output.contains("Enter confirm"));
    }

    #[test]
    fn automation_renders_report_errors() {
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Automation;
        app.snapshot.automation = Err(String::from("automation report failed"));

        let output = render_app(&mut app);

        assert!(output.contains("error: automation report failed"));
    }

    #[tokio::test]
    async fn automation_run_modal_captures_limit_and_blocks_invalid_limit() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Automation;

        handle_key(key(KeyCode::Char('n')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.automation_modal,
            Some(AutomationModal::RunPreview { ref limit_text })
                if limit_text == &DEFAULT_AUTOMATION_RUN_LIMIT.to_string()
        ));
        for _ in 0..DEFAULT_AUTOMATION_RUN_LIMIT.to_string().len() {
            handle_key(
                KeyEvent::new(KeyCode::Backspace, KeyModifiers::empty()),
                &mut app,
                &paths,
                &config_report,
            )
            .await
            .unwrap();
        }
        handle_key(key(KeyCode::Char('0')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        handle_key(key(KeyCode::Enter), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(matches!(
            app.automation_modal,
            Some(AutomationModal::RunPreview { ref limit_text }) if limit_text == "0"
        ));
        assert_eq!(app.status, "automation run blocked: invalid limit");
    }

    #[tokio::test]
    async fn automation_apply_requires_loaded_run_and_apply_confirmation() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Automation;

        handle_key(key(KeyCode::Char('a')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(app.automation_modal.is_none());
        assert_eq!(
            app.status,
            "automation apply unavailable: load a persisted run first"
        );

        app.automation_detail_report = Some(Ok(crate::automation::AutomationShowReport {
            detail: sample_automation_run_detail(),
        }));
        handle_key(key(KeyCode::Char('a')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.automation_modal,
            Some(AutomationModal::ApplyRun {
                run_id: 42,
                ref confirm_text
            }) if confirm_text.is_empty()
        ));

        handle_key(key(KeyCode::Enter), &mut app, &paths, &config_report)
            .await
            .unwrap();
        assert!(matches!(
            app.automation_modal,
            Some(AutomationModal::ApplyRun {
                run_id: 42,
                ref confirm_text
            }) if confirm_text.is_empty()
        ));
        assert_eq!(
            app.status,
            "automation apply blocked: type APPLY before Enter"
        );
    }

    #[tokio::test]
    async fn automation_show_modal_rejects_invalid_run_id() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Automation;

        handle_key(key(KeyCode::Char('o')), &mut app, &paths, &config_report)
            .await
            .unwrap();
        handle_key(key(KeyCode::Enter), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(matches!(
            app.automation_modal,
            Some(AutomationModal::ShowRun { ref run_id_text }) if run_id_text.is_empty()
        ));
        assert_eq!(app.status, "automation show blocked: invalid run id");
    }

    #[tokio::test]
    async fn automation_editor_key_queues_terminal_action() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Automation;

        handle_key(key(KeyCode::Char('e')), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert_eq!(
            app.pending_terminal_action,
            Some(TerminalAction::EditAutomationRules)
        );
    }

    #[test]
    fn automation_renders_loaded_run_candidate_detail() {
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Automation;
        app.snapshot.automation = Ok(sample_automation_rollout_report());
        app.automation_detail_report = Some(Ok(crate::automation::AutomationShowReport {
            detail: sample_automation_run_detail(),
        }));

        let output = render_app(&mut app);

        assert!(output.contains("Saved run 42"));
        assert!(output.contains("Candidate detail"));
        assert!(output.contains("candidate_id: 7"));
        assert!(
            automation_match_summary(&sample_automation_run_detail().candidates[0].reason)
                .contains("from=sender@example.com")
        );
    }

    #[tokio::test]
    async fn snapshot_load_does_not_create_runtime_state() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();

        assert!(!paths.runtime_root.exists());
        let snapshot = load_snapshot(&paths, &config_report).await;

        assert!(snapshot.doctor.is_ok());
        assert!(snapshot.verification.is_ok());
        assert!(snapshot.workflows.is_err());
        assert!(!paths.runtime_root.exists());
    }

    #[tokio::test]
    async fn search_read_only_does_not_create_runtime_state() {
        let temp_dir = TempDir::new().unwrap();
        let paths = WorkspacePaths::from_repo_root(temp_dir.path().to_path_buf());
        let config_report = config::resolve(&paths).unwrap();

        let error = mailbox::search_read_only(
            &config_report,
            SearchRequest {
                terms: String::from("invoice"),
                label: None,
                from_address: None,
                after: None,
                before: None,
                limit: TUI_SEARCH_LIMIT,
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("no active Gmail account"));
        assert!(!paths.runtime_root.exists());
    }

    fn empty_snapshot() -> Snapshot {
        Snapshot {
            doctor: Err(String::from("not loaded")),
            verification: Err(String::from("not loaded")),
            workflows: Err(String::from("not loaded")),
            automation: Err(String::from("not loaded")),
        }
    }

    fn snapshot_with_workflows(count: usize) -> Snapshot {
        let mut snapshot = empty_snapshot();
        snapshot.workflows = Ok(crate::workflows::WorkflowListReport {
            stage: None,
            triage_bucket: None,
            workflows: (1..=count).map(sample_workflow).collect(),
        });
        snapshot
    }

    fn sample_automation_rollout_report() -> crate::automation::AutomationRolloutReport {
        crate::automation::AutomationRolloutReport {
            verification: crate::audit::VerificationAuditReport {
                account_id: Some(String::from("gmail:me@example.com")),
                authenticated: true,
                rules_file_path: "automation.toml".into(),
                rules_file_exists: true,
                bootstrap_query: None,
                bootstrap_recent_days: None,
                mailbox: None,
                store: crate::audit::VerificationStoreSummary {
                    database_exists: true,
                    schema_version: Some(16),
                    message_count: 1,
                    indexed_message_count: 1,
                    attachment_count: 0,
                    vaulted_attachment_count: 0,
                    attachment_export_count: 0,
                    workflow_count: 0,
                    automation_run_count: 1,
                },
                label_summary: crate::audit::VerificationLabelSummary {
                    total_label_count: 1,
                    empty_user_label_count: 0,
                    normalized_overlap_count: 0,
                    numbered_overlap_count: 0,
                },
                readiness: crate::audit::VerificationReadiness {
                    manual_mutation_ready: true,
                    sender_rule_tuning_ready: true,
                    list_header_rule_tuning_ready: true,
                    draft_send_canary_ready: false,
                    deep_audit_sync_recommended: false,
                },
                warnings: Vec::new(),
                next_steps: Vec::new(),
            },
            rules: None,
            selected_rule_ids: vec![String::from("archive-sender")],
            selected_rule_count: 1,
            candidate_count: 1,
            candidates: Vec::new(),
            blocked_rule_ids: Vec::new(),
            blockers: Vec::new(),
            warnings: Vec::new(),
            next_steps: Vec::new(),
            command_plan: Vec::new(),
        }
    }

    fn sample_automation_run_detail() -> AutomationRunDetail {
        AutomationRunDetail {
            run: AutomationRunRecord {
                run_id: 42,
                account_id: String::from("gmail:me@example.com"),
                rule_file_path: String::from(".mailroom/automation.toml"),
                rule_file_hash: String::from("hash"),
                selected_rule_ids: vec![String::from("archive-sender")],
                status: AutomationRunStatus::Previewed,
                candidate_count: 1,
                created_at_epoch_s: 1_700_000_000,
                applied_at_epoch_s: None,
            },
            candidates: vec![AutomationRunCandidateRecord {
                candidate_id: 7,
                run_id: 42,
                account_id: String::from("gmail:me@example.com"),
                rule_id: String::from("archive-sender"),
                thread_id: String::from("thread-1"),
                message_id: String::from("message-1"),
                internal_date_epoch_ms: 1_700_000_000_000,
                subject: String::from("Automation subject"),
                from_header: String::from("Sender <sender@example.com>"),
                from_address: Some(String::from("sender@example.com")),
                snippet: String::from("snippet"),
                label_names: vec![String::from("INBOX")],
                attachment_count: 0,
                has_list_unsubscribe: false,
                list_id_header: None,
                list_unsubscribe_header: None,
                list_unsubscribe_post_header: None,
                precedence_header: None,
                auto_submitted_header: None,
                action: AutomationActionSnapshot {
                    kind: AutomationActionKind::Archive,
                    add_label_ids: Vec::new(),
                    add_label_names: Vec::new(),
                    remove_label_ids: vec![String::from("INBOX")],
                    remove_label_names: vec![String::from("INBOX")],
                },
                reason: AutomationMatchReason {
                    from_address: Some(String::from("sender@example.com")),
                    subject_terms: Vec::new(),
                    label_names: vec![String::from("INBOX")],
                    older_than_days: Some(14),
                    has_attachments: None,
                    has_list_unsubscribe: None,
                    list_id_terms: Vec::new(),
                    precedence_values: Vec::new(),
                },
                apply_status: None,
                applied_at_epoch_s: None,
                apply_error: None,
                created_at_epoch_s: 1_700_000_000,
            }],
            events: Vec::new(),
        }
    }

    fn sample_workflow(index: usize) -> WorkflowRecord {
        WorkflowRecord {
            workflow_id: index as i64,
            account_id: String::from("account-1"),
            thread_id: format!("thread-{index}"),
            current_stage: WorkflowStage::Triage,
            triage_bucket: Some(TriageBucket::Urgent),
            note: String::from("reply soon"),
            snoozed_until_epoch_s: None,
            follow_up_due_epoch_s: None,
            latest_message_id: Some(format!("message-{index}")),
            latest_message_internal_date_epoch_ms: Some(1_700_000_000_000),
            latest_message_subject: format!("Subject {index}"),
            latest_message_from_header: String::from("sender@example.com"),
            latest_message_snippet: String::from("Snippet"),
            current_draft_revision_id: None,
            gmail_draft_id: None,
            gmail_draft_message_id: None,
            gmail_draft_thread_id: None,
            last_remote_sync_epoch_s: None,
            last_sent_message_id: None,
            last_cleanup_action: None,
            workflow_version: 1,
            created_at_epoch_s: 1_700_000_000,
            updated_at_epoch_s: 1_700_000_000,
        }
    }

    fn sample_workflow_detail() -> WorkflowDetail {
        WorkflowDetail {
            workflow: sample_workflow(1),
            current_draft: Some(DraftRevisionDetail {
                revision: DraftRevisionRecord {
                    draft_revision_id: 7,
                    workflow_id: 1,
                    account_id: String::from("account-1"),
                    thread_id: String::from("thread-1"),
                    source_message_id: String::from("message-1"),
                    reply_mode: ReplyMode::Reply,
                    subject: String::from("Re: Subject 1"),
                    to_addresses: vec![String::from("recipient@example.com")],
                    cc_addresses: Vec::new(),
                    bcc_addresses: Vec::new(),
                    body_text: String::from("Draft response"),
                    created_at_epoch_s: 1_700_000_000,
                },
                attachments: vec![DraftAttachmentRecord {
                    attachment_id: 9,
                    draft_revision_id: 7,
                    path: String::from("/tmp/reply.txt"),
                    file_name: String::from("reply.txt"),
                    mime_type: String::from("text/plain"),
                    size_bytes: 42,
                    created_at_epoch_s: 1_700_000_000,
                }],
            }),
            events: Vec::new(),
        }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn ctrl_key(character: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(character), KeyModifiers::CONTROL)
    }

    fn render_app(app: &mut TuiApp) -> String {
        render_app_with_size(app, 96, 24)
    }

    fn render_app_with_size(app: &mut TuiApp, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, app)).unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }
}
