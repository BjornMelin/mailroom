use crate::automation::{self, AutomationRolloutRequest, DEFAULT_AUTOMATION_ROLLOUT_LIMIT};
use crate::config::ConfigReport;
use crate::doctor::DoctorReport;
use crate::mailbox::{self, SearchReport, SearchRequest};
use crate::store;
use crate::workflows::{WorkflowActionReport, WorkflowListReport};
use crate::{audit, workflows, workspace};
use anyhow::Result as AnyhowResult;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Tabs, Wrap};
use std::time::Duration;
use tokio::task::spawn_blocking;

const EVENT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const TUI_SEARCH_LIMIT: usize = 15;
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
        terminal.draw(|frame| render(frame, &app))?;

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
    workflow_modal: Option<WorkflowModal>,
    workflow_action_report: Option<std::result::Result<WorkflowActionReport, String>>,
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
            workflow_modal: None,
            workflow_action_report: None,
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
        self.snapshot = load_snapshot(paths, config_report).await;
        self.normalize_workflow_selection();
        self.status = String::from("reports refreshed");
    }

    fn next_view(&mut self) {
        let next = (self.view.index() + 1) % VIEWS.len();
        self.view = VIEWS[next];
        self.search_editing = false;
        self.workflow_modal = None;
    }

    fn previous_view(&mut self) {
        let previous = self.view.index().checked_sub(1).unwrap_or(VIEWS.len() - 1);
        self.view = VIEWS[previous];
        self.search_editing = false;
        self.workflow_modal = None;
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

    fn selected_thread_id(&self) -> Option<String> {
        self.selected_workflow()
            .map(|workflow| workflow.thread_id.clone())
    }

    fn normalize_workflow_selection(&mut self) {
        let row_count = self.workflow_rows().len();
        if row_count == 0 {
            self.workflow_selection = 0;
        } else if self.workflow_selection >= row_count {
            self.workflow_selection = row_count - 1;
        }
    }

    fn select_next_workflow(&mut self) {
        let row_count = self.workflow_rows().len();
        if row_count == 0 {
            self.workflow_selection = 0;
            return;
        }
        self.workflow_selection = (self.workflow_selection + 1) % row_count;
    }

    fn select_previous_workflow(&mut self) {
        let row_count = self.workflow_rows().len();
        if row_count == 0 {
            self.workflow_selection = 0;
            return;
        }
        self.workflow_selection = self
            .workflow_selection
            .checked_sub(1)
            .unwrap_or(row_count - 1);
    }

    fn open_workflow_modal(&mut self, modal: WorkflowModal) {
        if self.selected_workflow().is_none() {
            self.status = String::from("workflow action unavailable: no workflow row selected");
            return;
        }
        self.workflow_modal = Some(modal);
        self.status = String::from("workflow confirmation active; Enter confirms, Esc cancels");
    }

    async fn confirm_workflow_modal(&mut self, config_report: &ConfigReport) {
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
                let until = if until.trim().is_empty() {
                    None
                } else {
                    Some(until.trim().to_owned())
                };
                workflows::snooze_workflow(config_report, thread_id, until).await
            }
        };

        match result {
            Ok(report) => {
                let action = report.action.to_string();
                self.workflow_action_report = Some(Ok(report));
                self.refresh_workflows(config_report, Some(&selected_thread_id))
                    .await;
                self.status = format!("workflow action complete: {action}");
            }
            Err(error) => {
                self.workflow_action_report = Some(Err(error.to_string()));
                self.status = String::from("workflow action failed");
            }
        }
    }

    async fn refresh_workflows(
        &mut self,
        config_report: &ConfigReport,
        selected_thread_id: Option<&str>,
    ) {
        self.snapshot.workflows = workflows::list_workflows_read_only(config_report, None, None)
            .await
            .map_err(|error| error.to_string());
        if let Some(thread_id) = selected_thread_id
            && let Some(index) = self
                .workflow_rows()
                .iter()
                .position(|workflow| workflow.thread_id == thread_id)
        {
            self.workflow_selection = index;
        }
        self.normalize_workflow_selection();
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
}

impl WorkflowModal {
    fn title(&self) -> &'static str {
        match self {
            Self::Triage { .. } => "Confirm triage set",
            Self::Promote { .. } => "Confirm workflow promote",
            Self::Snooze { .. } => "Confirm workflow snooze",
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
        }
    }

    fn cycle_next(&mut self) {
        match self {
            Self::Triage { bucket } => *bucket = next_triage_bucket(*bucket),
            Self::Promote { target } => *target = next_tui_promote_target(*target),
            Self::Snooze { .. } => {}
        }
    }

    fn cycle_previous(&mut self) {
        match self {
            Self::Triage { bucket } => *bucket = previous_triage_bucket(*bucket),
            Self::Promote { target } => *target = previous_tui_promote_target(*target),
            Self::Snooze { .. } => {}
        }
    }
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
        return handle_workflow_modal_key(key, app, config_report).await;
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
                app.open_workflow_modal(WorkflowModal::Triage {
                    bucket: store::workflows::TriageBucket::NeedsReplySoon,
                });
                return Ok(false);
            }
            KeyCode::Char('p') => {
                app.open_workflow_modal(WorkflowModal::Promote {
                    target: store::workflows::WorkflowStage::FollowUp,
                });
                return Ok(false);
            }
            KeyCode::Char('z') => {
                app.open_workflow_modal(WorkflowModal::Snooze {
                    until: String::new(),
                });
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
    config_report: &ConfigReport,
) -> AnyhowResult<bool> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            app.workflow_modal = None;
            app.status = String::from("workflow action canceled");
        }
        KeyCode::Enter => app.confirm_workflow_modal(config_report).await,
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
            if let Some(WorkflowModal::Snooze { until }) = &mut app.workflow_modal {
                until.pop();
            }
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
            if let Some(WorkflowModal::Snooze { until }) = &mut app.workflow_modal
                && (key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT)
            {
                until.push(value);
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

fn render(frame: &mut Frame<'_>, app: &TuiApp) {
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
        Span::raw("q quit | tab view | 1-5 jump | / search | r refresh | workflows: j/k t/p/z | "),
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

fn render_workflows(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    match &app.snapshot.workflows {
        Ok(report) => {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
                .split(area);
            let rows = report
                .workflows
                .iter()
                .take(50)
                .enumerate()
                .map(|(index, workflow)| {
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
                "Workflows ({}) - j/k select, t triage, p promote, z snooze",
                report.workflows.len()
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
        lines.push(Line::default());
        lines.push(Line::from("Actions require confirmation:"));
        lines.push(Line::from("t triage bucket | p promote | z snooze/clear"));
        lines.push(Line::from(
            "Promote targets are follow_up or ready_to_send only.",
        ));
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
    let popup = centered_rect(68, 11, area);
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
            lines.push(metric(
                "until",
                if until.is_empty() {
                    String::from("<clear>")
                } else {
                    until.clone()
                },
            ));
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

fn render_automation(frame: &mut Frame<'_>, area: Rect, app: &TuiApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(6)])
        .split(area);

    match &app.snapshot.automation {
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

            let rows = report.candidates.iter().take(50).map(|candidate| {
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
                    .title("Candidate preview"),
            );
            frame.render_widget(table, chunks[1]);
        }
        Err(error) => render_text_panel(frame, area, "Automation", vec![error_line(error)]),
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
            Line::from(
                "3 Workflows: inspect rows and confirm local triage/promote/snooze actions.",
            ),
            Line::from("4 Automation: inspect rollout readiness and preview candidates."),
            Line::from("5 Help: key bindings and safety posture."),
            Line::from(""),
            Line::from("Workflow actions call existing workflow services only after confirmation."),
            Line::from("No view sends drafts, archives mail, labels mail, trashes mail,"),
            Line::from("applies automation, exports attachments, or writes automation snapshots."),
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

fn bool_word(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
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
        Snapshot, TUI_SEARCH_LIMIT, TuiApp, View, WorkflowModal, failed_diagnostic_reports,
        handle_key, load_snapshot, render, truncate,
    };
    use crate::config;
    use crate::mailbox::{self, SearchRequest};
    use crate::store::workflows::{TriageBucket, WorkflowRecord, WorkflowStage};
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
                bucket: TriageBucket::NeedsReplySoon
            })
        ));

        handle_key(key(KeyCode::Esc), &mut app, &paths, &config_report)
            .await
            .unwrap();

        assert!(app.workflow_modal.is_none());
        assert_eq!(app.status, "workflow action canceled");
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

        let output = render_app(&app);

        assert!(output.contains("error: doctor failed"));
        assert!(output.contains("error: verification failed"));
    }

    #[test]
    fn search_renders_search_errors() {
        let mut app = TuiApp::new(empty_snapshot(), Some(String::from("invoice")));
        app.search_report = Some(Err(String::from("search failed")));

        let output = render_app(&app);

        assert!(output.contains("error: search failed"));
    }

    #[test]
    fn workflows_render_report_errors() {
        let mut app = TuiApp::new(empty_snapshot(), None);
        app.view = View::Workflows;
        app.snapshot.workflows = Err(String::from("workflow report failed"));

        let output = render_app(&app);

        assert!(output.contains("error: workflow report failed"));
    }

    #[test]
    fn workflows_render_selected_detail_and_confirmation_modal() {
        let mut app = TuiApp::new(snapshot_with_workflows(1), None);
        app.view = View::Workflows;
        app.workflow_modal = Some(WorkflowModal::Triage {
            bucket: TriageBucket::Urgent,
        });

        let output = render_app(&app);

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

        let output = render_app(&app);

        assert!(output.contains("error: automation report failed"));
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

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn render_app(app: &TuiApp) -> String {
        let backend = TestBackend::new(96, 24);
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
