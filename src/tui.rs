use crate::automation::{self, AutomationRolloutRequest, DEFAULT_AUTOMATION_ROLLOUT_LIMIT};
use crate::config::ConfigReport;
use crate::doctor::DoctorReport;
use crate::mailbox::{self, SearchReport, SearchRequest};
use crate::workflows::WorkflowListReport;
use crate::{audit, workspace};
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs, Wrap};
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
) -> Result<()> {
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
    search_report: Option<Result<SearchReport, String>>,
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
            status: String::from(
                "read-only mode: no Gmail or local mutation actions are available",
            ),
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
        match mailbox::search(config_report, request).await {
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
        self.status = String::from("read-only reports refreshed");
    }

    fn next_view(&mut self) {
        let next = (self.view.index() + 1) % VIEWS.len();
        self.view = VIEWS[next];
        self.search_editing = false;
    }

    fn previous_view(&mut self) {
        let previous = self.view.index().checked_sub(1).unwrap_or(VIEWS.len() - 1);
        self.view = VIEWS[previous];
        self.search_editing = false;
    }
}

#[derive(Debug, Clone)]
struct Snapshot {
    doctor: Result<DoctorReport, String>,
    verification: Result<audit::VerificationAuditReport, String>,
    workflows: Result<WorkflowListReport, String>,
    automation: Result<automation::AutomationRolloutReport, String>,
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
        Err(error) => (
            Err(format!("diagnostic task failed: {error}")),
            Err(format!("verification task failed: {error}")),
        ),
    };

    let workflows = crate::workflows::list_workflows(config_report, None, None)
        .await
        .map_err(|error| error.to_string());
    let automation = automation::rollout(
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

async fn handle_key(
    key: KeyEvent,
    app: &mut TuiApp,
    paths: &workspace::WorkspacePaths,
    config_report: &ConfigReport,
) -> Result<bool> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Ok(true);
    }

    if app.search_editing {
        return handle_search_key(key, app, config_report).await;
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

async fn handle_search_key(
    key: KeyEvent,
    app: &mut TuiApp,
    config_report: &ConfigReport,
) -> Result<bool> {
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
        Span::raw("q quit | tab view | 1-5 jump | / search | enter submit | r refresh | "),
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
            let rows = report.workflows.iter().take(50).map(|workflow| {
                Row::new(vec![
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
                ])
            });
            let table = Table::new(
                rows,
                [
                    Constraint::Length(6),
                    Constraint::Length(14),
                    Constraint::Length(18),
                    Constraint::Percentage(42),
                    Constraint::Percentage(26),
                ],
            )
            .header(
                Row::new(vec!["ID", "Stage", "Bucket", "Subject", "From"])
                    .style(Style::default().add_modifier(Modifier::BOLD)),
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("Workflows ({})", report.workflows.len())),
            );
            frame.render_widget(table, area);
        }
        Err(error) => render_text_panel(frame, area, "Workflows", vec![error_line(error)]),
    }
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
            Line::from("Read-only operator shell"),
            Line::from(""),
            Line::from("1 Dashboard: auth, store, mailbox, and readiness summary."),
            Line::from("2 Search: run local SQLite FTS queries against synced mail."),
            Line::from("3 Workflows: inspect thread workflow queue rows."),
            Line::from("4 Automation: inspect rollout readiness and preview candidates."),
            Line::from("5 Help: key bindings and safety posture."),
            Line::from(""),
            Line::from(
                "No view sends drafts, archives mail, labels mail, trashes mail, applies automation,",
            ),
            Line::from("exports attachments, or writes automation snapshots."),
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
    use super::{Snapshot, TuiApp, View, truncate};

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

    fn empty_snapshot() -> Snapshot {
        Snapshot {
            doctor: Err(String::from("not loaded")),
            verification: Err(String::from("not loaded")),
            workflows: Err(String::from("not loaded")),
            automation: Err(String::from("not loaded")),
        }
    }
}
