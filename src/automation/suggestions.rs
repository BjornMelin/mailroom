use super::AutomationServiceError;
use super::model::{
    AutomationMatchRule, AutomationRule, AutomationRuleAction, AutomationRuleSet,
    AutomationRuleSuggestion, AutomationRuleSuggestionSample, AutomationRulesSuggestReport,
    AutomationRulesSuggestRequest,
};
use super::rules::active_rules_path;
use crate::config::ConfigReport;
use crate::store::automation::AutomationThreadCandidate;
use std::collections::{BTreeMap, BTreeSet};

const INBOX_LABEL: &str = "INBOX";
const RULE_ID_SOURCE_LIMIT: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SuggestionKind {
    ListId,
    Sender,
}

impl SuggestionKind {
    const fn confidence(self) -> &'static str {
        match self {
            Self::ListId => "high",
            Self::Sender => "medium",
        }
    }

    const fn source(self) -> &'static str {
        match self {
            Self::ListId => "list_id",
            Self::Sender => "sender",
        }
    }

    const fn priority(self) -> i64 {
        match self {
            Self::ListId => 200,
            Self::Sender => 150,
        }
    }

    const fn rank(self) -> usize {
        match self {
            Self::ListId => 2,
            Self::Sender => 1,
        }
    }

    const fn rule_prefix(self) -> &'static str {
        match self {
            Self::ListId => "archive-list",
            Self::Sender => "archive-sender",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SuggestionKey {
    kind: SuggestionKind,
    value: String,
    evidence: SuggestionEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SuggestionEvidence {
    ListId,
    ListUnsubscribe,
    Precedence(String),
}

#[derive(Debug, Clone)]
struct SuggestionGroup {
    key: SuggestionKey,
    matched_thread_count: usize,
    samples: Vec<AutomationRuleSuggestionSample>,
}

pub(crate) fn suggest_rules_from_candidates(
    config_report: &ConfigReport,
    account_id: String,
    candidates: &[AutomationThreadCandidate],
    request: &AutomationRulesSuggestRequest,
    now_epoch_ms: i64,
) -> Result<AutomationRulesSuggestReport, AutomationServiceError> {
    let rules_path = active_rules_path(config_report);
    let inspected_thread_count = candidates.len();
    let (eligible_thread_count, groups) = group_candidates(candidates, request, now_epoch_ms);
    let mut suggestions = groups
        .into_values()
        .filter(|group| group.matched_thread_count >= request.min_thread_count)
        .map(|group| build_suggestion(group, request))
        .collect::<Result<Vec<_>, _>>()?;

    suggestions.sort_by(|left, right| {
        suggestion_rank(right)
            .cmp(&suggestion_rank(left))
            .then_with(|| left.rule_id.cmp(&right.rule_id))
    });
    suggestions.truncate(request.limit);
    deduplicate_rule_ids(&mut suggestions)?;

    let mut warnings = Vec::new();
    if inspected_thread_count == 0 {
        warnings.push(String::from(
            "Local mailbox cache is empty; run `mailroom sync run --json` before using suggestions.",
        ));
    } else if suggestions.is_empty() {
        warnings.push(format!(
            "No recurring older INBOX list or bulk senders met the minimum thread threshold of {}.",
            request.min_thread_count
        ));
    }

    let next_steps = vec![
        format!(
            "Review the disabled TOML snippets and copy selected rules into {}.",
            rules_path.display()
        ),
        String::from(
            "Enable one low-surprise archive rule, validate it, run rollout, then create and inspect a small preview snapshot.",
        ),
        String::from("Keep trash and unsubscribe execution out of first-wave automation."),
    ];

    let command_plan = vec![
        String::from("cargo run -- automation rules validate --json"),
        String::from("cargo run -- automation rollout --rule <rule-id> --limit 10 --json"),
        String::from("cargo run -- automation run --rule <rule-id> --limit 10 --json"),
        String::from("cargo run -- automation show <run-id> --json"),
        String::from("cargo run -- automation apply <run-id> --execute --json"),
    ];

    Ok(AutomationRulesSuggestReport {
        account_id,
        rules_path,
        inspected_thread_count,
        eligible_thread_count,
        suggestion_count: suggestions.len(),
        min_thread_count: request.min_thread_count,
        older_than_days: request.older_than_days,
        suggestions,
        warnings,
        next_steps,
        command_plan,
    })
}

fn group_candidates(
    candidates: &[AutomationThreadCandidate],
    request: &AutomationRulesSuggestRequest,
    now_epoch_ms: i64,
) -> (usize, BTreeMap<SuggestionKey, SuggestionGroup>) {
    let cutoff_epoch_ms =
        now_epoch_ms.saturating_sub(i64::from(request.older_than_days).saturating_mul(86_400_000));
    let mut eligible_thread_count = 0usize;
    let mut groups = BTreeMap::new();

    for candidate in candidates {
        if !is_candidate_eligible(candidate, cutoff_epoch_ms) {
            continue;
        }
        let Some(key) = suggestion_key(candidate) else {
            continue;
        };
        eligible_thread_count += 1;
        let sample = AutomationRuleSuggestionSample {
            thread_id: candidate.thread_id.clone(),
            message_id: candidate.message_id.clone(),
            subject: candidate.subject.clone(),
            from_address: candidate.from_address.clone(),
            list_id_header: candidate.list_id_header.clone(),
            internal_date_epoch_ms: candidate.internal_date_epoch_ms,
        };

        let group = groups
            .entry(key.clone())
            .or_insert_with(|| SuggestionGroup {
                key,
                matched_thread_count: 0,
                samples: Vec::new(),
            });
        group.matched_thread_count += 1;
        retain_recent_sample(&mut group.samples, sample, request.sample_limit);
    }

    (eligible_thread_count, groups)
}

fn retain_recent_sample(
    samples: &mut Vec<AutomationRuleSuggestionSample>,
    sample: AutomationRuleSuggestionSample,
    sample_limit: usize,
) {
    if samples.len() < sample_limit {
        samples.push(sample);
        return;
    }

    let Some(oldest_index) = samples
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| sample_rank_key(left).cmp(&sample_rank_key(right)))
        .map(|(index, _)| index)
    else {
        return;
    };

    if sample_rank_key(&sample) > sample_rank_key(&samples[oldest_index]) {
        samples[oldest_index] = sample;
    }
}

fn sample_rank_key(sample: &AutomationRuleSuggestionSample) -> (i64, &str, &str) {
    (
        sample.internal_date_epoch_ms,
        sample.thread_id.as_str(),
        sample.message_id.as_str(),
    )
}

fn is_candidate_eligible(candidate: &AutomationThreadCandidate, cutoff_epoch_ms: i64) -> bool {
    has_label(&candidate.label_names, INBOX_LABEL)
        && candidate.internal_date_epoch_ms <= cutoff_epoch_ms
        && (has_list_unsubscribe(candidate)
            || normalized_precedence(candidate.precedence_header.as_deref()).is_some())
}

fn suggestion_key(candidate: &AutomationThreadCandidate) -> Option<SuggestionKey> {
    if has_list_unsubscribe(candidate)
        && let Some(list_id) = normalized_list_id(candidate.list_id_header.as_deref())
    {
        return Some(SuggestionKey {
            kind: SuggestionKind::ListId,
            value: list_id,
            evidence: SuggestionEvidence::ListId,
        });
    }

    let from_address = normalized_from_address(candidate.from_address.as_deref())?;
    if has_list_unsubscribe(candidate) {
        return Some(SuggestionKey {
            kind: SuggestionKind::Sender,
            value: from_address,
            evidence: SuggestionEvidence::ListUnsubscribe,
        });
    }
    normalized_precedence(candidate.precedence_header.as_deref()).map(|precedence| SuggestionKey {
        kind: SuggestionKind::Sender,
        value: from_address,
        evidence: SuggestionEvidence::Precedence(precedence),
    })
}

fn build_suggestion(
    group: SuggestionGroup,
    request: &AutomationRulesSuggestRequest,
) -> Result<AutomationRuleSuggestion, AutomationServiceError> {
    let rule_id = format!(
        "{}-{}",
        group.key.kind.rule_prefix(),
        slugify(&group.key.value)
    );
    let description = format!(
        "Archive older INBOX mail for {} after review.",
        group.key.value
    );
    let mut matcher = AutomationMatchRule {
        label_any: vec![String::from(INBOX_LABEL)],
        older_than_days: Some(request.older_than_days),
        ..AutomationMatchRule::default()
    };
    let mut match_fields = vec![
        String::from("label_any=INBOX"),
        format!("older_than_days={}", request.older_than_days),
    ];

    match group.key.kind {
        SuggestionKind::ListId => {
            matcher.has_list_unsubscribe = Some(true);
            matcher.list_id_contains = vec![group.key.value.clone()];
            match_fields.push(String::from("has_list_unsubscribe=true"));
            match_fields.push(format!("list_id_contains={}", group.key.value));
        }
        SuggestionKind::Sender => {
            matcher.from_address = Some(group.key.value.clone());
            match_fields.push(format!("from_address={}", group.key.value));
            match &group.key.evidence {
                SuggestionEvidence::ListId => {}
                SuggestionEvidence::ListUnsubscribe => {
                    matcher.has_list_unsubscribe = Some(true);
                    match_fields.push(String::from("has_list_unsubscribe=true"));
                }
                SuggestionEvidence::Precedence(precedence) => {
                    matcher.precedence = vec![precedence.clone()];
                    match_fields.push(format!("precedence={precedence}"));
                }
            }
        }
    }

    let rule = AutomationRule {
        id: rule_id.clone(),
        description: Some(description.clone()),
        enabled: false,
        priority: group.key.kind.priority(),
        matcher,
        action: AutomationRuleAction::Archive,
    };
    let toml = render_rule_toml(&rule)?;

    Ok(AutomationRuleSuggestion {
        rule_id,
        description,
        confidence: group.key.kind.confidence().to_owned(),
        source: group.key.kind.source().to_owned(),
        matched_thread_count: group.matched_thread_count,
        match_fields,
        sample_threads: group.samples,
        rule,
        toml,
    })
}

fn deduplicate_rule_ids(
    suggestions: &mut [AutomationRuleSuggestion],
) -> Result<(), AutomationServiceError> {
    let mut used_rule_ids = BTreeSet::new();
    for suggestion in suggestions {
        let base_rule_id = suggestion.rule_id.clone();
        let mut candidate_rule_id = base_rule_id.clone();
        let mut suffix = 2usize;
        while !used_rule_ids.insert(candidate_rule_id.clone()) {
            candidate_rule_id = format!("{base_rule_id}-{suffix}");
            suffix += 1;
        }
        if candidate_rule_id != suggestion.rule_id {
            suggestion.rule_id = candidate_rule_id.clone();
            suggestion.rule.id = candidate_rule_id;
            suggestion.toml = render_rule_toml(&suggestion.rule)?;
        }
    }
    Ok(())
}

fn render_rule_toml(rule: &AutomationRule) -> Result<String, AutomationServiceError> {
    toml::to_string_pretty(&AutomationRuleSet {
        rules: vec![rule.clone()],
    })
    .map_err(|source| AutomationServiceError::RuleTomlSerialize { source })
}

fn suggestion_rank(suggestion: &AutomationRuleSuggestion) -> (usize, usize, i64) {
    let confidence_rank = match suggestion.confidence.as_str() {
        "high" => SuggestionKind::ListId.rank(),
        _ => SuggestionKind::Sender.rank(),
    };
    let latest_epoch_ms = suggestion
        .sample_threads
        .iter()
        .map(|sample| sample.internal_date_epoch_ms)
        .max()
        .unwrap_or_default();
    (
        confidence_rank,
        suggestion.matched_thread_count,
        latest_epoch_ms,
    )
}

fn has_label(labels: &[String], expected: &str) -> bool {
    labels
        .iter()
        .any(|label| label.eq_ignore_ascii_case(expected))
}

fn has_list_unsubscribe(candidate: &AutomationThreadCandidate) -> bool {
    candidate
        .list_unsubscribe_header
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

fn normalized_list_id(header: Option<&str>) -> Option<String> {
    let value = header?
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim()
        .to_ascii_lowercase();
    (!value.is_empty()).then_some(value)
}

fn normalized_from_address(from_address: Option<&str>) -> Option<String> {
    let value = from_address?.trim().to_ascii_lowercase();
    (!value.is_empty()).then_some(value)
}

fn normalized_precedence(header: Option<&str>) -> Option<String> {
    let value = header?.trim().to_ascii_lowercase();
    let tokens: Vec<&str> = value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    ["bulk", "list", "junk"]
        .iter()
        .find(|expected| tokens.contains(expected))
        .map(|matched| (*matched).to_owned())
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_dash = false;
    for character in value.chars() {
        let next = if character.is_ascii_alphanumeric() {
            Some(character.to_ascii_lowercase())
        } else if character == '.' || character == '_' || character == '-' || character == '@' {
            Some('-')
        } else {
            None
        };
        if let Some(next) = next {
            if next == '-' {
                if !slug.is_empty() && !previous_was_dash {
                    slug.push(next);
                    previous_was_dash = true;
                }
            } else {
                slug.push(next);
                previous_was_dash = false;
            }
        }
        if slug.len() >= RULE_ID_SOURCE_LIMIT {
            break;
        }
    }
    let slug = slug.trim_matches('-').to_owned();
    if slug.is_empty() {
        String::from("unknown")
    } else {
        slug
    }
}

#[cfg(test)]
mod tests {
    use super::{normalized_precedence, slugify, suggest_rules_from_candidates};
    use crate::automation::model::AutomationRulesSuggestRequest;
    use crate::config::resolve;
    use crate::store::automation::AutomationThreadCandidate;
    use crate::workspace::WorkspacePaths;
    use tempfile::TempDir;

    #[test]
    fn suggests_disabled_list_rule_from_recurring_inbox_list_mail() {
        let (_repo_root, config_report) = config_report();
        let request = request();
        let candidates = vec![
            list_candidate("thread-1", "message-1", 100),
            list_candidate("thread-2", "message-2", 200),
            list_candidate("thread-3", "message-3", 300),
        ];

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &candidates,
            &request,
            2_000_000_000,
        )
        .unwrap();

        assert_eq!(report.inspected_thread_count, 3);
        assert_eq!(report.eligible_thread_count, 3);
        assert_eq!(report.suggestion_count, 1);
        let suggestion = &report.suggestions[0];
        assert_eq!(suggestion.rule_id, "archive-list-digest-example-com");
        assert_eq!(suggestion.confidence, "high");
        assert_eq!(suggestion.matched_thread_count, 3);
        assert!(!suggestion.rule.enabled);
        assert_eq!(
            suggestion.rule.matcher.label_any,
            vec![String::from("INBOX")]
        );
        assert_eq!(suggestion.rule.matcher.older_than_days, Some(14));
        assert_eq!(suggestion.rule.matcher.has_list_unsubscribe, Some(true));
        assert_eq!(
            suggestion.rule.matcher.list_id_contains,
            vec![String::from("digest.example.com")]
        );
        assert!(suggestion.toml.contains("[[rules]]"));
        assert!(suggestion.toml.contains("enabled = false"));
        assert!(suggestion.toml.contains("[rules.match]"));
        toml::from_str::<crate::automation::model::AutomationRuleSet>(&suggestion.toml).unwrap();
    }

    #[test]
    fn suggests_sender_rule_for_recurring_bulk_sender_without_list_id() {
        let (_repo_root, config_report) = config_report();
        let request = request();
        let candidates = vec![
            sender_candidate("thread-1", "message-1", 100),
            sender_candidate("thread-2", "message-2", 200),
            sender_candidate("thread-3", "message-3", 300),
        ];

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &candidates,
            &request,
            2_000_000_000,
        )
        .unwrap();

        let suggestion = &report.suggestions[0];
        assert_eq!(suggestion.rule_id, "archive-sender-notices-example-com");
        assert_eq!(suggestion.confidence, "medium");
        assert_eq!(
            suggestion.rule.matcher.from_address.as_deref(),
            Some("notices@example.com")
        );
        assert_eq!(
            suggestion.rule.matcher.precedence,
            vec![String::from("bulk")]
        );
        assert_eq!(suggestion.rule.matcher.has_list_unsubscribe, None);
    }

    #[test]
    fn sender_suggestions_keep_counted_evidence_aligned_with_matcher() {
        let (_repo_root, config_report) = config_report();
        let mut request = request();
        request.min_thread_count = 2;
        let candidates = vec![
            sender_unsubscribe_candidate("thread-u1", "message-u1", 100),
            sender_unsubscribe_candidate("thread-u2", "message-u2", 200),
            sender_candidate("thread-p1", "message-p1", 300),
            sender_candidate("thread-p2", "message-p2", 400),
        ];

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &candidates,
            &request,
            2_000_000_000,
        )
        .unwrap();

        assert_eq!(report.suggestion_count, 2);
        let unsubscribe_suggestion = report
            .suggestions
            .iter()
            .find(|suggestion| {
                suggestion.rule.matcher.has_list_unsubscribe == Some(true)
                    && suggestion.rule.matcher.precedence.is_empty()
            })
            .expect("unsubscribe-backed sender suggestion");
        assert_eq!(unsubscribe_suggestion.matched_thread_count, 2);

        let precedence_suggestion = report
            .suggestions
            .iter()
            .find(|suggestion| {
                suggestion.rule.matcher.has_list_unsubscribe.is_none()
                    && suggestion.rule.matcher.precedence == vec![String::from("bulk")]
            })
            .expect("precedence-backed sender suggestion");
        assert_eq!(precedence_suggestion.matched_thread_count, 2);
    }

    #[test]
    fn deduplicates_rule_ids_after_slug_collisions() {
        let (_repo_root, config_report) = config_report();
        let mut request = request();
        request.min_thread_count = 1;
        let candidates = vec![
            sender_collision_candidate("foo_bar@example.com", "thread-1", "message-1", 300),
            sender_collision_candidate("foo-bar@example.com", "thread-2", "message-2", 100),
        ];

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &candidates,
            &request,
            2_000_000_000,
        )
        .unwrap();

        assert_eq!(
            report
                .suggestions
                .iter()
                .map(|suggestion| suggestion.rule_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "archive-sender-foo-bar-example-com",
                "archive-sender-foo-bar-example-com-2",
            ]
        );
        let suffix_suggestion = &report.suggestions[1];
        assert_eq!(
            suffix_suggestion.rule.id,
            "archive-sender-foo-bar-example-com-2"
        );
        assert!(
            suffix_suggestion
                .toml
                .contains("id = \"archive-sender-foo-bar-example-com-2\"")
        );
        toml::from_str::<crate::automation::model::AutomationRuleSet>(&suffix_suggestion.toml)
            .unwrap();
    }

    #[test]
    fn ignores_new_threads_non_inbox_threads_and_small_groups() {
        let (_repo_root, config_report) = config_report();
        let mut request = request();
        request.min_thread_count = 2;
        let mut non_inbox = list_candidate("thread-1", "message-1", 100);
        non_inbox.label_names = vec![String::from("CATEGORY_PROMOTIONS")];
        let new_thread = list_candidate("thread-2", "message-2", 1_999_999_999);
        let one_old_thread = list_candidate("thread-3", "message-3", 100);

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &[non_inbox, new_thread, one_old_thread],
            &request,
            2_000_000_000,
        )
        .unwrap();

        assert_eq!(report.eligible_thread_count, 1);
        assert!(report.suggestions.is_empty());
        assert_eq!(report.warnings.len(), 1);
    }

    #[test]
    fn limits_suggestions_and_samples_deterministically() {
        let (_repo_root, config_report) = config_report();
        let mut request = request();
        request.limit = 1;
        request.sample_limit = 1;
        let candidates = vec![
            list_candidate_for("a.example.com", "thread-a1", "message-a1", 100),
            list_candidate_for("a.example.com", "thread-a2", "message-a2", 200),
            list_candidate_for("a.example.com", "thread-a3", "message-a3", 300),
            list_candidate_for("b.example.com", "thread-b1", "message-b1", 400),
            list_candidate_for("b.example.com", "thread-b2", "message-b2", 500),
            list_candidate_for("b.example.com", "thread-b3", "message-b3", 600),
            list_candidate_for("b.example.com", "thread-b4", "message-b4", 700),
        ];

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &candidates,
            &request,
            2_000_000_000,
        )
        .unwrap();

        assert_eq!(report.suggestion_count, 1);
        assert_eq!(report.suggestions[0].rule_id, "archive-list-b-example-com");
        assert_eq!(report.suggestions[0].sample_threads.len(), 1);
    }

    #[test]
    fn sample_limit_keeps_latest_samples_independent_of_input_order() {
        let (_repo_root, config_report) = config_report();
        let mut request = request();
        request.limit = 1;
        request.min_thread_count = 2;
        request.sample_limit = 1;
        let candidates = vec![
            list_candidate_for("a.example.com", "thread-a-old", "message-a-old", 100),
            list_candidate_for("b.example.com", "thread-b-old", "message-b-old", 400),
            list_candidate_for("b.example.com", "thread-b-new", "message-b-new", 500),
            list_candidate_for("a.example.com", "thread-a-new", "message-a-new", 900),
        ];

        let report = suggest_rules_from_candidates(
            &config_report,
            String::from("gmail:operator@example.com"),
            &candidates,
            &request,
            2_000_000_000,
        )
        .unwrap();

        assert_eq!(report.suggestion_count, 1);
        assert_eq!(report.suggestions[0].rule_id, "archive-list-a-example-com");
        assert_eq!(report.suggestions[0].sample_threads.len(), 1);
        assert_eq!(
            report.suggestions[0].sample_threads[0].thread_id,
            "thread-a-new"
        );
    }

    #[test]
    fn normalized_precedence_requires_exact_tokens() {
        assert_eq!(
            normalized_precedence(Some("bulk")),
            Some(String::from("bulk"))
        );
        assert_eq!(
            normalized_precedence(Some("x-priority; list")),
            Some(String::from("list"))
        );
        assert_eq!(normalized_precedence(Some("notbulk")), None);
        assert_eq!(normalized_precedence(Some("xlistx")), None);
    }

    #[test]
    fn slugify_normalizes_rule_ids() {
        assert_eq!(slugify("<Digest.Example_Com>"), "digest-example-com");
        assert_eq!(slugify("!!!"), "unknown");
    }

    fn request() -> AutomationRulesSuggestRequest {
        AutomationRulesSuggestRequest {
            limit: 10,
            min_thread_count: 3,
            older_than_days: 14,
            sample_limit: 3,
        }
    }

    fn config_report() -> (TempDir, crate::config::ConfigReport) {
        let repo_root = TempDir::with_prefix("mailroom-automation-suggest").unwrap();
        std::fs::create_dir(repo_root.path().join(".git")).unwrap();
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        (repo_root, config_report)
    }

    fn list_candidate(
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
    ) -> AutomationThreadCandidate {
        list_candidate_for(
            "digest.example.com",
            thread_id,
            message_id,
            internal_date_epoch_ms,
        )
    }

    fn list_candidate_for(
        list_id: &str,
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
    ) -> AutomationThreadCandidate {
        candidate(thread_id, message_id, internal_date_epoch_ms, |candidate| {
            candidate.list_id_header = Some(format!("<{list_id}>"));
            candidate.list_unsubscribe_header =
                Some(String::from("<mailto:unsubscribe@example.com>"));
        })
    }

    fn sender_candidate(
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
    ) -> AutomationThreadCandidate {
        candidate(thread_id, message_id, internal_date_epoch_ms, |candidate| {
            candidate.from_header = String::from("Notices <notices@example.com>");
            candidate.from_address = Some(String::from("notices@example.com"));
            candidate.precedence_header = Some(String::from("bulk"));
        })
    }

    fn sender_unsubscribe_candidate(
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
    ) -> AutomationThreadCandidate {
        candidate(thread_id, message_id, internal_date_epoch_ms, |candidate| {
            candidate.from_header = String::from("Notices <notices@example.com>");
            candidate.from_address = Some(String::from("notices@example.com"));
            candidate.list_unsubscribe_header =
                Some(String::from("<mailto:unsubscribe@example.com>"));
        })
    }

    fn sender_collision_candidate(
        from_address: &str,
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
    ) -> AutomationThreadCandidate {
        candidate(thread_id, message_id, internal_date_epoch_ms, |candidate| {
            candidate.from_header = format!("Sender <{from_address}>");
            candidate.from_address = Some(from_address.to_owned());
            candidate.precedence_header = Some(String::from("bulk"));
        })
    }

    fn candidate(
        thread_id: &str,
        message_id: &str,
        internal_date_epoch_ms: i64,
        update: impl FnOnce(&mut AutomationThreadCandidate),
    ) -> AutomationThreadCandidate {
        let mut candidate = AutomationThreadCandidate {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: thread_id.to_owned(),
            message_id: message_id.to_owned(),
            internal_date_epoch_ms,
            subject: format!("Subject for {thread_id}"),
            from_header: String::from("Digest <digest@example.com>"),
            from_address: Some(String::from("digest@example.com")),
            snippet: String::from("Snippet"),
            label_names: vec![String::from("INBOX")],
            attachment_count: 0,
            list_id_header: None,
            list_unsubscribe_header: None,
            list_unsubscribe_post_header: None,
            precedence_header: None,
            auto_submitted_header: None,
        };
        update(&mut candidate);
        candidate
    }
}
