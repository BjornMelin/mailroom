use super::AutomationServiceError;
use super::model::{
    AutomationRule, AutomationRuleAction, AutomationRuleSet, AutomationRuleSummary,
    AutomationRulesValidateReport,
};
use crate::config::ConfigReport;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub(crate) struct ResolvedAutomationRules {
    pub(crate) path: PathBuf,
    pub(crate) rule_file_hash: String,
    pub(crate) rules: Vec<AutomationRule>,
}

pub(crate) fn validate_rule_file(
    config_report: &ConfigReport,
) -> Result<AutomationRulesValidateReport, AutomationServiceError> {
    let (path, contents) = load_rule_file(config_report)?;
    let rule_file_hash = blake3::hash(contents.as_bytes()).to_hex().to_string();
    let rule_set = parse_rule_file(&path, &contents)?;

    Ok(AutomationRulesValidateReport {
        path,
        rule_file_hash,
        rule_count: rule_set.rules.len(),
        enabled_rule_count: rule_set.rules.iter().filter(|rule| rule.enabled).count(),
        rules: rule_set
            .rules
            .iter()
            .map(|rule| AutomationRuleSummary {
                id: rule.id.clone(),
                description: rule.description.clone(),
                enabled: rule.enabled,
                priority: rule.priority,
                action_kind: rule.action_kind(),
            })
            .collect(),
    })
}

pub(crate) fn resolve_rule_selection(
    config_report: &ConfigReport,
    selected_rule_ids: &[String],
) -> Result<ResolvedAutomationRules, AutomationServiceError> {
    let (path, contents) = load_rule_file(config_report)?;
    let rule_file_hash = blake3::hash(contents.as_bytes()).to_hex().to_string();
    let rule_set = parse_rule_file(&path, &contents)?;
    let requested = normalize_string_list(selected_rule_ids);

    let mut rules_with_order = rule_set
        .rules
        .into_iter()
        .enumerate()
        .filter(|(_, rule)| rule.enabled)
        .filter(|(_, rule)| requested.is_empty() || requested.contains(&rule.id))
        .collect::<Vec<_>>();

    if !requested.is_empty() {
        let available = rules_with_order
            .iter()
            .map(|(_, rule)| rule.id.clone())
            .collect::<BTreeSet<_>>();
        let missing = requested
            .iter()
            .filter(|id| !available.contains(*id))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(AutomationServiceError::RuleValidation {
                message: format!(
                    "requested rule ids are not enabled or do not exist: {}",
                    missing.join(", ")
                ),
            });
        }
    }

    if rules_with_order.is_empty() {
        return Err(AutomationServiceError::RuleValidation {
            message: String::from(
                "no enabled automation rules were selected; copy config/automation.example.toml to .mailroom/automation.toml and enable at least one rule",
            ),
        });
    }

    rules_with_order.sort_by(|(left_index, left_rule), (right_index, right_rule)| {
        right_rule
            .priority
            .cmp(&left_rule.priority)
            .then_with(|| left_index.cmp(right_index))
    });

    Ok(ResolvedAutomationRules {
        path,
        rule_file_hash,
        rules: rules_with_order.into_iter().map(|(_, rule)| rule).collect(),
    })
}

pub(crate) fn active_rules_path(config_report: &ConfigReport) -> PathBuf {
    config_report
        .config
        .workspace
        .runtime_root
        .join("automation.toml")
}

fn load_rule_file(
    config_report: &ConfigReport,
) -> Result<(PathBuf, String), AutomationServiceError> {
    let path = active_rules_path(config_report);
    if !path.exists() {
        return Err(AutomationServiceError::RuleFileMissing { path });
    }
    let contents =
        std::fs::read_to_string(&path).map_err(|source| AutomationServiceError::RuleFileRead {
            path: path.clone(),
            source,
        })?;
    Ok((path, contents))
}

fn parse_rule_file(
    path: &Path,
    contents: &str,
) -> Result<AutomationRuleSet, AutomationServiceError> {
    let mut rule_set = toml::from_str::<AutomationRuleSet>(contents).map_err(|source| {
        AutomationServiceError::RuleFileParse {
            path: path.to_path_buf(),
            source,
        }
    })?;
    validate_rules(&mut rule_set)?;
    Ok(rule_set)
}

fn validate_rules(rule_set: &mut AutomationRuleSet) -> Result<(), AutomationServiceError> {
    let mut seen_ids = BTreeSet::new();
    for rule in &mut rule_set.rules {
        rule.id = rule.id.trim().to_owned();
        if rule.id.is_empty() {
            return Err(AutomationServiceError::RuleValidation {
                message: String::from("automation rule ids must not be empty"),
            });
        }
        if !seen_ids.insert(rule.id.clone()) {
            return Err(AutomationServiceError::RuleValidation {
                message: format!("automation rule id `{}` is duplicated", rule.id),
            });
        }

        if let Some(description) = &rule.description {
            let trimmed = description.trim();
            rule.description = (!trimmed.is_empty()).then_some(trimmed.to_owned());
        }

        normalize_match_lists(rule);

        if rule.matcher.older_than_days == Some(0) {
            return Err(AutomationServiceError::RuleValidation {
                message: format!(
                    "automation rule `{}` uses older_than_days=0; use a value greater than zero",
                    rule.id
                ),
            });
        }

        let has_predicate = rule.matcher.from_address.is_some()
            || !rule.matcher.subject_contains.is_empty()
            || !rule.matcher.label_any.is_empty()
            || rule.matcher.older_than_days.is_some()
            || rule.matcher.has_attachments.is_some()
            || rule.matcher.has_list_unsubscribe.is_some()
            || !rule.matcher.list_id_contains.is_empty()
            || !rule.matcher.precedence.is_empty();
        if !has_predicate {
            return Err(AutomationServiceError::RuleValidation {
                message: format!(
                    "automation rule `{}` must define at least one match predicate",
                    rule.id
                ),
            });
        }

        if let AutomationRuleAction::Label { add, remove } = &mut rule.action {
            *add = normalize_string_list(add);
            *remove = normalize_string_list(remove);
            if add.is_empty() && remove.is_empty() {
                return Err(AutomationServiceError::RuleValidation {
                    message: format!(
                        "automation rule `{}` label action must add or remove at least one label",
                        rule.id
                    ),
                });
            }

            let remove_labels = remove.iter().cloned().collect::<BTreeSet<_>>();
            let overlapping_labels = add
                .iter()
                .filter(|label| remove_labels.contains(*label))
                .cloned()
                .collect::<Vec<_>>();
            if !overlapping_labels.is_empty() {
                return Err(AutomationServiceError::RuleValidation {
                    message: format!(
                        "automation rule `{}` label action cannot add and remove the same label: {}",
                        rule.id,
                        overlapping_labels.join(", ")
                    ),
                });
            }
        }
    }

    Ok(())
}

fn normalize_match_lists(rule: &mut AutomationRule) {
    rule.matcher.from_address = rule
        .matcher
        .from_address
        .take()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    rule.matcher.subject_contains = normalize_string_list(&rule.matcher.subject_contains);
    rule.matcher.label_any = normalize_string_list(&rule.matcher.label_any);
    rule.matcher.list_id_contains = normalize_string_list(&rule.matcher.list_id_contains);
    rule.matcher.precedence = normalize_string_list(&rule.matcher.precedence)
        .into_iter()
        .map(|value| value.to_ascii_lowercase())
        .collect();
}

fn normalize_string_list(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .filter_map(|value| {
            let owned = value.to_owned();
            seen.insert(owned.clone()).then_some(owned)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{active_rules_path, resolve_rule_selection, validate_rule_file};
    use crate::config::resolve;
    use crate::workspace::WorkspacePaths;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn validate_rule_file_rejects_duplicate_ids() {
        let repo_root = unique_temp_dir("mailroom-automation-rules-duplicate");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        fs::write(
            active_rules_path(&config_report),
            r#"
[[rules]]
id = "dup"
priority = 100
[rules.match]
from_address = "newsletter@example.com"
[rules.action]
kind = "archive"

[[rules]]
id = "dup"
priority = 50
[rules.match]
subject_contains = ["digest"]
[rules.action]
kind = "trash"
"#,
        )
        .unwrap();

        let error = validate_rule_file(&config_report).unwrap_err();
        assert!(error.to_string().contains("duplicated"));

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn validate_rule_file_rejects_label_actions_with_overlapping_add_and_remove_labels() {
        let repo_root = unique_temp_dir("mailroom-automation-rules-overlap");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        fs::write(
            active_rules_path(&config_report),
            r#"
[[rules]]
id = "label-overlap"
priority = 100
[rules.match]
subject_contains = ["digest"]
[rules.action]
kind = "label"
add = ["INBOX", " Review "]
remove = ["INBOX"]
"#,
        )
        .unwrap();

        let error = validate_rule_file(&config_report).unwrap_err();
        assert_eq!(
            error.to_string(),
            "automation rule `label-overlap` label action cannot add and remove the same label: INBOX"
        );

        fs::remove_dir_all(repo_root).unwrap();
    }

    #[test]
    fn resolve_rule_selection_only_returns_enabled_requested_rules() {
        let repo_root = unique_temp_dir("mailroom-automation-rules-selection");
        let paths = WorkspacePaths::from_repo_root(repo_root.clone());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        fs::write(
            active_rules_path(&config_report),
            r#"
[[rules]]
id = "archive-digest"
priority = 100
[rules.match]
subject_contains = ["digest"]
[rules.action]
kind = "archive"

[[rules]]
id = "disabled-rule"
enabled = false
priority = 200
[rules.match]
subject_contains = ["disabled"]
[rules.action]
kind = "trash"
"#,
        )
        .unwrap();

        let resolved =
            resolve_rule_selection(&config_report, &[String::from("archive-digest")]).unwrap();
        assert_eq!(resolved.rules.len(), 1);
        assert_eq!(resolved.rules[0].id, "archive-digest");

        fs::remove_dir_all(repo_root).unwrap();
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }
}
