use crate::mailbox::SyncRunReport;
use crate::store;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{self, Display};

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowListReport {
    pub stage: Option<store::workflows::WorkflowStage>,
    pub triage_bucket: Option<store::workflows::TriageBucket>,
    pub workflows: Vec<store::workflows::WorkflowRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowShowReport {
    pub detail: store::workflows::WorkflowDetail,
}

#[derive(Debug, Clone, Serialize)]
pub struct CleanupPreview {
    pub action: store::workflows::CleanupAction,
    pub execute: bool,
    pub add_label_names: Vec<String>,
    pub remove_label_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowActionReport {
    pub action: WorkflowAction,
    pub workflow: store::workflows::WorkflowRecord,
    pub current_draft: Option<store::workflows::DraftRevisionDetail>,
    pub cleanup_preview: Option<CleanupPreview>,
    pub sync_report: Option<SyncRunReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowAction {
    CleanupPreview,
    CleanupApplied,
    WorkflowPromoted,
    WorkflowSnoozed,
    WorkflowSnoozeCleared,
    TriageSet,
    DraftStarted,
    DraftBodySet,
    DraftSent,
    DraftAttachmentAdded,
    DraftAttachmentRemoved,
    Other(String),
}

impl WorkflowAction {
    fn as_str(&self) -> &str {
        match self {
            Self::CleanupPreview => "cleanup_preview",
            Self::CleanupApplied => "cleanup_applied",
            Self::WorkflowPromoted => "workflow_promoted",
            Self::WorkflowSnoozed => "workflow_snoozed",
            Self::WorkflowSnoozeCleared => "workflow_snooze_cleared",
            Self::TriageSet => "triage_set",
            Self::DraftStarted => "draft_started",
            Self::DraftBodySet => "draft_body_set",
            Self::DraftSent => "draft_sent",
            Self::DraftAttachmentAdded => "draft_attachment_added",
            Self::DraftAttachmentRemoved => "draft_attachment_removed",
            Self::Other(value) => value.as_str(),
        }
    }
}

impl Display for WorkflowAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for WorkflowAction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for WorkflowAction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "cleanup_preview" => Self::CleanupPreview,
            "cleanup_applied" => Self::CleanupApplied,
            "workflow_promoted" => Self::WorkflowPromoted,
            "workflow_snoozed" => Self::WorkflowSnoozed,
            "workflow_snooze_cleared" => Self::WorkflowSnoozeCleared,
            "triage_set" => Self::TriageSet,
            "draft_started" => Self::DraftStarted,
            "draft_body_set" => Self::DraftBodySet,
            "draft_sent" => Self::DraftSent,
            "draft_attachment_added" => Self::DraftAttachmentAdded,
            "draft_attachment_removed" => Self::DraftAttachmentRemoved,
            _ => Self::Other(value),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::WorkflowAction;

    #[test]
    fn workflow_action_serializes_to_stable_snake_case_strings() {
        assert_eq!(
            serde_json::to_string(&WorkflowAction::DraftAttachmentRemoved).unwrap(),
            "\"draft_attachment_removed\""
        );
        assert_eq!(
            serde_json::to_string(&WorkflowAction::Other(String::from("custom_action"))).unwrap(),
            "\"custom_action\""
        );
    }

    #[test]
    fn workflow_action_deserializes_known_and_unknown_values() {
        let known: WorkflowAction = serde_json::from_str("\"workflow_promoted\"").unwrap();
        let unknown: WorkflowAction = serde_json::from_str("\"custom_action\"").unwrap();

        assert!(matches!(known, WorkflowAction::WorkflowPromoted));
        assert!(matches!(unknown, WorkflowAction::Other(value) if value == "custom_action"));
    }
}
