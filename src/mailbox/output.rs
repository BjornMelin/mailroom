use crate::mailbox::{SearchReport, SyncRunReport};
use anyhow::Result;

impl SyncRunReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            print!("{}", self.render_plain());
        }

        Ok(())
    }

    fn render_plain(&self) -> String {
        [
            format!("mode={}", self.mode),
            format!("fallback_from_history={}", self.fallback_from_history),
            format!("bootstrap_query={}", self.bootstrap_query),
            format!("cursor_history_id={}", self.cursor_history_id),
            format!("pages_fetched={}", self.pages_fetched),
            format!("messages_listed={}", self.messages_listed),
            format!("messages_upserted={}", self.messages_upserted),
            format!("messages_deleted={}", self.messages_deleted),
            format!("labels_synced={}", self.labels_synced),
            format!("store_message_count={}", self.store_message_count),
            format!("store_label_count={}", self.store_label_count),
            format!(
                "store_indexed_message_count={}",
                self.store_indexed_message_count
            ),
        ]
        .join("\n")
            + "\n"
    }
}

impl SearchReport {
    pub fn print(&self, json: bool) -> Result<()> {
        if json {
            println!("{}", serde_json::to_string_pretty(self)?);
        } else {
            print!("{}", self.render_plain());
        }

        Ok(())
    }

    fn render_plain(&self) -> String {
        let mut lines = vec![format!("terms={}", self.terms)];
        if let Some(label) = &self.label {
            lines.push(format!("label={label}"));
        }
        if let Some(from_address) = &self.from_address {
            lines.push(format!("from={from_address}"));
        }
        if let Some(after_epoch_ms) = self.after_epoch_ms {
            lines.push(format!("after_epoch_ms={after_epoch_ms}"));
        }
        if let Some(before_epoch_ms) = self.before_epoch_ms {
            lines.push(format!("before_epoch_ms={before_epoch_ms}"));
        }
        lines.push(format!("limit={}", self.limit));
        lines.push(format!("result_count={}", self.results.len()));
        lines.push(String::from(
            "results_format=tsv\tmessage_id\tinternal_date_epoch_ms\tfrom_header\tsubject",
        ));
        lines.extend(self.results.iter().map(|result| {
            format!(
                "{}\t{}\t{}\t{}",
                result.message_id,
                result.internal_date_epoch_ms,
                result.from_header.replace('\t', " "),
                result.subject.replace('\t', " "),
            )
        }));
        lines.join("\n") + "\n"
    }
}

#[cfg(test)]
mod tests {
    use super::SearchReport;
    use crate::store;

    #[test]
    fn render_plain_search_report_uses_tsv_result_rows() {
        let report = SearchReport {
            terms: String::from("alpha"),
            label: Some(String::from("INBOX")),
            from_address: Some(String::from("alice@example.com")),
            after_epoch_ms: Some(10),
            before_epoch_ms: Some(20),
            limit: 5,
            results: vec![store::mailbox::SearchResult {
                message_id: String::from("m-1"),
                thread_id: String::from("t-1"),
                internal_date_epoch_ms: 123,
                subject: String::from("Alpha launch checklist"),
                from_header: String::from("Alice Example <alice@example.com>"),
                from_address: Some(String::from("alice@example.com")),
                recipient_headers: String::from("ops@example.com"),
                snippet: String::from("snippet"),
                label_names: vec![String::from("INBOX")],
                thread_message_count: 1,
                rank: 0.5,
            }],
        };

        let rendered = report.render_plain();

        assert!(rendered.contains(
            "results_format=tsv\tmessage_id\tinternal_date_epoch_ms\tfrom_header\tsubject"
        ));
        assert!(
            rendered
                .contains("m-1\t123\tAlice Example <alice@example.com>\tAlpha launch checklist")
        );
    }
}
