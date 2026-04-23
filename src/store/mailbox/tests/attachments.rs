use super::*;

#[test]
fn attachment_catalog_round_trips_through_message_upserts() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachments-catalog");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();

    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Quarterly statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("m-1:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    let attachments = list_attachments(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentListQuery {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: None,
            message_id: None,
            filename: None,
            mime_type: None,
            fetched_only: false,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].attachment_key, "m-1:1.2");
    assert_eq!(attachments[0].filename, "statement.pdf");

    let detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "m-1:1.2",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.gmail_attachment_id.as_deref(), Some("att-1"));
    assert_eq!(detail.mime_type, "application/pdf");

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.attachment_count, 1);
    assert_eq!(mailbox.vaulted_attachment_count, 0);
    assert_eq!(mailbox.attachment_export_count, 0);
}

#[test]
fn attachment_vault_state_and_export_events_are_reflected_in_reads() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachments-vault");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Quarterly statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("m-1:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("m-1:1.2"),
            content_hash: String::from("abc123"),
            relative_path: String::from("blake3/ab/abc123"),
            size_bytes: 42,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap();
    record_attachment_export(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentExportEventInput {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("m-1:1.2"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            destination_path: String::from("/tmp/export/statement.pdf"),
            content_hash: String::from("abc123"),
            exported_at_epoch_s: 102,
        },
    )
    .unwrap();

    let fetched_only = list_attachments(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentListQuery {
            account_id: String::from("gmail:operator@example.com"),
            thread_id: None,
            message_id: None,
            filename: None,
            mime_type: None,
            fetched_only: true,
            limit: 10,
        },
    )
    .unwrap();
    assert_eq!(fetched_only.len(), 1);
    assert_eq!(fetched_only[0].export_count, 1);

    let detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "m-1:1.2",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.vault_content_hash.as_deref(), Some("abc123"));
    assert_eq!(detail.export_count, 1);

    let mailbox = inspect_mailbox(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
    )
    .unwrap()
    .unwrap();
    assert_eq!(mailbox.attachment_count, 1);
    assert_eq!(mailbox.vaulted_attachment_count, 1);
    assert_eq!(mailbox.attachment_export_count, 1);
}

#[test]
fn set_attachment_vault_state_errors_when_attachment_key_is_missing() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachment-vault-missing");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let error = set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("missing:1.2"),
            content_hash: String::from("abc123"),
            relative_path: String::from("blake3/ab/abc123"),
            size_bytes: 42,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        MailboxWriteError::AttachmentNotFound {
            account_id,
            attachment_key
        } if account_id == "gmail:operator@example.com" && attachment_key == "missing:1.2"
    ));
}

#[test]
fn record_attachment_export_errors_when_attachment_key_is_missing() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachment-export-missing");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let error = record_attachment_export(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentExportEventInput {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("missing:1.2"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            destination_path: String::from("/tmp/export/missing.pdf"),
            content_hash: String::from("abc123"),
            exported_at_epoch_s: 101,
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        MailboxWriteError::AttachmentNotFound {
            account_id,
            attachment_key
        } if account_id == "gmail:operator@example.com" && attachment_key == "missing:1.2"
    ));
}

#[test]
fn attachment_vault_updates_are_account_scoped_for_shared_attachment_keys() {
    let repo_root = unique_temp_dir("mailroom-mailbox-attachment-vault-account-scope");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("other@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:other@example.com",
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:operator@example.com"),
            message_id: String::from("m-1"),
            thread_id: String::from("t-1"),
            history_id: String::from("101"),
            internal_date_epoch_ms: 1_700_000_000_000,
            snippet: String::from("Statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("operator@example.com"),
            to_header: String::from("operator@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("shared:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-1")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[GmailMessageUpsertInput {
            account_id: String::from("gmail:other@example.com"),
            message_id: String::from("m-2"),
            thread_id: String::from("t-2"),
            history_id: String::from("102"),
            internal_date_epoch_ms: 1_700_000_000_100,
            snippet: String::from("Statement attached"),
            subject: String::from("Statement"),
            from_header: String::from("Billing <billing@example.com>"),
            from_address: Some(String::from("billing@example.com")),
            recipient_headers: String::from("other@example.com"),
            to_header: String::from("other@example.com"),
            cc_header: String::new(),
            bcc_header: String::new(),
            reply_to_header: String::new(),
            size_estimate: 123,
            automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
            label_ids: vec![String::from("INBOX")],
            label_names_text: String::from("INBOX"),
            attachments: vec![GmailAttachmentUpsertInput {
                attachment_key: String::from("shared:1.2"),
                part_id: String::from("1.2"),
                gmail_attachment_id: Some(String::from("att-2")),
                filename: String::from("statement.pdf"),
                mime_type: String::from("application/pdf"),
                size_bytes: 42,
                content_disposition: Some(String::from("attachment; filename=\"statement.pdf\"")),
                content_id: None,
                is_inline: false,
            }],
        }],
        100,
    )
    .unwrap();

    set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: String::from("gmail:operator@example.com"),
            attachment_key: String::from("shared:1.2"),
            content_hash: String::from("hash-a"),
            relative_path: String::from("blake3/ha/hash-a"),
            size_bytes: 42,
            fetched_at_epoch_s: 101,
        },
    )
    .unwrap();

    let operator_detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:operator@example.com",
        "shared:1.2",
    )
    .unwrap()
    .unwrap();
    let other_detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        "gmail:other@example.com",
        "shared:1.2",
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        operator_detail.vault_content_hash.as_deref(),
        Some("hash-a")
    );
    assert!(other_detail.vault_content_hash.is_none());
}

#[test]
fn finalize_full_sync_from_stage_preserves_attachment_vault_state() {
    let repo_root = unique_temp_dir("mailroom-mailbox-stage-vault-preserve");
    let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
    paths.ensure_runtime_dirs().unwrap();
    let config_report = resolve(&paths).unwrap();
    init(&config_report).unwrap();

    let account_id = "gmail:operator@example.com";
    accounts::upsert_active(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &accounts::UpsertAccountInput {
            email_address: String::from("operator@example.com"),
            history_id: String::from("100"),
            messages_total: 1,
            threads_total: 1,
            access_scope: String::from("scope:a"),
            refreshed_at_epoch_s: 100,
        },
    )
    .unwrap();
    replace_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
        100,
    )
    .unwrap();
    upsert_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &[mailbox_message(
            account_id,
            "message-1",
            "Before vault preserve",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
        100,
    )
    .unwrap();
    set_attachment_vault_state(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        &AttachmentVaultStateUpdate {
            account_id: account_id.to_owned(),
            attachment_key: String::from("message-1:1.1"),
            content_hash: String::from("hash-123"),
            relative_path: String::from("vault/message-1.bin"),
            size_bytes: 2048,
            fetched_at_epoch_s: 123,
        },
    )
    .unwrap();

    reset_full_sync_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
    )
    .unwrap();
    stage_full_sync_labels(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[gmail_label("INBOX", "INBOX", "system")],
    )
    .unwrap();
    stage_full_sync_messages(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        &[mailbox_message(
            account_id,
            "message-1",
            "After vault preserve",
            &["INBOX"],
            "INBOX",
            &["1.1"],
        )],
    )
    .unwrap();
    finalize_full_sync_from_stage(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        200,
        &full_sync_state(account_id, 200),
    )
    .unwrap();

    let detail = get_attachment_detail(
        &config_report.config.store.database_path,
        config_report.config.store.busy_timeout_ms,
        account_id,
        "message-1:1.1",
    )
    .unwrap()
    .unwrap();
    assert_eq!(detail.vault_content_hash.as_deref(), Some("hash-123"));
    assert_eq!(
        detail.vault_relative_path.as_deref(),
        Some("vault/message-1.bin")
    );
    assert_eq!(detail.vault_size_bytes, Some(2048));
    assert_eq!(detail.vault_fetched_at_epoch_s, Some(123));
}

#[test]
#[ignore = "benchmark harness; run manually with: cargo test benchmark_attachment_lane_full_sync_tiers -- --ignored --nocapture"]
fn benchmark_attachment_lane_full_sync_tiers() {
    let tiers = [("small", 250_usize), ("medium", 1_000), ("large", 3_000)];

    for (tier_name, message_count) in tiers {
        let repo_root = unique_temp_dir("mailroom-bench-full-sync");
        let paths = WorkspacePaths::from_repo_root(repo_root.path().to_path_buf());
        paths.ensure_runtime_dirs().unwrap();
        let config_report = resolve(&paths).unwrap();
        init(&config_report).unwrap();
        accounts::upsert_active(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            &accounts::UpsertAccountInput {
                email_address: String::from("operator@example.com"),
                history_id: String::from("100"),
                messages_total: i64::try_from(message_count).unwrap(),
                threads_total: i64::try_from(message_count).unwrap(),
                access_scope: String::from("scope:a"),
                refreshed_at_epoch_s: 100,
            },
        )
        .unwrap();

        let labels = [gmail_label("INBOX", "INBOX", "system")];
        replace_labels(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &labels,
            100,
        )
        .unwrap();

        let messages = (0..message_count)
            .map(|index| GmailMessageUpsertInput {
                account_id: String::from("gmail:operator@example.com"),
                message_id: format!("m-{index}"),
                thread_id: format!("t-{index}"),
                history_id: format!("{}", 200 + index),
                internal_date_epoch_ms: 1_700_000_000_000 + i64::try_from(index).unwrap(),
                snippet: format!("Benchmark mailbox payload {index}"),
                subject: format!("Benchmark subject {index}"),
                from_header: String::from("Benchmark <bench@example.com>"),
                from_address: Some(String::from("bench@example.com")),
                recipient_headers: String::from("operator@example.com"),
                to_header: String::from("operator@example.com"),
                cc_header: String::new(),
                bcc_header: String::new(),
                reply_to_header: String::new(),
                size_estimate: 4096,
                automation_headers: crate::store::mailbox::GmailAutomationHeaders::default(),
                label_ids: vec![String::from("INBOX")],
                label_names_text: String::from("INBOX"),
                attachments: vec![GmailAttachmentUpsertInput {
                    attachment_key: format!("m-{index}:1.1"),
                    part_id: String::from("1.1"),
                    gmail_attachment_id: Some(format!("att-{index}")),
                    filename: format!("bench-{index}.bin"),
                    mime_type: String::from("application/octet-stream"),
                    size_bytes: 1024,
                    content_disposition: Some(String::from("attachment")),
                    content_id: None,
                    is_inline: false,
                }],
            })
            .collect::<Vec<_>>();

        let started_at = Instant::now();
        commit_full_sync(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
            "gmail:operator@example.com",
            &labels,
            &messages,
            300,
            &SyncStateUpdate {
                account_id: String::from("gmail:operator@example.com"),
                cursor_history_id: Some(format!("{}", 300 + message_count)),
                bootstrap_query: String::from("newer_than:90d"),
                last_sync_mode: SyncMode::Full,
                last_sync_status: SyncStatus::Ok,
                last_error: None,
                last_sync_epoch_s: 300,
                last_full_sync_success_epoch_s: Some(300),
                last_incremental_sync_success_epoch_s: None,
                pipeline_enabled: false,
                pipeline_list_queue_high_water: 0,
                pipeline_write_queue_high_water: 0,
                pipeline_write_batch_count: 0,
                pipeline_writer_wait_ms: 0,
                pipeline_fetch_batch_count: 0,
                pipeline_fetch_batch_avg_ms: 0,
                pipeline_fetch_batch_max_ms: 0,
                pipeline_writer_tx_count: 0,
                pipeline_writer_tx_avg_ms: 0,
                pipeline_writer_tx_max_ms: 0,
                pipeline_reorder_buffer_high_water: 0,
                pipeline_staged_message_count: 0,
                pipeline_staged_delete_count: 0,
                pipeline_staged_attachment_count: 0,
            },
        )
        .unwrap();
        let elapsed = started_at.elapsed();

        let mailbox = inspect_mailbox(
            &config_report.config.store.database_path,
            config_report.config.store.busy_timeout_ms,
        )
        .unwrap()
        .unwrap();
        assert_eq!(mailbox.message_count, i64::try_from(message_count).unwrap());
        assert_eq!(
            mailbox.attachment_count,
            i64::try_from(message_count).unwrap()
        );

        let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
        let per_message_us = elapsed.as_secs_f64() * 1_000_000.0 / message_count as f64;
        println!(
            "{{\"bench\":\"attachment_lane.full_sync\",\"tier\":\"{tier_name}\",\"messages\":{message_count},\"attachments\":{message_count},\"elapsed_ms\":{elapsed_ms:.3},\"per_message_us\":{per_message_us:.3}}}"
        );
    }
}
