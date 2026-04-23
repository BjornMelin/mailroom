use crate::CliInputError;
use crate::cli::{SyncCommand, SyncPerfCommand, SyncProfileArg, SyncRunArgs};
use crate::{config, mailbox, workspace};
use anyhow::Result;

pub(crate) async fn handle_sync_command(
    paths: &workspace::WorkspacePaths,
    command: SyncCommand,
) -> Result<()> {
    let config_report = config::resolve(paths)?;

    match command {
        SyncCommand::Run(args) | SyncCommand::Benchmark(args) => {
            mailbox::sync_run_with_options(&config_report, resolve_sync_run_options(&args)?)
                .await?
                .print(args.json)?
        }
        SyncCommand::History { limit, json } => mailbox::sync_history(&config_report, limit)
            .await?
            .print(json)?,
        SyncCommand::Perf {
            command: SyncPerfCommand::Explain { limit, json },
        }
        | SyncCommand::PerfExplain { limit, json } => {
            mailbox::sync_perf_explain(&config_report, limit)
                .await?
                .print(json)?
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncProfileDefaults {
    force_full: bool,
    recent_days: u32,
    quota_units_per_minute: u32,
    message_fetch_concurrency: usize,
}

fn default_sync_profile_defaults() -> SyncProfileDefaults {
    SyncProfileDefaults {
        force_full: false,
        recent_days: mailbox::DEFAULT_BOOTSTRAP_RECENT_DAYS,
        quota_units_per_minute: mailbox::DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE,
        message_fetch_concurrency: mailbox::DEFAULT_MESSAGE_FETCH_CONCURRENCY,
    }
}

fn sync_profile_defaults(profile: SyncProfileArg) -> SyncProfileDefaults {
    match profile {
        SyncProfileArg::DeepAudit => SyncProfileDefaults {
            force_full: true,
            recent_days: 365,
            quota_units_per_minute: 9_000,
            message_fetch_concurrency: 3,
        },
    }
}

fn resolve_sync_run_options(args: &SyncRunArgs) -> Result<mailbox::SyncRunOptions> {
    let defaults = args
        .profile
        .map(sync_profile_defaults)
        .unwrap_or_else(default_sync_profile_defaults);
    let recent_days = args.recent_days.unwrap_or(defaults.recent_days);
    if recent_days == 0 {
        return Err(CliInputError::RecentDaysZero.into());
    }
    let quota_units_per_minute = args
        .quota_units_per_minute
        .unwrap_or(defaults.quota_units_per_minute);
    if quota_units_per_minute == 0 {
        return Err(CliInputError::QuotaUnitsPerMinuteZero.into());
    }
    let message_fetch_concurrency = args
        .message_fetch_concurrency
        .unwrap_or(defaults.message_fetch_concurrency);
    if message_fetch_concurrency == 0 {
        return Err(CliInputError::MessageFetchConcurrencyZero.into());
    }

    Ok(mailbox::SyncRunOptions {
        force_full: args.full || defaults.force_full,
        recent_days,
        quota_units_per_minute,
        message_fetch_concurrency,
    })
}

#[cfg(test)]
mod tests {
    use super::resolve_sync_run_options;
    use crate::CliInputError;
    use crate::cli::{Cli, Commands, SyncCommand, SyncProfileArg, SyncRunArgs};
    use clap::{CommandFactory, Parser, error::ErrorKind};

    #[test]
    fn resolve_sync_run_options_uses_legacy_defaults_without_profile() {
        let args = SyncRunArgs {
            full: false,
            profile: None,
            recent_days: None,
            quota_units_per_minute: None,
            message_fetch_concurrency: None,
            json: false,
        };

        let options = resolve_sync_run_options(&args).unwrap();

        assert_eq!(
            options.recent_days,
            crate::mailbox::DEFAULT_BOOTSTRAP_RECENT_DAYS
        );
        assert_eq!(
            options.quota_units_per_minute,
            crate::mailbox::DEFAULT_SYNC_QUOTA_UNITS_PER_MINUTE
        );
        assert_eq!(
            options.message_fetch_concurrency,
            crate::mailbox::DEFAULT_MESSAGE_FETCH_CONCURRENCY
        );
        assert!(!options.force_full);
    }

    #[test]
    fn resolve_sync_run_options_applies_deep_audit_profile_defaults() {
        let args = SyncRunArgs {
            full: false,
            profile: Some(SyncProfileArg::DeepAudit),
            recent_days: None,
            quota_units_per_minute: None,
            message_fetch_concurrency: None,
            json: false,
        };

        let options = resolve_sync_run_options(&args).unwrap();

        assert_eq!(options.recent_days, 365);
        assert_eq!(options.quota_units_per_minute, 9_000);
        assert_eq!(options.message_fetch_concurrency, 3);
        assert!(options.force_full);
    }

    #[test]
    fn resolve_sync_run_options_keeps_explicit_overrides_authoritative() {
        let args = SyncRunArgs {
            full: false,
            profile: Some(SyncProfileArg::DeepAudit),
            recent_days: Some(180),
            quota_units_per_minute: Some(8_000),
            message_fetch_concurrency: Some(2),
            json: false,
        };

        let options = resolve_sync_run_options(&args).unwrap();

        assert_eq!(options.recent_days, 180);
        assert_eq!(options.quota_units_per_minute, 8_000);
        assert_eq!(options.message_fetch_concurrency, 2);
        assert!(options.force_full);
    }

    #[test]
    fn resolve_sync_run_options_rejects_zero_overrides() {
        let args = SyncRunArgs {
            full: false,
            profile: None,
            recent_days: Some(0),
            quota_units_per_minute: Some(0),
            message_fetch_concurrency: Some(0),
            json: false,
        };

        let error = resolve_sync_run_options(&args).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::RecentDaysZero)
        ));
        assert_eq!(error.to_string(), "--recent-days must be greater than zero");
    }

    #[test]
    fn resolve_sync_run_options_rejects_zero_quota_override() {
        let args = SyncRunArgs {
            full: false,
            profile: None,
            recent_days: None,
            quota_units_per_minute: Some(0),
            message_fetch_concurrency: None,
            json: false,
        };

        let error = resolve_sync_run_options(&args).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::QuotaUnitsPerMinuteZero)
        ));
        assert_eq!(
            error.to_string(),
            "--quota-units-per-minute must be greater than zero"
        );
    }

    #[test]
    fn resolve_sync_run_options_rejects_zero_message_fetch_concurrency_override() {
        let args = SyncRunArgs {
            full: false,
            profile: None,
            recent_days: None,
            quota_units_per_minute: None,
            message_fetch_concurrency: Some(0),
            json: false,
        };

        let error = resolve_sync_run_options(&args).unwrap_err();
        assert!(matches!(
            error.downcast_ref::<CliInputError>(),
            Some(CliInputError::MessageFetchConcurrencyZero)
        ));
        assert_eq!(
            error.to_string(),
            "--message-fetch-concurrency must be greater than zero"
        );
    }

    #[test]
    fn sync_run_and_benchmark_parse_the_same_profile_surface() {
        let run_args = extract_sync_run_args([
            "mailroom",
            "sync",
            "run",
            "--profile",
            "deep-audit",
            "--quota-units-per-minute",
            "8000",
        ]);
        let benchmark_args = extract_sync_run_args([
            "mailroom",
            "sync",
            "benchmark",
            "--profile",
            "deep-audit",
            "--quota-units-per-minute",
            "8000",
        ]);

        assert_eq!(
            run_args,
            SyncRunArgs {
                full: false,
                profile: Some(SyncProfileArg::DeepAudit),
                recent_days: None,
                quota_units_per_minute: Some(8_000),
                message_fetch_concurrency: None,
                json: false,
            }
        );
        assert_eq!(run_args, benchmark_args);
        assert_eq!(
            resolve_sync_run_options(&run_args).unwrap(),
            resolve_sync_run_options(&benchmark_args).unwrap()
        );
    }

    #[test]
    fn sync_profile_help_lists_deep_audit_profile() {
        let mut command = Cli::command();
        let error = command
            .try_get_matches_from_mut(["mailroom", "sync", "run", "--help"])
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        let rendered = error.to_string();
        assert!(rendered.contains("--profile <PROFILE>"));
        assert!(rendered.contains("deep-audit"));
    }

    #[test]
    fn sync_profile_rejects_unknown_value() {
        let error =
            Cli::try_parse_from(["mailroom", "sync", "run", "--profile", "unknown"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidValue);
        let rendered = error.to_string();
        assert!(rendered.contains("deep-audit"));
        assert!(rendered.contains("--profile <PROFILE>"));
    }

    fn extract_sync_run_args<I, T>(args: I) -> SyncRunArgs
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli = Cli::try_parse_from(args).unwrap();
        match cli.command {
            Commands::Sync {
                command: SyncCommand::Run(args),
            }
            | Commands::Sync {
                command: SyncCommand::Benchmark(args),
            } => args,
            other => panic!("expected sync run or benchmark args, got {other:?}"),
        }
    }
}
