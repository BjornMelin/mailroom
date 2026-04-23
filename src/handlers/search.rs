use crate::cli::SearchArgs;
use crate::{config, mailbox, workspace};
use anyhow::Result;

pub(crate) async fn handle_search_command(
    paths: &workspace::WorkspacePaths,
    args: SearchArgs,
) -> Result<()> {
    let config_report = config::resolve(paths)?;
    mailbox::search(
        &config_report,
        mailbox::SearchRequest {
            terms: args.terms,
            label: args.label,
            from_address: args.from_address,
            after: args.after,
            before: args.before,
            limit: args.limit,
        },
    )
    .await?
    .print(args.json)?;

    Ok(())
}
