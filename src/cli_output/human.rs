use anyhow::Error as AnyhowError;

pub(crate) fn print_human_failure(error: &AnyhowError) {
    eprintln!("{error:#}");
}
