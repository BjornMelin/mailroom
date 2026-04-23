mod errors;
mod human;
mod json;

pub(crate) use errors::{describe_error, exit_code};
pub(crate) use human::print_human_failure;
pub(crate) use json::{print_json_failure, print_json_success, write_json_success};

#[cfg(test)]
mod tests;
