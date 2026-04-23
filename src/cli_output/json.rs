use super::errors::JsonErrorBody;
use anyhow::Result;
use serde::Serialize;
use std::io::Write;

#[derive(Debug, Serialize)]
pub(super) struct JsonSuccessEnvelope<'a, T> {
    success: bool,
    data: &'a T,
}

#[derive(Debug, Serialize)]
pub(super) struct JsonFailureEnvelope<'a> {
    success: bool,
    error: &'a JsonErrorBody,
}

pub(crate) fn print_json_success<T: Serialize>(data: &T) -> Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    write_json_success(&mut stdout, data)
}

pub(crate) fn print_json_failure(error: &JsonErrorBody) -> Result<()> {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    write_json_failure(&mut stdout, error)
}

pub(crate) fn write_json_success<W: Write, T: Serialize>(writer: &mut W, data: &T) -> Result<()> {
    serde_json::to_writer_pretty(&mut *writer, &json_success_value(data))?;
    writeln!(writer)?;
    Ok(())
}

fn write_json_failure<W: Write>(writer: &mut W, error: &JsonErrorBody) -> Result<()> {
    serde_json::to_writer_pretty(&mut *writer, &json_failure_value(error))?;
    writeln!(writer)?;
    Ok(())
}

pub(super) fn json_success_value<T: Serialize>(data: &T) -> JsonSuccessEnvelope<'_, T> {
    JsonSuccessEnvelope {
        success: true,
        data,
    }
}

pub(super) fn json_failure_value(error: &JsonErrorBody) -> JsonFailureEnvelope<'_> {
    JsonFailureEnvelope {
        success: false,
        error,
    }
}
