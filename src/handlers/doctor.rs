use crate::doctor;
use crate::{config, configured_paths, workspace};
use anyhow::Result;

pub(crate) fn handle_doctor_command(paths: &workspace::WorkspacePaths, json: bool) -> Result<()> {
    let config_report = config::resolve(paths)?;
    let configured_paths = configured_paths(&config_report)?;
    doctor::DoctorReport::inspect(&configured_paths, config_report)?.print(json)?;
    Ok(())
}
