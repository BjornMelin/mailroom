use super::preserve_sync_error;
use anyhow::anyhow;

#[test]
fn preserve_sync_error_returns_original_error_when_failure_persistence_also_fails() {
    let error = preserve_sync_error(anyhow!("sync failed"), Err(anyhow!("persist failed")));

    assert_eq!(error.to_string(), "sync failed");
}

#[test]
fn preserve_sync_error_returns_original_error_when_failure_persistence_succeeds() {
    let error = preserve_sync_error(anyhow!("sync failed"), Ok(()));

    assert_eq!(error.to_string(), "sync failed");
}
