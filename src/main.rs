#[tokio::main]
async fn main() -> std::process::ExitCode {
    mailroom::run().await
}
