#[tokio::main]
async fn main() -> anyhow::Result<()> {
    mailroom::run().await
}
