use clap::Parser;
use icicle::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Cli::parse().run().await
}
