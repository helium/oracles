use clap::Parser;
use mobile_verifier::{
    cli::{generate, server},
    Result,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Verify Shares")]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand)]
pub enum Cmd {
    Generate(generate::Cmd),
    Server(server::Cmd),
}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "mobile_verifier=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    match Cli::parse().cmd {
        Cmd::Generate(cmd) => cmd.run().await?,
        Cmd::Server(cmd) => cmd.run().await?,
    }

    Ok(())
}
