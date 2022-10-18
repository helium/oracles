use clap::Parser;
use poc_mobile_verifier::{
    cli::{generate, server},
    Result,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,

    #[clap(long = "dotenv-path", short = 'e', default_value = ".env")]
    dotenv_path: std::path::PathBuf,
}

#[derive(clap::Subcommand)]
pub enum Cmd {
    Generate(generate::Cmd),
    Server(server::Cmd),
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    dotenv::from_path(&cli.dotenv_path)?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "mobile_verifier=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    match cli.cmd {
        Cmd::Generate(cmd) => cmd.run().await?,
        Cmd::Server(cmd) => cmd.run().await?,
    }

    Ok(())
}
