use clap::Parser;
use mobile_rewards::{mk_db_pool, Result, Server};
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = env!("CARGO_PKG_DESCRIPTION"))]
pub struct Cli {
    #[clap(long = "dotenv-path", short = 'e', default_value = ".env")]
    dotenv_path: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result {
    let cli = Cli::parse();
    dotenv::from_path(&cli.dotenv_path)?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "mobile_rewards=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Install the prometheus metrics exporter
    poc_metrics::install_metrics();

    // Create database pool
    let pool = mk_db_pool(10).await?;
    sqlx::migrate!().run(&pool).await?;

    // Configure shutdown trigger
    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // Reward server keypair from env
    let keypair_file = std::env::var("REWARD_SERVER_KEYPAIR")?;
    let rs_keypair = load_from_file(&keypair_file)?;

    // Reward server
    let mut reward_server = Server::new(pool.clone(), rs_keypair).await?;

    reward_server.run(shutdown_listener.clone()).await?;
    Ok(())
}

pub fn load_from_file(path: &str) -> Result<helium_crypto::Keypair> {
    let data = std::fs::read(path)?;
    Ok(helium_crypto::Keypair::try_from(&data[..])?)
}
