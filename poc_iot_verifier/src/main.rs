use clap::Parser;
use poc_iot_verifier::{loader, mk_db_pool, purger, runner, Result};
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
            dotenv::var("RUST_LOG").unwrap_or_else(|_| {
                "poclora_report=info,poc_store=info,poclora_verifier=info".into()
            }),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Create database pool and run migrations
    let pool = mk_db_pool(2).await?;
    tracing::info!("running migrations");
    sqlx::migrate!().run(&pool).await?;

    // configure shutdown trigger
    let (shutdown_trigger, shutdown) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    let loader = loader::Loader::from_env().await?;
    let runner = runner::Runner::from_env().await?;
    let purger = purger::Purger::from_env().await?;
    tokio::try_join!(
        runner.run(&shutdown),
        loader.run(&shutdown),
        purger.run(&shutdown)
    )
    .map(|_| ())
}
