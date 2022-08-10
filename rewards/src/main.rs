use clap::Parser;
use poc5g_rewards::{
    follower::Follower, keypair::load_from_file, mk_db_pool, server::Server, Result,
};
use tokio::{signal, sync::broadcast};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Reward Server")]
pub struct Cli {}

#[tokio::main]
async fn main() -> Result {
    dotenv::dotenv()?;
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            dotenv::var("RUST_LOG").unwrap_or_else(|_| "poc5g_rewards=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let _cli = Cli::parse();

    // Create database pool
    let pool = mk_db_pool(10).await?;
    sqlx::migrate!().run(&pool).await?;

    // configure shutdown trigger
    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    let (trigger_sender, trigger_receiver) = broadcast::channel(2);
    let mut follower = Follower::new(pool.clone(), trigger_sender).await?;

    // reward server keypair from env
    let rs_keypair = load_from_file(&dotenv::var("REWARD_SERVER_KEYPAIR")?)?;

    // reward server
    let mut reward_server = Server::new(pool.clone(), trigger_receiver, rs_keypair).await?;

    tokio::try_join!(
        follower.run(shutdown_listener.clone()),
        reward_server.run(shutdown_listener.clone())
    )?;

    Ok(())
}
