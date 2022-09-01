use crate::{keypair::load_from_file, mk_db_pool, server::Server, Result};
use tokio::signal;

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self) -> Result {
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
        let rs_keypair = load_from_file(&dotenv::var("REWARD_SERVER_KEYPAIR")?)?;

        // Reward server
        let mut reward_server = Server::new(pool.clone(), rs_keypair).await?;

        reward_server.run(shutdown_listener.clone()).await?;
        Ok(())
    }
}
