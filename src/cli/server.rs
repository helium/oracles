use crate::{api::server, cli, env_var, Follower, Result};
use tokio::{signal, sync::broadcast};

/// Starts the server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

/// First block that 5G hotspots were introduced (FreedomFi)
pub const DEFAULT_FOLLOWER_START_BLOCK: i64 = 995041;

impl Cmd {
    pub async fn run(&self) -> Result {
        // Create database pool
        let pool = cli::mk_db_pool(10).await?;
        sqlx::migrate!().run(&pool).await?;

        // configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // api server
        let api_server = server::api_server(pool.clone(), shutdown_listener.clone());

        // grpc server
        let grpc_server = server::grpc_server(pool.clone(), shutdown_listener.clone());

        // chain follower
        let follower_uri = dotenv::var("FOLLOWER_URI")?;
        let follower_start_block = env_var("FOLLOWER_START_BLOCK", DEFAULT_FOLLOWER_START_BLOCK)?;
        let (trigger_sender, _trigger_receiver) = broadcast::channel(2);
        let mut follower = Follower::new(
            follower_uri.try_into()?,
            pool.clone(),
            follower_start_block,
            trigger_sender,
        )
        .await?;

        // reward server
        // let mut reward_server = RewardServer::new(follower_uri, base_path, trigger_receiver.clone(), shutdown_listener.clone());

        tokio::try_join!(
            api_server,
            grpc_server,
            follower.run(shutdown_listener.clone()),
            // reward_server.run(shutdown_listener.clone())
        )?;

        Ok(())
    }
}
