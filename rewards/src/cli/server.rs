use crate::{follower::Follower, keypair::load_from_file, mk_db_pool, server::Server, Result};
use tokio::{signal, sync::broadcast};

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self) -> Result {
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
}
