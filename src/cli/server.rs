use crate::{api::server, cli, Follower, Result};
use tokio::signal;

/// Starts the server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self) -> Result {
        let follower_uri = dotenv::var("FOLLOWER_URI")?;

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
        let mut follower = Follower::new(follower_uri.try_into()?, pool.clone())?;

        tokio::try_join!(
            api_server,
            grpc_server,
            follower.run(shutdown_listener.clone())
        )?;

        Ok(())
    }
}
