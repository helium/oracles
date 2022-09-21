use crate::{mk_db_pool, Result, Server};

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self) -> Result {
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        Server::new(mk_db_pool(10).await?)
            .await?
            .run(shutdown_listener)
            .await?;

        Ok(())
    }
}
