use crate::{run_server, Result};
use sqlx::postgres::PgPoolOptions;

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self) -> Result {
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let db_connection_str = dotenv::var("DATABASE_URL")?;

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&db_connection_str)
            .await?;

        sqlx::migrate!().run(&pool).await?;

        run_server(pool, shutdown_listener).await?;

        Ok(())
    }
}
