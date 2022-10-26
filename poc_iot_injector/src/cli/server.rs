use crate::{server::Server, Result, Settings};
use tokio::signal;

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let pool = settings.database.connect().await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // poc_iot_injector server
        let mut poc_iot_injector_server = Server::new(settings).await?;

        poc_iot_injector_server
            .run(shutdown_listener.clone())
            .await?;
        Ok(())
    }
}
