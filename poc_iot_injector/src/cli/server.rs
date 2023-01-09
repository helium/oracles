use crate::{server::Server, Settings};
use anyhow::{Error, Result};
use futures_util::TryFutureExt;
use tokio::signal;

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let pool = settings.database.connect(2).await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // poc_iot_injector server
        let mut poc_iot_injector_server = Server::new(settings).await?;

        tokio::try_join!(poc_iot_injector_server
            .run(&shutdown_listener)
            .map_err(Error::from),)?;

        tracing::info!("Shutting down injector server");

        Ok(())
    }
}
