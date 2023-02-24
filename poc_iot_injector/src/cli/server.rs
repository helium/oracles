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

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // Create database pool
        let (pool, db_join_handle) = settings
            .database
            .connect(10, shutdown_listener.clone())
            .await?;
        sqlx::migrate!().run(&pool).await?;

        // poc_iot_injector server
        let mut poc_iot_injector_server = Server::new(settings, pool).await?;

        tokio::try_join!(
            db_join_handle.map_err(Error::from),
            poc_iot_injector_server
                .run(&shutdown_listener)
                .map_err(Error::from),
        )?;

        tracing::info!("Shutting down injector server");

        Ok(())
    }
}
