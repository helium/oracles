use crate::{server::Server, Settings};
use anyhow::{Error, Result};
use file_store::{
    file_info_poller::LookbackBehavior, file_source, iot_valid_poc::IotPoc, FileStore, FileType,
};
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

        let file_store = FileStore::from_settings(&settings.verifier).await?;

        let (receiver, source_join_handle) = file_source::continuous_source::<IotPoc>()
            .db(pool)
            .store(file_store)
            .file_type(FileType::IotPoc)
            .lookback(LookbackBehavior::Max(settings.max_lookback_age()))
            .build()?
            .start(shutdown_listener.clone())
            .await?;

        // poc_iot_injector server
        let mut poc_iot_injector_server = Server::new(settings).await?;

        let _ = tokio::try_join!(
            poc_iot_injector_server
                .run(&shutdown_listener, receiver)
                .map_err(Error::from),
            source_join_handle.map_err(Error::from),
        )?;

        tracing::info!("Shutting down injector server");

        Ok(())
    }
}
