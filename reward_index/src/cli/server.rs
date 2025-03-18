use anyhow::Result;
use file_store::{
    file_info_poller::LookbackBehavior, file_source, reward_manifest::RewardManifest, FileStore,
    FileType,
};
use task_manager::TaskManager;

use crate::{telemetry, Indexer, Settings};

#[derive(Debug, clap::Args)]
pub struct Server;

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let file_store = FileStore::from_settings(&settings.verifier).await?;
        let (receiver, server) = file_source::continuous_source::<RewardManifest, _>()
            .state(pool.clone())
            .store(file_store.clone())
            .prefix(FileType::RewardManifest.to_string())
            .lookback(LookbackBehavior::StartAfter(settings.start_after))
            .poll_duration(settings.interval)
            .offset(settings.interval * 2)
            .create()
            .await?;

        // Reward server
        let indexer = Indexer::from_settings(settings, pool, file_store, receiver).await?;

        TaskManager::builder()
            .add_task(server)
            .add_task(indexer)
            .build()
            .start()
            .await
    }
}
