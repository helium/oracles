use crate::{
    verifier::{Verifier, VerifierDaemon},
    Settings,
};
use anyhow::{Error, Result};
use chrono::Duration;
use file_store::{file_sink, file_upload, FileStore, FileType};
use futures_util::TryFutureExt;
use price::PriceTracker;

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::start_metrics(&settings.metrics)?;

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let (pool, db_join_handle) = settings
            .database
            .connect(env!("CARGO_PKG_NAME"), shutdown_listener.clone())
            .await?;
        sqlx::migrate!().run(&pool).await?;

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        // Heartbeats
        let (heartbeats, mut heartbeats_server) = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_heartbeat"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Speedtest averages
        let (speedtest_avgs, mut speedtest_avgs_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Radio share rewards
        let (radio_rewards, mut radio_rewards_server) = file_sink::FileSinkBuilder::new(
            FileType::RadioRewardShare,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_radio_reward_shares"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        // Reward manifest
        let (reward_manifests, mut reward_manifests_server) = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_reward_manifest"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let follower = settings.follower.connect_follower();

        let reward_period_hours = settings.rewards;
        let verifications_per_period = settings.verifications;
        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (price_tracker, tracker_process) =
            PriceTracker::start(&settings.price_tracker, shutdown_listener.clone()).await?;

        let verifier = Verifier::new(file_store, follower);

        let verifier_daemon = VerifierDaemon {
            verification_offset: settings.verification_offset_duration(),
            pool,
            heartbeats,
            speedtest_avgs,
            radio_rewards,
            reward_manifests,
            reward_period_hours,
            verifications_per_period,
            verifier,
            price_tracker,
        };

        tokio::try_join!(
            db_join_handle.map_err(Error::from),
            heartbeats_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            speedtest_avgs_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            radio_rewards_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            reward_manifests_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            verifier_daemon.run(&shutdown_listener),
            tracker_process.map_err(Error::from),
        )?;

        tracing::info!("Shutting down verifier server");

        Ok(())
    }
}
