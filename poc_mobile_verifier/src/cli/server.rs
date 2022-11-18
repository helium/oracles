use crate::{
    verifier::{Verifier, VerifierDaemon},
    Settings,
};
use anyhow::{Error, Result};
use chrono::Duration;
use file_store::{file_sink, file_upload, FileStore, FileType};
use futures_util::TryFutureExt;

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        poc_metrics::install_metrics();

        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        let pool = settings.database.connect(10).await?;
        sqlx::migrate!().run(&pool).await?;

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        // Heartbeats
        let (heartbeats_tx, heartbeats_rx) = file_sink::message_channel(50);
        let mut heartbeats = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            heartbeats_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Speedtest averages
        let (speedtest_avg_tx, speedtest_avg_rx) = file_sink::message_channel(50);
        let mut speedtest_avgs = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            speedtest_avg_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Radio share rewards
        let (radio_rewards_tx, radio_rewards_rx) = file_sink::message_channel(50);
        let mut subnet_rewards = file_sink::FileSinkBuilder::new(
            FileType::RadioRewardShare,
            store_base_path,
            radio_rewards_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(15))
        .write_manifest(true)
        .create()
        .await?;

        // Reward manifest
        let (reward_manifest_tx, reward_manifest_rx) = file_sink::message_channel(50);
        let mut reward_manifests = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            store_base_path,
            reward_manifest_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        let follower = settings.follower.connect_follower()?;

        let reward_period_hours = settings.rewards;
        let verifications_per_period = settings.verifications;
        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let verifier = Verifier::new(file_store, follower);

        let verifier_daemon = VerifierDaemon {
            verification_offset: settings.verification_offset_duration(),
            pool,
            heartbeats_tx,
            speedtest_avg_tx,
            radio_rewards_tx,
            reward_manifest_tx,
            reward_period_hours,
            verifications_per_period,
            verifier,
        };

        tokio::try_join!(
            heartbeats.run(&shutdown_listener).map_err(Error::from),
            speedtest_avgs.run(&shutdown_listener).map_err(Error::from),
            subnet_rewards.run(&shutdown_listener).map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            reward_manifests
                .run(&shutdown_listener)
                .map_err(Error::from),
            verifier_daemon.run(&shutdown_listener),
        )?;

        tracing::info!("Shutting down verifier server");

        Ok(())
    }
}
