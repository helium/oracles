use crate::{verifier::VerifierDaemon, Settings};
use anyhow::{Error, Result};
use chrono::Duration;
use file_store::{file_sink, file_upload, FileStore, FileType, file_source, heartbeat::CellHeartbeatIngestReport, file_info_poller::LookbackBehavior, speedtest::CellSpeedtestIngestReport};
use futures_util::TryFutureExt;
use mobile_config::Client;
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

        let ingest = FileStore::from_settings(&settings.ingest).await?;

        // Heartbeats
        let (heartbeats, heartbeats_join_handle) =
            file_source::continuous_source::<CellHeartbeatIngestReport>()
            .db(pool.clone())
            .store(ingest.clone())
            .lookback(LookbackBehavior::StartAfter(settings.start_after()))
            .file_type(FileType::CellHeartbeatIngestReport)
            .build()?
            .start(shutdown_listener.clone())
            .await?;

        // Speedtests
        let (speedtests, speedtests_join_handle) =
            file_source::continuous_source::<CellSpeedtestIngestReport>()
            .db(pool.clone())
            .store(ingest.clone())
            .lookback(LookbackBehavior::StartAfter(settings.start_after()))
            .file_type(FileType::CellSpeedtestIngestReport)
            .build()?
            .start(shutdown_listener.clone())
            .await?;

        // Valid Heartbeats
        let (valid_heartbeats, mut valid_heartbeats_server) = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_heartbeat"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Valid Speedtests
        let (valid_speedtests, mut valid_speedtests_server) = file_sink::FileSinkBuilder::new(
            FileType::SpeedtestAvg,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_speedtest_average"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .auto_commit(false)
        .roll_time(Duration::minutes(15))
        .create()
        .await?;

        // Mobile rewards
        let (mobile_rewards, mut mobile_rewards_server) = file_sink::FileSinkBuilder::new(
            FileType::MobileRewardShare,
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

        let reward_period_hours = settings.rewards;
        let verifications_per_period = settings.verifications;
        let config_client = Client::from_settings(&settings.config_client)?;
        let data_transfer_ingest = FileStore::from_settings(&settings.data_transfer_ingest).await?;

        let (price_tracker, tracker_process) =
            PriceTracker::start(&settings.price_tracker, shutdown_listener.clone()).await?;

        // let verifier = Verifier::new(config_client, ingest);

        let verifier_daemon = VerifierDaemon {
            pool,
            valid_heartbeats,
            valid_speedtests,
            mobile_rewards,
            reward_manifests,
            reward_period_hours,
            price_tracker,
            data_transfer_ingest,
            config_client,
            heartbeats,
            speedtests
        };

        tokio::try_join!(
            db_join_handle.map_err(Error::from),
            valid_heartbeats_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            valid_speedtests_server
                .run(&shutdown_listener)
                .map_err(Error::from),
            mobile_rewards_server
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
