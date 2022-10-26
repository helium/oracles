use crate::Result;
use crate::{
    env_var,
    error::Error,
    verifier::{Verifier, VerifierDaemon},
};
use file_store::{file_sink, file_upload, FileStore, FileType};
use futures_util::TryFutureExt;
use helium_proto::services::{follower, Endpoint, Uri};
use sqlx::postgres::PgPoolOptions;

use super::{CONNECT_TIMEOUT, DEFAULT_URI, RPC_TIMEOUT};

pub const DEFAULT_REWARD_PERIOD_HOURS: i64 = 24;
pub const DEFAULT_VERIFICATIONS_PER_PERIOD: i32 = 8;

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

        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_env_with_prefix("OUTPUT", file_upload_rx).await?;

        let store_path = dotenv::var("VERIFIER_STORE")?;
        let store_base_path = std::path::Path::new(&store_path);

        // Heartbeats
        let (heartbeats_tx, heartbeats_rx) = file_sink::message_channel(50);
        let mut heartbeats = file_sink::FileSinkBuilder::new(
            FileType::ValidatedHeartbeat,
            store_base_path,
            heartbeats_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
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
        .create()
        .await?;

        // Subnetwork rewards
        let (subnet_rewards_tx, subnet_rewards_rx) = file_sink::message_channel(50);
        let mut subnet_rewards = file_sink::FileSinkBuilder::new(
            FileType::SubnetworkRewards,
            store_base_path,
            subnet_rewards_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        let follower = follower::Client::new(
            Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(RPC_TIMEOUT)
                .connect_lazy(),
        );

        let reward_period_hours = env_var("REWARD_PERIOD", DEFAULT_REWARD_PERIOD_HOURS)?;
        let verifications_per_period =
            env_var("VERIFICATIONS_PER_PERIOD", DEFAULT_VERIFICATIONS_PER_PERIOD)?;
        let file_store = FileStore::from_env_with_prefix("INPUT").await?;

        let verifier = Verifier::new(file_store, follower).await?;

        let verifier_daemon = VerifierDaemon {
            pool,
            heartbeats_tx,
            speedtest_avg_tx,
            subnet_rewards_tx,
            reward_period_hours,
            verifications_per_period,
            verifier,
        };

        tokio::try_join!(
            heartbeats.run(&shutdown_listener).map_err(Error::from),
            speedtest_avgs.run(&shutdown_listener).map_err(Error::from),
            subnet_rewards.run(&shutdown_listener).map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            // I don't _think_ that this needs to be in a task.
            verifier_daemon.run(&shutdown_listener),
        )?;

        tracing::info!("Shutting down verifier server");

        Ok(())
    }
}
