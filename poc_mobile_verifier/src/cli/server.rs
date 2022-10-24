use crate::Result;
use crate::{
    env_var,
    error::Error,
    verifier::{Verifier, VerifierDaemon},
};
use db_store::MetaValue;
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

        // valid shares
        let (valid_shares_tx, valid_shares_rx) = file_sink::message_channel(50);
        let mut shares_sink =
            file_sink::FileSinkBuilder::new(FileType::Shares, store_base_path, valid_shares_rx)
                .deposits(Some(file_upload_tx.clone()))
                .create()
                .await?;

        // invalid shares
        let (invalid_shares_tx, invalid_shares_rx) = file_sink::message_channel(50);
        let mut invalid_shares_sink = file_sink::FileSinkBuilder::new(
            FileType::InvalidShares,
            store_base_path,
            invalid_shares_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        // subnetwork rewards
        let (subnet_rewards_tx, subnet_rewards_rx) = file_sink::message_channel(50);
        let mut subnet_sink = file_sink::FileSinkBuilder::new(
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

        let last_verified_end_time =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_verified_end_time", || 0).await?;
        let last_rewarded_end_time =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_rewarded_end_time", || 0).await?;
        let next_rewarded_end_time =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "next_rewarded_end_time", || 0).await?;

        let verifier = Verifier::new(file_store, follower).await?;

        let verifier_daemon = VerifierDaemon {
            pool,
            valid_shares_tx,
            invalid_shares_tx,
            subnet_rewards_tx,
            reward_period_hours,
            verifications_per_period,
            last_verified_end_time,
            last_rewarded_end_time,
            next_rewarded_end_time,
            verifier,
        };

        tokio::try_join!(
            shares_sink.run(&shutdown_listener).map_err(Error::from),
            invalid_shares_sink
                .run(&shutdown_listener)
                .map_err(Error::from),
            subnet_sink.run(&shutdown_listener).map_err(Error::from),
            file_upload.run(&shutdown_listener).map_err(Error::from),
            // I don't _think_ that this needs to be in a task.
            verifier_daemon.run(&shutdown_listener),
        )?;

        tracing::info!("Shutting down verifier server");

        Ok(())
    }
}
