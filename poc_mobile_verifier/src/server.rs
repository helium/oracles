use std::ops::Range;

use crate::{
    env_var,
    error::{Error, Result},
    heartbeats::Heartbeats,
    subnetwork_rewards::SubnetworkRewards,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{file_sink, file_upload, FileStore, FileType};
use futures_util::TryFutureExt;
use helium_proto::services::poc_mobile::Shares;
use helium_proto::services::{follower, Channel, Endpoint, Uri};
use sqlx::{Pool, Postgres, Transaction};
use tokio::time::sleep;

pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

pub const DEFAULT_REWARD_PERIOD_HOURS: i64 = 24;
pub const DEFAULT_VERIFICATIONS_PER_PERIOD: i32 = 8;

pub async fn run_server(pool: Pool<Postgres>, shutdown: triggered::Listener) -> Result {
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_env_with_prefix("OUTPUT", file_upload_rx).await?;

    let store_path = dotenv::var("VERIFIER_STORE")?;
    let store_base_path = std::path::Path::new(&store_path);

    // valid shares
    let (_shares_tx, shares_rx) = file_sink::message_channel(50);
    let mut shares_sink =
        file_sink::FileSinkBuilder::new(FileType::Shares, store_base_path, shares_rx)
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
    let default_last_verified_end_time = env_var("LAST_VERIFIED_END_TIME", 0)?;
    let default_last_rewarded_end_time = env_var("LAST_REWARDED_END_TIME", 0)?;

    let last_verified_end_time =
        MetaValue::<i64>::fetch_or_insert_with(&pool, "last_verified_end_time", move || {
            default_last_verified_end_time
        })
        .await?;
    let last_rewarded_end_time =
        MetaValue::<i64>::fetch_or_insert_with(&pool, "last_rewarded_end_time", move || {
            default_last_rewarded_end_time
        })
        .await?;
    let heartbeats =
        Heartbeats::new(&pool, Utc.timestamp(*last_verified_end_time.value(), 0)).await?;
    let file_store = FileStore::from_env_with_prefix("INPUT").await?;

    let verifier = Verifier {
        pool,
        follower,
        invalid_shares_tx,
        subnet_rewards_tx,
        reward_period_hours,
        verifications_per_period,
        heartbeats,
        file_store,
        last_verified_end_time,
        last_rewarded_end_time,
    };

    let server = tokio::spawn(async move { verifier.run().await });

    // TODO: select with shutdown
    tokio::try_join!(
        flatten(server),
        shares_sink.run(&shutdown).map_err(Error::from),
        invalid_shares_sink.run(&shutdown).map_err(Error::from),
        subnet_sink.run(&shutdown).map_err(Error::from),
        file_upload.run(&shutdown).map_err(Error::from),
    )?;

    Ok(())
}

async fn flatten(handle: tokio::task::JoinHandle<Result>) -> Result {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Error::JoinError(err)),
    }
}

struct Verifier {
    pub pool: Pool<Postgres>,
    pub follower: follower::Client<Channel>,
    pub invalid_shares_tx: file_sink::MessageSender,
    pub subnet_rewards_tx: file_sink::MessageSender,
    pub reward_period_hours: i64,
    pub verifications_per_period: i32,
    pub heartbeats: Heartbeats,
    pub file_store: FileStore,
    pub last_verified_end_time: MetaValue<i64>,
    pub last_rewarded_end_time: MetaValue<i64>,
}

impl Verifier {
    async fn run(mut self) -> Result {
        tracing::info!("Starting verifier service");

        let reward_period = Duration::hours(self.reward_period_hours);
        let verification_period = reward_period / self.verifications_per_period;

        loop {
            let now = Utc::now();
            let verify_epoch = self.get_verify_epoch(now);

            // If we started up and the last verification epoch was too recent,
            // we do not want to re-verify.
            let verify_epoch_duration = epoch_duration(&verify_epoch);
            let sleep_duration = if verify_epoch_duration >= verification_period {
                tracing::info!("Verifying epoch: {:?}", verify_epoch);
                // Attempt to verify the current epoch:
                self.verify_epoch(verify_epoch).await?;
                verification_period
            } else {
                verification_period - verify_epoch_duration
            };

            // If the current duration since the last reward is exceeded, attempt to
            // submit rewards
            let rewards_epoch = self.get_rewards_epoch(now);
            if epoch_duration(&rewards_epoch) >= reward_period {
                tracing::info!("Rewarding epoch: {:?}", rewards_epoch);
                self.reward_shares(rewards_epoch).await?;
            }

            // TODO: Address drift in some way?
            sleep(
                sleep_duration
                    .to_std()
                    .map_err(|_| Error::OutOfRangeError)?,
            )
            .await;
        }
    }

    async fn verify_epoch(&mut self, epoch: Range<DateTime<Utc>>) -> Result {
        let mut transaction = self.pool.begin().await?;
        let res = self.try_verify_epoch(&mut transaction, epoch).await;
        if res.is_ok() {
            transaction.commit().await?;
        } else {
            transaction.rollback().await?;
        }
        res
    }

    // TODO: Return invalid shares
    async fn try_verify_epoch(
        &mut self,
        exec: &mut Transaction<'_, Postgres>,
        epoch: Range<DateTime<Utc>>,
    ) -> Result {
        // Validate the heartbeats in the current epoch
        let invalid_shares = self
            .heartbeats
            .validate_heartbeats(exec, &epoch, &self.file_store)
            .await?;

        // TODO: Add speedtests

        // Update the last verified end time:
        self.last_verified_end_time
            .update(exec, epoch.end.timestamp() as i64)
            .await?;

        // Write out invalid shares:
        file_sink::write(
            &self.invalid_shares_tx,
            Shares {
                shares: invalid_shares,
            },
        )
        .await?;

        Ok(())
    }

    async fn reward_shares(&mut self, epoch: Range<DateTime<Utc>>) -> Result {
        let mut transaction = self.pool.begin().await?;
        let res = self.try_reward_shares(&mut transaction, epoch).await;
        if res.is_ok() {
            transaction.commit().await?;
        } else {
            transaction.rollback().await?;
        }
        res
    }

    async fn try_reward_shares(
        &mut self,
        exec: &mut Transaction<'_, Postgres>,
        epoch: Range<DateTime<Utc>>,
    ) -> Result {
        SubnetworkRewards::from_epoch(self.follower.clone(), &epoch, &self.heartbeats)
            .await?
            .write(&self.subnet_rewards_tx)
            .await?;

        // Clear the heartbeats database
        self.heartbeats.clear(exec).await?;

        // Update the last rewarded end time:
        self.last_rewarded_end_time
            .update(exec, epoch.end.timestamp() as i64)
            .await?;

        Ok(())
    }

    fn get_verify_epoch(&self, now: DateTime<Utc>) -> Range<DateTime<Utc>> {
        Utc.timestamp(*self.last_verified_end_time.value(), 0)..now
    }

    fn get_rewards_epoch(&self, now: DateTime<Utc>) -> Range<DateTime<Utc>> {
        Utc.timestamp(*self.last_rewarded_end_time.value(), 0)..now
    }
}

fn epoch_duration(epoch: &Range<DateTime<Utc>>) -> Duration {
    epoch.end - epoch.start
}
