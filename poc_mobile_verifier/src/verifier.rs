use std::ops::Range;

use crate::{
    error::{Error, Result},
    heartbeats::Heartbeats,
    shares::Shares,
    subnetwork_rewards::SubnetworkRewards,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{file_sink, FileStore};
use helium_proto::services::{follower, Channel};
use sqlx::{Pool, Postgres, Transaction};
use tokio::time::sleep;

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub valid_shares_tx: file_sink::MessageSender,
    pub invalid_shares_tx: file_sink::MessageSender,
    pub subnet_rewards_tx: file_sink::MessageSender,
    pub reward_period_hours: i64,
    pub verifications_per_period: i32,
    pub verifier: Verifier,
}

impl VerifierDaemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        let reward_period = Duration::hours(self.reward_period_hours);
        let verification_period = reward_period / self.verifications_per_period;

        loop {
            let mut transaction = self.pool.begin().await?;

            let now = Utc::now();
            // Maybe name these "epoch_since_*" and "epoch_since_*_duration"
            let verify_epoch = self.verifier.get_verify_epoch(now);
            let reward_epoch = self.verifier.get_reward_epoch(now);
            let verify_epoch_duration = epoch_duration(&verify_epoch);
            let reward_epoch_duration = epoch_duration(&reward_epoch);

            // If we started up and the last verification epoch was too recent,
            // we do not want to re-verify.
            let mut sleep_duration = if verify_epoch_duration >= verification_period
                // We always want to verify before a reward 
                || reward_epoch_duration >= reward_period
            {
                tracing::info!("Verifying epoch: {:?}", verify_epoch);
                // Attempt to verify the current epoch:
                self.verifier
                    .verify_epoch(&mut transaction, verify_epoch)
                    .await?
                    .write(&self.valid_shares_tx, &self.invalid_shares_tx)
                    .await?;
                verification_period
            } else {
                verification_period - verify_epoch_duration
            };

            // If the current duration since the last reward is exceeded, attempt to
            // submit rewards
            if reward_epoch_duration >= reward_period {
                tracing::info!("Rewarding epoch: {:?}", reward_epoch);
                self.verifier
                    .reward_epoch(&mut transaction, reward_epoch)
                    .await?
                    .write(&self.subnet_rewards_tx)
                    .await?;
            } else if reward_epoch_duration + sleep_duration >= reward_period {
                // If the next epoch is a reward period, cut off sleep duration.
                // This ensures that verifying will always end up being aligned with
                // the desired reward period.
                sleep_duration = reward_period - reward_epoch_duration;
            }

            transaction.commit().await?;

            tracing::info!("Sleeping...");
            let shutdown = shutdown.clone();
            tokio::select! {
                _ = shutdown => return Ok(()),
                _ = sleep(
                    sleep_duration
                        .to_std()
                        .map_err(|_| Error::OutOfRangeError)?,
                ) => (),
            }
        }
    }
}

pub struct Verifier {
    pub file_store: FileStore,
    pub follower: follower::Client<Channel>,
    pub last_verified_end_time: MetaValue<i64>,
    pub last_rewarded_end_time: MetaValue<i64>,
}

impl Verifier {
    pub async fn new(
        exec: impl sqlx::Executor<'_, Database = sqlx::Postgres> + Copy,
        file_store: FileStore,
        follower: follower::Client<Channel>,
    ) -> Result<Self> {
        let last_verified_end_time =
            MetaValue::<i64>::fetch_or_insert_with(exec, "last_verified_end_time", || 0).await?;
        let last_rewarded_end_time =
            MetaValue::<i64>::fetch_or_insert_with(exec, "last_rewarded_end_time", || 0).await?;
        Ok(Self {
            file_store,
            follower,
            last_verified_end_time,
            last_rewarded_end_time,
        })
    }

    pub async fn verify_epoch(
        &mut self,
        exec: &mut Transaction<'_, Postgres>,
        epoch: Range<DateTime<Utc>>,
    ) -> Result<Shares> {
        let shares = Shares::validate_heartbeats(exec, &self.file_store, &epoch).await?;

        // TODO: Validate speedtests

        // Update the last verified end time:
        self.last_verified_end_time
            .update(exec, epoch.end.timestamp() as i64)
            .await?;

        Ok(shares)
    }

    pub async fn reward_epoch(
        &mut self,
        exec: &mut Transaction<'_, Postgres>,
        epoch: Range<DateTime<Utc>>,
    ) -> Result<SubnetworkRewards> {
        let heartbeats =
            Heartbeats::new(exec, Utc.timestamp(*self.last_rewarded_end_time.value(), 0)).await?;

        let rewards =
            SubnetworkRewards::from_epoch(self.follower.clone(), &epoch, &heartbeats).await?;

        // Clear the heartbeats database
        // TODO: should the truncation be bound to a given epoch?
        // It's not intended that any heartbeats will exists outside the
        // current epoch, but it might be better to code defensively.
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(&mut *exec)
            .await?;

        // Update the last rewarded end time:
        self.last_rewarded_end_time
            .update(exec, epoch.end.timestamp() as i64)
            .await?;

        Ok(rewards)
    }

    pub fn get_verify_epoch(&self, now: DateTime<Utc>) -> Range<DateTime<Utc>> {
        Utc.timestamp(*self.last_verified_end_time.value(), 0)..now
    }

    pub fn get_reward_epoch(&self, now: DateTime<Utc>) -> Range<DateTime<Utc>> {
        Utc.timestamp(*self.last_rewarded_end_time.value(), 0)..now
    }
}

fn epoch_duration(epoch: &Range<DateTime<Utc>>) -> Duration {
    epoch.end - epoch.start
}
