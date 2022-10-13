use std::ops::Range;

use crate::{
    error::{Error, Result},
    heartbeats::{Heartbeat, Heartbeats},
    shares::Shares,
    subnetwork_rewards::SubnetworkRewards,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{file_sink, FileStore};
use helium_proto::services::{follower, Channel};
use sqlx::{Pool, Postgres};
use tokio::time::sleep;

pub struct VerifierDaemon {
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
                    .verify_epoch(
                        verify_epoch,
                        &self.valid_shares_tx,
                        &self.invalid_shares_tx,
                    )
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
                    .reward_epoch(reward_epoch, &self.subnet_rewards_tx)
                    .await?
            } else if reward_epoch_duration + sleep_duration >= reward_period {
                // If the next epoch is a reward period, cut off sleep duration.
                // This ensures that verifying will always end up being aligned with
                // the desired reward period.
                sleep_duration = reward_period - reward_epoch_duration;
            }

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
    pub pool: Pool<Postgres>,
    pub file_store: FileStore,
    pub follower: follower::Client<Channel>,
    pub last_verified_end_time: MetaValue<i64>,
    pub last_rewarded_end_time: MetaValue<i64>,
}

impl Verifier {
    pub async fn new(
        pool: Pool<Postgres>,
        file_store: FileStore,
        follower: follower::Client<Channel>,
    ) -> Result<Self> {
        let last_verified_end_time =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_verified_end_time", || 0).await?;
        let last_rewarded_end_time =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_rewarded_end_time", || 0).await?;
        Ok(Self {
            pool,
            file_store,
            follower,
            last_verified_end_time,
            last_rewarded_end_time,
        })
    }

    pub async fn verify_epoch(
        &mut self,
        epoch: Range<DateTime<Utc>>,
        valid_shares_tx: &file_sink::MessageSender,
        invalid_shares_tx: &file_sink::MessageSender,
    ) -> Result {
        let shares = Shares::validate_heartbeats(&self.pool, &self.file_store, &epoch).await?;

        let transaction = self.pool.begin().await?;

        // Should we remove the heartbeats that were not new
        // from valid shares
        for share in shares.valid_shares.clone() {
            let heartbeat = Heartbeat::from(share);
            heartbeat.save(&self.pool).await?;
        }

        // Update the last verified end time:
        self.last_verified_end_time
            .update(&self.pool, epoch.end.timestamp() as i64)
            .await?;

        transaction.commit().await?;

        shares.write(valid_shares_tx, invalid_shares_tx).await?;

        Ok(())
    }

    pub async fn reward_epoch(
        &mut self,
        epoch: Range<DateTime<Utc>>,
        subnet_rewards_tx: &file_sink::MessageSender,
    ) -> Result {
        let heartbeats =
            Heartbeats::validated(&self.pool, Utc.timestamp(*self.last_rewarded_end_time.value(), 0))
                .await?;

        let rewards =
            SubnetworkRewards::from_epoch(self.follower.clone(), &epoch, &heartbeats).await?;

        let transaction = self.pool.begin().await?;

        // Clear the heartbeats database
        // TODO: should the truncation be bound to a given epoch?
        // It's not intended that any heartbeats will exists outside the
        // current epoch, but it might be better to code defensively.
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(&self.pool)
            .await?;

        // Update the last rewarded end time:
        self.last_rewarded_end_time
            .update(&self.pool, epoch.end.timestamp() as i64)
            .await?;

        transaction.commit().await?;

        rewards.write(subnet_rewards_tx).await?;

        Ok(())
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
