use std::ops::Range;

use crate::{
    error::{Error, Result},
    heartbeats::{Heartbeat, Heartbeats},
    speedtests::{SpeedtestAverages, SpeedtestStore},
    subnetwork_rewards::SubnetworkRewards,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{file_sink, FileStore};
use helium_proto::services::{follower, Channel};
use sqlx::{Pool, Postgres};
use tokio::time::sleep;

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub heartbeats_tx: file_sink::MessageSender,
    pub speedtest_avg_tx: file_sink::MessageSender,
    pub subnet_rewards_tx: file_sink::MessageSender,
    pub reward_period_hours: i64,
    pub verifications_per_period: i32,
    pub last_verified_end_time: MetaValue<i64>,
    pub last_rewarded_end_time: MetaValue<i64>,
    pub next_rewarded_end_time: MetaValue<i64>,
    pub verifier: Verifier,
}

impl VerifierDaemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        let reward_period = Duration::hours(self.reward_period_hours);
        let verification_period = reward_period / self.verifications_per_period;

        loop {
            let now = Utc::now();
            let epoch_since_last_verify = self.epoch_since_last_verify(now);
            let epoch_since_last_verify_duration = epoch_duration(&epoch_since_last_verify);

            let last_rewarded_end_time = Utc.timestamp(*self.last_rewarded_end_time.value(), 0);
            let next_rewarded_end_time = Utc.timestamp(*self.next_rewarded_end_time.value(), 0);

            // If we started up and the last verification epoch was too recent,
            // we do not want to re-verify.
            let mut sleep_duration = if epoch_since_last_verify_duration >= verification_period
                // We always want to verify before a reward 
                || now >= next_rewarded_end_time
            {
                let epoch_duration = epoch_since_last_verify_duration.min(verification_period);
                let last_verified_end_time = Utc.timestamp(*self.last_verified_end_time.value(), 0);
                let epoch = last_verified_end_time
                    ..(last_verified_end_time + epoch_duration).min(next_rewarded_end_time);
                tracing::info!("Verifying epoch: {:?}", epoch);
                // Attempt to verify the current epoch:
                self.verify_epoch(epoch).await?;
                if epoch_since_last_verify_duration - epoch_duration > verification_period {
                    Duration::zero()
                } else {
                    verification_period
                }
            } else {
                verification_period - epoch_since_last_verify_duration
            };

            // If the current duration since the last reward is exceeded, attempt to
            // submit rewards
            if now >= next_rewarded_end_time {
                let epoch = last_rewarded_end_time..next_rewarded_end_time;
                tracing::info!("Rewarding epoch: {:?}", epoch);
                self.reward_epoch(epoch).await?
            } else if now + sleep_duration >= next_rewarded_end_time {
                // If the next verification epoch straddles a reward epoch, cut off sleep
                // duration. This ensures that verifying will always end up being aligned
                // with the desired reward period.
                sleep_duration = next_rewarded_end_time - now;
            }

            let sleep_duration = sleep_duration
                .to_std()
                .map_err(|_| Error::OutOfRangeError)?;

            tracing::info!(
                "Sleeping for {}",
                humantime::format_duration(sleep_duration)
            );
            let shutdown = shutdown.clone();
            tokio::select! {
                _ = shutdown => return Ok(()),
                _ = sleep(sleep_duration) => (),
            }
        }
    }

    pub async fn verify_epoch(&mut self, epoch: Range<DateTime<Utc>>) -> Result {
        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = self.verifier.verify_epoch(&self.pool, &epoch).await?;

        let mut transaction = self.pool.begin().await?;

        // TODO: switch to a bulk transaction
        for heartbeat in heartbeats.into_iter() {
            heartbeat.write(&self.heartbeats_tx).await?;
            heartbeat.save(&mut transaction).await?;
        }

        for speedtest in speedtests.into_iter() {
            speedtest.write(&self.speedtest_avg_tx).await?;
            speedtest.save(&mut transaction).await?;
        }

        // Update the last verified end time:
        self.last_verified_end_time
            .update(&mut transaction, epoch.end.timestamp() as i64)
            .await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn reward_epoch(&mut self, epoch: Range<DateTime<Utc>>) -> Result {
        let heartbeats = Heartbeats::validated(&self.pool, epoch.start).await?;
        let speedtests = SpeedtestAverages::validated(&self.pool, epoch.end).await?;

        let rewards = self
            .verifier
            .reward_epoch(&epoch, heartbeats, speedtests)
            .await?;

        let mut transaction = self.pool.begin().await?;

        // Clear the heartbeats table:
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(&mut transaction)
            .await?;

        // Update the last and next rewarded end time:
        self.last_rewarded_end_time
            .update(&mut transaction, epoch.end.timestamp() as i64)
            .await?;

        self.next_rewarded_end_time
            .update(
                &mut transaction,
                (epoch.end + Duration::hours(self.reward_period_hours)).timestamp() as i64,
            )
            .await?;

        transaction.commit().await?;

        rewards
            .write(&self.subnet_rewards_tx)
            .await?
            // Await the returned one shot to ensure that we wrote the file
            .await??;

        Ok(())
    }

    pub fn epoch_since_last_verify(&self, now: DateTime<Utc>) -> Range<DateTime<Utc>> {
        Utc.timestamp(*self.last_verified_end_time.value(), 0)..now
    }
}

pub struct Verifier {
    pub file_store: FileStore,
    pub follower: follower::Client<Channel>,
}

impl Verifier {
    pub async fn new(file_store: FileStore, follower: follower::Client<Channel>) -> Result<Self> {
        Ok(Self {
            file_store,
            follower,
        })
    }

    pub async fn verify_epoch(
        &mut self,
        pool: impl SpeedtestStore + Copy,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<VerifiedEpoch> {
        let heartbeats = Heartbeat::validate_heartbeats(&self.file_store, epoch).await?;
        let speedtests =
            SpeedtestAverages::validate_speedtests(pool, &self.file_store, epoch).await?;
        Ok(VerifiedEpoch {
            heartbeats,
            speedtests,
        })
    }

    pub async fn reward_epoch(
        &mut self,
        epoch: &Range<DateTime<Utc>>,
        heartbeats: Heartbeats,
        speedtests: SpeedtestAverages,
    ) -> Result<SubnetworkRewards> {
        SubnetworkRewards::from_epoch(self.follower.clone(), epoch, heartbeats, speedtests).await
    }
}

pub struct VerifiedEpoch {
    pub heartbeats: Vec<Heartbeat>,
    pub speedtests: SpeedtestAverages,
}

fn epoch_duration(epoch: &Range<DateTime<Utc>>) -> Duration {
    epoch.end - epoch.start
}
