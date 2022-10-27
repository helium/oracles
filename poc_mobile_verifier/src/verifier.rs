use std::ops::Range;

use crate::{
    error::{Error, Result},
    heartbeats::{Heartbeat, Heartbeats},
    scheduler::Scheduler,
    speedtests::{SpeedtestAverages, SpeedtestStore},
    subnetwork_rewards::SubnetworkRewards,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, FileStore};
use helium_proto::services::{follower, Channel};
use sqlx::{PgExecutor, Pool, Postgres};
use tokio::time::sleep;

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub heartbeats_tx: file_sink::MessageSender,
    pub speedtest_avg_tx: file_sink::MessageSender,
    pub subnet_rewards_tx: file_sink::MessageSender,
    pub reward_period_hours: i64,
    pub verifications_per_period: i32,
    pub verifier: Verifier,
}

impl VerifierDaemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        let reward_period_length = Duration::hours(self.reward_period_hours);
        let verification_period_length = reward_period_length / self.verifications_per_period;

        loop {
            let now = Utc::now();

            let scheduler = Scheduler::new(
                verification_period_length,
                reward_period_length,
                last_verified_end_time(&self.pool).await?,
                last_rewarded_end_time(&self.pool).await?,
                next_rewarded_end_time(&self.pool).await?,
            );

            if scheduler.should_verify(now) {
                tracing::info!("Verifying epoch: {:?}", scheduler.verification_period);
                self.verify(&scheduler).await?;
            }

            if scheduler.should_reward(now) {
                tracing::info!("Rewarding epoch: {:?}", scheduler.reward_period);
                self.reward(&scheduler).await?
            }

            let sleep_duration = scheduler.sleep_duration(Utc::now())?;

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

    pub async fn verify(&mut self, scheduler: &Scheduler) -> Result {
        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = self
            .verifier
            .verify_epoch(&self.pool, &scheduler.verification_period)
            .await?;

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

        save_last_verified_end_time(&mut transaction, &scheduler.verification_period.end).await?;

        transaction.commit().await?;

        Ok(())
    }

    pub async fn reward(&mut self, scheduler: &Scheduler) -> Result {
        let heartbeats = Heartbeats::validated(&self.pool, scheduler.reward_period.start).await?;
        let speedtests =
            SpeedtestAverages::validated(&self.pool, scheduler.reward_period.end).await?;

        let rewards = self
            .verifier
            .reward_epoch(&scheduler.reward_period, heartbeats, speedtests)
            .await?;

        let mut transaction = self.pool.begin().await?;

        // Clear the heartbeats table:
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(&mut transaction)
            .await?;

        save_last_rewarded_end_time(&mut transaction, &scheduler.reward_period.end).await?;
        save_next_rewarded_end_time(&mut transaction, &scheduler.next_reward_period().end).await?;

        transaction.commit().await?;

        rewards
            .write(&self.subnet_rewards_tx)
            .await?
            // Await the returned one shot to ensure that we wrote the file
            .await??;

        Ok(())
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

async fn last_verified_end_time(exec: impl PgExecutor<'_>) -> Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(exec, "last_verified_end_time").await?, 0))
}

async fn save_last_verified_end_time(exec: impl PgExecutor<'_>, value: &DateTime<Utc>) -> Result {
    meta::store(exec, "last_verified_end_time", value.timestamp() as i64)
        .await
        .map_err(Error::from)
}

async fn last_rewarded_end_time(exec: impl PgExecutor<'_>) -> Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(exec, "last_rewarded_end_time").await?, 0))
}

async fn save_last_rewarded_end_time(exec: impl PgExecutor<'_>, value: &DateTime<Utc>) -> Result {
    meta::store(exec, "last_rewarded_end_time", value.timestamp() as i64)
        .await
        .map_err(Error::from)
}

async fn next_rewarded_end_time(exec: impl PgExecutor<'_>) -> Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(exec, "next_rewarded_end_time").await?, 0))
}

async fn save_next_rewarded_end_time(exec: impl PgExecutor<'_>, value: &DateTime<Utc>) -> Result {
    meta::store(exec, "next_rewarded_end_time", value.timestamp() as i64)
        .await
        .map_err(Error::from)
}
