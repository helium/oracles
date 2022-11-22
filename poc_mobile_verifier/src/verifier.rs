use crate::{
    heartbeats::{Heartbeat, Heartbeats},
    ingest,
    owner_shares::{OwnerShares, ResolveError},
    scheduler::Scheduler,
    speedtests::{FetchError, SpeedtestAverages, SpeedtestRollingAverage, SpeedtestStore},
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, file_sink_write, traits::TimestampEncode, FileStore};
use futures::{stream::Stream, StreamExt};
use helium_proto::{
    services::{follower, Channel},
    RewardManifest, SubnetworkReward, SubnetworkRewards,
};
use sqlx::{PgExecutor, Pool, Postgres};
use std::{collections::HashMap, ops::Range};
use tokio::pin;
use tokio::time::sleep;

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub heartbeats_tx: file_sink::MessageSender,
    pub speedtest_avg_tx: file_sink::MessageSender,
    pub radio_rewards_tx: file_sink::MessageSender,
    pub reward_manifest_tx: file_sink::MessageSender,
    pub subnetwork_rewards_tx: file_sink::MessageSender,
    pub reward_period_hours: i64,
    pub verifications_per_period: i32,
    pub verification_offset: Duration,
    pub verifier: Verifier,
}

impl VerifierDaemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
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
                self.verification_offset,
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

    pub async fn verify(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = self
            .verifier
            .verify_epoch(&self.pool, &scheduler.verification_period)
            .await?;

        let mut transaction = self.pool.begin().await?;

        pin!(heartbeats);
        pin!(speedtests);

        // TODO: switch to a bulk transaction
        while let Some(heartbeat) = heartbeats.next().await {
            heartbeat.write(&self.heartbeats_tx).await?;
            heartbeat.save(&mut transaction).await?;
        }

        while let Some(speedtest) = speedtests.next().await.transpose()? {
            speedtest.write(&self.speedtest_avg_tx).await?;
            speedtest.save(&mut transaction).await?;
        }

        save_last_verified_end_time(&mut transaction, &scheduler.verification_period.end).await?;
        transaction.commit().await?;

        Ok(())
    }

    pub async fn reward(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
        let heartbeats = Heartbeats::validated(&self.pool, scheduler.reward_period.start).await?;
        let speedtests =
            SpeedtestAverages::validated(&self.pool, scheduler.reward_period.end).await?;

        let rewards = self.verifier.reward_epoch(heartbeats, speedtests).await?;

        let mut owner_rewards: HashMap<Vec<u8>, u64> = HashMap::new();

        for reward_share in rewards.into_radio_shares(&scheduler.reward_period)? {
            *owner_rewards
                .entry(reward_share.owner_key.clone())
                .or_default() += reward_share.amount;

            file_sink_write!("radio_reward_shares", &self.radio_rewards_tx, reward_share)
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }

        let written_files = file_sink::fetch_manifest(&self.radio_rewards_tx)
            .await?
            .await??;

        // Write out the manifest file
        file_sink_write!(
            "reward_manifest",
            &self.reward_manifest_tx,
            RewardManifest {
                start_timestamp: scheduler.reward_period.start.encode_timestamp(),
                end_timestamp: scheduler.reward_period.end.encode_timestamp(),
                written_files
            }
        )
        .await?
        .await??;

        // Temporarily continue to write out subnetwork rewards
        let mut rewards: Vec<_> = owner_rewards
            .into_iter()
            .map(|(account, amount)| SubnetworkReward { account, amount })
            .collect();
        rewards.sort_by(|a, b| a.account.cmp(&b.account));
        file_sink_write!(
            "subnetwork_rewards",
            &self.subnetwork_rewards_tx,
            SubnetworkRewards {
                start_epoch: scheduler.reward_period.start.encode_timestamp(),
                end_epoch: scheduler.reward_period.end.encode_timestamp(),
                rewards,
            }
        )
        .await?
        .await??;

        let mut transaction = self.pool.begin().await?;

        // Clear the heartbeats table:
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(&mut transaction)
            .await?;

        save_last_rewarded_end_time(&mut transaction, &scheduler.reward_period.end).await?;
        save_next_rewarded_end_time(&mut transaction, &scheduler.next_reward_period().end).await?;

        transaction.commit().await?;

        Ok(())
    }
}

pub struct Verifier {
    pub file_store: FileStore,
    pub follower: follower::Client<Channel>,
}

impl Verifier {
    pub fn new(file_store: FileStore, follower: follower::Client<Channel>) -> Self {
        Self {
            file_store,
            follower,
        }
    }

    pub async fn verify_epoch<'a>(
        &mut self,
        pool: impl SpeedtestStore + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> file_store::Result<
        VerifiedEpoch<
            impl Stream<Item = Heartbeat> + 'a,
            impl Stream<Item = Result<SpeedtestRollingAverage, FetchError>> + 'a,
        >,
    > {
        let heartbeats = Heartbeat::validate_heartbeats(
            ingest::ingest_heartbeats(&self.file_store, epoch).await?,
            epoch,
        )
        .await;

        let speedtests = SpeedtestRollingAverage::validate_speedtests(
            ingest::ingest_speedtests(&self.file_store, epoch).await?,
            pool,
        )
        .await;

        Ok(VerifiedEpoch {
            heartbeats,
            speedtests,
        })
    }

    pub async fn reward_epoch(
        &mut self,
        heartbeats: Heartbeats,
        speedtests: SpeedtestAverages,
    ) -> Result<OwnerShares, ResolveError> {
        OwnerShares::aggregate(&mut self.follower, heartbeats, speedtests).await
    }
}

pub struct VerifiedEpoch<H, S> {
    pub heartbeats: H,
    pub speedtests: S,
}

async fn last_verified_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(exec, "last_verified_end_time").await?, 0))
}

async fn save_last_verified_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_verified_end_time", value.timestamp()).await
}

async fn last_rewarded_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(exec, "last_rewarded_end_time").await?, 0))
}

async fn save_last_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_rewarded_end_time", value.timestamp()).await
}

async fn next_rewarded_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(exec, "next_rewarded_end_time").await?, 0))
}

async fn save_next_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "next_rewarded_end_time", value.timestamp()).await
}
