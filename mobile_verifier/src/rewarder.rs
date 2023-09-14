use crate::{
    coverage, data_session,
    heartbeats::{self, HeartbeatReward},
    reward_shares::{MapperShares, PocShares, TransferRewards},
    speedtests,
    speedtests_average::SpeedtestAverages,
    subscriber_location, telemetry,
};
use anyhow::bail;
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode};
use helium_proto::RewardManifest;
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use std::ops::Range;
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder {
    pool: Pool<Postgres>,
    reward_period_duration: Duration,
    reward_offset: Duration,
    mobile_rewards: FileSinkClient,
    reward_manifests: FileSinkClient,
    price_tracker: PriceTracker,
}

impl Rewarder {
    pub fn new(
        pool: Pool<Postgres>,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient,
        reward_manifests: FileSinkClient,
        price_tracker: PriceTracker,
    ) -> Self {
        Self {
            pool,
            reward_period_duration,
            reward_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
        }
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            let last_rewarded_end_time = last_rewarded_end_time(&self.pool).await?;
            let next_rewarded_end_time = next_rewarded_end_time(&self.pool).await?;
            let scheduler = Scheduler::new(
                self.reward_period_duration,
                last_rewarded_end_time,
                next_rewarded_end_time,
                self.reward_offset,
            );
            let now = Utc::now();
            let sleep_duration = if scheduler.should_reward(now) {
                if self.is_data_current(&scheduler.reward_period).await? {
                    self.reward(&scheduler).await?;
                    continue;
                } else {
                    Duration::minutes(REWARDS_NOT_CURRENT_DELAY_PERIOD).to_std()?
                }
            } else {
                scheduler.sleep_duration(now)?
            };

            tracing::info!(
                "Sleeping for {}",
                humantime::format_duration(sleep_duration)
            );

            tokio::select! {
                _ = shutdown.clone() => break,
                _ = sleep(sleep_duration) => (),
            }
        }

        Ok(())
    }

    async fn disable_complete_data_checks_until(&self) -> db_store::Result<DateTime<Utc>> {
        Utc.timestamp_opt(
            meta::fetch(&self.pool, "disable_complete_data_checks_until").await?,
            0,
        )
        .single()
        .ok_or(db_store::Error::DecodeError)
    }

    pub async fn is_data_current(
        &self,
        reward_period: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<bool> {
        // Check if we have heartbeats and speedtests past the end of the reward period
        if reward_period.end >= self.disable_complete_data_checks_until().await? {
            if sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM heartbeats WHERE latest_timestamp >= $1",
            )
            .bind(reward_period.end)
            .fetch_one(&self.pool)
            .await?
                == 0
            {
                tracing::info!("No heartbeats found past reward period");
                return Ok(false);
            }

            if sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM speedtests WHERE timestamp >= $1")
                .bind(reward_period.end)
                .fetch_one(&self.pool)
                .await?
                == 0
            {
                tracing::info!("No speedtests found past reward period");
                return Ok(false);
            }
        } else {
            tracing::info!("Complete data checks are disabled for this reward period");
        }

        Ok(true)
    }

    pub async fn reward(&self, scheduler: &Scheduler) -> anyhow::Result<()> {
        let reward_period = &scheduler.reward_period;

        tracing::info!(
            "Rewarding for period: {} to {}",
            reward_period.start,
            reward_period.end
        );

        let heartbeats = HeartbeatReward::validated(&self.pool, reward_period);
        let speedtest_averages =
            SpeedtestAverages::aggregate_epoch_averages(reward_period.end, &self.pool).await?;
        let poc_rewards = PocShares::aggregate(heartbeats, &speedtest_averages).await?;
        let mobile_price = self
            .price_tracker
            .price(&helium_proto::BlockchainTokenTypeV1::Mobile)
            .await?;

        // Mobile prices are supplied in 10^6, so we must convert them to Decimal
        let mobile_bone_price = Decimal::from(mobile_price)
                / dec!(1_000_000)  // Per Mobile token
                / dec!(1_000_000); // Per Bone
        let transfer_rewards = TransferRewards::from_transfer_sessions(
            mobile_bone_price,
            data_session::aggregate_hotspot_data_sessions_to_dc(&self.pool, reward_period).await?,
            &poc_rewards,
            reward_period,
        )
        .await;

        // It's important to gauge the scale metric. If this value is < 1.0, we are in
        // big trouble.
        let Some(scale) = transfer_rewards.reward_scale().to_f64() else {
            bail!("The data transfer rewards scale cannot be converted to a float");
        };
        telemetry::data_transfer_rewards_scale(scale);

        if let Some(mobile_reward_shares) =
            poc_rewards.into_rewards(transfer_rewards.reward_sum(), reward_period)
        {
            for mobile_reward_share in mobile_reward_shares {
                self.mobile_rewards
                    .write(mobile_reward_share, [])
                    .await?
                    // Await the returned one shot to ensure that we wrote the file
                    .await??;
            }

            for mobile_reward_share in transfer_rewards.into_rewards(reward_period) {
                self.mobile_rewards
                    .write(mobile_reward_share, [])
                    .await?
                    // Await the returned one shot to ensure that we wrote the file
                    .await??;
            }
        }

        // Mapper rewards currently include rewards for discovery mapping only.
        // Verification mapping rewards to be added
        // Any subscriber for which the carrier has submitted a location sharing report
        // during the epoch will be eligible for discovery mapping rewards

        // get subscriber location shares this epoch
        let location_shares =
            subscriber_location::aggregate_location_shares(&self.pool, reward_period).await?;

        // determine mapping shares based on location shares and data transferred
        let mapping_shares = MapperShares::new(location_shares);
        let rewards_per_share = mapping_shares.rewards_per_share(reward_period)?;

        // translate discovery mapping shares into subscriber rewards
        for mapping_share in
            mapping_shares.into_subscriber_rewards(reward_period, rewards_per_share)
        {
            self.mobile_rewards
                .write(mapping_share.clone(), [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }

        let written_files = self.mobile_rewards.commit().await?.await??;

        let mut transaction = self.pool.begin().await?;

        // clear out the various db tables
        heartbeats::clear_heartbeats(&mut transaction, &reward_period.start).await?;
        speedtests::clear_speedtests(&mut transaction, &reward_period.start).await?;
        data_session::clear_hotspot_data_sessions(&mut transaction, &reward_period.end).await?;
        coverage::clear_coverage_objects(&mut transaction, &reward_period.start).await?;
        // subscriber_location::clear_location_shares(&mut transaction, &reward_period.end).await?;

        let next_reward_period = scheduler.next_reward_period();
        save_last_rewarded_end_time(&mut transaction, &next_reward_period.start).await?;
        save_next_rewarded_end_time(&mut transaction, &next_reward_period.end).await?;
        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        self.reward_manifests
            .write(
                RewardManifest {
                    start_timestamp: reward_period.start.encode_timestamp(),
                    end_timestamp: reward_period.end.encode_timestamp(),
                    written_files,
                },
                [],
            )
            .await?
            .await??;

        self.reward_manifests.commit().await?;
        telemetry::last_rewarded_end_time(next_reward_period.start);
        Ok(())
    }
}

pub async fn last_rewarded_end_time(db: &Pool<Postgres>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(db, "last_rewarded_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn next_rewarded_end_time(db: &Pool<Postgres>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(db, "next_rewarded_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_last_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_rewarded_end_time", value.timestamp()).await
}

async fn save_next_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "next_rewarded_end_time", value.timestamp()).await
}
