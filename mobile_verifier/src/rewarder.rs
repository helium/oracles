use crate::{
    coverage, data_session,
    heartbeats::{self, HeartbeatReward},
    reward_shares::{self, CoveragePoints, MapperShares, ServiceProviderShares, TransferRewards},
    speedtests,
    speedtests_average::SpeedtestAverages,
    subscriber_location, telemetry,
};
use anyhow::bail;
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode};

use helium_proto::services::{
    poc_mobile as proto, poc_mobile::mobile_reward_share::Reward as ProtoReward,
    poc_mobile::UnallocatedReward, poc_mobile::UnallocatedRewardType,
};
use helium_proto::RewardManifest;
use mobile_config::client::{carrier_service_client::CarrierServiceVerifier, ClientError};
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::*, Decimal};
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use std::ops::Range;
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder<A> {
    pool: Pool<Postgres>,
    carrier_client: A,
    reward_period_duration: Duration,
    reward_offset: Duration,
    mobile_rewards: FileSinkClient,
    reward_manifests: FileSinkClient,
    price_tracker: PriceTracker,
    max_distance_to_asserted: u32,
}

impl<A> Rewarder<A>
where
    A: CarrierServiceVerifier<Error = ClientError>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: Pool<Postgres>,
        carrier_client: A,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient,
        reward_manifests: FileSinkClient,
        price_tracker: PriceTracker,
        max_distance_to_asserted: u32,
    ) -> Self {
        Self {
            pool,
            carrier_client,
            reward_period_duration,
            reward_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            max_distance_to_asserted,
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
                biased;
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
                "SELECT COUNT(*) FROM cbrs_heartbeats WHERE latest_timestamp >= $1",
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

        let mobile_price = self
            .price_tracker
            .price(&helium_proto::BlockchainTokenTypeV1::Mobile)
            .await?;

        // Mobile prices are supplied in 10^6, so we must convert them to Decimal
        let mobile_bone_price = Decimal::from(mobile_price)
                / dec!(1_000_000)  // Per Mobile token
                / dec!(1_000_000); // Per Bone

        // process rewards for poc and data transfer
        self.reward_poc_and_dc(reward_period, mobile_bone_price)
            .await?;

        // process rewards for mappers
        self.reward_mappers(reward_period).await?;

        // process rewards for service providers
        self.reward_service_providers(reward_period, mobile_bone_price)
            .await?;

        // process rewards for oracles
        self.reward_oracles(reward_period).await?;

        let written_files = self.mobile_rewards.commit().await?.await??;

        let mut transaction = self.pool.begin().await?;
        // clear out the various db tables
        heartbeats::clear_heartbeats(&mut transaction, &reward_period.start).await?;
        speedtests::clear_speedtests(&mut transaction, &reward_period.start).await?;
        data_session::clear_hotspot_data_sessions(&mut transaction, &reward_period.start).await?;
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

    async fn reward_poc_and_dc(
        &self,
        reward_period: &Range<DateTime<Utc>>,
        mobile_bone_price: Decimal,
    ) -> anyhow::Result<()> {
        let transfer_rewards = TransferRewards::from_transfer_sessions(
            mobile_bone_price,
            data_session::aggregate_hotspot_data_sessions_to_dc(&self.pool, reward_period).await?,
            reward_period,
        )
        .await;
        let transfer_rewards_sum = transfer_rewards.reward_sum();

        // It's important to gauge the scale metric. If this value is < 1.0, we are in
        // big trouble.
        let Some(scale) = transfer_rewards.reward_scale().to_f64() else {
            bail!("The data transfer rewards scale cannot be converted to a float");
        };
        telemetry::data_transfer_rewards_scale(scale);

        self.reward_poc(reward_period, transfer_rewards_sum).await?;
        self.reward_dc(reward_period, transfer_rewards).await?;

        Ok(())
    }

    async fn reward_poc(
        &self,
        reward_period: &Range<DateTime<Utc>>,
        transfer_reward_sum: Decimal,
    ) -> anyhow::Result<()> {
        let heartbeats = HeartbeatReward::validated(&self.pool, reward_period, self.max_distance_to_asserted);
        let speedtest_averages =
            SpeedtestAverages::aggregate_epoch_averages(reward_period.end, &self.pool).await?;
        let coverage_points = CoveragePoints::aggregate_points(
            &self.pool,
            heartbeats,
            &speedtest_averages,
            reward_period.end,
        )
        .await?;

        let total_poc_rewards =
            reward_shares::get_scheduled_tokens_for_poc(reward_period.end - reward_period.start)
                - transfer_reward_sum;

        if let Some(mobile_reward_shares) =
            coverage_points.into_rewards(total_poc_rewards, reward_period)
        {
            // handle poc reward outputs
            let mut allocated_poc_rewards = 0_u64;
            for (poc_reward_amount, mobile_reward_share) in mobile_reward_shares {
                allocated_poc_rewards += poc_reward_amount;
                self.mobile_rewards
                    .write(mobile_reward_share, [])
                    .await?
                    // Await the returned one shot to ensure that we wrote the file
                    .await??;
            }
            // write out any unallocated poc reward
            let unallocated_poc_reward_amount = (total_poc_rewards
                - Decimal::from(allocated_poc_rewards))
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
            self.write_unallocated_reward(
                UnallocatedRewardType::Poc,
                unallocated_poc_reward_amount,
                reward_period,
            )
            .await?;
        };
        Ok(())
    }

    async fn reward_dc(
        &self,
        reward_period: &Range<DateTime<Utc>>,
        transfer_rewards: TransferRewards,
    ) -> anyhow::Result<()> {
        // handle dc reward outputs
        let mut allocated_dc_rewards = 0_u64;
        let total_dc_rewards = transfer_rewards.total();
        for (dc_reward_amount, mobile_reward_share) in transfer_rewards.into_rewards(reward_period)
        {
            allocated_dc_rewards += dc_reward_amount;
            self.mobile_rewards
                .write(mobile_reward_share, [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }
        // write out any unallocated dc reward
        let unallocated_dc_reward_amount = (total_dc_rewards - Decimal::from(allocated_dc_rewards))
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        self.write_unallocated_reward(
            UnallocatedRewardType::Data,
            unallocated_dc_reward_amount,
            reward_period,
        )
        .await?;
        Ok(())
    }

    async fn reward_service_providers(
        &self,
        reward_period: &Range<DateTime<Utc>>,
        mobile_bone_price: Decimal,
    ) -> anyhow::Result<()> {
        let payer_dc_sessions =
            data_session::sum_data_sessions_to_dc_by_payer(&self.pool, reward_period).await?;
        let sp_shares =
            ServiceProviderShares::from_payers_dc(payer_dc_sessions, &self.carrier_client).await?;
        let total_sp_rewards = reward_shares::get_scheduled_tokens_for_service_providers(
            reward_period.end - reward_period.start,
        );
        let rewards_per_share = sp_shares.rewards_per_share(total_sp_rewards, mobile_bone_price)?;
        // translate service provider shares into service provider rewards
        // track the amount of allocated reward value as we go
        let mut allocated_sp_rewards = 0_u64;
        for (amount, sp_share) in
            sp_shares.into_service_provider_rewards(reward_period, rewards_per_share)
        {
            allocated_sp_rewards += amount;
            self.mobile_rewards
                .write(sp_share.clone(), [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }

        // write out any unallocated service provider reward
        let unallocated_sp_reward_amount = (total_sp_rewards - Decimal::from(allocated_sp_rewards))
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        self.write_unallocated_reward(
            UnallocatedRewardType::ServiceProvider,
            unallocated_sp_reward_amount,
            reward_period,
        )
        .await?;
        Ok(())
    }

    async fn reward_mappers(&self, reward_period: &Range<DateTime<Utc>>) -> anyhow::Result<()> {
        // Mapper rewards currently include rewards for discovery mapping only.
        // Verification mapping rewards to be added
        // get subscriber location shares this epoch
        let location_shares =
            subscriber_location::aggregate_location_shares(&self.pool, reward_period).await?;

        // determine mapping shares based on location shares and data transferred
        let mapping_shares = MapperShares::new(location_shares);
        let total_mappers_pool = reward_shares::get_scheduled_tokens_for_mappers(
            reward_period.end - reward_period.start,
        );
        let rewards_per_share = mapping_shares.rewards_per_share(total_mappers_pool)?;

        // translate discovery mapping shares into subscriber rewards
        let mut allocated_mapping_rewards = 0_u64;
        for (reward_amount, mapping_share) in
            mapping_shares.into_subscriber_rewards(reward_period, rewards_per_share)
        {
            allocated_mapping_rewards += reward_amount;
            self.mobile_rewards
                .write(mapping_share.clone(), [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }

        // write out any unallocated mapping rewards
        let unallocated_mapping_reward_amount = (total_mappers_pool
            - Decimal::from(allocated_mapping_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        self.write_unallocated_reward(
            UnallocatedRewardType::Mapper,
            unallocated_mapping_reward_amount,
            reward_period,
        )
        .await?;
        Ok(())
    }

    async fn reward_oracles(&self, reward_period: &Range<DateTime<Utc>>) -> anyhow::Result<()> {
        // atm 100% of oracle rewards are assigned to 'unallocated'
        let total_oracle_rewards = reward_shares::get_scheduled_tokens_for_oracles(
            reward_period.end - reward_period.start,
        );
        let allocated_oracle_rewards = 0_u64;
        let unallocated_oracle_reward_amount = (total_oracle_rewards
            - Decimal::from(allocated_oracle_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        self.write_unallocated_reward(
            UnallocatedRewardType::Oracle,
            unallocated_oracle_reward_amount,
            reward_period,
        )
        .await?;
        Ok(())
    }

    async fn write_unallocated_reward(
        &self,
        unallocated_type: UnallocatedRewardType,
        unallocated_amount: u64,
        reward_period: &'_ Range<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        if unallocated_amount > 0 {
            let unallocated_reward = proto::MobileRewardShare {
                start_period: reward_period.start.encode_timestamp(),
                end_period: reward_period.end.encode_timestamp(),
                reward: Some(ProtoReward::UnallocatedReward(UnallocatedReward {
                    reward_type: unallocated_type as i32,
                    amount: unallocated_amount,
                })),
            };
            self.mobile_rewards
                .write(unallocated_reward, [])
                .await?
                .await??;
        };
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
