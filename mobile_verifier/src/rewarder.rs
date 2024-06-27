use crate::{
    boosting_oracles::db::check_for_unprocessed_data_sets,
    coverage, data_session,
    heartbeats::{self, HeartbeatReward},
    radio_threshold,
    reward_shares::{
        self, CoverageShares, DataTransferAndPocAllocatedRewardShares, MapperShares,
        ServiceProviderShares, TransferRewards,
    },
    speedtests,
    speedtests_average::SpeedtestAverages,
    subscriber_location, telemetry, Settings,
};
use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload::FileUpload,
    traits::TimestampEncode,
    FileType,
};
use futures_util::TryFutureExt;
use helium_proto::services::{
    poc_mobile as proto, poc_mobile::mobile_reward_share::Reward as ProtoReward,
    poc_mobile::UnallocatedReward, poc_mobile::UnallocatedRewardType,
};
use helium_proto::RewardManifest;
use mobile_config::{
    boosted_hex_info::BoostedHexes,
    client::{
        carrier_service_client::CarrierServiceVerifier,
        hex_boosting_client::HexBoostingInfoResolver, ClientError,
    },
};
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::*, Decimal};
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder<A, B> {
    pool: Pool<Postgres>,
    carrier_client: A,
    hex_service_client: B,
    reward_period_duration: Duration,
    reward_offset: Duration,
    pub mobile_rewards: FileSinkClient,
    reward_manifests: FileSinkClient,
    price_tracker: PriceTracker,
    speedtest_averages: FileSinkClient,
}

impl<A, B> Rewarder<A, B>
where
    A: CarrierServiceVerifier<Error = ClientError> + Send + Sync + 'static,
    B: HexBoostingInfoResolver<Error = ClientError> + Send + Sync + 'static,
{
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        carrier_service_verifier: A,
        hex_boosting_info_resolver: B,
        speedtests_avg: FileSinkClient,
    ) -> anyhow::Result<impl ManagedTask> {
        let (price_tracker, price_daemon) = PriceTracker::new_tm(&settings.price_tracker).await?;

        let (mobile_rewards, mobile_rewards_server) = file_sink::FileSinkBuilder::new(
            FileType::MobileRewardShare,
            settings.store_base_path(),
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_radio_reward_shares"),
        )
        .auto_commit(false)
        .create()
        .await?;

        let (reward_manifests, reward_manifests_server) = file_sink::FileSinkBuilder::new(
            FileType::RewardManifest,
            settings.store_base_path(),
            file_upload,
            concat!(env!("CARGO_PKG_NAME"), "_reward_manifest"),
        )
        .auto_commit(false)
        .create()
        .await?;

        let rewarder = Rewarder::new(
            pool.clone(),
            carrier_service_verifier,
            hex_boosting_info_resolver,
            settings.reward_period,
            settings.reward_period_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            speedtests_avg,
        );

        Ok(TaskManager::builder()
            .add_task(price_daemon)
            .add_task(mobile_rewards_server)
            .add_task(reward_manifests_server)
            .add_task(rewarder)
            .build())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: Pool<Postgres>,
        carrier_client: A,
        hex_service_client: B,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient,
        reward_manifests: FileSinkClient,
        price_tracker: PriceTracker,
        speedtest_averages: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            carrier_client,
            hex_service_client,
            reward_period_duration,
            reward_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            speedtest_averages,
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
                    chrono::Duration::minutes(REWARDS_NOT_CURRENT_DELAY_PERIOD).to_std()?
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
                tracing::info!("No cbrs heartbeats found past reward period");
                return Ok(false);
            }

            if sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM wifi_heartbeats WHERE latest_timestamp >= $1",
            )
            .bind(reward_period.end)
            .fetch_one(&self.pool)
            .await?
                == 0
            {
                tracing::info!("No wifi heartbeats found past reward period");
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

            if check_for_unprocessed_data_sets(&self.pool, reward_period.end).await? {
                tracing::info!("Data sets still need to be processed");
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
        reward_poc_and_dc(
            &self.pool,
            &self.hex_service_client,
            &self.mobile_rewards,
            &self.speedtest_averages,
            reward_period,
            mobile_bone_price,
        )
        .await?;

        // process rewards for mappers
        reward_mappers(&self.pool, &self.mobile_rewards, reward_period).await?;

        // process rewards for service providers
        reward_service_providers(
            &self.pool,
            &self.carrier_client,
            &self.mobile_rewards,
            reward_period,
            mobile_bone_price,
        )
        .await?;

        // process rewards for oracles
        reward_oracles(&self.mobile_rewards, reward_period).await?;

        self.speedtest_averages.commit().await?;
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
}

impl<A, B> ManagedTask for Rewarder<A, B>
where
    A: CarrierServiceVerifier<Error = ClientError> + Send + Sync + 'static,
    B: HexBoostingInfoResolver<Error = ClientError> + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

pub async fn reward_poc_and_dc(
    pool: &Pool<Postgres>,
    hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    mobile_rewards: &FileSinkClient,
    speedtest_avg_sink: &FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
    mobile_bone_price: Decimal,
) -> anyhow::Result<()> {
    let mut reward_shares = DataTransferAndPocAllocatedRewardShares::new(reward_period);

    let transfer_rewards = TransferRewards::from_transfer_sessions(
        mobile_bone_price,
        data_session::aggregate_hotspot_data_sessions_to_dc(pool, reward_period).await?,
        &reward_shares,
    )
    .await;

    // It's important to gauge the scale metric. If this value is < 1.0, we are in
    // big trouble.
    let Some(scale) = transfer_rewards.reward_scale().to_f64() else {
        bail!("The data transfer rewards scale cannot be converted to a float");
    };
    telemetry::data_transfer_rewards_scale(scale);

    // reward dc before poc so that we can calculate the unallocated dc reward
    // and carry this into the poc pool
    let dc_unallocated_amount = reward_dc(mobile_rewards, reward_period, transfer_rewards).await?;

    // any poc unallocated gets attributed to the unallocated reward
    reward_shares.unallocated = dc_unallocated_amount;
    let poc_unallocated_amount = reward_poc(
        pool,
        hex_service_client,
        mobile_rewards,
        speedtest_avg_sink,
        reward_period,
        &reward_shares,
    )
    .await?
    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
    .to_u64()
    .unwrap_or(0);

    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::Poc,
        poc_unallocated_amount,
        reward_period,
    )
    .await?;

    Ok(())
}

async fn reward_poc(
    pool: &Pool<Postgres>,
    hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    mobile_rewards: &FileSinkClient,
    speedtest_avg_sink: &FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
    reward_shares: &DataTransferAndPocAllocatedRewardShares,
) -> anyhow::Result<Decimal> {
    let heartbeats = HeartbeatReward::validated(pool, reward_period);
    let speedtest_averages =
        SpeedtestAverages::aggregate_epoch_averages(reward_period.end, pool).await?;

    speedtest_averages.write_all(speedtest_avg_sink).await?;

    let boosted_hexes = BoostedHexes::get_all(hex_service_client).await?;

    let verified_radio_thresholds =
        radio_threshold::verified_radio_thresholds(pool, reward_period).await?;

    let coverage_shares = CoverageShares::new(
        pool,
        heartbeats,
        &speedtest_averages,
        &boosted_hexes,
        &verified_radio_thresholds,
        reward_period,
    )
    .await?;

    let total_poc_rewards =
        reward_shares.poc + reward_shares.boosted_poc + reward_shares.unallocated;

    let unallocated_poc_amount = if let Some(mobile_reward_shares) =
        coverage_shares.into_rewards(reward_shares, reward_period)
    {
        // handle poc reward outputs
        let mut allocated_poc_rewards = 0_u64;
        for (poc_reward_amount, mobile_reward_share) in mobile_reward_shares {
            allocated_poc_rewards += poc_reward_amount;
            mobile_rewards
                .write(mobile_reward_share, [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }
        // calculate any unallocated poc reward
        total_poc_rewards - Decimal::from(allocated_poc_rewards)
    } else {
        // default unallocated poc reward to the total poc reward
        total_poc_rewards
    };
    Ok(unallocated_poc_amount)
}

pub async fn reward_dc(
    mobile_rewards: &FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
    transfer_rewards: TransferRewards,
) -> anyhow::Result<Decimal> {
    // handle dc reward outputs
    let mut allocated_dc_rewards = 0_u64;
    let total_dc_rewards = transfer_rewards.total();
    for (dc_reward_amount, mobile_reward_share) in transfer_rewards.into_rewards(reward_period) {
        allocated_dc_rewards += dc_reward_amount;
        mobile_rewards
            .write(mobile_reward_share, [])
            .await?
            // Await the returned one shot to ensure that we wrote the file
            .await??;
    }
    // for Dc we return the unallocated amount rather than writing it out to as an unallocated reward
    // it then gets added to the poc pool
    // we return the full decimal value just to ensure we allocate all to poc
    let unallocated_dc_reward_amount = total_dc_rewards - Decimal::from(allocated_dc_rewards);
    Ok(unallocated_dc_reward_amount)
}

pub async fn reward_mappers(
    pool: &Pool<Postgres>,
    mobile_rewards: &FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    // Mapper rewards currently include rewards for discovery mapping only.
    // Verification mapping rewards to be added
    // get subscriber location shares this epoch
    let location_shares =
        subscriber_location::aggregate_location_shares(pool, reward_period).await?;

    // determine mapping shares based on location shares and data transferred
    let mapping_shares = MapperShares::new(location_shares);
    let total_mappers_pool =
        reward_shares::get_scheduled_tokens_for_mappers(reward_period.end - reward_period.start);
    let rewards_per_share = mapping_shares.rewards_per_share(total_mappers_pool)?;

    // translate discovery mapping shares into subscriber rewards
    let mut allocated_mapping_rewards = 0_u64;
    for (reward_amount, mapping_share) in
        mapping_shares.into_subscriber_rewards(reward_period, rewards_per_share)
    {
        allocated_mapping_rewards += reward_amount;
        mobile_rewards
            .write(mapping_share.clone(), [])
            .await?
            // Await the returned one shot to ensure that we wrote the file
            .await??;
    }

    // write out any unallocated mapping rewards
    let unallocated_mapping_reward_amount = total_mappers_pool
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
        - allocated_mapping_rewards;
    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::Mapper,
        unallocated_mapping_reward_amount,
        reward_period,
    )
    .await?;
    Ok(())
}

pub async fn reward_oracles(
    mobile_rewards: &FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    // atm 100% of oracle rewards are assigned to 'unallocated'
    let total_oracle_rewards =
        reward_shares::get_scheduled_tokens_for_oracles(reward_period.end - reward_period.start);
    let allocated_oracle_rewards = 0_u64;
    let unallocated_oracle_reward_amount = total_oracle_rewards
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
        - allocated_oracle_rewards;
    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::Oracle,
        unallocated_oracle_reward_amount,
        reward_period,
    )
    .await?;
    Ok(())
}

pub async fn reward_service_providers(
    pool: &Pool<Postgres>,
    carrier_client: &impl CarrierServiceVerifier<Error = ClientError>,
    mobile_rewards: &FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
    mobile_bone_price: Decimal,
) -> anyhow::Result<()> {
    let payer_dc_sessions =
        data_session::sum_data_sessions_to_dc_by_payer(pool, reward_period).await?;
    let sp_shares =
        ServiceProviderShares::from_payers_dc(payer_dc_sessions, carrier_client).await?;
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
        mobile_rewards.write(sp_share.clone(), []).await?.await??;
    }
    // write out any unallocated service provider reward
    let unallocated_sp_reward_amount = total_sp_rewards
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0)
        - allocated_sp_rewards;
    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::ServiceProvider,
        unallocated_sp_reward_amount,
        reward_period,
    )
    .await?;
    Ok(())
}

async fn write_unallocated_reward(
    mobile_rewards: &FileSinkClient,
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
        mobile_rewards
            .write(unallocated_reward, [])
            .await?
            .await??;
    };
    Ok(())
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
