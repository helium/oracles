use crate::{
    boosting_oracles::db::check_for_unprocessed_data_sets,
    coverage, data_session,
    heartbeats::{self, HeartbeatReward},
    radio_threshold,
    reward_shares::{
        self, CalculatedPocRewardShares, CoverageShares, DataTransferAndPocAllocatedRewardBuckets,
        MapperShares, TransferRewards,
    },
    service_provider::{self, ServiceProviderDCSessions, ServiceProviderPromotions},
    sp_boosted_rewards_bans, speedtests,
    speedtests_average::SpeedtestAverages,
    subscriber_location, subscriber_verified_mapping_event, telemetry, unique_connections,
    Settings, MOBILE_SUB_DAO_ONCHAIN_ADDRESS,
};
use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::{
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, TimestampEncode},
};
use futures_util::TryFutureExt;

use self::boosted_hex_eligibility::BoostedHexEligibility;
use helium_proto::{
    reward_manifest::RewardData::MobileRewardData,
    services::poc_mobile::{
        self as proto, mobile_reward_share::Reward as ProtoReward,
        service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioBanType,
        MobileRewardShare, UnallocatedReward, UnallocatedRewardType,
    },
    MobileRewardData as ManifestMobileRewardData, RewardManifest,
};
use mobile_config::{
    boosted_hex_info::BoostedHexes,
    client::{
        carrier_service_client::CarrierServiceVerifier,
        hex_boosting_client::HexBoostingInfoResolver,
        sub_dao_client::SubDaoEpochRewardInfoResolver, ClientError,
    },
    sub_dao_epoch_reward_info::ResolvedSubDaoEpochRewardInfo,
};
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::*, Decimal};
use sqlx::{PgExecutor, Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tokio::time::sleep;

pub mod boosted_hex_eligibility;
mod db;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder<A, B, C> {
    sub_dao_address: String,
    pool: Pool<Postgres>,
    carrier_client: A,
    hex_service_client: B,
    sub_dao_epoch_reward_client: C,
    reward_period_duration: Duration,
    reward_offset: Duration,
    pub mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_manifests: FileSinkClient<RewardManifest>,
    price_tracker: PriceTracker,
    speedtest_averages: FileSinkClient<proto::SpeedtestAvg>,
}

impl<A, B, C> Rewarder<A, B, C>
where
    A: CarrierServiceVerifier<Error = ClientError> + Send + Sync + 'static,
    B: HexBoostingInfoResolver<Error = ClientError> + Send + Sync + 'static,
    C: SubDaoEpochRewardInfoResolver<Error = ClientError> + Send + Sync + 'static,
{
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        carrier_service_verifier: A,
        hex_boosting_info_resolver: B,
        sub_dao_epoch_reward_info_resolver: C,
        speedtests_avg: FileSinkClient<proto::SpeedtestAvg>,
    ) -> anyhow::Result<impl ManagedTask> {
        let (price_tracker, price_daemon) = PriceTracker::new_tm(&settings.price_tracker).await?;

        let (mobile_rewards, mobile_rewards_server) = MobileRewardShare::file_sink(
            settings.store_base_path(),
            file_upload.clone(),
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let (reward_manifests, reward_manifests_server) = RewardManifest::file_sink(
            settings.store_base_path(),
            file_upload,
            FileSinkCommitStrategy::Manual,
            FileSinkRollTime::Default,
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        let rewarder = Rewarder::new(
            pool.clone(),
            carrier_service_verifier,
            hex_boosting_info_resolver,
            sub_dao_epoch_reward_info_resolver,
            settings.reward_period,
            settings.reward_period_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            speedtests_avg,
        )?;

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
        sub_dao_epoch_reward_client: C,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
        reward_manifests: FileSinkClient<RewardManifest>,
        price_tracker: PriceTracker,
        speedtest_averages: FileSinkClient<proto::SpeedtestAvg>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            sub_dao_address: MOBILE_SUB_DAO_ONCHAIN_ADDRESS.into(),
            pool,
            carrier_client,
            hex_service_client,
            sub_dao_epoch_reward_client,
            reward_period_duration,
            reward_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            speedtest_averages,
        })
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            let next_reward_epoch = next_reward_epoch(&self.pool).await?;
            let reward_info = self
                .sub_dao_epoch_reward_client
                .resolve_info(&self.sub_dao_address, next_reward_epoch)
                .await?
                .ok_or(anyhow::anyhow!(
                    "No reward info found for epoch {}",
                    next_reward_epoch
                ))?;

            let scheduler = Scheduler::new(
                self.reward_period_duration,
                reward_info.epoch_period.start,
                reward_info.epoch_period.end,
                self.reward_offset,
            );
            let now = Utc::now();
            let sleep_duration = if scheduler.should_trigger(now) {
                if self.is_data_current(&scheduler.schedule_period).await? {
                    self.reward(reward_info).await?;
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
        // Check if we have heartbeats and speedtests and unique connections past the end of the reward period
        if reward_period.end >= self.disable_complete_data_checks_until().await? {
            if db::no_cbrs_heartbeats(&self.pool, reward_period).await? {
                tracing::info!("No cbrs heartbeats found past reward period");
                return Ok(false);
            }

            if db::no_wifi_heartbeats(&self.pool, reward_period).await? {
                tracing::info!("No wifi heartbeats found past reward period");
                return Ok(false);
            }

            if db::no_speedtests(&self.pool, reward_period).await? {
                tracing::info!("No speedtests found past reward period");
                return Ok(false);
            }

            if db::no_unique_connections(&self.pool, reward_period).await? {
                tracing::info!("No unique connections found past reward period");
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

    pub async fn reward(&self, reward_info: ResolvedSubDaoEpochRewardInfo) -> anyhow::Result<()> {
        tracing::info!(
            "Rewarding for epoch {} period: {} to {}",
            reward_info.epoch,
            reward_info.epoch_period.start,
            reward_info.epoch_period.end
        );

        let hnt_price = self
            .price_tracker
            .price(&helium_proto::BlockchainTokenTypeV1::Hnt)
            .await?;

        let hnt_bone_price = PriceConverter::pricer_format_to_hnt_bones(hnt_price);

        // process rewards for poc and data transfer
        let poc_dc_shares = reward_poc_and_dc(
            &self.pool,
            &self.hex_service_client,
            &self.mobile_rewards,
            &self.speedtest_averages,
            &reward_info,
            hnt_bone_price,
        )
        .await?;

        // process rewards for mappers
        reward_mappers(&self.pool, &self.mobile_rewards, &reward_info).await?;

        // process rewards for service providers
        let dc_sessions = service_provider::get_dc_sessions(
            &self.pool,
            &self.carrier_client,
            &reward_info.epoch_period,
        )
        .await?;
        let sp_promotions =
            service_provider::get_promotions(&self.carrier_client, &reward_info.epoch_period.start)
                .await?;
        reward_service_providers(
            dc_sessions,
            sp_promotions.clone(),
            &self.mobile_rewards,
            &reward_info,
            hnt_bone_price,
        )
        .await?;

        // process rewards for oracles
        reward_oracles(&self.mobile_rewards, &reward_info).await?;

        self.speedtest_averages.commit().await?;
        let written_files = self.mobile_rewards.commit().await?.await??;

        let mut transaction = self.pool.begin().await?;
        // clear out the various db tables
        heartbeats::clear_heartbeats(&mut transaction, &reward_info.epoch_period.start).await?;
        speedtests::clear_speedtests(&mut transaction, &reward_info.epoch_period.start).await?;
        data_session::clear_hotspot_data_sessions(
            &mut transaction,
            &reward_info.epoch_period.start,
        )
        .await?;
        coverage::clear_coverage_objects(&mut transaction, &reward_info.epoch_period.start).await?;
        sp_boosted_rewards_bans::clear_bans(&mut transaction, reward_info.epoch_period.start)
            .await?;
        subscriber_verified_mapping_event::clear(&mut transaction, &reward_info.epoch_period.start)
            .await?;
        unique_connections::db::clear(&mut transaction, &reward_info.epoch_period.start).await?;
        // subscriber_location::clear_location_shares(&mut transaction, &reward_period.end).await?;

        save_next_reward_epoch(&mut transaction, reward_info.epoch + 1).await?;

        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        let reward_data = ManifestMobileRewardData {
            poc_bones_per_reward_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.normal.to_string(),
            }),
            boosted_poc_bones_per_reward_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.boost.to_string(),
            }),
            service_provider_promotions: sp_promotions.into_proto(),
        };
        self.reward_manifests
            .write(
                RewardManifest {
                    start_timestamp: reward_info.epoch_period.start.encode_timestamp(),
                    end_timestamp: reward_info.epoch_period.end.encode_timestamp(),
                    written_files,
                    reward_data: Some(MobileRewardData(reward_data)),
                    epoch: reward_info.epoch,
                    price: hnt_price,
                },
                [],
            )
            .await?
            .await??;

        self.reward_manifests.commit().await?;
        telemetry::last_rewarded_end_time(reward_info.epoch_period.end);
        Ok(())
    }
}

impl<A, B, C> ManagedTask for Rewarder<A, B, C>
where
    A: CarrierServiceVerifier<Error = ClientError> + Send + Sync + 'static,
    B: HexBoostingInfoResolver<Error = ClientError> + Send + Sync + 'static,
    C: SubDaoEpochRewardInfoResolver<Error = ClientError> + Send + Sync + 'static,
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
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    speedtest_avg_sink: &FileSinkClient<proto::SpeedtestAvg>,
    reward_info: &ResolvedSubDaoEpochRewardInfo,
    hnt_bone_price: Decimal,
) -> anyhow::Result<CalculatedPocRewardShares> {
    let mut reward_shares =
        DataTransferAndPocAllocatedRewardBuckets::new(reward_info.epoch_emissions);

    let transfer_rewards = TransferRewards::from_transfer_sessions(
        hnt_bone_price,
        data_session::aggregate_hotspot_data_sessions_to_dc(pool, &reward_info.epoch_period)
            .await?,
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
    let dc_unallocated_amount = reward_dc(
        mobile_rewards,
        reward_info,
        transfer_rewards,
        &reward_shares,
    )
    .await?;

    reward_shares.handle_unallocated_data_transfer(dc_unallocated_amount);
    let (poc_unallocated_amount, calculated_poc_reward_shares) = reward_poc(
        pool,
        hex_service_client,
        mobile_rewards,
        speedtest_avg_sink,
        reward_info,
        reward_shares,
    )
    .await?;

    let poc_unallocated_amount = poc_unallocated_amount
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);

    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::Poc,
        poc_unallocated_amount,
        reward_info,
    )
    .await?;

    Ok(calculated_poc_reward_shares)
}

async fn reward_poc(
    pool: &Pool<Postgres>,
    hex_service_client: &impl HexBoostingInfoResolver<Error = ClientError>,
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    speedtest_avg_sink: &FileSinkClient<proto::SpeedtestAvg>,
    reward_info: &ResolvedSubDaoEpochRewardInfo,
    reward_shares: DataTransferAndPocAllocatedRewardBuckets,
) -> anyhow::Result<(Decimal, CalculatedPocRewardShares)> {
    let heartbeats = HeartbeatReward::validated(pool, &reward_info.epoch_period);
    let speedtest_averages =
        SpeedtestAverages::aggregate_epoch_averages(reward_info.epoch_period.end, pool).await?;

    speedtest_averages.write_all(speedtest_avg_sink).await?;

    let boosted_hexes = BoostedHexes::get_all(hex_service_client).await?;

    let unique_connections = unique_connections::db::get(pool, reward_period).await?;

    let boosted_hex_eligibility = BoostedHexEligibility::new(
        radio_threshold::verified_radio_thresholds(pool, &reward_info.epoch_period).await?,
        sp_boosted_rewards_bans::db::get_banned_radios(
            pool,
            SpBoostedRewardsBannedRadioBanType::BoostedHex,
            reward_info.epoch_period.end,
        )
        .await?,
        unique_connections.clone(),
    );

    let poc_banned_radios = sp_boosted_rewards_bans::db::get_banned_radios(
        pool,
        SpBoostedRewardsBannedRadioBanType::Poc,
        reward_info.epoch_period.end,
    )
    .await?;

    let coverage_shares = CoverageShares::new(
        pool,
        heartbeats,
        &speedtest_averages,
        &boosted_hexes,
        &boosted_hex_eligibility,
        &poc_banned_radios,
        &unique_connections,
        &reward_info.epoch_period,
    )
    .await?;

    let total_poc_rewards = reward_shares.total_poc();

    let (unallocated_poc_amount, calculated_poc_rewards_per_share) =
        if let Some((calculated_poc_rewards_per_share, mobile_reward_shares)) =
            coverage_shares.into_rewards(reward_shares, reward_info)
        {
            // handle poc reward outputs
            let mut allocated_poc_rewards = 0_u64;
            for (poc_reward_amount, mobile_reward_share_v1, mobile_reward_share_v2) in
                mobile_reward_shares
            {
                allocated_poc_rewards += poc_reward_amount;
                mobile_rewards
                    .write(mobile_reward_share_v1, [])
                    .await?
                    // Await the returned one shot to ensure that we wrote the file
                    .await??;
                mobile_rewards
                    .write(mobile_reward_share_v2, [])
                    .await?
                    // Await the returned one shot ot ensure that we wrote the file
                    .await??;
            }
            // calculate any unallocated poc reward
            (
                total_poc_rewards - Decimal::from(allocated_poc_rewards),
                calculated_poc_rewards_per_share,
            )
        } else {
            // default unallocated poc reward to the total poc reward
            (total_poc_rewards, CalculatedPocRewardShares::default())
        };
    Ok((unallocated_poc_amount, calculated_poc_rewards_per_share))
}

pub async fn reward_dc(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &ResolvedSubDaoEpochRewardInfo,
    transfer_rewards: TransferRewards,
    reward_shares: &DataTransferAndPocAllocatedRewardBuckets,
) -> anyhow::Result<Decimal> {
    // handle dc reward outputs
    let mut allocated_dc_rewards = 0_u64;
    for (dc_reward_amount, mobile_reward_share) in transfer_rewards.into_rewards(reward_info) {
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
    let unallocated_dc_reward_amount =
        reward_shares.data_transfer - Decimal::from(allocated_dc_rewards);
    Ok(unallocated_dc_reward_amount)
}

pub async fn reward_mappers(
    pool: &Pool<Postgres>,
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &ResolvedSubDaoEpochRewardInfo,
) -> anyhow::Result<()> {
    // Mapper rewards currently include rewards for discovery mapping only.
    // Verification mapping rewards to be added
    // get subscriber location shares this epoch
    let location_shares =
        subscriber_location::aggregate_location_shares(pool, &reward_info.epoch_period).await?;

    // Verification mappers can only earn rewards if they qualified for disco mapping
    // rewards during the same reward_period
    let vsme_shares = subscriber_verified_mapping_event::aggregate_verified_mapping_events(
        pool,
        &reward_info.epoch_period,
    )
    .await?
    .into_iter()
    .filter(|vsme| location_shares.contains(&vsme.subscriber_id))
    .collect();

    // determine mapping shares based on location shares and data transferred
    let mapping_shares = MapperShares::new(location_shares, vsme_shares);
    let total_mappers_pool =
        reward_shares::get_scheduled_tokens_for_mappers(reward_info.epoch_emissions);
    let rewards_per_share = mapping_shares.rewards_per_share(total_mappers_pool)?;

    // translate discovery mapping shares into subscriber rewards
    let mut allocated_mapping_rewards = 0_u64;

    for (reward_amount, mapping_share) in
        mapping_shares.into_subscriber_rewards(reward_info, rewards_per_share)
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
        reward_info,
    )
    .await?;

    Ok(())
}

pub async fn reward_oracles(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &ResolvedSubDaoEpochRewardInfo,
) -> anyhow::Result<()> {
    // atm 100% of oracle rewards are assigned to 'unallocated'
    let total_oracle_rewards =
        reward_shares::get_scheduled_tokens_for_oracles(reward_info.epoch_emissions);
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
        reward_info,
    )
    .await?;
    Ok(())
}

pub async fn reward_service_providers(
    dc_sessions: ServiceProviderDCSessions,
    sp_promotions: ServiceProviderPromotions,
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &ResolvedSubDaoEpochRewardInfo,
    mobile_bone_price: Decimal,
) -> anyhow::Result<()> {
    use service_provider::ServiceProviderRewardInfos;

    let total_sp_rewards = service_provider::get_scheduled_tokens(reward_info.epoch_emissions);

    let sps = ServiceProviderRewardInfos::new(
        dc_sessions,
        sp_promotions,
        total_sp_rewards,
        mobile_bone_price,
        reward_info.clone(),
    );

    let mut unallocated_sp_rewards = total_sp_rewards
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);

    for (amount, reward) in sps.iter_rewards() {
        unallocated_sp_rewards -= amount;
        mobile_rewards.write(reward, []).await?.await??;
    }

    // write out any unallocated service provider reward
    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::ServiceProvider,
        unallocated_sp_rewards,
        reward_info,
    )
    .await?;
    Ok(())
}

async fn write_unallocated_reward(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    unallocated_type: UnallocatedRewardType,
    unallocated_amount: u64,
    reward_info: &'_ ResolvedSubDaoEpochRewardInfo,
) -> anyhow::Result<()> {
    if unallocated_amount > 0 {
        let unallocated_reward = proto::MobileRewardShare {
            start_period: reward_info.epoch_period.start.encode_timestamp(),
            end_period: reward_info.epoch_period.end.encode_timestamp(),
            epoch: reward_info.epoch,
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

pub async fn next_reward_epoch(db: &Pool<Postgres>) -> db_store::Result<u64> {
    meta::fetch(db, "next_reward_epoch").await
}

async fn save_next_reward_epoch(exec: impl PgExecutor<'_>, value: u64) -> db_store::Result<()> {
    meta::store(exec, "next_reward_epoch", value).await
}
