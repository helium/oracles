use crate::{
    banning,
    boosting_oracles::db::check_for_unprocessed_data_sets,
    coverage, data_session,
    heartbeats::{self, HeartbeatReward},
    resolve_subdao_pubkey,
    reward_shares::{
        get_scheduled_tokens_for_service_providers, CalculatedPocRewardShares, CoverageShares,
        DataTransferAndPocAllocatedRewardBuckets, TransferRewards,
    },
    speedtests,
    speedtests_average::SpeedtestAverages,
    telemetry, unique_connections, PriceInfo, Settings,
};
use anyhow::bail;
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, file_upload::FileUpload, traits::TimestampEncode};
use file_store_oracles::{
    network_common::service_provider_reward_type::ServiceProviderRewardType,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt}
};

use self::boosted_hex_eligibility::BoostedHexEligibility;
use crate::reward_shares::HELIUM_MOBILE_SERVICE_REWARD_BONES;
use helium_proto::{
    reward_manifest::RewardData::MobileRewardData,
    services::poc_mobile::{
        self as proto, mobile_reward_share::Reward as ProtoReward, MobileRewardShare,
        UnallocatedReward, UnallocatedRewardType,
    },
    MobileRewardData as ManifestMobileRewardData, MobileRewardToken, RewardManifest,
    ServiceProvider,
};
use mobile_config::{
    boosted_hex_info::BoostedHexes,
    client::{
        hex_boosting_client::HexBoostingInfoResolver, sub_dao_client::SubDaoEpochRewardInfoResolver,
    },
    sub_dao_epoch_reward_info::EpochRewardInfo,
    EpochInfo,
};
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::*, Decimal};
use solana::{SolPubkey, Token};
use sqlx::{PgExecutor, Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tokio::time::sleep;

pub mod boosted_hex_eligibility;
mod db;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder<B, C> {
    sub_dao: SolPubkey,
    pool: Pool<Postgres>,
    hex_service_client: B,
    sub_dao_epoch_reward_client: C,
    reward_period_duration: Duration,
    reward_offset: Duration,
    pub mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_manifests: FileSinkClient<RewardManifest>,
    price_tracker: PriceTracker,
    speedtest_averages: FileSinkClient<proto::SpeedtestAvg>,
}

impl<B, C> Rewarder<B, C>
where
    B: HexBoostingInfoResolver,
    C: SubDaoEpochRewardInfoResolver,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        hex_boosting_info_resolver: B,
        sub_dao_epoch_reward_info_resolver: C,
        speedtests_avg: FileSinkClient<proto::SpeedtestAvg>,
    ) -> anyhow::Result<impl ManagedTask> {
        let (price_tracker, price_daemon) = PriceTracker::new(&settings.price_tracker).await?;

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
        hex_service_client: B,
        sub_dao_epoch_reward_client: C,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
        reward_manifests: FileSinkClient<RewardManifest>,
        price_tracker: PriceTracker,
        speedtest_averages: FileSinkClient<proto::SpeedtestAvg>,
    ) -> anyhow::Result<Self> {
        // get the subdao address
        let sub_dao = resolve_subdao_pubkey();
        tracing::info!("Mobile SubDao pubkey: {}", sub_dao);

        Ok(Self {
            sub_dao,
            pool,
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
        tracing::info!("Starting rewarder");

        loop {
            let next_reward_epoch = next_reward_epoch(&self.pool).await?;
            let next_reward_epoch_period = EpochInfo::from(next_reward_epoch);

            let scheduler = Scheduler::new(
                self.reward_period_duration,
                next_reward_epoch_period.period.start,
                next_reward_epoch_period.period.end,
                self.reward_offset,
            );

            let now = Utc::now();
            let sleep_duration = if scheduler.should_trigger(now) {
                if self.is_data_current(&scheduler.schedule_period).await? {
                    match self.reward(next_reward_epoch).await {
                        Ok(_) => {
                            tracing::info!("Successfully rewarded for epoch {}", next_reward_epoch);
                            continue;
                        }
                        Err(e) => {
                            tracing::error!("Failed to reward: {}", e);
                            chrono::Duration::minutes(REWARDS_NOT_CURRENT_DELAY_PERIOD).to_std()?
                        }
                    }
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

        tracing::info!("Stopping rewarder");
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

    pub async fn reward(&self, next_reward_epoch: u64) -> anyhow::Result<()> {
        tracing::info!(
            "Resolving reward info for epoch: {}, subdao: {}",
            next_reward_epoch,
            self.sub_dao
        );

        let reward_info = self
            .sub_dao_epoch_reward_client
            .resolve_info(&self.sub_dao.to_string(), next_reward_epoch)
            .await?
            .ok_or(anyhow::anyhow!(
                "No reward info found for epoch {}",
                next_reward_epoch
            ))?;

        let pricer_hnt_price = self
            .price_tracker
            .price(&helium_proto::BlockchainTokenTypeV1::Hnt)
            .await?;

        let price_info = PriceInfo::new(pricer_hnt_price, Token::Hnt.decimals());

        tracing::info!(
            "Rewarding for epoch {} period: {} to {} with hnt bone price: {} and reward pool: {}",
            reward_info.epoch_day,
            reward_info.epoch_period.start,
            reward_info.epoch_period.end,
            price_info.price_per_bone,
            reward_info.epoch_emissions,
        );

        // process rewards for poc and data transfer
        let poc_dc_shares = reward_poc_and_dc(
            &self.pool,
            &self.hex_service_client,
            self.mobile_rewards.clone(),
            &reward_info,
            price_info.clone(),
        )
        .await?;

        // process rewards for service providers
        reward_service_providers(self.mobile_rewards.clone(), &reward_info).await?;

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
        unique_connections::db::clear(&mut transaction, &reward_info.epoch_period.start).await?;
        banning::clear_bans(&mut transaction, reward_info.epoch_period.start).await?;

        save_next_reward_epoch(&mut *transaction, reward_info.epoch_day + 1).await?;

        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        let reward_data = ManifestMobileRewardData {
            poc_bones_per_reward_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.normal.to_string(),
            }),
            boosted_poc_bones_per_reward_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.boost.to_string(),
            }),
            service_provider_promotions: vec![],
            token: MobileRewardToken::Hnt as i32,
        };
        self.reward_manifests
            .write(
                RewardManifest {
                    start_timestamp: reward_info.epoch_period.start.encode_timestamp(),
                    end_timestamp: reward_info.epoch_period.end.encode_timestamp(),
                    written_files,
                    reward_data: Some(MobileRewardData(reward_data)),
                    epoch: reward_info.epoch_day,
                    price: price_info.price_in_bones,
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

impl<B, C> ManagedTask for Rewarder<B, C>
where
    B: HexBoostingInfoResolver,
    C: SubDaoEpochRewardInfoResolver,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

pub async fn reward_poc_and_dc(
    pool: &Pool<Postgres>,
    hex_service_client: &impl HexBoostingInfoResolver,
    mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    price_info: PriceInfo,
) -> anyhow::Result<CalculatedPocRewardShares> {
    let mut reward_shares =
        DataTransferAndPocAllocatedRewardBuckets::new(reward_info.epoch_emissions);

    let transfer_rewards = TransferRewards::from_transfer_sessions(
        price_info,
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
        &mobile_rewards,
        reward_info,
        transfer_rewards,
        &reward_shares,
    )
    .await?;

    reward_shares.handle_unallocated_data_transfer(dc_unallocated_amount);
    let (poc_unallocated_amount, calculated_poc_reward_shares) = reward_poc(
        pool,
        hex_service_client,
        &mobile_rewards,
        reward_info,
        reward_shares,
    )
    .await?;

    let poc_unallocated_amount = poc_unallocated_amount
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);

    write_unallocated_reward(
        &mobile_rewards,
        UnallocatedRewardType::Poc,
        poc_unallocated_amount,
        reward_info,
    )
    .await?;

    Ok(calculated_poc_reward_shares)
}

pub async fn reward_poc(
    pool: &Pool<Postgres>,
    hex_service_client: &impl HexBoostingInfoResolver,
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    reward_shares: DataTransferAndPocAllocatedRewardBuckets,
) -> anyhow::Result<(Decimal, CalculatedPocRewardShares)> {
    let heartbeats = HeartbeatReward::validated(pool, &reward_info.epoch_period);
    let speedtest_averages =
        SpeedtestAverages::aggregate_epoch_averages(reward_info.epoch_period.end, pool).await?;

    let boosted_hexes = BoostedHexes::get_all(hex_service_client).await?;

    let unique_connections = unique_connections::db::get(pool, &reward_info.epoch_period).await?;

    let boosted_hex_eligibility = BoostedHexEligibility::new(unique_connections.clone());

    let banned_radios = banning::BannedRadios::new(pool, reward_info.epoch_period.end).await?;

    let coverage_shares = CoverageShares::new(
        pool,
        heartbeats,
        &speedtest_averages,
        &boosted_hexes,
        &boosted_hex_eligibility,
        &banned_radios,
        &unique_connections,
        &reward_info.epoch_period,
    )
    .await?;

    let total_poc_rewards = reward_shares.total_poc();

    let (unallocated_poc_amount, calculated_poc_rewards_per_share) =
        if let Some((calculated_poc_rewards_per_share, mobile_reward_shares)) =
            coverage_shares.into_rewards(reward_shares, &reward_info.epoch_period)
        {
            // handle poc reward outputs
            let mut allocated_poc_rewards = 0_u64;
            let mut count_rewarded_radios = 0;
            for (poc_reward_amount, mobile_reward_share_v2) in mobile_reward_shares {
                allocated_poc_rewards += poc_reward_amount;
                count_rewarded_radios += 1;
                mobile_rewards
                    .write(mobile_reward_share_v2, [])
                    .await?
                    // await the returned one shot to ensure that we wrote the file
                    .await??;
            }
            telemetry::poc_rewarded_radios(count_rewarded_radios);
            // calculate any unallocated poc reward
            (
                total_poc_rewards - Decimal::from(allocated_poc_rewards),
                calculated_poc_rewards_per_share,
            )
        } else {
            telemetry::poc_rewarded_radios(0);
            // default unallocated poc reward to the total poc reward
            (total_poc_rewards, CalculatedPocRewardShares::default())
        };
    Ok((unallocated_poc_amount, calculated_poc_rewards_per_share))
}

pub async fn reward_dc(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    transfer_rewards: TransferRewards,
    reward_shares: &DataTransferAndPocAllocatedRewardBuckets,
) -> anyhow::Result<Decimal> {
    // handle dc reward outputs
    let mut allocated_dc_rewards = 0_u64;
    let mut count_rewarded_gateways = 0;
    for (dc_reward_amount, mobile_reward_share) in transfer_rewards.into_rewards(reward_info) {
        allocated_dc_rewards += dc_reward_amount;
        count_rewarded_gateways += 1;
        mobile_rewards
            .write(mobile_reward_share, [])
            .await?
            // Await the returned one shot to ensure that we wrote the file
            .await??;
    }
    telemetry::data_transfer_rewarded_gateways(count_rewarded_gateways);
    // for Dc we return the unallocated amount rather than writing it out to as an unallocated reward
    // it then gets added to the poc pool
    // we return the full decimal value just to ensure we allocate all to poc
    let unallocated_dc_reward_amount =
        reward_shares.data_transfer - Decimal::from(allocated_dc_rewards);
    Ok(unallocated_dc_reward_amount)
}

pub async fn reward_service_providers(
    mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
) -> anyhow::Result<()> {
    let total_sp_rewards = get_scheduled_tokens_for_service_providers(reward_info.epoch_emissions);
    let sp_reward_amount = total_sp_rewards
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);

    // Write a ServiceProviderReward for HeliumMobile Subscriber Wallet for 450 HNT
    write_service_provider_reward(
        &mobile_rewards,
        reward_info,
        HELIUM_MOBILE_SERVICE_REWARD_BONES,
        ServiceProvider::HeliumMobile,
        ServiceProviderRewardType::Subscriber,
    )
    .await?;

    // Remaining rewards goes to HeliumMobile Network Wallet
    let remaining_reward_amount = sp_reward_amount - HELIUM_MOBILE_SERVICE_REWARD_BONES;
    write_service_provider_reward(
        &mobile_rewards,
        reward_info,
        remaining_reward_amount,
        ServiceProvider::HeliumMobile,
        ServiceProviderRewardType::Network,
    )
    .await?;

    Ok(())
}

async fn write_unallocated_reward(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    unallocated_type: UnallocatedRewardType,
    unallocated_amount: u64,
    reward_info: &'_ EpochRewardInfo,
) -> anyhow::Result<()> {
    if unallocated_amount > 0 {
        let unallocated_reward = proto::MobileRewardShare {
            start_period: reward_info.epoch_period.start.encode_timestamp(),
            end_period: reward_info.epoch_period.end.encode_timestamp(),
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

async fn write_service_provider_reward(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    reward_amount: u64,
    service_provider_id: ServiceProvider,
    service_provider_reward_type: ServiceProviderRewardType,
) -> anyhow::Result<()> {
    let reward = proto::MobileRewardShare {
        start_period: reward_info.epoch_period.start.encode_timestamp(),
        end_period: reward_info.epoch_period.end.encode_timestamp(),
        reward: Some(ProtoReward::ServiceProviderReward(
            proto::ServiceProviderReward {
                service_provider_id: service_provider_id.into(),
                amount: reward_amount,
                service_provider_reward_type: service_provider_reward_type.to_string(),
            },
        )),
    };

    mobile_rewards.write(reward, []).await?.await??;
    Ok(())
}
