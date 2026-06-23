use crate::{
    banning,
    boosting_oracles::db::check_for_unprocessed_data_sets,
    coverage,
    data_session::{self, DataSessionSource, RewardableDataByHotspot},
    heartbeats::{self, HeartbeatReward},
    iceberg, resolve_subdao_pubkey,
    reward_shares::{
        data_transfer::{self, into_proto_rewards, to_iceberg_rewards},
        dc_to_hnt_bones, get_scheduled_tokens_for_data_transfer,
        get_scheduled_tokens_for_service_providers, CalculatedPocRewardShares, CoverageShares,
        DataTransferAndPocAllocatedRewardBuckets,
    },
    speedtests,
    speedtests_average::SpeedtestAverages,
    telemetry, unique_connections, PriceInfo, Settings, ToProtoDecimal,
};
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, file_upload::FileUpload, traits::TimestampEncode};
use file_store_oracles::traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt};

use self::boosted_hex_eligibility::BoostedHexEligibility;
use crate::reward_shares::{RewardableEntityKey, HELIUM_MOBILE_SERVICE_REWARD_BONES};
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
    client::sub_dao_client::SubDaoEpochRewardInfoResolver,
    sub_dao_epoch_reward_info::EpochRewardInfo, EpochInfo,
};
use price_tracker::{PriceProvider, PriceTracker};
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

pub struct Rewarder<C> {
    sub_dao: SolPubkey,
    pool: Pool<Postgres>,
    sub_dao_epoch_reward_client: C,
    reward_period_duration: Duration,
    reward_offset: Duration,
    pub mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_manifests: FileSinkClient<RewardManifest>,
    price_tracker: PriceTracker,
    reward_writers: Option<iceberg::RewardWriters>,
    data_session_source: DataSessionSource,
}

impl<C> Rewarder<C>
where
    C: SubDaoEpochRewardInfoResolver,
{
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        sub_dao_epoch_reward_info_resolver: C,
        reward_writers: Option<iceberg::RewardWriters>,
        trino: Option<trino_client::Client>,
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

        let data_session_source = DataSessionSource::new(pool.clone(), trino);

        let rewarder = Rewarder::new(
            pool.clone(),
            sub_dao_epoch_reward_info_resolver,
            settings.reward_period,
            settings.reward_period_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            reward_writers,
            data_session_source,
        )?;

        Ok(TaskManager::builder()
            .add_task(price_daemon)
            .add_task(mobile_rewards_server)
            .add_task(reward_manifests_server)
            .add_task(rewarder)
            .build())
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pool: Pool<Postgres>,
        sub_dao_epoch_reward_client: C,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
        reward_manifests: FileSinkClient<RewardManifest>,
        price_tracker: PriceTracker,
        reward_writers: Option<iceberg::RewardWriters>,
        data_session_source: DataSessionSource,
    ) -> anyhow::Result<Self> {
        // get the subdao address
        let sub_dao = resolve_subdao_pubkey();
        tracing::info!("Mobile SubDao pubkey: {}", sub_dao);

        Ok(Self {
            sub_dao,
            pool,
            sub_dao_epoch_reward_client,
            reward_period_duration,
            reward_offset,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            reward_writers,
            data_session_source,
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

        // Postgres data is current, so rewards will run. Record (as a metric)
        // whether Trino's burned-session data was ready too. Non-blocking: Trino
        // is being validated, not yet trusted to gate rewards.
        self.data_session_source
            .record_trino_readiness(reward_period)
            .await;

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

        let write_id = format!("rewards-epoch-{}", reward_info.epoch_day);
        let reward_ctx = self
            .reward_writers
            .as_ref()
            .map(|writers| (writers, write_id.as_str()));

        // process rewards for poc and data transfer
        let poc_dc_shares = reward_poc_and_dc(
            &self.pool,
            &self.data_session_source,
            self.mobile_rewards.clone(),
            &reward_info,
            price_info.clone(),
            reward_ctx,
        )
        .await?;

        // process rewards for service providers
        reward_service_providers(self.mobile_rewards.clone(), &reward_info, reward_ctx).await?;

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
            poc_bones_per_reward_share: Some(poc_dc_shares.normal.proto_decimal()),
            boosted_poc_bones_per_reward_share: None,
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

impl<C> ManagedTask for Rewarder<C>
where
    C: SubDaoEpochRewardInfoResolver,
{
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

pub async fn reward_poc_and_dc(
    pool: &Pool<Postgres>,
    data_session_source: &DataSessionSource,
    mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    price_info: PriceInfo,
    reward_ctx: Option<(&iceberg::RewardWriters, &str)>,
) -> anyhow::Result<CalculatedPocRewardShares> {
    let reward_shares = DataTransferAndPocAllocatedRewardBuckets::new(reward_info.epoch_emissions);

    let hotspot_data_sessions = data_session_source
        .load_data_sessions(&reward_info.epoch_period)
        .await?;

    // PoC has been removed from rewards: data transfer consumes the entire pool,
    // and `reward_dc` writes out its own rounding remainder as unallocated.
    reward_dc(
        &mobile_rewards,
        reward_info,
        &price_info,
        hotspot_data_sessions,
        reward_ctx,
    )
    .await?;

    // PoC rewards are disabled: data transfer consumes the whole pool above, so
    // the PoC bucket is zero and `reward_poc` emits nothing — every radio reward
    // floors to 0 and is filtered out by `into_rewards`. This branch still runs
    // its coverage queries each epoch for no output.
    // TODO: remove `reward_poc` (and the inputs it reads) once PoC is fully torn
    // out of the pipeline.
    let (_poc_unallocated_amount, calculated_poc_reward_shares) = reward_poc(
        pool,
        &mobile_rewards,
        reward_info,
        reward_shares,
        reward_ctx,
    )
    .await?;

    Ok(calculated_poc_reward_shares)
}

pub async fn reward_poc(
    pool: &Pool<Postgres>,
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    reward_shares: DataTransferAndPocAllocatedRewardBuckets,
    reward_ctx: Option<(&iceberg::RewardWriters, &str)>,
) -> anyhow::Result<(Decimal, CalculatedPocRewardShares)> {
    let heartbeats = HeartbeatReward::validated(pool, &reward_info.epoch_period);
    let speedtest_averages =
        SpeedtestAverages::aggregate_epoch_averages(reward_info.epoch_period.end, pool).await?;

    let unique_connections = unique_connections::db::get(pool, &reward_info.epoch_period).await?;

    let boosted_hex_eligibility = BoostedHexEligibility::new(unique_connections.clone());

    let banned_radios = banning::BannedRadios::new(pool, reward_info.epoch_period.end).await?;

    let coverage_shares = CoverageShares::new(
        pool,
        heartbeats,
        &speedtest_averages,
        &boosted_hex_eligibility,
        &banned_radios,
        &unique_connections,
        &reward_info.epoch_period,
    )
    .await?;

    let total_poc_rewards = reward_shares.total_poc();

    let collect_iceberg = reward_ctx.is_some();
    let (unallocated_poc_amount, calculated_poc_rewards_per_share) =
        if let Some((calculated_poc_rewards_per_share, mobile_reward_shares)) =
            coverage_shares.into_rewards(reward_shares, &reward_info.epoch_period)
        {
            // handle poc reward outputs
            let mut allocated_poc_rewards = 0_u64;
            let mut count_rewarded_radios = 0;
            let mut iceberg_radio_rewards = Vec::new();
            let mut iceberg_covered_hexes = Vec::new();

            for (poc_reward_amount, radio_reward_v2) in mobile_reward_shares {
                allocated_poc_rewards += poc_reward_amount;
                count_rewarded_radios += 1;

                if collect_iceberg {
                    iceberg_radio_rewards.push(
                        iceberg::radio_reward::IcebergRadioReward::from_radio_reward(
                            &radio_reward_v2,
                            reward_info.epoch_period.start,
                            reward_info.epoch_period.end,
                        ),
                    );
                    iceberg_covered_hexes.extend(
                        iceberg::radio_reward_covered_hex::from_radio_reward(
                            &radio_reward_v2,
                            reward_info.epoch_period.start,
                            reward_info.epoch_period.end,
                        ),
                    );
                }

                let mobile_reward_share = proto::MobileRewardShare {
                    start_period: reward_info.epoch_period.start.encode_timestamp(),
                    end_period: reward_info.epoch_period.end.encode_timestamp(),
                    reward: Some(proto::mobile_reward_share::Reward::RadioRewardV2(
                        radio_reward_v2,
                    )),
                };

                mobile_rewards
                    .write(mobile_reward_share, [])
                    .await?
                    // await the returned one shot to ensure that we wrote the file
                    .await??;
            }
            if let Some((writers, id)) = reward_ctx {
                writers
                    .write_proof_of_coverage(id, iceberg_radio_rewards)
                    .await?;
                writers
                    .write_covered_hexes(id, iceberg_covered_hexes)
                    .await?;
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
    price_info: &PriceInfo,
    rewardable: RewardableDataByHotspot,
    reward_ctx: Option<(&iceberg::RewardWriters, &str)>,
) -> anyhow::Result<()> {
    // Section the first...
    // wherein, we get all the required data.
    let pool = {
        let total_emission_pool =
            get_scheduled_tokens_for_data_transfer(reward_info.epoch_emissions);

        // TODO: lift price to receive a full PriceInfo here.
        let reward_sum: Decimal = rewardable
            .values()
            .map(|r| dc_to_hnt_bones(Decimal::from(r.rewardable_dc), price_info.price_per_bone))
            .sum();

        // Gauge how the HNT value of all rewardable data credits compares to the
        // data-transfer pool. A scale < 1.0 means demand exceeds the pool (data
        // transfer is subsidized at less than its full DC value) — the same health
        // signal as before. It no longer feeds the allocation, which now always
        // distributes the full pool.
        let scale = if reward_sum > total_emission_pool {
            total_emission_pool / reward_sum
        } else {
            Decimal::ONE
        };

        let Some(scale) = scale.to_f64() else {
            anyhow::bail!("The data transfer rewards scale cannot be converted to a float");
        };
        telemetry::data_transfer_rewards_scale(scale);

        total_emission_pool
    };

    // Distribute the whole pool across hotspots in proportion to their data
    // credits; rounding dust comes back as `unallocated`.
    let allocation = data_transfer::allocate(
        pool,
        rewardable
            .into_iter()
            .map(|(hotspot_key, r)| data_transfer::GatewayDataTransfer {
                hotspot_key,
                rewardable_dc: r.rewardable_dc,
                rewardable_bytes: r.rewardable_bytes,
            }),
    );

    let rewards = allocation
        .rewards
        .into_iter()
        .filter(|r| r.reward != 0)
        .map(|g| g.into_gateway_reward(price_info.price_in_bones))
        .collect::<Vec<_>>();

    telemetry::data_transfer_rewarded_gateways(rewards.len() as u64);

    if let Some((writers, id)) = reward_ctx {
        let rewards = to_iceberg_rewards(&rewards, &reward_info.epoch_period);
        writers.write_data_transfer(id, rewards).await?;
    }

    let proto_rewards = into_proto_rewards(rewards, &reward_info.epoch_period);
    mobile_rewards.write_all(proto_rewards).await?;

    // Data transfer consumes the whole pool; only integer-rounding dust (or the
    // entire pool when there was no data transfer) is written out as unallocated.
    write_unallocated_reward(
        mobile_rewards,
        UnallocatedRewardType::Poc,
        allocation.unallocated,
        reward_info,
        reward_ctx,
    )
    .await?;

    Ok(())
}

pub async fn reward_service_providers(
    mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    reward_ctx: Option<(&iceberg::RewardWriters, &str)>,
) -> anyhow::Result<()> {
    let total_sp_rewards = get_scheduled_tokens_for_service_providers(reward_info.epoch_emissions);
    let sp_reward_amount = total_sp_rewards
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);

    let subscriber_amount = std::cmp::min(sp_reward_amount, HELIUM_MOBILE_SERVICE_REWARD_BONES);
    let network_amount = sp_reward_amount.saturating_sub(subscriber_amount);

    // Write a ServiceProviderReward for HeliumMobile Subscriber Wallet for 450 HNT
    let subscriber_share = proto::ServiceProviderReward {
        service_provider_id: ServiceProvider::HeliumMobile.into(),
        rewardable_entity_key: RewardableEntityKey::Subscriber.to_string(),
        amount: subscriber_amount,
    };

    // Remaining rewards goes to HeliumMobile Network Wallet
    let network_share = proto::ServiceProviderReward {
        service_provider_id: ServiceProvider::HeliumMobile.into(),
        rewardable_entity_key: RewardableEntityKey::Network.to_string(),
        amount: network_amount,
    };

    if let Some((writers, id)) = reward_ctx {
        use iceberg::service_provider_reward::IcebergServiceProviderReward;
        let start = reward_info.epoch_period.start;
        let end = reward_info.epoch_period.end;

        writers
            .write_service_provider(
                id,
                vec![
                    IcebergServiceProviderReward::from_sp_reward(&subscriber_share, start, end),
                    IcebergServiceProviderReward::from_sp_reward(&network_share, start, end),
                ],
            )
            .await?;
    }

    let subscriber_reward = proto::MobileRewardShare {
        start_period: reward_info.epoch_period.start.encode_timestamp(),
        end_period: reward_info.epoch_period.end.encode_timestamp(),
        reward: Some(ProtoReward::ServiceProviderReward(subscriber_share)),
    };
    let network_reward = proto::MobileRewardShare {
        start_period: reward_info.epoch_period.start.encode_timestamp(),
        end_period: reward_info.epoch_period.end.encode_timestamp(),
        reward: Some(ProtoReward::ServiceProviderReward(network_share)),
    };

    mobile_rewards.write(subscriber_reward, []).await?.await??;
    mobile_rewards.write(network_reward, []).await?.await??;

    Ok(())
}

async fn write_unallocated_reward(
    mobile_rewards: &FileSinkClient<proto::MobileRewardShare>,
    unallocated_type: UnallocatedRewardType,
    unallocated_amount: u64,
    reward_info: &'_ EpochRewardInfo,
    reward_ctx: Option<(&iceberg::RewardWriters, &str)>,
) -> anyhow::Result<()> {
    if unallocated_amount == 0 {
        return Ok(());
    }

    let reward = UnallocatedReward {
        reward_type: unallocated_type as i32,
        amount: unallocated_amount,
    };

    if let Some((writers, id)) = reward_ctx {
        writers
            .write_unallocated(
                id,
                vec![
                    iceberg::unallocated_reward::IcebergUnallocatedReward::from_reward(
                        &reward,
                        reward_info.epoch_period.start,
                        reward_info.epoch_period.end,
                    ),
                ],
            )
            .await?;
    }

    let reward_share = proto::MobileRewardShare {
        start_period: reward_info.epoch_period.start.encode_timestamp(),
        end_period: reward_info.epoch_period.end.encode_timestamp(),
        reward: Some(ProtoReward::UnallocatedReward(reward)),
    };
    mobile_rewards.write(reward_share, []).await?.await??;

    Ok(())
}

pub async fn next_reward_epoch(db: &Pool<Postgres>) -> db_store::Result<u64> {
    meta::fetch(db, "next_reward_epoch").await
}

async fn save_next_reward_epoch(exec: impl PgExecutor<'_>, value: u64) -> db_store::Result<()> {
    meta::store(exec, "next_reward_epoch", value).await
}
