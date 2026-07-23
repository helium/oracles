use crate::{
    banning,
    data_session::{self, DataSessionSource},
    heartbeats, iceberg, resolve_dao_pubkey, resolve_subdao_pubkey,
    reward_shares::{
        data_transfer::{self, into_proto_rewards, to_iceberg_rewards, DataTransferAllocation},
        hip_149_reward_pools,
    },
    speedtests, telemetry, unique_connections, PriceInfo, Settings,
};
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, file_upload::FileUpload, traits::TimestampEncode};
use file_store_oracles::traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt};

use crate::reward_shares::{
    dc_to_hnt_bones, RewardableEntityKey, HELIUM_MOBILE_SERVICE_REWARD_BONES,
};
use helium_proto::{
    reward_manifest::RewardData::MobileRewardData,
    services::poc_mobile::{
        self as proto, mobile_reward_share::Reward as ProtoReward, MobileRewardShare,
        UnallocatedReward, UnallocatedRewardType,
    },
    MobileRewardData as ManifestMobileRewardData, MobileRewardToken, RewardManifest,
    ServiceProvider,
};
use mobile_config::{sub_dao_epoch_reward_info::EpochRewardInfo, EpochInfo};
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::*, Decimal};
use solana::Token;
use sqlx::{PgExecutor, Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tokio::time::sleep;

mod db;
pub mod epoch_reward_info;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder {
    pool: Pool<Postgres>,
    reward_period_duration: Duration,
    reward_offset: Duration,
    pub mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_manifests: FileSinkClient<RewardManifest>,
    /// Trino client, used both to resolve the epoch's on-chain reward inputs
    /// (see [`epoch_reward_info`]) and by [`DataSessionSource`].
    trino: trino_client::Client,
    reward_writers: Option<iceberg::RewardWriters>,
    data_session_source: DataSessionSource,
}

impl Rewarder {
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        reward_writers: Option<iceberg::RewardWriters>,
        trino: trino_client::Client,
    ) -> anyhow::Result<impl ManagedTask> {
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

        let data_session_source = DataSessionSource::new(pool.clone(), Some(trino.clone()));

        let rewarder = Rewarder::new(
            pool.clone(),
            settings.reward_period,
            settings.reward_period_offset,
            mobile_rewards,
            reward_manifests,
            trino,
            reward_writers,
            data_session_source,
        )?;

        Ok(TaskManager::builder()
            .add_task(mobile_rewards_server)
            .add_task(reward_manifests_server)
            .add_task(rewarder)
            .build())
    }

    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pool: Pool<Postgres>,
        reward_period_duration: Duration,
        reward_offset: Duration,
        mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
        reward_manifests: FileSinkClient<RewardManifest>,
        trino: trino_client::Client,
        reward_writers: Option<iceberg::RewardWriters>,
        data_session_source: DataSessionSource,
    ) -> anyhow::Result<Self> {
        // The mobile rewarder always targets the mobile sub-DAO / HNT DAO; log
        // the resolved addresses for ops visibility (pinned by the
        // `resolvers_match_onchain_addresses` test in the crate lib).
        tracing::info!(
            "Rewarding mobile sub_dao: {}, hnt dao: {}",
            resolve_subdao_pubkey(),
            resolve_dao_pubkey()
        );

        Ok(Self {
            pool,
            reward_period_duration,
            reward_offset,
            mobile_rewards,
            reward_manifests,
            trino,
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
                if self
                    .is_data_current(next_reward_epoch, &scheduler.schedule_period)
                    .await?
                {
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
        epoch: u64,
        reward_period: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<bool> {
        // The HNT reward price is recovered from the epoch's on-chain deployer
        // cap, so it is mandatory (unlike the complete-data checks below, which
        // can be disabled). Wait and retry until the DAO epoch has closed and
        // been indexed into Trino.
        if epoch_reward_info::resolve(&self.trino, epoch_reward_info::SOLANA_SCHEMA, epoch)
            .await?
            .is_none()
        {
            tracing::info!(epoch, "On-chain reward data not yet available; waiting");
            return Ok(false);
        }

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

            if db::no_data_transfer_sessions(&self.data_session_source, reward_period).await? {
                tracing::info!("No Burned Data Transfer Sessions found past reward period");
                return Ok(false);
            }
        } else {
            tracing::info!("Complete data checks are disabled for this reward period");
        }

        Ok(true)
    }

    pub async fn reward(&self, next_reward_epoch: u64) -> anyhow::Result<()> {
        tracing::info!("Resolving reward info for epoch: {next_reward_epoch}");

        // Resolve the epoch's reward inputs — the emissions split to distribute
        // and the HNT price to stamp — from a single Trino snapshot (replacing
        // the mobile-config gRPC client). The `is_data_current` gate already
        // confirmed they're available.
        let epoch_reward_info::ResolvedEpoch {
            reward_info,
            price_in_bones,
        } = epoch_reward_info::resolve(
            &self.trino,
            epoch_reward_info::SOLANA_SCHEMA,
            next_reward_epoch,
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("on-chain reward data unavailable for epoch {next_reward_epoch}")
        })?;

        let price_info = PriceInfo::new(price_in_bones, Token::Hnt.decimals());

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

        // HIP-149: data transfer consumes the entire pool; PoC is not rewarded.
        reward_dc(
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
        unique_connections::db::clear(&mut transaction, &reward_info.epoch_period.start).await?;
        banning::clear_bans(&mut transaction, reward_info.epoch_period.start).await?;

        save_next_reward_epoch(&mut *transaction, reward_info.epoch_day + 1).await?;

        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        let reward_data = ManifestMobileRewardData {
            // HIP-149: PoC is not rewarded, so the per-share value is the zero default.
            poc_bones_per_reward_share: None,
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

impl ManagedTask for Rewarder {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

/// Data-transfer reward path (HIP-149): data transfer consumes the entire
/// emissions pool and Proof-of-Coverage is not rewarded.
pub async fn reward_dc(
    data_session_source: &DataSessionSource,
    mobile_rewards: FileSinkClient<proto::MobileRewardShare>,
    reward_info: &EpochRewardInfo,
    price_info: PriceInfo,
    reward_ctx: Option<(&iceberg::RewardWriters, &str)>,
) -> anyhow::Result<()> {
    let rewardable = data_session_source
        .load_data_sessions(&reward_info.epoch_period)
        .await?;

    // HIP-149: data transfer is the residual of `hnt_rewards_issued` after the
    // flat 24% service-provider cut, so the cap/backstop shift is absorbed here
    // rather than over-/under-allocating. See `reward_shares::emissions_split`.
    let pool = Decimal::from(hip_149_reward_pools(reward_info).data_transfer);
    // Demand is the HNT-bone value of the burned DC at the epoch price — a
    // telemetry input only (the payout rate is `pool / total_dc`, price-free).
    // `dc_to_hnt_bones` is linear, so summing per hotspot is unnecessary.
    let demand = dc_to_hnt_bones(
        Decimal::from(rewardable.total_dc()),
        price_info.price_per_bone,
    );
    let total_bytes = rewardable.total_bytes();

    // Distribute the whole pool across hotspots in proportion to their data
    // credits; rounding dust comes back as `unallocated`.
    let DataTransferAllocation {
        rewards,
        unallocated,
    } = data_transfer::allocate(pool, rewardable.into_gw_data_transfer());

    let gw_rewards = rewards
        .into_iter()
        .filter(|r| r.reward != 0)
        .map(|g| g.into_gateway_reward(price_info.price_in_bones))
        .collect::<Vec<_>>();

    // All the data-transfer reward metrics, recorded together. The scale
    // (`pool / demand`) and the *target* price per GB are input-derived; the
    // *actual* price per GB uses the bones we really distributed, so it sits at or
    // just below the target rate by the unallocated rounding dust.
    let distributed_bones = data_transfer::distributed_bones(&gw_rewards);
    let scale = data_transfer::scale(pool, demand);
    let target = data_transfer::price_per_gb(pool, total_bytes, price_info.price_per_bone);
    let actual =
        data_transfer::price_per_gb(distributed_bones, total_bytes, price_info.price_per_bone);

    telemetry::data_transfer_rewarded_gateways(gw_rewards.len() as u64);
    telemetry::data_transfer_rewards_scale(scale.to_f64().unwrap_or_default());
    telemetry::data_transfer_target_price_per_gb(target.to_f64().unwrap_or_default());
    telemetry::data_transfer_actual_price_per_gb(actual.to_f64().unwrap_or_default());

    if let Some((writers, id)) = reward_ctx {
        let rewards = to_iceberg_rewards(&gw_rewards, &reward_info.epoch_period);
        writers.write_data_transfer(id, rewards).await?;
    }

    let proto_rewards = into_proto_rewards(gw_rewards, &reward_info.epoch_period);
    mobile_rewards.write_all(proto_rewards).await?;

    // Data transfer consumes the whole pool; only integer-rounding dust (or the
    // entire pool when there was no data transfer) is written out as unallocated.
    write_unallocated_reward(
        &mobile_rewards,
        UnallocatedRewardType::Data,
        unallocated,
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
    // HIP-149: a flat 24% of total emissions (see `reward_shares::emissions_split`).
    let sp_reward_amount = hip_149_reward_pools(reward_info).service_provider;

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
