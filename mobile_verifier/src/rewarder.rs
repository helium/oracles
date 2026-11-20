use crate::{
    iceberg, resolve_dao_pubkey, resolve_subdao_pubkey,
    reward_shares::{
        data_transfer::{self, to_iceberg_rewards, DataTransferAllocation},
        hip_149_reward_pools,
    },
    telemetry, PriceInfo, Settings,
};
use chrono::{DateTime, Utc};

use crate::reward_shares::{dc_to_hnt_bones, RewardableEntityKey};
use helium_iceberg_oracles::rewards::{IcebergServiceProviderReward, IcebergUnallocatedReward};
use helium_proto::{
    services::poc_mobile::{self as proto, UnallocatedReward, UnallocatedRewardType},
    ServiceProvider,
};
use reward_scheduler::Scheduler;
use rust_decimal::{prelude::*, Decimal};
use solana::Token;
use sqlx::{Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tokio::time::sleep;

pub mod epoch_reward_info;
pub mod meta_state;

pub use epoch_reward_info::{EpochInfo, EpochRewardInfo};
pub use meta_state::RewarderState;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder {
    pool: Pool<Postgres>,
    reward_period_duration: Duration,
    reward_offset: Duration,
    /// Trino client, used both to resolve the epoch's on-chain reward inputs
    /// (see [`epoch_reward_info`]) and to read burned data-transfer sessions for
    /// the reward pool (see [`crate::iceberg::burned_session`]).
    trino: trino_client::Client,
    reward_writers: iceberg::RewardWriters,
    /// The rewarder's own `meta` state: `next_reward_epoch` and
    /// `disable_complete_data_checks_until`.
    state: RewarderState,
}

impl Rewarder {
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        reward_writers: iceberg::RewardWriters,
        trino: trino_client::Client,
        state: RewarderState,
    ) -> anyhow::Result<impl ManagedTask> {
        let rewarder = Rewarder::new(
            pool.clone(),
            settings.reward_period,
            settings.reward_period_offset,
            trino,
            reward_writers,
            state,
        )?;

        Ok(TaskManager::builder().add_task(rewarder).build())
    }

    pub fn new(
        pool: Pool<Postgres>,
        reward_period_duration: Duration,
        reward_offset: Duration,
        trino: trino_client::Client,
        reward_writers: iceberg::RewardWriters,
        state: RewarderState,
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
            trino,
            reward_writers,
            state,
        })
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting rewarder");

        loop {
            let next_reward_epoch = self.state.next_reward_epoch().await?;
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

        // Burned data-transfer sessions arrive continuously, so sessions landing
        // *past* the end of the reward period tell us the burn pipeline ran at
        // least through the period we're about to reward.
        //
        // This is the only data-currency gate. Heartbeats and speedtests were
        // gated on too, back when Proof-of-Coverage was rewarded and they were
        // reward inputs. Under HIP-149 they feed no part of the payout, so
        // blocking on them could only ever delay a reward that would have been
        // correct — they come from a different pipeline than burned sessions and
        // fail independently of it.
        if reward_period.end >= self.state.disable_complete_data_checks_until().await? {
            if iceberg::burned_session::no_burned_sessions(&self.trino, reward_period).await? {
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

        // HIP-149: data transfer consumes the entire pool; PoC is not rewarded.
        reward_dc(
            &self.trino,
            &reward_info,
            price_info.clone(),
            &self.reward_writers,
            &write_id,
        )
        .await?;

        // process rewards for service providers
        reward_service_providers(&reward_info, &self.reward_writers, &write_id).await?;

        let mut transaction = self.pool.begin().await?;
        self.state
            .save_next_reward_epoch(&mut transaction, reward_info.epoch_day + 1)
            .await?;

        transaction.commit().await?;

        // Mirror the committed state, if a mirror file is configured.
        self.state.mirror_to_file().await?;

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
    trino: &trino_client::Client,
    reward_info: &EpochRewardInfo,
    price_info: PriceInfo,
    reward_writers: &iceberg::RewardWriters,
    write_id: &str,
) -> anyhow::Result<()> {
    let rewardable = iceberg::burned_session::aggregate_hotspot_data_sessions_to_dc(
        trino,
        &reward_info.epoch_period,
    )
    .await?;

    // Data transfer is the residual of `hnt_rewards_issued` after the flat
    // service-provider cut, so the cap/backstop shift is absorbed here rather
    // than over-/under-allocating. Under HIP-150 that cut is zero and the pool is
    // the whole issued amount. See `reward_shares::emissions_split`.
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

    let rewards = to_iceberg_rewards(&gw_rewards, &reward_info.epoch_period);
    reward_writers
        .write_data_transfer(write_id, rewards)
        .await?;

    // Data transfer consumes the whole pool; only integer-rounding dust (or the
    // entire pool when there was no data transfer) is written out as unallocated.
    write_unallocated_reward(
        UnallocatedRewardType::Data,
        unallocated,
        reward_info,
        reward_writers,
        write_id,
    )
    .await?;

    Ok(())
}

pub async fn reward_service_providers(
    reward_info: &EpochRewardInfo,
    reward_writers: &iceberg::RewardWriters,
    write_id: &str,
) -> anyhow::Result<()> {
    // A flat percent of total emissions (see `reward_shares::emissions_split`).
    let sp_reward_amount = hip_149_reward_pools(reward_info).service_provider;

    // HIP-150 Decision 3: Nova Labs contributes its Service Provider Rewards to
    // the Deployer Data Reward Pool, so the pool is zero and *no reward is
    // written at all* — not a reward of zero. Consumers see no service-provider
    // rows for a suspended epoch rather than rows that claim an award of nothing.
    //
    // Guarded on the amount rather than deleted: the contribution runs to
    // 2027-07-31 and may be extended once, so this path comes back. Restoring it
    // is `SERVICE_PROVIDER_PERCENT` alone.
    if sp_reward_amount == 0 {
        tracing::info!("service provider pool is zero, skipping service provider rewards");
        return Ok(());
    }

    // The entire service-provider pool goes to the HeliumMobile Network Wallet.
    let network_share = proto::ServiceProviderReward {
        service_provider_id: ServiceProvider::HeliumMobile.into(),
        rewardable_entity_key: RewardableEntityKey::Network.to_string(),
        amount: sp_reward_amount,
    };

    reward_writers
        .write_service_provider(
            write_id,
            vec![IcebergServiceProviderReward::from_sp_reward(
                &network_share,
                reward_info.epoch_period.start,
                reward_info.epoch_period.end,
            )],
        )
        .await?;

    Ok(())
}

async fn write_unallocated_reward(
    unallocated_type: UnallocatedRewardType,
    unallocated_amount: u64,
    reward_info: &'_ EpochRewardInfo,
    reward_writers: &iceberg::RewardWriters,
    write_id: &str,
) -> anyhow::Result<()> {
    if unallocated_amount == 0 {
        return Ok(());
    }

    let reward = UnallocatedReward {
        reward_type: unallocated_type as i32,
        amount: unallocated_amount,
    };

    reward_writers
        .write_unallocated(
            write_id,
            vec![IcebergUnallocatedReward::from_reward(
                &reward,
                reward_info.epoch_period.start,
                reward_info.epoch_period.end,
            )],
        )
        .await?;

    Ok(())
}
