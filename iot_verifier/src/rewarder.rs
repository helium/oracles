use crate::{
    resolve_subdao_pubkey,
    reward_share::{self, GatewayShares},
    telemetry, PriceInfo,
};
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, traits::TimestampEncode};
use futures::future::LocalBoxFuture;
use helium_proto::{
    reward_manifest::RewardData::IotRewardData,
    services::poc_lora::{
        self as proto, iot_reward_share::Reward as ProtoReward, UnallocatedReward,
        UnallocatedRewardType,
    },
    IotRewardData as ManifestIotRewardData, IotRewardToken, RewardManifest,
};
use humantime_serde::re::humantime;
use iot_config::{
    client::{sub_dao_client::SubDaoEpochRewardInfoResolver, ClientError},
    sub_dao_epoch_reward_info::EpochRewardInfo,
    EpochInfo,
};
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use solana::{SolPubkey, Token};
use sqlx::{PgExecutor, PgPool, Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::ManagedTask;
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: Duration = Duration::from_secs(5 * 60);

pub struct Rewarder<A> {
    sub_dao: SolPubkey,
    pub pool: Pool<Postgres>,
    pub rewards_sink: file_sink::FileSinkClient<proto::IotRewardShare>,
    pub reward_manifests_sink: file_sink::FileSinkClient<RewardManifest>,
    pub reward_period_hours: Duration,
    pub reward_offset: Duration,
    pub price_tracker: PriceTracker,
    sub_dao_epoch_reward_client: A,
}

pub struct RewardPocDcDataPoints {
    beacon_rewards_per_share: Decimal,
    witness_rewards_per_share: Decimal,
    dc_transfer_rewards_per_share: Decimal,
}

impl<A> ManagedTask for Rewarder<A>
where
    A: SubDaoEpochRewardInfoResolver<Error = ClientError> + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl<A> Rewarder<A>
where
    A: SubDaoEpochRewardInfoResolver<Error = ClientError> + Send + Sync + 'static,
{
    pub fn new(
        pool: PgPool,
        rewards_sink: file_sink::FileSinkClient<proto::IotRewardShare>,
        reward_manifests_sink: file_sink::FileSinkClient<RewardManifest>,
        reward_period_hours: Duration,
        reward_offset: Duration,
        price_tracker: PriceTracker,
        sub_dao_epoch_reward_client: A,
    ) -> anyhow::Result<Self> {
        // get the subdao address
        let sub_dao = resolve_subdao_pubkey();
        tracing::info!("Iot SubDao pubkey: {}", sub_dao);
        Ok(Self {
            sub_dao,
            pool,
            rewards_sink,
            reward_manifests_sink,
            reward_period_hours,
            reward_offset,
            price_tracker,
            sub_dao_epoch_reward_client,
        })
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting rewarder");

        loop {
            let next_reward_epoch = next_reward_epoch(&self.pool).await?;
            let next_reward_epoch_period = EpochInfo::from(next_reward_epoch);

            let scheduler = Scheduler::new(
                self.reward_period_hours,
                next_reward_epoch_period.period.start,
                next_reward_epoch_period.period.end,
                self.reward_offset,
            );

            let now = Utc::now();
            let sleep_duration = if scheduler.should_trigger(now) {
                if self.data_current_check(&scheduler.schedule_period).await? {
                    match self.reward(next_reward_epoch).await {
                        Ok(()) => {
                            tracing::info!("Successfully rewarded for epoch {}", next_reward_epoch);
                            scheduler.sleep_duration(Utc::now())?
                        }
                        Err(e) => {
                            tracing::error!("Failed to reward: {}", e);
                            REWARDS_NOT_CURRENT_DELAY_PERIOD
                        }
                    }
                } else {
                    REWARDS_NOT_CURRENT_DELAY_PERIOD
                }
            } else {
                scheduler.sleep_duration(Utc::now())?
            };

            tracing::info!(
                "rewards will be rerun in {}",
                humantime::format_duration(sleep_duration)
            );

            let shutdown = shutdown.clone();
            tokio::select! {
                biased;
                _ = shutdown => break,
                _ = sleep(sleep_duration) => (),
            }
        }

        tracing::info!("Stopping rewarder");
        Ok(())
    }

    pub async fn reward(&mut self, next_reward_epoch: u64) -> anyhow::Result<()> {
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

        // process rewards for poc and dc
        let poc_dc_shares = reward_poc_and_dc(
            &self.pool,
            &self.rewards_sink,
            &reward_info,
            price_info.clone(),
        )
        .await?;

        // process rewards for the operational fund
        reward_operational(&self.rewards_sink, &reward_info).await?;

        // process rewards for the oracle
        reward_oracles(&self.rewards_sink, &reward_info).await?;

        // commit the filesink
        let written_files = self.rewards_sink.commit().await?.await??;

        let mut transaction = self.pool.begin().await?;

        // Clear gateway shares table period to end of reward period
        GatewayShares::clear_rewarded_shares(&mut transaction, reward_info.epoch_period.start)
            .await?;

        save_next_reward_epoch(&mut *transaction, reward_info.epoch_day + 1).await?;

        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        let reward_data = ManifestIotRewardData {
            poc_bones_per_beacon_reward_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.beacon_rewards_per_share.to_string(),
            }),
            poc_bones_per_witness_reward_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.witness_rewards_per_share.to_string(),
            }),
            dc_bones_per_share: Some(helium_proto::Decimal {
                value: poc_dc_shares.dc_transfer_rewards_per_share.to_string(),
            }),
            token: IotRewardToken::Hnt as i32,
        };
        self.reward_manifests_sink
            .write(
                RewardManifest {
                    start_timestamp: reward_info.epoch_period.start.encode_timestamp(),
                    end_timestamp: reward_info.epoch_period.end.encode_timestamp(),
                    written_files,
                    reward_data: Some(IotRewardData(reward_data)),
                    epoch: reward_info.epoch_day,
                    price: price_info.price_in_bones,
                },
                [],
            )
            .await?
            .await??;
        self.reward_manifests_sink.commit().await?;
        telemetry::last_rewarded_end_time(reward_info.epoch_period.end);
        Ok(())
    }

    async fn data_current_check(
        &self,
        reward_period: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<bool> {
        // Check if we have gateway shares past the end of the reward period
        if reward_period.end >= self.disable_complete_data_checks_until().await? {
            if sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM gateway_shares WHERE reward_timestamp >= $1",
            )
            .bind(reward_period.end)
            .fetch_one(&self.pool)
            .await?
                == 0
            {
                tracing::info!("No gateway_shares found past reward period");
                return Ok(false);
            }

            if sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM gateway_dc_shares WHERE reward_timestamp >= $1",
            )
            .bind(reward_period.end)
            .fetch_one(&self.pool)
            .await?
                == 0
            {
                tracing::info!("No gateway_dc_shares found past reward period");
                return Ok(false);
            }
        } else {
            tracing::info!("data validity checks are disabled for this reward period");
        }
        Ok(true)
    }

    async fn disable_complete_data_checks_until(&self) -> db_store::Result<DateTime<Utc>> {
        Utc.timestamp_opt(
            meta::fetch(&self.pool, "disable_complete_data_checks_until").await?,
            0,
        )
        .single()
        .ok_or(db_store::Error::DecodeError)
    }
}
pub async fn reward_poc_and_dc(
    pool: &Pool<Postgres>,
    rewards_sink: &file_sink::FileSinkClient<proto::IotRewardShare>,
    reward_info: &EpochRewardInfo,
    price_info: PriceInfo,
) -> anyhow::Result<RewardPocDcDataPoints> {
    let reward_shares =
        reward_share::aggregate_reward_shares(pool, &reward_info.epoch_period).await?;
    let gateway_shares = GatewayShares::new(reward_shares)?;
    let (beacon_rewards_per_share, witness_rewards_per_share, dc_transfer_rewards_per_share) =
        gateway_shares
            .calculate_rewards_per_share(reward_info.epoch_emissions, price_info)
            .await?;

    // get the total poc and dc rewards for the period
    let (total_beacon_rewards, total_witness_rewards) =
        reward_share::get_scheduled_poc_tokens(reward_info.epoch_emissions, dec!(0.0));
    let total_dc_rewards = reward_share::get_scheduled_dc_tokens(reward_info.epoch_emissions);
    let total_poc_dc_reward_allocation =
        total_beacon_rewards + total_witness_rewards + total_dc_rewards;

    let mut allocated_gateway_rewards = 0_u64;
    for (gateway_reward_amount, reward_share) in gateway_shares.into_reward_shares(
        &reward_info.epoch_period,
        beacon_rewards_per_share,
        witness_rewards_per_share,
        dc_transfer_rewards_per_share,
    ) {
        rewards_sink
            .write(reward_share, [])
            .await?
            // Await the returned oneshot to ensure we wrote the file
            .await??;
        allocated_gateway_rewards += gateway_reward_amount;
    }
    // write out any unallocated poc reward
    let unallocated_poc_reward_amount = (total_poc_dc_reward_allocation
        - Decimal::from(allocated_gateway_rewards))
    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
    .to_u64()
    .unwrap_or(0);
    write_unallocated_reward(
        rewards_sink,
        UnallocatedRewardType::Poc,
        unallocated_poc_reward_amount,
        &reward_info.epoch_period,
    )
    .await?;
    Ok(RewardPocDcDataPoints {
        beacon_rewards_per_share,
        witness_rewards_per_share,
        dc_transfer_rewards_per_share,
    })
}

pub async fn reward_operational(
    rewards_sink: &file_sink::FileSinkClient<proto::IotRewardShare>,
    reward_info: &EpochRewardInfo,
) -> anyhow::Result<()> {
    let total_operational_rewards =
        reward_share::get_scheduled_ops_fund_tokens(reward_info.epoch_emissions);
    let allocated_operational_rewards = total_operational_rewards
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
    let op_fund_reward = proto::OperationalReward {
        amount: allocated_operational_rewards,
    };
    rewards_sink
        .write(
            proto::IotRewardShare {
                start_period: reward_info.epoch_period.start.encode_timestamp(),
                end_period: reward_info.epoch_period.end.encode_timestamp(),
                reward: Some(ProtoReward::OperationalReward(op_fund_reward)),
            },
            [],
        )
        .await?
        .await??;
    // write out any unallocated operation rewards
    // which for the operational fund can only relate to rounding issue
    // in practice this should always be zero as there can be a max of
    // one bone lost due to rounding when going from decimal to u64
    // but we run it anyway and if it is indeed zero nothing gets
    // written out anyway
    let unallocated_operation_reward_amount = (total_operational_rewards
        - Decimal::from(allocated_operational_rewards))
    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
    .to_u64()
    .unwrap_or(0);
    write_unallocated_reward(
        rewards_sink,
        UnallocatedRewardType::Operation,
        unallocated_operation_reward_amount,
        &reward_info.epoch_period,
    )
    .await?;
    Ok(())
}

pub async fn reward_oracles(
    rewards_sink: &file_sink::FileSinkClient<proto::IotRewardShare>,
    reward_info: &EpochRewardInfo,
) -> anyhow::Result<()> {
    // atm 100% of oracle rewards are assigned to 'unallocated'
    let total_oracle_rewards =
        reward_share::get_scheduled_oracle_tokens(reward_info.epoch_emissions);
    let allocated_oracle_rewards = 0_u64;
    let unallocated_oracle_reward_amount = (total_oracle_rewards
        - Decimal::from(allocated_oracle_rewards))
    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
    .to_u64()
    .unwrap_or(0);
    write_unallocated_reward(
        rewards_sink,
        UnallocatedRewardType::Oracle,
        unallocated_oracle_reward_amount,
        &reward_info.epoch_period,
    )
    .await?;
    Ok(())
}

async fn write_unallocated_reward(
    rewards_sink: &file_sink::FileSinkClient<proto::IotRewardShare>,
    unallocated_type: UnallocatedRewardType,
    unallocated_amount: u64,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    if unallocated_amount > 0 {
        let unallocated_reward = proto::IotRewardShare {
            start_period: reward_period.start.encode_timestamp(),
            end_period: reward_period.end.encode_timestamp(),
            reward: Some(ProtoReward::UnallocatedReward(UnallocatedReward {
                reward_type: unallocated_type as i32,
                amount: unallocated_amount,
            })),
        };
        rewards_sink.write(unallocated_reward, []).await?.await??;
    };
    Ok(())
}
pub async fn next_reward_epoch(db: &Pool<Postgres>) -> db_store::Result<u64> {
    meta::fetch(db, "next_reward_epoch").await
}

async fn save_next_reward_epoch(exec: impl PgExecutor<'_>, value: u64) -> db_store::Result<()> {
    meta::store(exec, "next_reward_epoch", value).await
}
