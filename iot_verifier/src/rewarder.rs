use crate::{
    reward_share::{self, GatewayShares},
    telemetry,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, traits::TimestampEncode};
use futures::future::LocalBoxFuture;
use helium_proto::services::poc_lora as proto;
use helium_proto::services::poc_lora::iot_reward_share::Reward as ProtoReward;
use helium_proto::services::poc_lora::{UnallocatedReward, UnallocatedRewardType};
use helium_proto::RewardManifest;
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::prelude::*;
use sqlx::{PgExecutor, PgPool, Pool, Postgres};
use std::ops::Range;
use task_manager::ManagedTask;
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder {
    pub pool: Pool<Postgres>,
    pub rewards_sink: file_sink::FileSinkClient,
    pub reward_manifests_sink: file_sink::FileSinkClient,
    pub reward_period_hours: i64,
    pub reward_offset: Duration,
    pub price_tracker: PriceTracker,
}

impl ManagedTask for Rewarder {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl Rewarder {
    pub async fn new(
        pool: PgPool,
        rewards_sink: file_sink::FileSinkClient,
        reward_manifests_sink: file_sink::FileSinkClient,
        reward_period_hours: i64,
        reward_offset: Duration,
        price_tracker: PriceTracker,
    ) -> Self {
        Self {
            pool,
            rewards_sink,
            reward_manifests_sink,
            reward_period_hours,
            reward_offset,
            price_tracker,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting rewarder");

        let reward_period_length = Duration::hours(self.reward_period_hours);

        loop {
            let now = Utc::now();

            let scheduler = Scheduler::new(
                reward_period_length,
                fetch_rewarded_timestamp("last_rewarded_end_time", &self.pool).await?,
                fetch_rewarded_timestamp("next_rewarded_end_time", &self.pool).await?,
                self.reward_offset,
            );

            let sleep_duration = if scheduler.should_reward(now) {
                let iot_price = self
                    .price_tracker
                    .price(&helium_proto::BlockchainTokenTypeV1::Iot)
                    .await?;
                tracing::info!(
                    "Rewarding for period: {:?} with iot_price: {iot_price}",
                    scheduler.reward_period
                );
                if self.data_current_check(&scheduler.reward_period).await? {
                    self.reward(&scheduler, Decimal::from(iot_price)).await?;
                    scheduler.sleep_duration(Utc::now())?
                } else {
                    tracing::info!(
                        "rewards will be retried in {REWARDS_NOT_CURRENT_DELAY_PERIOD} minutes:"
                    );
                    Duration::minutes(REWARDS_NOT_CURRENT_DELAY_PERIOD).to_std()?
                }
            } else {
                scheduler.sleep_duration(Utc::now())?
            };

            let shutdown = shutdown.clone();
            tokio::select! {
                biased;
                _ = shutdown => break,
                _ = sleep(sleep_duration) => (),
            }
        }
        tracing::info!("stopping rewarder");
        Ok(())
    }

    pub async fn reward(
        &mut self,
        scheduler: &Scheduler,
        iot_price: Decimal,
    ) -> anyhow::Result<()> {
        let reward_period = &scheduler.reward_period;

        // process rewards for poc and dc
        reward_poc_and_dc(&self.pool, &self.rewards_sink, reward_period, iot_price).await?;
        // process rewards for the operational fund
        reward_operational(&self.rewards_sink, reward_period).await?;
        // process rewards for the oracle
        reward_oracles(&self.rewards_sink, reward_period).await?;

        // commit the filesink
        let written_files = self.rewards_sink.commit().await?.await??;

        // purge db
        let mut transaction = self.pool.begin().await?;
        // Clear gateway shares table period to end of reward period
        GatewayShares::clear_rewarded_shares(&mut transaction, scheduler.reward_period.start)
            .await?;
        save_rewarded_timestamp(
            "last_rewarded_end_time",
            &scheduler.reward_period.end,
            &mut transaction,
        )
        .await?;
        save_rewarded_timestamp(
            "next_rewarded_end_time",
            &scheduler.next_reward_period().end,
            &mut transaction,
        )
        .await?;
        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        self.reward_manifests_sink
            .write(
                RewardManifest {
                    start_timestamp: scheduler.reward_period.start.encode_timestamp(),
                    end_timestamp: scheduler.reward_period.end.encode_timestamp(),
                    written_files,
                },
                [],
            )
            .await?
            .await??;
        self.reward_manifests_sink.commit().await?;
        telemetry::last_rewarded_end_time(scheduler.reward_period.end);
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
    rewards_sink: &file_sink::FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
    iot_price: Decimal,
) -> anyhow::Result<()> {
    // aggregate the poc and dc data per gateway
    let mut gateway_reward_shares = GatewayShares::aggregate(pool, reward_period).await?;

    // work out rewards per share and sum up total usage for poc and dc
    gateway_reward_shares.calculate_rewards_per_share_and_total_usage(reward_period, iot_price);
    let total_gateway_reward_allocation = gateway_reward_shares.total_rewards_for_poc_and_dc;

    let mut allocated_gateway_rewards = 0_u64;
    for (gateway_reward_amount, reward_share) in
        gateway_reward_shares.into_iot_reward_shares(reward_period)
    {
        rewards_sink
            .write(reward_share, [])
            .await?
            // Await the returned oneshot to ensure we wrote the file
            .await??;
        allocated_gateway_rewards += gateway_reward_amount;
    }
    // write out any unallocated poc reward
    let unallocated_poc_reward_amount = (total_gateway_reward_allocation
        - Decimal::from(allocated_gateway_rewards))
    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
    .to_u64()
    .unwrap_or(0);
    write_unallocated_reward(
        rewards_sink,
        UnallocatedRewardType::Poc,
        unallocated_poc_reward_amount,
        reward_period,
    )
    .await?;
    Ok(())
}

pub async fn reward_operational(
    rewards_sink: &file_sink::FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    let total_operational_rewards =
        reward_share::get_scheduled_ops_fund_tokens(reward_period.end - reward_period.start);
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
                start_period: reward_period.start.encode_timestamp(),
                end_period: reward_period.end.encode_timestamp(),
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
        reward_period,
    )
    .await?;
    Ok(())
}

pub async fn reward_oracles(
    rewards_sink: &file_sink::FileSinkClient,
    reward_period: &Range<DateTime<Utc>>,
) -> anyhow::Result<()> {
    // atm 100% of oracle rewards are assigned to 'unallocated'
    let total_oracle_rewards =
        reward_share::get_scheduled_oracle_tokens(reward_period.end - reward_period.start);
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
        reward_period,
    )
    .await?;
    Ok(())
}

async fn write_unallocated_reward(
    rewards_sink: &file_sink::FileSinkClient,
    unallocated_type: UnallocatedRewardType,
    unallocated_amount: u64,
    reward_period: &'_ Range<DateTime<Utc>>,
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

pub async fn fetch_rewarded_timestamp(
    timestamp_key: &str,
    db: impl PgExecutor<'_>,
) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(db, timestamp_key).await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_rewarded_timestamp(
    timestamp_key: &str,
    value: &DateTime<Utc>,
    db: impl PgExecutor<'_>,
) -> db_store::Result<()> {
    meta::store(db, timestamp_key, value.timestamp()).await
}
