use crate::{
    reward_share::{self, GatewayShares},
    telemetry,
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
    IotRewardData as ManifestIotRewardData, RewardManifest,
};
use humantime_serde::re::humantime;
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, PgPool, Pool, Postgres};
use std::{ops::Range, time::Duration};
use task_manager::ManagedTask;
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: Duration = Duration::from_secs(5 * 60);

pub struct Rewarder {
    pub pool: Pool<Postgres>,
    pub rewards_sink: file_sink::FileSinkClient<proto::IotRewardShare>,
    pub reward_manifests_sink: file_sink::FileSinkClient<RewardManifest>,
    pub reward_period_hours: Duration,
    pub reward_offset: Duration,
    pub price_tracker: PriceTracker,
}

pub struct RewardPocDcDataPoints {
    beacon_rewards_per_share: Decimal,
    witness_rewards_per_share: Decimal,
    dc_transfer_rewards_per_share: Decimal,
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
        rewards_sink: file_sink::FileSinkClient<proto::IotRewardShare>,
        reward_manifests_sink: file_sink::FileSinkClient<RewardManifest>,
        reward_period_hours: Duration,
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

        let reward_period_length = self.reward_period_hours;

        loop {
            let now = Utc::now();

            let scheduler = Scheduler::new(
                reward_period_length,
                fetch_rewarded_timestamp("last_rewarded_end_time", &self.pool).await?,
                fetch_rewarded_timestamp("next_rewarded_end_time", &self.pool).await?,
                self.reward_offset,
            );

            let sleep_duration = if scheduler.should_trigger(now) {
                let iot_price = self
                    .price_tracker
                    .price(&helium_proto::BlockchainTokenTypeV1::Iot)
                    .await?;
                tracing::info!(
                    "Rewarding for period: {:?} with iot_price: {iot_price}",
                    scheduler.schedule_period
                );
                if self.data_current_check(&scheduler.schedule_period).await? {
                    self.reward(&scheduler, Decimal::from(iot_price)).await?;
                    scheduler.sleep_duration(Utc::now())?
                } else {
                    tracing::info!(
                        "rewards will be retried in {}",
                        humantime::format_duration(REWARDS_NOT_CURRENT_DELAY_PERIOD)
                    );
                    REWARDS_NOT_CURRENT_DELAY_PERIOD
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
        let reward_period = &scheduler.schedule_period;

        // process rewards for poc and dc
        let poc_dc_shares =
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
        GatewayShares::clear_rewarded_shares(&mut transaction, scheduler.schedule_period.start)
            .await?;
        save_rewarded_timestamp(
            "last_rewarded_end_time",
            &scheduler.schedule_period.end,
            &mut transaction,
        )
        .await?;
        save_rewarded_timestamp(
            "next_rewarded_end_time",
            &scheduler.next_trigger_period().end,
            &mut transaction,
        )
        .await?;
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
        };
        self.reward_manifests_sink
            .write(
                RewardManifest {
                    start_timestamp: scheduler.schedule_period.start.encode_timestamp(),
                    end_timestamp: scheduler.schedule_period.end.encode_timestamp(),
                    written_files,
                    reward_data: Some(IotRewardData(reward_data)),
                    epoch: 0, // TODO: replace placeholder value
                    price: 0, // TODO: replace placeholder value
                },
                [],
            )
            .await?
            .await??;
        self.reward_manifests_sink.commit().await?;
        telemetry::last_rewarded_end_time(scheduler.schedule_period.end);
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
    reward_period: &Range<DateTime<Utc>>,
    iot_price: Decimal,
) -> anyhow::Result<RewardPocDcDataPoints> {
    let reward_shares = reward_share::aggregate_reward_shares(pool, reward_period).await?;
    let gateway_shares = GatewayShares::new(reward_shares)?;
    let (beacon_rewards_per_share, witness_rewards_per_share, dc_transfer_rewards_per_share) =
        gateway_shares
            .calculate_rewards_per_share(reward_period, iot_price)
            .await?;

    // get the total poc and dc rewards for the period
    let (total_beacon_rewards, total_witness_rewards) =
        reward_share::get_scheduled_poc_tokens(reward_period.end - reward_period.start, dec!(0.0));
    let total_dc_rewards =
        reward_share::get_scheduled_dc_tokens(reward_period.end - reward_period.start);
    let total_poc_dc_reward_allocation =
        total_beacon_rewards + total_witness_rewards + total_dc_rewards;

    let mut allocated_gateway_rewards = 0_u64;
    for (gateway_reward_amount, reward_share) in gateway_shares.into_iot_reward_shares(
        reward_period,
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
        reward_period,
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
    rewards_sink: &file_sink::FileSinkClient<proto::IotRewardShare>,
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
    rewards_sink: &file_sink::FileSinkClient<proto::IotRewardShare>,
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
