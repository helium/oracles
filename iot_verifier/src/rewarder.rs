use std::time::Duration;

use crate::{
    reward_share::{operational_rewards, GatewayShares},
    scheduler::Scheduler,
};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, traits::TimestampEncode};
use helium_proto::RewardManifest;

use price::PriceTracker;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use tokio::time::sleep;

const HALT_REWARDS_DB_KEY: &str = "halt_rewards_override";
// set min counts for the number of beacon, witnesses and packets we expect
// to be within a rewardable period
// note: these are floor levels, the averages are expected to be higher
//       but if we drop below the levels defined here, rewards will be delayed
//       enabling a manual review
const BEACON_MIN_REWARDABLE_COUNT: u64 = 800_000; // 200K gateways @ 4 beacons per day
const WITNESS_MIN_REWARDABLE_COUNT: u64 = 4_000_000; // 5 witnesses per above beacon
const PACKET_MIN_REWARDABLE_COUNT: u64 = 100_000; // TODO: work out a sensible packet floor level
const AVG_SCALING_FACTOR: Decimal = dec!(0.8);

pub struct Rewarder {
    pub pool: Pool<Postgres>,
    pub rewards_sink: file_sink::FileSinkClient,
    pub reward_manifests_sink: file_sink::FileSinkClient,
    pub reward_period_hours: i64,
    pub reward_offset: ChronoDuration,
    pub rewards_retry_interval: Duration,
    pub rewards_max_delay_duration: ChronoDuration,
}

#[derive(sqlx::Type, Debug, Clone, PartialEq, Eq, Hash)]
#[sqlx(type_name = "reward_type", rename_all = "snake_case")]
pub enum RewardType {
    Beacon,
    Witness,
    Packet,
}

impl Rewarder {
    pub async fn run(
        mut self,
        price_tracker: PriceTracker,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("Starting iot verifier rewarder");

        let reward_period_length = ChronoDuration::hours(self.reward_period_hours);

        loop {
            let now = Utc::now();

            let scheduler = Scheduler::new(
                reward_period_length,
                fetch_rewarded_timestamp("last_rewarded_end_time", &self.pool).await?,
                fetch_rewarded_timestamp("next_rewarded_end_time", &self.pool).await?,
                self.reward_offset,
            );

            let sleep_duration = if scheduler.should_reward(now) {
                let iot_price = price_tracker
                    .price(&helium_proto::BlockchainTokenTypeV1::Iot)
                    .await?;
                tracing::info!(
                    "Rewarding for period: {:?} with iot_price: {iot_price}",
                    scheduler.reward_period
                );
                self.reward(&scheduler, Decimal::from(iot_price)).await?
            } else {
                scheduler.sleep_until_next_epoch(Utc::now())?
            };

            tracing::info!(
                "Sleeping for {}",
                humantime::format_duration(sleep_duration)
            );

            let shutdown = shutdown.clone();
            tokio::select! {
                _ = shutdown => return Ok(()),
                _ = sleep(sleep_duration) => (),
            }
        }
    }

    pub async fn reward(
        &mut self,
        scheduler: &Scheduler,
        iot_price: Decimal,
    ) -> anyhow::Result<Duration> {
        let gateway_reward_shares =
            GatewayShares::aggregate(&self.pool, &scheduler.reward_period).await?;
        // get a count of our summed shares by reward type
        let GatewayShares {
            total_beacon_count,
            total_packet_count,
            total_witness_count,
            ..
        } = gateway_reward_shares;

        if !self
            .threshold_check(&gateway_reward_shares, scheduler)
            .await?
        {
            return Ok(self.rewards_retry_interval);
        }
        for reward_share in
            gateway_reward_shares.into_iot_reward_shares(&scheduler.reward_period, iot_price)
        {
            self.rewards_sink
                .write(reward_share, [])
                .await?
                // Await the returned oneshot to ensure we wrote the file
                .await??;
        }

        self.rewards_sink
            .write(operational_rewards::compute(&scheduler.reward_period), [])
            .await?
            // Await the returned oneshot to ensure we wrote the file
            .await??;
        let written_files = self.rewards_sink.commit().await?.await??;

        let mut transaction = self.pool.begin().await?;
        // Clear gateway shares table period to end of reward period
        GatewayShares::clear_rewarded_shares(&mut transaction, scheduler.reward_period.end).await?;
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

        clear_halt_rewards_override(&mut transaction).await?;

        save_history(
            &mut transaction,
            total_beacon_count,
            total_witness_count,
            total_packet_count,
            scheduler.reward_period.end,
        )
        .await?;

        history_db::purge(&mut transaction).await?;

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
        let sleep_duration = scheduler.sleep_until_next_epoch(Utc::now())?;
        Ok(sleep_duration)
    }

    async fn threshold_check(
        &self,
        gateway_reward_shares: &GatewayShares,
        scheduler: &Scheduler,
    ) -> anyhow::Result<bool> {
        let duration_since_last_rewards_end =
            scheduler.time_since_last_reward_period_ended(Utc::now());
        let halt_rewards_override = fetch_halt_rewards_override(&self.pool).await?;
        // get the current averages from historic data in the db
        let (scaled_beacon_avg, scaled_witness_avg, scaled_packet_avg) =
            self.get_historic_averages().await?;

        tracing::info!(%gateway_reward_shares.total_beacon_count,
            %gateway_reward_shares.total_witness_count,
            %gateway_reward_shares.total_packet_count,
            %scaled_beacon_avg,
            %scaled_witness_avg,
            %scaled_packet_avg,
            %halt_rewards_override);

        // if we are above our thesholds then we are good to reward
        if gateway_reward_shares.total_beacon_count >= scaled_beacon_avg
            && gateway_reward_shares.total_witness_count >= scaled_witness_avg
            && gateway_reward_shares.total_packet_count >= scaled_packet_avg
        {
            return Ok(true);
        }
        // we are below our thresholds
        // fail this check until a max duration after when the rewards
        // were meant to be triggered, unless the override has been set
        // if override is set then we will keep failing until it is cleared
        if duration_since_last_rewards_end > self.rewards_max_delay_duration
            && !halt_rewards_override
        {
            return Ok(true);
        }
        tracing::warn!(
            "rewards failed threshold check.  rewarding for this epoch has been suspended..."
        );
        Ok(false)
    }

    async fn get_historic_averages(&self) -> anyhow::Result<(u64, u64, u64)> {
        let beacon_avg =
            history_db::get_avg(&self.pool, RewardType::Beacon, BEACON_MIN_REWARDABLE_COUNT)
                .await?;
        let witness_avg = history_db::get_avg(
            &self.pool,
            RewardType::Witness,
            WITNESS_MIN_REWARDABLE_COUNT,
        )
        .await?;
        let packet_avg =
            history_db::get_avg(&self.pool, RewardType::Packet, PACKET_MIN_REWARDABLE_COUNT)
                .await?;
        Ok((beacon_avg, witness_avg, packet_avg))
    }
}

async fn fetch_rewarded_timestamp(
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

pub async fn fetch_halt_rewards_override(db: impl sqlx::PgExecutor<'_>) -> db_store::Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
    SELECT EXISTS(SELECT value from meta where key = $1 and value = 'true')
    "#,
    )
    .bind(HALT_REWARDS_DB_KEY)
    .fetch_one(db)
    .await?)
}

async fn clear_halt_rewards_override(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> db_store::Result<()> {
    meta::store(txn, HALT_REWARDS_DB_KEY, "false").await
}

async fn save_history(
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    total_beacon_count: u64,
    total_witness_count: u64,
    total_packet_count: u64,
    epoch_end: DateTime<Utc>,
) -> anyhow::Result<()> {
    history_db::insert(
        txn,
        RewardType::Beacon,
        total_beacon_count as i64,
        epoch_end,
    )
    .await?;
    history_db::insert(
        txn,
        RewardType::Witness,
        total_witness_count as i64,
        epoch_end,
    )
    .await?;
    history_db::insert(
        txn,
        RewardType::Packet,
        total_packet_count as i64,
        epoch_end,
    )
    .await?;
    Ok(())
}

mod history_db {
    use super::*;

    pub async fn insert(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        reward_type: RewardType,
        count: i64,
        epoch_ts: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
        INSERT INTO rewards_history(reward_type, count, epoch_ts) VALUES($1, $2, $3)
        "#,
        )
        .bind(reward_type)
        .bind(count)
        .bind(epoch_ts)
        .execute(tx)
        .await?;

        Ok(())
    }

    pub async fn get_avg(
        db: impl sqlx::PgExecutor<'_>,
        reward_type: RewardType,
        default: u64,
    ) -> anyhow::Result<u64> {
        let result = sqlx::query_scalar::<_, Decimal>(
            r#"
        SELECT COALESCE(AVG(count), 0) FROM rewards_history where reward_type = $1
        "#,
        )
        .bind(reward_type)
        .fetch_one(db)
        .await?;
        let result = (result * AVG_SCALING_FACTOR).to_u64().unwrap_or(default);
        if result < default {
            Ok(default)
        } else {
            Ok(result)
        }
    }
    pub async fn purge(db: impl sqlx::PgExecutor<'_>) -> anyhow::Result<()> {
        //TODO make max history period a setting?
        sqlx::query(
            r#"
        DELETE FROM rewards_history where created_at <  NOW() - INTERVAL '90 DAYS'
        "#,
        )
        .execute(db)
        .await?;

        Ok(())
    }
}
