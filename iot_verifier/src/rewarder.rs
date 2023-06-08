use crate::reward_share::{operational_rewards, GatewayShares};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, traits::TimestampEncode};
use helium_proto::RewardManifest;
use price::PriceTracker;
use reward_scheduler::Scheduler;
use rust_decimal::prelude::*;
use sqlx::{PgExecutor, Pool, Postgres};
use std::ops::Range;
use tokio::time::sleep;

const REWARDS_NOT_CURRENT_DELAY_PERIOD: i64 = 5;

pub struct Rewarder {
    pub pool: Pool<Postgres>,
    pub rewards_sink: file_sink::FileSinkClient,
    pub reward_manifests_sink: file_sink::FileSinkClient,
    pub reward_period_hours: i64,
    pub reward_offset: Duration,
}

impl Rewarder {
    pub async fn run(
        mut self,
        price_tracker: PriceTracker,
        shutdown: &triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("Starting iot verifier rewarder");

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
                let iot_price = price_tracker
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
    ) -> anyhow::Result<()> {
        let gateway_reward_shares =
            GatewayShares::aggregate(&self.pool, &scheduler.reward_period).await?;

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
        metrics::gauge!(
            "last_rewarded_end_time",
            scheduler.reward_period.end.timestamp() as f64
        );
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
