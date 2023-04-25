use crate::{
    reward_share::{operational_rewards, GatewayShares},
    scheduler::Scheduler,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, traits::TimestampEncode};
use helium_proto::RewardManifest;
use price::PriceTracker;
use rust_decimal::prelude::*;
use sqlx::{PgExecutor, Pool, Postgres};
use tokio::time::sleep;

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

            if scheduler.should_reward(now) {
                let iot_price = price_tracker
                    .price(&helium_proto::BlockchainTokenTypeV1::Iot)
                    .await?;
                tracing::info!(
                    "Rewarding for period: {:?} with iot_price: {iot_price}",
                    scheduler.reward_period
                );
                self.reward(&scheduler, Decimal::from(iot_price)).await?
            }

            let sleep_duration = scheduler.sleep_duration(Utc::now())?;

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

        Ok(())
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
