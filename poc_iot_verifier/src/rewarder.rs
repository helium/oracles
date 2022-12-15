use crate::{reward_share::GatewayShares, scheduler::Scheduler};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, file_sink_write, traits::TimestampEncode};
use helium_proto::RewardManifest;
use sqlx::{PgExecutor, Pool, Postgres};
use tokio::time::sleep;

pub struct Rewarder {
    pub pool: Pool<Postgres>,
    pub gateway_rewards_tx: file_sink::MessageSender,
    pub reward_manifest_tx: file_sink::MessageSender,
    pub reward_period_hours: i64,
    pub reward_offset: Duration,
}

impl Rewarder {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
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
                tracing::info!("Rewarding for period: {:?}", scheduler.reward_period);
                self.reward(&scheduler).await?
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

    pub async fn reward(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
        let reward_shares = GatewayShares::aggregate(&self.pool, &scheduler.reward_period).await?;
        for reward_share in reward_shares.into_gateway_reward_shares(&scheduler.reward_period) {
            file_sink_write!(
                "gateway_reward_shares",
                &self.gateway_rewards_tx,
                reward_share
            )
            .await?
            // Await the returned oneshot to ensure we wrote the file
            .await??;
        }

        let written_files = file_sink::commit(&self.gateway_rewards_tx).await?.await??;

        // Write the rewards manifest for the completed period
        file_sink_write!(
            "iot_reward_manifest",
            &self.reward_manifest_tx,
            RewardManifest {
                start_timestamp: scheduler.reward_period.start.encode_timestamp(),
                end_timestamp: scheduler.reward_period.end.encode_timestamp(),
                written_files
            }
        )
        .await?
        .await??;

        file_sink::commit(&self.reward_manifest_tx).await?;

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

        Ok(())
    }
}

async fn fetch_rewarded_timestamp(
    timestamp_key: &str,
    db: impl PgExecutor<'_>,
) -> db_store::Result<DateTime<Utc>> {
    Ok(Utc.timestamp(meta::fetch(db, timestamp_key).await?, 0))
}

async fn save_rewarded_timestamp(
    timestamp_key: &str,
    value: &DateTime<Utc>,
    db: impl PgExecutor<'_>,
) -> db_store::Result<()> {
    meta::store(db, timestamp_key, value.timestamp()).await
}
