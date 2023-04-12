use crate::{
    heartbeats::{Heartbeat, Heartbeats},
    ingest,
    reward_shares::{PocShares, TransferRewards},
    scheduler::Scheduler,
    speedtests::{FetchError, SpeedtestAverages, SpeedtestRollingAverage, SpeedtestStore},
};
use anyhow::bail;
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode, FileStore};
use futures::{stream::Stream, StreamExt};
use helium_proto::RewardManifest;
use mobile_config::{client::ClientError, Client};
use price::PriceTracker;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use std::ops::Range;
use tokio::pin;
use tokio::time::sleep;

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub heartbeats: FileSinkClient,
    pub speedtest_avgs: FileSinkClient,
    pub radio_rewards: FileSinkClient,
    pub mobile_rewards: FileSinkClient,
    pub reward_manifests: FileSinkClient,
    pub reward_period_hours: i64,
    pub verifications_per_period: i32,
    pub verification_offset: Duration,
    pub verifier: Verifier,
    pub price_tracker: PriceTracker,
}

impl VerifierDaemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting verifier service");

        let reward_period_length = Duration::hours(self.reward_period_hours);
        let verification_period_length = reward_period_length / self.verifications_per_period;

        loop {
            let now = Utc::now();

            let scheduler = Scheduler::new(
                verification_period_length,
                reward_period_length,
                last_verified_end_time(&self.pool).await?,
                last_rewarded_end_time(&self.pool).await?,
                next_rewarded_end_time(&self.pool).await?,
                self.verification_offset,
            );

            if scheduler.should_verify(now) {
                tracing::info!("Verifying epoch: {:?}", scheduler.verification_period);
                self.verify(&scheduler).await?;
            }

            if scheduler.should_reward(now) {
                tracing::info!("Rewarding epoch: {:?}", scheduler.reward_period);
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

    pub async fn verify(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
        let VerifiedEpoch {
            heartbeats,
            speedtests,
        } = self
            .verifier
            .verify_epoch(&self.pool, &scheduler.verification_period)
            .await?;

        let mut transaction = self.pool.begin().await?;

        pin!(heartbeats);
        pin!(speedtests);

        // TODO: switch to a bulk transaction
        while let Some(heartbeat) = heartbeats.next().await.transpose()? {
            heartbeat.write(&self.heartbeats).await?;
            heartbeat.save(&mut transaction).await?;
        }

        while let Some(speedtest) = speedtests.next().await.transpose()? {
            speedtest.write(&self.speedtest_avgs).await?;
            speedtest.save(&mut transaction).await?;
        }

        save_last_verified_end_time(&mut transaction, &scheduler.verification_period.end).await?;
        transaction.commit().await?;

        Ok(())
    }

    pub async fn reward(&mut self, scheduler: &Scheduler) -> anyhow::Result<()> {
        let heartbeats = Heartbeats::validated(&self.pool).await?;
        let speedtests =
            SpeedtestAverages::validated(&self.pool, scheduler.reward_period.end).await?;

        let poc_rewards = PocShares::aggregate(heartbeats, speedtests).await;
        let mobile_price = self
            .price_tracker
            .price(&helium_proto::BlockchainTokenTypeV1::Mobile)
            .await?;
        // Mobile prices are supplied in 10^6, so we must convert them to Decimal
        let mobile_price = Decimal::from(mobile_price) / dec!(1_000_000);
        let transfer_rewards = TransferRewards::from_transfer_sessions(
            mobile_price,
            ingest::ingest_valid_data_transfers(
                &self.verifier.file_store,
                &scheduler.reward_period,
            )
            .await,
            &scheduler.reward_period,
        )
        .await;

        // It's important to gauge the scale metric. If this value is < 1.0, we are in
        // big trouble.
        let Some(scale) = transfer_rewards.reward_scale().to_f64() else {
            bail!("The data transfer rewards scale cannot be converted to a float");
        };
        metrics::gauge!("data_transfer_rewards_scale", scale);

        for (radio_reward_share, mobile_reward_share) in
            poc_rewards.into_rewards(&transfer_rewards, &scheduler.reward_period)
        {
            self.radio_rewards
                .write(radio_reward_share, [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
            self.mobile_rewards
                .write(mobile_reward_share, [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }

        let written_files = self.mobile_rewards.commit().await?.await??;

        // Write out the manifest file
        self.reward_manifests
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

        self.reward_manifests.commit().await?;

        let mut transaction = self.pool.begin().await?;

        // Clear the heartbeats table:
        sqlx::query("TRUNCATE TABLE heartbeats;")
            .execute(&mut transaction)
            .await?;

        save_last_rewarded_end_time(&mut transaction, &scheduler.reward_period.end).await?;
        save_next_rewarded_end_time(&mut transaction, &scheduler.next_reward_period().end).await?;

        transaction.commit().await?;

        Ok(())
    }
}

pub struct Verifier {
    pub config_client: Client,
    pub file_store: FileStore,
}

impl Verifier {
    pub fn new(config_client: Client, file_store: FileStore) -> Self {
        Self {
            config_client,
            file_store,
        }
    }

    pub async fn verify_epoch<'a>(
        &'a self,
        pool: impl SpeedtestStore + Copy + 'a,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> file_store::Result<
        VerifiedEpoch<
            impl Stream<Item = Result<Heartbeat, ClientError>> + 'a,
            impl Stream<Item = Result<SpeedtestRollingAverage, FetchError>> + 'a,
        >,
    > {
        let heartbeats = Heartbeat::validate_heartbeats(
            &self.config_client,
            ingest::ingest_heartbeats(&self.file_store, epoch).await,
            epoch,
        )
        .await;

        let speedtests = SpeedtestRollingAverage::validate_speedtests(
            &self.config_client,
            ingest::ingest_speedtests(&self.file_store, epoch).await,
            pool,
        )
        .await;

        Ok(VerifiedEpoch {
            heartbeats,
            speedtests,
        })
    }
}

pub struct VerifiedEpoch<H, S> {
    pub heartbeats: H,
    pub speedtests: S,
}

async fn last_verified_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(exec, "last_verified_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_last_verified_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_verified_end_time", value.timestamp()).await
}

async fn last_rewarded_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(exec, "last_rewarded_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_last_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_rewarded_end_time", value.timestamp()).await
}

async fn next_rewarded_end_time(exec: impl PgExecutor<'_>) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(exec, "next_rewarded_end_time").await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

async fn save_next_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "next_rewarded_end_time", value.timestamp()).await
}
