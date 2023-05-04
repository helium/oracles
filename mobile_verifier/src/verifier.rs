use crate::{
    heartbeats::{Heartbeat, HeartbeatReward},
    ingest,
    reward_shares::{PocShares, TransferRewards},
    speedtests::{SpeedtestAverages, SpeedtestRollingAverage},
};
use anyhow::bail;
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    heartbeat::CellHeartbeatIngestReport, speedtest::CellSpeedtestIngestReport,
    traits::TimestampEncode, FileStore,
};
use futures::StreamExt;
use helium_proto::RewardManifest;
use mobile_config::Client;
use price::PriceTracker;
use retainer::Cache;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{PgExecutor, Pool, Postgres};
use std::{
    ops::Range,
    pin::pin,
    sync::Arc,
    time
};
use tokio::{
    sync::mpsc::Receiver,
    task::{JoinError, JoinHandle},
    time::sleep,
};

pub struct VerifierDaemon {
    pub pool: Pool<Postgres>,
    pub heartbeats: Receiver<FileInfoStream<CellHeartbeatIngestReport>>,
    pub speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
    pub valid_heartbeats: FileSinkClient,
    pub valid_speedtests: FileSinkClient,
    pub mobile_rewards: FileSinkClient,
    pub reward_manifests: FileSinkClient,
    pub data_transfer_ingest: FileStore,
    pub reward_period_hours: i64,
    pub config_client: Client,
    pub price_tracker: PriceTracker,
}

impl VerifierDaemon {
    pub async fn run(self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting verifier service");

        let Self {
            pool,
            mut heartbeats,
            mut speedtests,
            valid_heartbeats,
            valid_speedtests,
            config_client,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            data_transfer_ingest,
            reward_period_hours,
        } = self;


        let heartbeat_pool = pool.clone();
        let heartbeat_config_client = config_client.clone();
        let mut heartbeat_verification_task: JoinHandle<anyhow::Result<()>> =
            tokio::spawn(async move {
                let heartbeat_cache = Arc::new(Cache::<(String, DateTime<Utc>), ()>::new());

                let cache_clone = heartbeat_cache.clone();
                tokio::spawn(async move {
                    cache_clone
                        .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 3))
                        .await
                });

                loop {
                    let Some(file) = heartbeats.recv().await else {
                        bail!("Heartbeat report file stream was dropped unexpectedly");
                    };
                    let epoch =
                        (file.file_info.timestamp - Duration::hours(1))..file.file_info.timestamp;
                    let mut transaction = heartbeat_pool.begin().await?;
                    let reports = file.into_stream(&mut transaction).await?;

                    let mut validated_heartbeats = pin!(
                        Heartbeat::validate_heartbeats(&heartbeat_config_client, reports, &epoch)
                            .await
                    );

                    while let Some(heartbeat) = validated_heartbeats.next().await.transpose()? {
                        heartbeat.write(&valid_heartbeats).await?;
                        let key = (heartbeat.cbsd_id.clone(), heartbeat.truncated_timestamp()?);
                        if heartbeat_cache.get(&key).await.is_none() {
                            heartbeat.save(&mut transaction).await?;
                            heartbeat_cache
                                .insert(key, (), time::Duration::from_secs(60 * 60 * 2))
                                .await;
                        }
                    }

                    transaction.commit().await?;
                }
            });

        let speedtest_pool = pool.clone();
        let speedtest_config_client = config_client.clone();
        let mut speedtest_verification_task: JoinHandle<anyhow::Result<()>> =
            tokio::spawn(async move {
                loop {
                    let Some(file) = speedtests.recv().await else {
                        bail!("Speedtest report file stream was dropped unexpectedly");
                    };
                    let mut transaction = speedtest_pool.begin().await?;
                    let reports = file.into_stream(&mut transaction).await?;

                    let mut validated_speedtests = pin!(
                        SpeedtestRollingAverage::validate_speedtests(
                            &speedtest_config_client,
                            reports.map(|s| s.report),
                            &mut transaction,
                        )
                        .await?
                    );
                    while let Some(speedtest) = validated_speedtests.next().await.transpose()? {
                        speedtest.write(&valid_speedtests).await?;
                        speedtest.save(&mut transaction).await?;
                    }

                    transaction.commit().await?;
                }
            });

        let rewarder = Rewarder {
            pool,
            mobile_rewards,
            reward_manifests,
            price_tracker,
            data_transfer_ingest,
            reward_period_duration: Duration::hours(reward_period_hours),
        };

        loop {
            let last_rewarded_end_time = rewarder.last_rewarded_end_time().await?;
            let next_rewarded_end_time = rewarder.next_rewarded_end_time().await?;
            let now = Utc::now();
            let next_reward = if now > next_rewarded_end_time {
                Duration::zero()
            } else {
                next_rewarded_end_time - Utc::now()
            }.to_std()?;
            tracing::info!(
                "Next reward will be given in {}",
                humantime::format_duration(next_reward)
            );
            tokio::select! {
                _ = sleep(next_reward) =>
                    rewarder.reward(&(last_rewarded_end_time..next_rewarded_end_time)).await?,
                e = &mut heartbeat_verification_task => return flatten(e),
                e = &mut speedtest_verification_task => return flatten(e),
                _ = shutdown.clone() => {
                    break;
                }
            }
        }

        Ok(())
    }
}

fn flatten(j: Result<Result<(), anyhow::Error>, JoinError>) -> Result<(), anyhow::Error> {
    match j {
        Ok(e) => e,
        Err(e) => Err(anyhow::Error::from(e)),
    }
}

pub struct Rewarder {
    pool: Pool<Postgres>,
    reward_period_duration: Duration,
    mobile_rewards: FileSinkClient,
    reward_manifests: FileSinkClient,
    price_tracker: PriceTracker,
    data_transfer_ingest: FileStore,
}

impl Rewarder {
    async fn last_rewarded_end_time(&self) -> db_store::Result<DateTime<Utc>> {
        Utc.timestamp_opt(meta::fetch(&self.pool, "last_rewarded_end_time").await?, 0)
            .single()
            .ok_or(db_store::Error::DecodeError)
    }

    async fn next_rewarded_end_time(&self) -> db_store::Result<DateTime<Utc>> {
        Utc.timestamp_opt(meta::fetch(&self.pool, "next_rewarded_end_time").await?, 0)
            .single()
            .ok_or(db_store::Error::DecodeError)
    }

    pub async fn reward(&self, reward_period: &Range<DateTime<Utc>>) -> anyhow::Result<()> {
        let heartbeats = HeartbeatReward::validated(&self.pool, reward_period);
        let speedtests = SpeedtestAverages::validated(&self.pool, reward_period.end).await?;

        let poc_rewards = PocShares::aggregate(heartbeats, speedtests).await?;
        let mobile_price = self
            .price_tracker
            .price(&helium_proto::BlockchainTokenTypeV1::Mobile)
            .await?;
        // Mobile prices are supplied in 10^6, so we must convert them to Decimal
        let mobile_bone_price = Decimal::from(mobile_price)
                / dec!(1_000_000)  // Per Mobile token
                / dec!(1_000_000); // Per Bone
        let transfer_rewards = TransferRewards::from_transfer_sessions(
            mobile_bone_price,
            ingest::ingest_valid_data_transfers(&self.data_transfer_ingest, reward_period).await,
            &poc_rewards,
            reward_period,
        )
        .await;

        // It's important to gauge the scale metric. If this value is < 1.0, we are in
        // big trouble.
        let Some(scale) = transfer_rewards.reward_scale().to_f64() else {
            bail!("The data transfer rewards scale cannot be converted to a float");
        };
        metrics::gauge!("data_transfer_rewards_scale", scale);

        for mobile_reward_share in poc_rewards.into_rewards(&transfer_rewards, reward_period) {
            self.mobile_rewards
                .write(mobile_reward_share, [])
                .await?
                // Await the returned one shot to ensure that we wrote the file
                .await??;
        }

        let written_files = self.mobile_rewards.commit().await?.await??;

        let mut transaction = self.pool.begin().await?;

        // Clear the heartbeats of old heartbeats:
        sqlx::query("DELETE FROM TABLE heartbeats WHERE truncated_timestamp < $3")
            .bind(reward_period.end)
            .execute(&mut transaction)
            .await?;

        let next_reward_period = reward_period.end + self.reward_period_duration;
        save_last_rewarded_end_time(&mut transaction, &reward_period.end).await?;
        save_next_rewarded_end_time(&mut transaction, &next_reward_period).await?;
        transaction.commit().await?;

        // now that the db has been purged, safe to write out the manifest
        self.reward_manifests
            .write(
                RewardManifest {
                    start_timestamp: reward_period.start.encode_timestamp(),
                    end_timestamp: reward_period.end.encode_timestamp(),
                    written_files,
                },
                [],
            )
            .await?
            .await??;

        self.reward_manifests.commit().await?;
        Ok(())
    }
}

async fn save_last_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "last_rewarded_end_time", value.timestamp()).await
}

async fn save_next_rewarded_end_time(
    exec: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(exec, "next_rewarded_end_time", value.timestamp()).await
}
