use crate::speedtests_average::{SpeedtestAverage, SPEEDTEST_LAPSE};
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
};
use futures::{
    stream::{StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    SpeedtestIngestReportV1, SpeedtestVerificationResult,
    VerifiedSpeedtest as VerifiedSpeedtestProto,
};
use mobile_config::{gateway_info::GatewayInfoResolver, GatewayClient};
use sqlx::{postgres::PgRow, FromRow, Postgres, Row, Transaction};
use std::{collections::HashMap, time::Instant};
use tokio::sync::mpsc::Receiver;

const SPEEDTEST_AVG_MAX_DATA_POINTS: usize = 6;

pub type EpochSpeedTests = HashMap<PublicKeyBinary, Vec<Speedtest>>;

#[derive(Debug, Clone)]
pub struct Speedtest {
    pub report: CellSpeedtest,
}

impl FromRow<'_, PgRow> for Speedtest {
    fn from_row(row: &PgRow) -> sqlx::Result<Speedtest> {
        Ok(Self {
            report: CellSpeedtest {
                pubkey: row.get::<PublicKeyBinary, &str>("pubkey"),
                serial: row.get::<String, &str>("serial_num"),
                upload_speed: row.get::<i64, &str>("upload_speed") as u64,
                download_speed: row.get::<i64, &str>("download_speed") as u64,
                timestamp: row.get::<DateTime<Utc>, &str>("timestamp"),
                latency: row.get::<i32, &str>("latency") as u32,
            },
        })
    }
}

pub struct SpeedtestDaemon {
    pool: sqlx::Pool<sqlx::Postgres>,
    gateway_client: GatewayClient,
    speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
    speedtest_avg_file_sink: FileSinkClient,
    verified_speedtest_file_sink: FileSinkClient,
}

impl SpeedtestDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_client: GatewayClient,
        speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
        speedtest_avg_file_sink: FileSinkClient,
        verified_speedtest_file_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            gateway_client,
            speedtests,
            speedtest_avg_file_sink,
            verified_speedtest_file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.clone() => {
                        tracing::info!("SpeedtestDaemon shutting down");
                        break;
                    }
                    Some(file) = self.speedtests.recv() => {
			let start = Instant::now();
			self.process_file(file).await?;
			metrics::histogram!("speedtest_processing_time", Instant::now() - start);
                    }
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_file(
        &self,
        file: FileInfoStream<CellSpeedtestIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing speedtest file {}", file.file_info.key);
        let mut transaction = self.pool.begin().await?;
        let mut speedtests = file.into_stream(&mut transaction).await?;
        while let Some(speedtest_report) = speedtests.next().await {
            let result = self.validate_speedtest(&speedtest_report).await?;
            if result == SpeedtestVerificationResult::SpeedtestValid {
                save_speedtest(&speedtest_report.report, &mut transaction).await?;
                let latest_speedtests = get_latest_speedtests_for_pubkey(
                    &speedtest_report.report.pubkey,
                    &mut transaction,
                )
                .await?;
                let average = SpeedtestAverage::from(&latest_speedtests);
                average
                    .write(&self.speedtest_avg_file_sink, latest_speedtests)
                    .await?;
            }
            // write out paper trail of speedtest validity
            self.write_verified_speedtest(speedtest_report, result)
                .await?;
        }
        self.speedtest_avg_file_sink.commit().await?;
        self.verified_speedtest_file_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn validate_speedtest(
        &self,
        speedtest: &CellSpeedtestIngestReport,
    ) -> anyhow::Result<SpeedtestVerificationResult> {
        let pubkey = speedtest.report.pubkey.clone();
        if self
            .gateway_client
            .resolve_gateway_info(&pubkey)
            .await?
            .is_some()
        {
            Ok(SpeedtestVerificationResult::SpeedtestValid)
        } else {
            Ok(SpeedtestVerificationResult::SpeedtestGatewayNotFound)
        }
    }

    pub async fn write_verified_speedtest(
        &self,
        speedtest_report: CellSpeedtestIngestReport,
        result: SpeedtestVerificationResult,
    ) -> anyhow::Result<()> {
        let ingest_report: SpeedtestIngestReportV1 = speedtest_report.try_into()?;
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let proto = VerifiedSpeedtestProto {
            report: Some(ingest_report),
            result: result as i32,
            timestamp,
        };
        self.verified_speedtest_file_sink
            .write(proto, &[("result", result.as_str_name())])
            .await?;
        Ok(())
    }
}

pub async fn save_speedtest(
    speedtest: &CellSpeedtest,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into speedtests (pubkey, upload_speed, download_speed, latency, serial_num, timestamp)
        values ($1, $2, $3, $4, $5, $6)
        on conflict (pubkey, timestamp) do nothing
        "#,
    )
    .bind(&speedtest.pubkey)
    .bind(speedtest.upload_speed as i64)
    .bind(speedtest.download_speed as i64)
    .bind(speedtest.latency as i32)
    .bind(&speedtest.serial)
    .bind(speedtest.timestamp)
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn get_latest_speedtests_for_pubkey(
    pubkey: &PublicKeyBinary,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<Vec<Speedtest>, sqlx::Error> {
    let speedtests = sqlx::query_as::<_, Speedtest>(
        "SELECT * FROM speedtests where pubkey = $1 order by timestamp desc limit $2",
    )
    .bind(pubkey)
    .bind(SPEEDTEST_AVG_MAX_DATA_POINTS as i64)
    .fetch_all(exec)
    .await?;
    Ok(speedtests)
}

pub async fn aggregate_epoch_speedtests<'a>(
    epoch_end: DateTime<Utc>,
    exec: &sqlx::Pool<sqlx::Postgres>,
) -> Result<EpochSpeedTests, sqlx::Error> {
    let mut speedtests = EpochSpeedTests::new();
    // use latest speedtest which are no older than N hours, defined by SPEEDTEST_LAPSE
    let start = epoch_end - Duration::hours(SPEEDTEST_LAPSE);
    // pull the last N most recent speedtests from prior to the epoch end for each pubkey
    let mut rows = sqlx::query_as::<_, Speedtest>(
        "select * from (
            SELECT distinct(pubkey), upload_speed, download_speed, latency, timestamp, serial_num, row_number()
            over (partition by pubkey order by timestamp desc) as count FROM speedtests where timestamp >= $1 and timestamp < $2
        ) as tmp
        where count <= $3"
    )
    .bind(start)
    .bind(epoch_end)
    .bind(SPEEDTEST_AVG_MAX_DATA_POINTS as i64)
    .fetch(exec);
    // collate the returned speedtests based on pubkey
    while let Some(speedtest) = rows.try_next().await? {
        speedtests
            .entry(speedtest.report.pubkey.clone())
            .or_default()
            .push(speedtest);
    }
    Ok(speedtests)
}

pub async fn clear_speedtests(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    epoch_end: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let oldest_ts = *epoch_end - Duration::hours(SPEEDTEST_LAPSE);
    sqlx::query("DELETE FROM speedtests WHERE timestamp < $1")
        .bind(oldest_ts)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
