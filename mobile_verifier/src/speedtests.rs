use crate::speedtests_average::SpeedtestAverage;
use chrono::{DateTime, Utc};
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
use mobile_config::{gateway_info::GatewayInfoResolver, GatewayClient};
use sqlx::{postgres::PgRow, FromRow, Postgres, Row, Transaction};
use std::{collections::HashMap, ops::Range};
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
                serial: row.get::<String, &str>("serial"),
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
    file_sink: FileSinkClient,
}

impl SpeedtestDaemon {
    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        gateway_client: GatewayClient,
        speedtests: Receiver<FileInfoStream<CellSpeedtestIngestReport>>,
        file_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            gateway_client,
            speedtests,
            file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.clone() => {
                        tracing::info!("SpeedtestDaemon shutting down");
                        break;
                    }
                    Some(file) = self.speedtests.recv() => self.process_file(file).await?,
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
            let pubkey = speedtest_report.report.pubkey.clone();
            if self
                .gateway_client
                .resolve_gateway_info(&pubkey)
                .await
                .is_ok()
            {
                save_speedtest(&speedtest_report.report, &mut transaction).await?;
                let latest_speedtests =
                    get_latest_speedtests_for_pubkey(&pubkey, &mut transaction).await?;
                let average = SpeedtestAverage::from(&latest_speedtests);
                average.write(&self.file_sink, latest_speedtests).await?;
            }
        }
        self.file_sink.commit().await?;
        transaction.commit().await?;
        Ok(())
    }
}

pub async fn save_speedtest(
    speedtest: &CellSpeedtest,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into speedtests (pubkey, upload_speed, download_speed, latency, serial, timestamp)
        values ($1, $2, $3, $4, $5, $6)
        on conflict (pubkey, timestamp) do nothing
        "#,
    )
    .bind(speedtest.pubkey.clone())
    .bind(speedtest.upload_speed as i64)
    .bind(speedtest.download_speed as i64)
    .bind(speedtest.latency as i64)
    .bind(speedtest.serial.clone())
    .bind(speedtest.timestamp)
    .execute(exec)
    .await?;
    Ok(())
}

pub async fn get_latest_speedtests_for_pubkey(
    pubkey: &PublicKeyBinary,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<Vec<Speedtest>, sqlx::Error> {
    let mut speedtests = Vec::new();

    let mut rows = sqlx::query_as::<_, Speedtest>(
        "SELECT * FROM speedtests where pubkey = $1 order by timestamp desc limit $2",
    )
    .bind(pubkey)
    .bind(SPEEDTEST_AVG_MAX_DATA_POINTS as i64)
    .fetch(exec);

    while let Some(speedtest) = rows.try_next().await? {
        speedtests.push(speedtest);
    }
    Ok(speedtests)
}

pub async fn aggregate_epoch_speedtests<'a>(
    epoch_end: DateTime<Utc>,
    exec: &sqlx::Pool<sqlx::Postgres>,
) -> Result<EpochSpeedTests, sqlx::Error> {
    let mut speedtests = EpochSpeedTests::new();
    // pull the last N most recent speedtests from prior to the epoch end for each pubkey
    let mut rows = sqlx::query_as::<_, Speedtest>(
        "select * from (
            SELECT distinct(pubkey), upload_speed, download_speed, latency, timestamp, serial, row_number()
            over (partition by pubkey order by timestamp desc) as count FROM speedtests where timestamp < $1
        ) as tmp
        where count < $2"
    )
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
    reward_period: &Range<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM speedtests WHERE timestamp < $1")
        .bind(reward_period.start)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
