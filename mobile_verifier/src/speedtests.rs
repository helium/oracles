use crate::speedtests_average::SpeedtestAverage;
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream, file_sink::FileSinkClient,
    speedtest::CellSpeedtestIngestReport,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use mobile_config::{gateway_info::GatewayInfoResolver, GatewayClient};
use sqlx::{FromRow, Postgres, Transaction, Type};
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;

const SPEEDTEST_AVG_MAX_DATA_POINTS: usize = 6;

pub type EpochSpeedTests = HashMap<PublicKeyBinary, Vec<Speedtest>>;

#[derive(Debug, Clone, Type, FromRow)]
#[sqlx(type_name = "speedtest")]
pub struct Speedtest {
    pub pubkey: PublicKeyBinary,
    pub upload_speed: i64,
    pub download_speed: i64,
    pub latency: i32,
    pub timestamp: DateTime<Utc>,
}

impl Speedtest {
    #[cfg(test)]
    pub fn new(
        pubkey: PublicKeyBinary,
        timestamp: DateTime<Utc>,
        upload_speed: i64,
        download_speed: i64,
        latency: i32,
    ) -> Self {
        Self {
            pubkey,
            timestamp,
            upload_speed,
            download_speed,
            latency,
        }
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
        file_info_stream: FileInfoStream<CellSpeedtestIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Processing speedtest file {}",
            file_info_stream.file_info.key
        );
        let mut transaction = self.pool.begin().await?;
        // process the speedtest reports from the file, if valid insert to the db
        // and recalcuate a new average
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async {
                let pubkey = report.report.pubkey.clone();
                if self
                    .gateway_client
                    .resolve_gateway_info(&pubkey.clone())
                    .await
                    .is_ok()
                {
                    save_speedtest_to_db(report, &mut transaction).await?;
                    let latest_speedtests: Vec<Speedtest> =
                        get_latest_speedtests_for_pubkey(&pubkey, &mut transaction).await?;
                    let average = SpeedtestAverage::from(&latest_speedtests);
                    average.write(&self.file_sink, latest_speedtests).await?;
                }
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        // db work all done, commit the reports to s3
        self.file_sink.commit().await?;

        Ok(())
    }
}

pub async fn get_latest_speedtests_for_pubkey<'a>(
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
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
) -> Result<EpochSpeedTests, sqlx::Error> {
    let mut speedtests = EpochSpeedTests::new();
    // pull the last N most recent speedtests from prior to the epoch end for each pubkey
    let mut rows = sqlx::query_as::<_, Speedtest>(
        "select * from (
            SELECT distinct(pubkey), upload_speed, download_speed, latency, timestamp, row_number()
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
            .entry(speedtest.pubkey.clone())
            .or_default()
            .push(speedtest);
    }
    Ok(speedtests)
}

pub async fn save_speedtest_to_db(
    report: CellSpeedtestIngestReport,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into speedtests (pubkey, upload_speed, download_speed, latency, timestamp)
        values ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(report.report.pubkey)
    .bind(report.report.upload_speed as i64)
    .bind(report.report.download_speed as i64)
    .bind(report.report.latency as i64)
    .bind(report.report.timestamp)
    .execute(exec)
    .await?;
    Ok(())
}
