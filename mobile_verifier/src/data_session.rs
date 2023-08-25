use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, mobile_transfer::ValidDataTransferSession};
use futures::{
    stream::{Stream, StreamExt, TryStreamExt},
    TryFutureExt,
};
use helium_crypto::PublicKeyBinary;
use sqlx::{PgPool, Postgres, Transaction};
use std::{collections::HashMap, ops::Range};
use tokio::sync::mpsc::Receiver;

pub struct DataSessionIngestor {
    pub pool: PgPool,
}

pub type HotspotMap = HashMap<PublicKeyBinary, u64>;

impl DataSessionIngestor {
    pub fn new(pool: sqlx::Pool<sqlx::Postgres>) -> Self {
        Self { pool }
    }

    pub async fn run(
        self,
        mut receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
        shutdown: triggered::Listener,
    ) -> anyhow::Result<()> {
        tracing::info!("starting DataSessionIngestor");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.clone() => {
                        tracing::info!("DataSessionIngestor shutting down");
                        break;
                    }
                    Some(file) = receiver.recv() => self.process_file(file).await?,
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
        file_info_stream: FileInfoStream<ValidDataTransferSession>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            file = file_info_stream.file_info.key,
            "handling valid data transfer file"
        );
        let mut transaction = self.pool.begin().await?;
        let file_ts = file_info_stream.file_info.timestamp;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                let data_session = HotspotDataSession::from_valid_data_session(report, file_ts);
                data_session.save(&mut transaction).await?;
                metrics::increment_counter!("oracles_mobile_verifier_ingest_hotspot_data_session");
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
pub struct HotspotDataSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub num_dcs: i64,
    pub received_timestamp: DateTime<Utc>,
}

impl HotspotDataSession {
    pub async fn save(self, db: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO hotspot_data_transfer_sessions (pub_key, payer, upload_bytes, download_bytes, num_dcs, received_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(self.pub_key)
        .bind(self.payer)
        .bind(self.upload_bytes)
        .bind(self.download_bytes)
        .bind(self.num_dcs)
        .bind(self.received_timestamp)
        .execute(&mut *db)
        .await?;
        Ok(())
    }
    fn from_valid_data_session(
        v: ValidDataTransferSession,
        received_timestamp: DateTime<Utc>,
    ) -> HotspotDataSession {
        Self {
            pub_key: v.pub_key,
            payer: v.payer,
            upload_bytes: v.upload_bytes as i64,
            download_bytes: v.download_bytes as i64,
            num_dcs: v.num_dcs as i64,
            received_timestamp,
        }
    }
}

pub async fn aggregate_hotspot_data_sessions_to_dc<'a>(
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
    epoch: &'a Range<DateTime<Utc>>,
) -> Result<HotspotMap, sqlx::Error> {
    let stream = sqlx::query_as::<_, HotspotDataSession>(
        r#"
        SELECT *
        FROM hotspot_data_transfer_sessions
        WHERE received_timestamp >= $1 and received_timestamp < $2
        "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch(exec);
    data_sessions_to_dc(stream).await
}

pub async fn data_sessions_to_dc<'a>(
    stream: impl Stream<Item = Result<HotspotDataSession, sqlx::Error>>,
) -> Result<HotspotMap, sqlx::Error> {
    tokio::pin!(stream);
    let mut map = HotspotMap::new();
    while let Some(session) = stream.try_next().await? {
        *map.entry(session.pub_key).or_default() += session.num_dcs as u64
    }
    Ok(map)
}

pub async fn clear_hotspot_data_sessions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from hotspot_data_transfer_sessions where received_timestamp < $1")
        .bind(reward_period.end)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
