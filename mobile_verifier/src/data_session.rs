use chrono::{DateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, file_source, BucketClient};
use file_store_oracles::{mobile_transfer::ValidDataTransferSession, FileType};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use rust_decimal::Decimal;
use sqlx::{PgPool, Pool, Postgres, Transaction};
use std::{collections::HashMap, ops::Range, time::Instant};
use task_manager::{ChannelConsumer, ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::{
    reward_shares::{data_transfer, dc_to_hnt_bones},
    PriceInfo, Settings,
};

pub struct DataSessionIngestor {
    pub receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
    pub pool: PgPool,
}

/// Per-hotspot rewardable data-transfer totals — the aggregated *input* to data
/// transfer rewards, not a reward itself.
#[derive(Default, PartialEq, Eq)]
pub struct RewardableData {
    pub rewardable_bytes: u64,
    pub rewardable_dc: u64,
}

#[derive(Default)]
pub struct RewardableDataByHotspot(HashMap<PublicKeyBinary, RewardableData>);

impl RewardableDataByHotspot {
    pub fn into_gw_data_transfer(self) -> Vec<data_transfer::GatewayDataTransfer<PublicKeyBinary>> {
        self.0
            .into_iter()
            .map(|(hotspot_key, r)| data_transfer::GatewayDataTransfer {
                hotspot_key,
                rewardable_dc: r.rewardable_dc,
                rewardable_bytes: r.rewardable_bytes,
            })
            .collect()
    }

    pub fn reward_sum(&self, price_info: &PriceInfo) -> Decimal {
        self.values()
            .map(|r| dc_to_hnt_bones(Decimal::from(r.rewardable_dc), price_info.price_per_bone))
            .sum()
    }
}

impl std::ops::Deref for RewardableDataByHotspot {
    type Target = HashMap<PublicKeyBinary, RewardableData>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for RewardableDataByHotspot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromIterator<(PublicKeyBinary, RewardableData)> for RewardableDataByHotspot {
    fn from_iter<T: IntoIterator<Item = (PublicKeyBinary, RewardableData)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl DataSessionIngestor {
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        bucket_client: BucketClient,
    ) -> anyhow::Result<impl ManagedTask> {
        // data transfers
        let (data_session_ingest, data_session_ingest_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .lookback_start_after(settings.start_after)
            .prefix(FileType::ValidDataTransferSession.to_string())
            .create()
            .await?;

        let data_session_ingestor = DataSessionIngestor::new(pool.clone(), data_session_ingest);

        Ok(TaskManager::builder()
            .add_task(data_session_ingest_server)
            .add_task(task_manager::channel_consumer(data_session_ingestor))
            .build())
    }

    pub fn new(
        pool: sqlx::Pool<sqlx::Postgres>,
        receiver: Receiver<FileInfoStream<ValidDataTransferSession>>,
    ) -> Self {
        Self { pool, receiver }
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
                metrics::counter!("oracles_mobile_verifier_ingest_hotspot_data_session")
                    .increment(1);
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}

impl ChannelConsumer for DataSessionIngestor {
    type Item = FileInfoStream<ValidDataTransferSession>;
    type Error = anyhow::Error;

    async fn recv(&mut self) -> Option<Self::Item> {
        self.receiver.recv().await
    }

    async fn handle(&mut self, file: Self::Item) -> anyhow::Result<()> {
        let start = Instant::now();
        self.process_file(file).await?;
        metrics::histogram!("valid_data_transfer_session_processing_time").record(start.elapsed());
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
pub struct HotspotDataSession {
    pub pub_key: PublicKeyBinary,
    pub payer: PublicKeyBinary,
    pub upload_bytes: i64,
    pub download_bytes: i64,
    pub rewardable_bytes: i64,
    pub num_dcs: i64,
    pub received_timestamp: DateTime<Utc>,
    pub burn_timestamp: DateTime<Utc>,
}

impl HotspotDataSession {
    pub async fn save(self, db: &mut Transaction<'_, Postgres>) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO hotspot_data_transfer_sessions (
                pub_key,
                payer,
                upload_bytes,
                download_bytes,
                rewardable_bytes,
                num_dcs,
                received_timestamp,
                burn_timestamp
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (pub_key, payer, burn_timestamp) DO NOTHING;
            "#,
        )
        .bind(self.pub_key)
        .bind(self.payer)
        .bind(self.upload_bytes)
        .bind(self.download_bytes)
        .bind(self.rewardable_bytes)
        .bind(self.num_dcs)
        .bind(self.received_timestamp)
        .bind(self.burn_timestamp)
        .execute(&mut **db)
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
            rewardable_bytes: v.rewardable_bytes as i64,
            num_dcs: v.num_dcs as i64,
            received_timestamp,
            burn_timestamp: v.burn_timestamp,
        }
    }
}

pub async fn aggregate_hotspot_data_sessions_to_dc<'a>(
    exec: impl sqlx::PgExecutor<'a> + Copy + 'a,
    epoch: &'a Range<DateTime<Utc>>,
) -> Result<RewardableDataByHotspot, sqlx::Error> {
    let stream = sqlx::query_as::<_, HotspotDataSession>(
        r#"
        SELECT
            pub_key,
            payer,
            upload_bytes,
            download_bytes,
            COALESCE(rewardable_bytes, upload_bytes + download_bytes) AS rewardable_bytes,
            num_dcs,
            received_timestamp,
            burn_timestamp
        FROM hotspot_data_transfer_sessions
        WHERE burn_timestamp >= $1 AND burn_timestamp < $2;
        "#,
    )
    .bind(epoch.start)
    .bind(epoch.end)
    .fetch(exec);
    data_sessions_to_dc(stream).await
}

pub async fn data_sessions_to_dc(
    stream: impl Stream<Item = Result<HotspotDataSession, sqlx::Error>>,
) -> Result<RewardableDataByHotspot, sqlx::Error> {
    tokio::pin!(stream);
    let mut map = RewardableDataByHotspot::default();
    while let Some(session) = stream.try_next().await? {
        let totals = map.entry(session.pub_key).or_default();
        totals.rewardable_dc += session.num_dcs as u64;
        totals.rewardable_bytes += session.rewardable_bytes as u64;
    }
    Ok(map)
}

pub async fn clear_hotspot_data_sessions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("delete from hotspot_data_transfer_sessions where received_timestamp < $1")
        .bind(timestamp)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Where the reward pipeline reads hotspot data-transfer sessions from.
///
/// Migrates data-session reads from Postgres to Trino incrementally, driven
/// purely by whether a Trino client is configured:
/// - no client  → [`DataSessionSource::Postgres`]: read from Postgres only.
/// - has client → [`DataSessionSource::Compare`]: reward from Postgres (source of truth)
///   but also read Trino and emit divergence metrics.
///
/// Final cutover (once the metrics show Trino consistently matches Postgres):
/// drop the `pool`, leaving a Trino-only variant, and stop the Postgres ingest.
/// Keeping this here next to the Postgres aggregation means that cutover is a
/// single-file change.
pub enum DataSessionSource {
    Postgres {
        pool: PgPool,
    },
    Compare {
        pool: PgPool,
        trino: trino_client::Client,
    },
}

impl DataSessionSource {
    /// `Compare` when a Trino client is configured, otherwise `Postgres`.
    pub fn new(pool: PgPool, trino: Option<trino_client::Client>) -> Self {
        match trino {
            Some(trino) => DataSessionSource::Compare { pool, trino },
            None => DataSessionSource::Postgres { pool },
        }
    }

    /// Record, as a metric, whether Trino's burned-session data is ready for this
    /// reward period — data exists past the period end, the same freshness signal
    /// the heartbeat/speedtest checks use. No-op without Trino, and it never
    /// blocks: a query failure is reported as "not ready" rather than propagated.
    pub async fn record_trino_readiness(&self, reward_period: &Range<DateTime<Utc>>) {
        let DataSessionSource::Compare { trino, .. } = self else {
            return;
        };
        let ready =
            match crate::iceberg::burned_session::no_burned_sessions(trino, reward_period).await {
                Ok(empty) => !empty,
                Err(err) => {
                    tracing::error!(?err, "failed to check trino burned-session readiness");
                    false
                }
            };
        crate::telemetry::data_session::trino_ready(ready);
    }

    pub async fn load_data_sessions(
        &self,
        epoch: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<RewardableDataByHotspot> {
        match self {
            DataSessionSource::Postgres { pool } => {
                Ok(aggregate_hotspot_data_sessions_to_dc(pool, epoch).await?)
            }
            // Reward from Postgres (the source of truth during the migration),
            // but also read Trino and emit divergence metrics. A Trino read
            // failure must not break rewarding, so it's logged, not propagated.
            DataSessionSource::Compare { pool, trino } => {
                let postgres = aggregate_hotspot_data_sessions_to_dc(pool, epoch).await?;
                match crate::iceberg::burned_session::aggregate_hotspot_data_sessions_to_dc(
                    trino, epoch,
                )
                .await
                {
                    Ok(iceberg) => compare_data_sessions(&postgres, &iceberg),
                    Err(err) => tracing::error!(
                        ?err,
                        "failed to read data sessions from trino for comparison"
                    ),
                }
                Ok(postgres)
            }
        }
    }
}

/// Outcome of comparing the Postgres and Trino aggregates for an epoch.
#[derive(Debug, PartialEq, Eq)]
struct SessionComparison {
    /// Hotspots whose totals differ, or that appear in only one source.
    divergent_hotspots: u64,
    /// Signed total-DC delta (trino - postgres).
    dc_delta: i128,
    /// Signed total-rewardable-bytes delta (trino - postgres).
    bytes_delta: i128,
}

impl SessionComparison {
    /// The sources agree exactly.
    fn matches(&self) -> bool {
        self.divergent_hotspots == 0
    }
}

/// Pure comparison of the two aggregates — no side effects, so it's unit tested.
fn compare(
    postgres: &RewardableDataByHotspot,
    trino: &RewardableDataByHotspot,
) -> SessionComparison {
    let pg_total_dc: u64 = postgres.values().map(|r| r.rewardable_dc).sum();
    let trino_total_dc: u64 = trino.values().map(|r| r.rewardable_dc).sum();
    let pg_total_bytes: u64 = postgres.values().map(|r| r.rewardable_bytes).sum();
    let trino_total_bytes: u64 = trino.values().map(|r| r.rewardable_bytes).sum();

    let mut divergent_hotspots = 0u64;
    for (key, pg) in postgres.iter() {
        // Differs if the hotspot is missing from trino, or its totals don't match.
        if trino.get(key) != Some(pg) {
            divergent_hotspots += 1;
        }
    }
    // Hotspots present only in trino.
    divergent_hotspots += trino.keys().filter(|k| !postgres.contains_key(*k)).count() as u64;

    SessionComparison {
        divergent_hotspots,
        dc_delta: trino_total_dc as i128 - pg_total_dc as i128,
        bytes_delta: trino_total_bytes as i128 - pg_total_bytes as i128,
    }
}

/// Compare the Postgres and Trino aggregates and emit the divergence metrics.
/// `matches` is the headline signal; the deltas let Grafana chart *how far* off
/// Trino is when it doesn't. Emitted as metrics, not logs, so they can be
/// charted over the migration.
fn compare_data_sessions(postgres: &RewardableDataByHotspot, trino: &RewardableDataByHotspot) {
    let comparison = compare(postgres, trino);
    crate::telemetry::data_session::matches(comparison.matches());
    crate::telemetry::data_session::dc_divergence(comparison.dc_delta as i64);
    crate::telemetry::data_session::bytes_divergence(comparison.bytes_delta as i64);
    crate::telemetry::data_session::hotspot_divergence(comparison.divergent_hotspots);
}

#[cfg(test)]
mod tests {
    use super::*;
    use helium_crypto::PublicKeyBinary;

    fn pubkey(b: u8) -> PublicKeyBinary {
        PublicKeyBinary::from(vec![b])
    }

    fn totals(dc: u64, bytes: u64) -> RewardableData {
        RewardableData {
            rewardable_dc: dc,
            rewardable_bytes: bytes,
        }
    }

    #[test]
    fn empty_sources_match() {
        let comparison = compare(
            &RewardableDataByHotspot::default(),
            &RewardableDataByHotspot::default(),
        );
        assert!(comparison.matches());
        assert_eq!(
            comparison,
            SessionComparison {
                divergent_hotspots: 0,
                dc_delta: 0,
                bytes_delta: 0,
            }
        );
    }

    #[test]
    fn identical_sources_match() {
        let pg = RewardableDataByHotspot::from_iter([
            (pubkey(1), totals(100, 1000)),
            (pubkey(2), totals(50, 500)),
        ]);
        let trino = RewardableDataByHotspot::from_iter([
            (pubkey(1), totals(100, 1000)),
            (pubkey(2), totals(50, 500)),
        ]);

        let comparison = compare(&pg, &trino);
        assert!(comparison.matches());
        assert_eq!(
            comparison,
            SessionComparison {
                divergent_hotspots: 0,
                dc_delta: 0,
                bytes_delta: 0,
            }
        );
    }

    #[test]
    fn mismatched_totals_diverge_with_signed_delta() {
        let pg = RewardableDataByHotspot::from_iter([(pubkey(1), totals(100, 1000))]);
        let trino = RewardableDataByHotspot::from_iter([(pubkey(1), totals(150, 1000))]); // 50 more DC in trino

        let comparison = compare(&pg, &trino);
        assert!(!comparison.matches());
        assert_eq!(
            comparison,
            SessionComparison {
                divergent_hotspots: 1,
                dc_delta: 50,
                bytes_delta: 0,
            }
        );
    }

    #[test]
    fn hotspot_only_in_postgres_diverges() {
        let pg = RewardableDataByHotspot::from_iter([(pubkey(1), totals(100, 1000))]);
        let trino = RewardableDataByHotspot::default();

        let comparison = compare(&pg, &trino);
        assert!(!comparison.matches());
        assert_eq!(
            comparison,
            SessionComparison {
                divergent_hotspots: 1,
                dc_delta: -100, // trino is missing the hotspot entirely
                bytes_delta: -1000,
            }
        );
    }

    #[test]
    fn hotspot_only_in_trino_diverges() {
        let pg = RewardableDataByHotspot::default();
        let trino = RewardableDataByHotspot::from_iter([(pubkey(1), totals(100, 1000))]);

        let comparison = compare(&pg, &trino);
        assert!(!comparison.matches());
        assert_eq!(
            comparison,
            SessionComparison {
                divergent_hotspots: 1,
                dc_delta: 100,
                bytes_delta: 1000,
            }
        );
    }
}
