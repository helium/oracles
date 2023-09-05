use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    pin::pin,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use file_store::{
    coverage::CoverageObjectIngestReport, file_info_poller::FileInfoStream,
    file_sink::FileSinkClient, traits::TimestampEncode,
};
use futures::{
    stream::{BoxStream, Stream, StreamExt},
    TryFutureExt, TryStreamExt,
};
use h3o::{CellIndex, LatLng};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{self as proto, CoverageObjectValidity, SignalLevel as SignalLevelProto},
};
use mobile_config::client::AuthorizationClient;
use retainer::Cache;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{FromRow, Pool, Postgres, Transaction, Type};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

use crate::IsAuthorized;

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Type)]
#[sqlx(type_name = "signal_level")]
#[sqlx(rename_all = "lowercase")]
pub enum SignalLevel {
    None,
    Low,
    Medium,
    High,
}

impl From<SignalLevelProto> for SignalLevel {
    fn from(level: SignalLevelProto) -> Self {
        match level {
            SignalLevelProto::High => Self::High,
            SignalLevelProto::Medium => Self::Medium,
            SignalLevelProto::Low => Self::Low,
            SignalLevelProto::None => Self::None,
        }
    }
}

pub struct CoverageDaemon {
    pool: Pool<Postgres>,
    auth_client: AuthorizationClient,
    coverage_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
    file_sink: FileSinkClient,
}

impl CoverageDaemon {
    pub fn new(
        pool: Pool<Postgres>,
        auth_client: AuthorizationClient,
        coverage_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
        file_sink: FileSinkClient,
    ) -> Self {
        Self {
            pool,
            auth_client,
            coverage_objs,
            file_sink,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.clone() => {
                        tracing::info!("CoverageDaemon shutting down");
                        break;
                    }
                    Some(file) = self.coverage_objs.recv() => self.process_file(file).await?,
                }
            }

            Ok(())
        })
        .map_err(anyhow::Error::from)
        .and_then(|result| async move { result })
        .await
    }

    async fn process_file(
        &mut self,
        file: FileInfoStream<CoverageObjectIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing coverage object file {}", file.file_info.key);

        let mut transaction = self.pool.begin().await?;
        let reports = file.into_stream(&mut transaction).await?;

        let mut validated_coverage_objects = pin!(CoverageObject::validate_coverage_objects(
            &self.auth_client,
            reports
        ));

        while let Some(coverage_object) = validated_coverage_objects.next().await.transpose()? {
            coverage_object.write(&self.file_sink).await?;
            if coverage_object.is_valid() {
                coverage_object.save(&mut transaction).await?;
            }
        }

        self.file_sink.commit().await?;
        transaction.commit().await?;

        Ok(())
    }
}

pub struct CoverageObject {
    coverage_object: file_store::coverage::CoverageObject,
    validity: CoverageObjectValidity,
}

impl CoverageObject {
    pub fn is_valid(&self) -> bool {
        matches!(self.validity, CoverageObjectValidity::Valid)
    }

    pub fn validate_coverage_objects<'a>(
        auth_client: &'a impl IsAuthorized,
        coverage_objects: impl Stream<Item = CoverageObjectIngestReport> + 'a,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        coverage_objects.then(move |coverage_object_report| async move {
            Ok(CoverageObject {
                validity: validate_coverage_object(&coverage_object_report, auth_client).await?,
                coverage_object: coverage_object_report.report,
            })
        })
    }

    pub async fn write(&self, coverage_objects: &FileSinkClient) -> file_store::Result {
        coverage_objects
            .write(
                proto::CoverageObjectV1 {
                    coverage_object: Some(proto::CoverageObjectReqV1 {
                        pub_key: self.coverage_object.pub_key.clone().into(),
                        uuid: Vec::from(self.coverage_object.uuid.into_bytes()),
                        cbsd_id: self.coverage_object.cbsd_id.clone(),
                        coverage_claim_time: self
                            .coverage_object
                            .coverage_claim_time
                            .encode_timestamp(),
                        indoor: self.coverage_object.indoor,
                        coverage: self
                            .coverage_object
                            .coverage
                            .clone()
                            .into_iter()
                            .map(Into::into)
                            .collect(),
                        signature: self.coverage_object.signature.clone(),
                    }),
                    validity: self.validity as i32,
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub async fn save(self, transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<bool> {
        for hex in self.coverage_object.coverage {
            let location: u64 = hex.location.into();
            let inserted = sqlx::query(
                r#"
                INSERT INTO hex_coverage
                  (uuid, hex, indoor, cbsd_id, signal_level, coverage_claim_time, inserted_at)
                VALUES
                  ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(self.coverage_object.uuid)
            .bind(location as i64)
            .bind(self.coverage_object.indoor)
            .bind(&self.coverage_object.cbsd_id)
            .bind(SignalLevel::from(hex.signal_level))
            .bind(self.coverage_object.coverage_claim_time)
            .bind(Utc::now())
            .execute(&mut *transaction)
            .await?
            .rows_affected()
                > 0;
            if !inserted {
                tracing::error!(%self.coverage_object.uuid, %location, "Conflict inserting hex coverage");
            }
        }

        Ok(true)
    }
}

async fn validate_coverage_object(
    coverage_object: &CoverageObjectIngestReport,
    auth_client: &impl IsAuthorized,
) -> anyhow::Result<CoverageObjectValidity> {
    if !auth_client
        .is_authorized(&coverage_object.report.pub_key, NetworkKeyRole::MobilePcs)
        .await?
    {
        return Ok(CoverageObjectValidity::InvalidPubKey);
    }

    Ok(CoverageObjectValidity::Valid)
}

#[derive(Clone, FromRow)]
pub struct HexCoverage {
    pub uuid: Uuid,
    pub hex: i64,
    pub indoor: bool,
    pub cbsd_id: String,
    pub signal_level: SignalLevel,
    pub coverage_claim_time: DateTime<Utc>,
}

#[derive(Eq)]
struct CoverageLevel {
    cbsd_id: String,
    coverage_claim_time: DateTime<Utc>,
    hotspot: PublicKeyBinary,
    indoor: bool,
    signal_level: SignalLevel,
}

impl PartialEq for CoverageLevel {
    fn eq(&self, other: &Self) -> bool {
        self.coverage_claim_time == other.coverage_claim_time
    }
}

impl PartialOrd for CoverageLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.coverage_claim_time.cmp(&other.coverage_claim_time))
    }
}

impl Ord for CoverageLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.coverage_claim_time.cmp(&other.coverage_claim_time)
    }
}

impl CoverageLevel {
    fn coverage_points(&self) -> anyhow::Result<Decimal> {
        Ok(match (self.indoor, self.signal_level) {
            (true, SignalLevel::High) => dec!(400),
            (true, SignalLevel::Low) => dec!(100),
            (false, SignalLevel::High) => dec!(16),
            (false, SignalLevel::Medium) => dec!(8),
            (false, SignalLevel::Low) => dec!(4),
            (_, SignalLevel::None) => dec!(0),
            _ => anyhow::bail!("Indoor radio cannot have a signal level of medium"),
        })
    }
}

#[derive(PartialEq, Debug)]
pub struct CoverageReward {
    pub cbsd_id: String,
    pub points: Decimal,
    pub hotspot: PublicKeyBinary,
}

pub const MAX_RADIOS_PER_HEX: usize = 5;

#[async_trait::async_trait]
pub trait CoveredHexStream {
    async fn covered_hex_stream<'a>(
        &'a self,
        cbsd_id: &'a str,
        coverage_obj: &'a Uuid,
        seniority: &'a Seniority,
    ) -> Result<BoxStream<'a, Result<HexCoverage, sqlx::Error>>, sqlx::Error>;

    async fn fetch_seniority(
        &self,
        cbsd_id: &str,
        period_end: DateTime<Utc>,
    ) -> Result<Seniority, sqlx::Error>;
}

#[derive(Clone, sqlx::FromRow)]
pub struct Seniority {
    pub uuid: Uuid,
    pub seniority_ts: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub inserted_at: DateTime<Utc>,
    pub update_reason: i32,
}

impl Seniority {
    pub async fn fetch_latest(
        exec: &mut Transaction<'_, Postgres>,
        cbsd_id: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM seniority WHERE cbsd_id = $1 ORDER BY last_heartbeat DESC LIMIT 1",
        )
        .bind(cbsd_id)
        .fetch_optional(&mut *exec)
        .await
    }
}

#[async_trait::async_trait]
impl CoveredHexStream for Pool<Postgres> {
    async fn covered_hex_stream<'a>(
        &'a self,
        cbsd_id: &'a str,
        coverage_obj: &'a Uuid,
        seniority: &'a Seniority,
    ) -> Result<BoxStream<'a, Result<HexCoverage, sqlx::Error>>, sqlx::Error> {
        // Adjust the coverage. We can safely delete any seniority objects that appears
        // before the latest in the reward period
        sqlx::query("DELETE FROM seniority WHERE inserted_at < $1 AND cbsd_id = $2")
            .bind(seniority.inserted_at)
            .bind(cbsd_id)
            .execute(self)
            .await?;

        // Find the time of insertion for the currently in use coverage object
        let current_inserted_at: DateTime<Utc> = sqlx::query_scalar(
            "SELECT inserted_at FROM hex_coverage WHERE cbsd_id = $1 AND uuid = $2 LIMIT 1",
        )
        .bind(cbsd_id)
        .bind(coverage_obj)
        .fetch_one(self)
        .await?;

        // Delete any hex coverages that were inserted before the one we are currently using, as they are
        // no longer useful.
        sqlx::query(
            "DELETE FROM hex_coverage WHERE cbsd_id = $1 AND uuid != $2 AND inserted_at < $3",
        )
        .bind(cbsd_id)
        .bind(coverage_obj)
        .bind(current_inserted_at)
        .execute(self)
        .await?;

        Ok(
            sqlx::query_as("SELECT * FROM hex_coverage WHERE cbsd_id = $1 AND uuid = $2")
                .bind(cbsd_id)
                .bind(coverage_obj)
                .fetch(self)
                .map_ok(move |hc| HexCoverage {
                    coverage_claim_time: seniority.seniority_ts,
                    ..hc
                })
                .boxed(),
        )
    }

    async fn fetch_seniority(
        &self,
        cbsd_id: &str,
        period_end: DateTime<Utc>,
    ) -> Result<Seniority, sqlx::Error> {
        sqlx::query_as(
            r#"
            SELECT * FROM seniority
            WHERE
              cbsd_id = $1 AND
              inserted_at <= $2
            ORDER BY inserted_at DESC
            LIMIT 1
            "#,
        )
        .bind(cbsd_id)
        .bind(period_end)
        .fetch_one(self)
        .await
    }
}

#[derive(Default)]
pub struct CoveredHexes {
    hexes: HashMap<CellIndex, [BTreeMap<SignalLevel, BinaryHeap<CoverageLevel>>; 2]>,
}

impl CoveredHexes {
    pub async fn aggregate_coverage<E>(
        &mut self,
        hotspot: &PublicKeyBinary,
        covered_hexes: impl Stream<Item = Result<HexCoverage, E>>,
    ) -> Result<(), E> {
        let mut covered_hexes = std::pin::pin!(covered_hexes);

        while let Some(HexCoverage {
            hex,
            indoor,
            signal_level,
            coverage_claim_time,
            cbsd_id,
            ..
        }) = covered_hexes.next().await.transpose()?
        {
            self.hexes
                .entry(CellIndex::try_from(hex as u64).unwrap())
                .or_default()[indoor as usize]
                .entry(signal_level)
                .or_default()
                .push(CoverageLevel {
                    cbsd_id,
                    coverage_claim_time,
                    indoor,
                    signal_level,
                    hotspot: hotspot.clone(),
                });
        }

        Ok(())
    }

    /// Returns the radios that should be rewarded for giving coverage.
    pub fn into_coverage_rewards(self) -> impl Iterator<Item = CoverageReward> {
        self.hexes
            .into_values()
            .flat_map(|radios| {
                radios.into_iter().map(|mut radios| {
                    radios.pop_last().map(|(_, radios)| {
                        radios
                            .into_sorted_vec()
                            .into_iter()
                            .take(MAX_RADIOS_PER_HEX)
                            .map(|cl| CoverageReward {
                                points: cl.coverage_points().unwrap(),
                                hotspot: cl.hotspot,
                                cbsd_id: cl.cbsd_id,
                            })
                    })
                })
            })
            .flatten()
            .flatten()
            .filter(|r| r.points > Decimal::ZERO)
    }
}

type CoverageClaimTimeKey = (String, Option<Uuid>);

pub struct CoverageClaimTimeCache {
    cache: Arc<Cache<CoverageClaimTimeKey, DateTime<Utc>>>,
}

impl Default for CoverageClaimTimeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl CoverageClaimTimeCache {
    pub fn new() -> Self {
        let cache = Arc::new(Cache::new());
        let cache_clone = cache.clone();
        tokio::spawn(async move {
            cache_clone
                .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 24 * 5))
                .await
        });
        Self { cache }
    }

    pub async fn fetch_coverage_claim_time(
        &self,
        cbsd_id: &str,
        coverage_object: &Option<Uuid>,
        exec: &mut Transaction<'_, Postgres>,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        let key = (cbsd_id.to_string(), *coverage_object);
        if let Some(coverage_claim_time) = self.cache.get(&key).await {
            Ok(Some(*coverage_claim_time))
        } else {
            let coverage_claim_time: Option<DateTime<Utc>> = sqlx::query_scalar(
                r#"
                SELECT coverage_claim_time FROM hex_coverage WHERE cbsd_id = $1 AND uuid = $2 LIMIT 1
                "#,
            )
            .bind(cbsd_id)
            .bind(coverage_object)
            .fetch_optional(&mut *exec)
            .await?;
            if let Some(coverage_claim_time) = coverage_claim_time {
                self.cache
                    .insert(
                        key,
                        coverage_claim_time,
                        std::time::Duration::from_secs(60 * 60 * 24),
                    )
                    .await;
            }
            Ok(coverage_claim_time)
        }
    }
}

pub struct CoveredHexCache {
    pool: Pool<Postgres>,
    covered_hexes: Arc<Cache<Uuid, CachedCoverage>>,
}

impl CoveredHexCache {
    pub fn new(pool: &Pool<Postgres>) -> Self {
        let cache = Arc::new(Cache::new());
        let cache_clone = cache.clone();
        tokio::spawn(async move {
            cache_clone
                .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 24 * 2))
                .await
        });

        Self {
            covered_hexes: cache,
            pool: pool.clone(),
        }
    }

    pub async fn fetch_coverage(&self, uuid: &Uuid) -> Result<Option<CachedCoverage>, sqlx::Error> {
        if let Some(covered_hexes) = self.covered_hexes.get(uuid).await {
            return Ok(Some(covered_hexes.clone()));
        }
        let Some(cbsd_id) = sqlx::query_scalar("SELECT cbsd_id FROM hex_coverage WHERE uuid = $1")
            .bind(uuid)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let coverage: Vec<_> = sqlx::query_as("SELECT * FROM hex_coverage WHERE uuid = $1")
            .bind(uuid)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|HexCoverage { hex, .. }| CellIndex::try_from(hex as u64).unwrap())
            .collect();
        let cached_coverage = CachedCoverage { cbsd_id, coverage };
        let _ = self
            .covered_hexes
            .insert(
                *uuid,
                cached_coverage.clone(),
                std::time::Duration::from_secs(60 * 60 * 24),
            )
            .await;
        Ok(Some(cached_coverage))
    }
}

#[derive(Clone)]
pub struct CachedCoverage {
    pub cbsd_id: String,
    coverage: Vec<CellIndex>,
}

impl CachedCoverage {
    pub fn max_distance_km(&self, latlng: LatLng) -> f64 {
        self.coverage.iter().fold(0.0, |curr_max, curr_cov| {
            let cov = LatLng::from(*curr_cov);
            curr_max.max(cov.distance_km(latlng))
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::NaiveDate;
    use futures::stream::iter;

    fn default_hex_coverage(cbsd_id: &str, signal_level: SignalLevel) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: false,
            cbsd_id: cbsd_id.to_string(),
            signal_level,
            coverage_claim_time: DateTime::<Utc>::MIN_UTC,
        }
    }

    /// Test to ensure that if there are multiple radios with different signal levels
    /// in a given hex, that the one with the highest signal level is chosen.
    #[tokio::test]
    async fn ensure_max_signal_level_selected() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let mut covered_hexes = CoveredHexes::default();
        covered_hexes
            .aggregate_coverage(
                &owner,
                iter(vec![
                    anyhow::Ok(default_hex_coverage("1", SignalLevel::None)),
                    anyhow::Ok(default_hex_coverage("2", SignalLevel::Low)),
                    anyhow::Ok(default_hex_coverage("3", SignalLevel::Medium)),
                    anyhow::Ok(default_hex_coverage("4", SignalLevel::High)),
                    anyhow::Ok(default_hex_coverage("5", SignalLevel::Medium)),
                    anyhow::Ok(default_hex_coverage("6", SignalLevel::Low)),
                    anyhow::Ok(default_hex_coverage("7", SignalLevel::None)),
                ]),
            )
            .await
            .unwrap();
        let rewards: Vec<_> = covered_hexes.into_coverage_rewards().collect();
        assert_eq!(
            rewards,
            vec![CoverageReward {
                cbsd_id: "4".to_string(),
                hotspot: owner,
                points: dec!(16)
            }]
        );
    }

    fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(
            NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            Utc,
        )
    }

    fn hex_coverage_with_date(
        cbsd_id: &str,
        signal_level: SignalLevel,
        coverage_claim_time: DateTime<Utc>,
    ) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: false,
            cbsd_id: cbsd_id.to_string(),
            signal_level,
            coverage_claim_time,
        }
    }

    /// Test to ensure that if there are more than five radios with the highest signal
    /// level in a given hex, that the five oldest radios are chosen.
    #[tokio::test]
    async fn ensure_oldest_five_radios_selected() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let mut covered_hexes = CoveredHexes::default();
        covered_hexes
            .aggregate_coverage(
                &owner,
                iter(vec![
                    anyhow::Ok(hex_coverage_with_date(
                        "1",
                        SignalLevel::High,
                        date(1980, 1, 1),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "2",
                        SignalLevel::High,
                        date(1970, 1, 5),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "3",
                        SignalLevel::High,
                        date(1990, 2, 2),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "4",
                        SignalLevel::High,
                        date(1970, 1, 4),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "5",
                        SignalLevel::High,
                        date(1975, 3, 3),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "6",
                        SignalLevel::High,
                        date(1970, 1, 3),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "7",
                        SignalLevel::High,
                        date(1974, 2, 2),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "8",
                        SignalLevel::High,
                        date(1970, 1, 2),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "9",
                        SignalLevel::High,
                        date(1976, 5, 2),
                    )),
                    anyhow::Ok(hex_coverage_with_date(
                        "10",
                        SignalLevel::High,
                        date(1970, 1, 1),
                    )),
                ]),
            )
            .await
            .unwrap();
        let rewards: Vec<_> = covered_hexes.into_coverage_rewards().collect();
        assert_eq!(
            rewards,
            vec![
                CoverageReward {
                    cbsd_id: "10".to_string(),
                    hotspot: owner.clone(),
                    points: dec!(16)
                },
                CoverageReward {
                    cbsd_id: "8".to_string(),
                    hotspot: owner.clone(),
                    points: dec!(16)
                },
                CoverageReward {
                    cbsd_id: "6".to_string(),
                    hotspot: owner.clone(),
                    points: dec!(16)
                },
                CoverageReward {
                    cbsd_id: "4".to_string(),
                    hotspot: owner.clone(),
                    points: dec!(16)
                },
                CoverageReward {
                    cbsd_id: "2".to_string(),
                    hotspot: owner.clone(),
                    points: dec!(16)
                }
            ]
        );
    }
}
