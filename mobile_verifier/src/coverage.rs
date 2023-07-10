use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use file_store::{
    coverage::{CoverageObject, CoverageObjectIngestReport},
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    traits::TimestampEncode,
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
use sqlx::{FromRow, Pool, Postgres, Type};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

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
        let mut reports = file.into_stream(&mut transaction).await?;

        while let Some(CoverageObjectIngestReport { report, .. }) = reports.next().await {
            let CoverageObject {
                pub_key,
                uuid,
                cbsd_id,
                coverage_claim_time,
                indoor,
                coverage,
                signature,
            } = report;

            // TODO: Validate pub key is authorized
            let validity = if self
                .auth_client
                .verify_authorized_key(&pub_key, NetworkKeyRole::MobilePcs)
                .await?
            {
                CoverageObjectValidity::Valid
            } else {
                CoverageObjectValidity::InvalidPubKey
            };

            self.file_sink
                .write(
                    proto::CoverageObjectV1 {
                        coverage_object: Some(proto::CoverageObjectReqV1 {
                            pub_key: pub_key.into(),
                            uuid: Vec::from(uuid.into_bytes()),
                            cbsd_id: cbsd_id.clone(),
                            coverage_claim_time: coverage_claim_time.encode_timestamp(),
                            indoor,
                            // It's pretty weird that we have to convert these back and forth, but it
                            // shouldn't be too inefficient
                            coverage: coverage.clone().into_iter().map(Into::into).collect(),
                            signature,
                        }),
                        validity: validity as i32,
                    },
                    [],
                )
                .await?;

            if !matches!(validity, CoverageObjectValidity::Valid) {
                continue;
            }

            for hex in coverage {
                let location: u64 = hex.location.into();
                sqlx::query(
                    r#"
                    INSERT INTO coverage_objects
                    VALUES ($1, $2, $3, $4, $5, $6)
                    "#,
                )
                .bind(uuid)
                .bind(location as i64)
                .bind(indoor)
                .bind(cbsd_id.clone())
                .bind(SignalLevel::from(hex.signal_level))
                .bind(coverage_claim_time)
                .execute(&mut transaction)
                .await?;
            }
        }

        transaction.commit().await?;

        Ok(())
    }
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

#[derive(Default)]
pub struct CoveredHexes {
    hexes: HashMap<CellIndex, [BTreeMap<SignalLevel, BinaryHeap<CoverageLevel>>; 2]>,
}

pub fn covered_hex_stream<'a>(
    pool: &'a Pool<Postgres>,
    cbsd_id: &'a str,
    coverage_obj: &'a Uuid,
    latest_timestamp: &'a DateTime<Utc>,
) -> BoxStream<'a, Result<HexCoverage, sqlx::Error>> {
    #[derive(FromRow)]
    struct AdjustedClaimTime {
        coverage_claim_time: DateTime<Utc>,
    }

    sqlx::query_as("SELECT * FROM coverage WHERE cbsd_id = $1 AND uuid = $2")
        .bind(cbsd_id.to_string())
        .bind(*coverage_obj)
        .fetch(pool)
        .and_then(move |mut hc: HexCoverage| async move {
            let AdjustedClaimTime { coverage_claim_time: adjusted_claim_time } = sqlx::query_as(
                r#"
                INSERT INTO coverage_claim_time VALUES ($1, $2, $3, $4)
                ON CONFLICT (cbsd_id)
                DO UPDATE SET
                coverage_claim_time =
                    CASE WHEN EXCLUDED.last_heartbeat - coverage_claim_time.last_heartbeat > INTERVAL '3 days' THEN EXCLUDED.last_heartbeat ELSE heartbeat.coverage_claim_time END,
                last_heartbeat = EXCLUDED.last_heartbeat,
                uuid = EXCLUDED.uuid
                RETURNING coverage_claim_time
                "#
            ).bind(cbsd_id)
            .bind(coverage_obj)
                .bind(hc.coverage_claim_time)
                .bind(latest_timestamp)
                .fetch_one(pool)
                .await
                .unwrap(); // TODO: Fix
            hc.coverage_claim_time = adjusted_claim_time;
            Ok(hc)
        })
        .boxed()
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

    pub fn into_iter(self) -> impl Iterator<Item = CoverageReward> {
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
        let Some(cbsd_id) = sqlx::query_scalar("SELECT cbsd_id FROM coverage_claim_time WHERE uuid = $1")
            .bind(uuid)
            .fetch_optional(&self.pool)
            .await? else {
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
        let rewards: Vec<_> = covered_hexes.into_iter().collect();
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
        let rewards: Vec<_> = covered_hexes.into_iter().collect();
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
