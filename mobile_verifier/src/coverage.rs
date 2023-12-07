use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    pin::pin,
    sync::Arc,
    time::Instant,
};

use chrono::{DateTime, Utc};
use file_store::{
    coverage::{self, CoverageObjectIngestReport},
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
use sqlx::{FromRow, Pool, Postgres, QueryBuilder, Transaction, Type};
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

use crate::{
    heartbeats::{HbType, KeyType, OwnedKeyType},
    IsAuthorized,
};

#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Type)]
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
                #[rustfmt::skip]
                tokio::select! {
                    _ = shutdown.clone() => {
                        tracing::info!("CoverageDaemon shutting down");
                        break;
                    }
                    Some(file) = self.coverage_objs.recv() => {
			let start = Instant::now();
			self.process_file(file).await?;
			metrics::histogram!("coverage_object_processing_time", start.elapsed());
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
    pub coverage_object: file_store::coverage::CoverageObject,
    pub validity: CoverageObjectValidity,
}

impl CoverageObject {
    pub fn is_valid(&self) -> bool {
        matches!(self.validity, CoverageObjectValidity::Valid)
    }

    pub fn key(&self) -> KeyType<'_> {
        match self.coverage_object.key_type {
            coverage::KeyType::CbsdId(ref cbsd) => KeyType::Cbrs(cbsd.as_str()),
            coverage::KeyType::HotspotKey(ref hotspot_key) => KeyType::Wifi(hotspot_key),
        }
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
                        key_type: Some(self.coverage_object.key_type.clone().into()),
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
                        trust_score: self.coverage_object.trust_score,
                    }),
                    validity: self.validity as i32,
                },
                &[("validity", self.validity.as_str_name())],
            )
            .await?;
        Ok(())
    }

    pub async fn save(self, transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        let insertion_time = Utc::now();
        let key = self.key();
        let hb_type = key.hb_type();
        let key = key.to_owned();

        sqlx::query(r#"
            INSERT INTO coverage_objects(uuid, radio_type, radio_key, indoor, coverage_claim_time, trust_score, inserted_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (uuid) DO UPDATE SET
                radio_type = EXCLUDED.radio_type,
                radio_key = EXCLUDED.radio_key,
                indoor = EXCLUDED.indoor,
                coverage_claim_time = EXCLUDED.coverage_claim_time,
                trust_score = EXCLUDED.trust_score,
                inserted_at = EXCLUDED.inserted_at
        "#)
        .bind(self.coverage_object.uuid)
        .bind(hb_type)
        .bind(&key)
        .bind(self.coverage_object.indoor)
        .bind(self.coverage_object.coverage_claim_time)
        .bind(self.coverage_object.trust_score as i32)
        .bind(insertion_time)
        .execute(&mut *transaction)
        .await?;

        const NUMBER_OF_FIELDS_IN_QUERY: u16 = 4;
        const COVERAGE_MAX_BATCH_ENTRIES: usize = (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

        for hexes in self
            .coverage_object
            .coverage
            .chunks(COVERAGE_MAX_BATCH_ENTRIES)
        {
            QueryBuilder::new("INSERT INTO hexes (uuid, hex, signal_level, signal_power)")
                .push_values(hexes, |mut b, hex| {
                    let location: u64 = hex.location.into();

                    b.push_bind(self.coverage_object.uuid)
                        .push_bind(location as i64)
                        .push_bind(SignalLevel::from(hex.signal_level))
                        .push_bind(hex.signal_power);
                })
                .push(
                    r#"
                    ON CONFLICT (uuid, hex) DO UPDATE SET
                      signal_level = EXCLUDED.signal_level,
                      signal_power = EXCLUDED.signal_power
                    "#,
                )
                .build()
                .execute(&mut *transaction)
                .await?;
        }

        Ok(())
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
    pub radio_key: OwnedKeyType,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
    pub coverage_claim_time: DateTime<Utc>,
    pub inserted_at: DateTime<Utc>,
}

#[derive(Eq, Debug)]
struct IndoorCoverageLevel {
    radio_key: OwnedKeyType,
    seniority_timestamp: DateTime<Utc>,
    hotspot: PublicKeyBinary,
    signal_level: SignalLevel,
}

impl PartialEq for IndoorCoverageLevel {
    fn eq(&self, other: &Self) -> bool {
        self.seniority_timestamp == other.seniority_timestamp
    }
}

impl PartialOrd for IndoorCoverageLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndoorCoverageLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seniority_timestamp.cmp(&other.seniority_timestamp)
    }
}

impl IndoorCoverageLevel {
    fn coverage_points(&self) -> Decimal {
        match self.signal_level {
            SignalLevel::High => dec!(400),
            SignalLevel::Low => dec!(100),
            _ => dec!(0),
        }
    }
}

#[derive(Eq, Debug)]
struct OutdoorCoverageLevel {
    radio_key: OwnedKeyType,
    seniority_timestamp: DateTime<Utc>,
    hotspot: PublicKeyBinary,
    signal_power: i32,
    signal_level: SignalLevel,
}

impl PartialEq for OutdoorCoverageLevel {
    fn eq(&self, other: &Self) -> bool {
        self.signal_power == other.signal_power
            && self.seniority_timestamp == other.seniority_timestamp
    }
}

impl PartialOrd for OutdoorCoverageLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OutdoorCoverageLevel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.signal_power
            .cmp(&other.signal_power)
            .reverse()
            .then_with(|| self.seniority_timestamp.cmp(&other.seniority_timestamp))
    }
}

impl OutdoorCoverageLevel {
    fn coverage_points(&self) -> Decimal {
        match self.signal_level {
            SignalLevel::High => dec!(16),
            SignalLevel::Medium => dec!(8),
            SignalLevel::Low => dec!(4),
            SignalLevel::None => dec!(0),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct CoverageReward {
    pub radio_key: OwnedKeyType,
    pub points: Decimal,
    pub hotspot: PublicKeyBinary,
}

#[async_trait::async_trait]
pub trait CoveredHexStream {
    async fn covered_hex_stream<'a>(
        &'a self,
        radio_key: KeyType<'a>,
        coverage_obj: &'a Uuid,
        seniority: &'a Seniority,
    ) -> Result<BoxStream<'a, Result<HexCoverage, sqlx::Error>>, sqlx::Error>;

    async fn fetch_seniority(
        &self,
        key: KeyType<'_>,
        period_end: DateTime<Utc>,
    ) -> Result<Seniority, sqlx::Error>;
}

#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct Seniority {
    pub uuid: Uuid,
    pub seniority_ts: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub inserted_at: DateTime<Utc>,
    pub update_reason: i32,
}

impl Seniority {
    pub async fn fetch_latest(
        key: KeyType<'_>,
        exec: &mut Transaction<'_, Postgres>,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT uuid, seniority_ts, last_heartbeat, inserted_at, update_reason FROM seniority WHERE radio_key = $1 ORDER BY last_heartbeat DESC LIMIT 1",
        )
        .bind(key)
        .fetch_optional(&mut *exec)
        .await
    }
}

#[async_trait::async_trait]
impl CoveredHexStream for Pool<Postgres> {
    async fn covered_hex_stream<'a>(
        &'a self,
        key: KeyType<'a>,
        coverage_obj: &'a Uuid,
        seniority: &'a Seniority,
    ) -> Result<BoxStream<'a, Result<HexCoverage, sqlx::Error>>, sqlx::Error> {
        // Adjust the coverage. We can safely delete any seniority objects that appear
        // before the latest in the reward period:
        sqlx::query("DELETE FROM seniority WHERE inserted_at < $1 AND radio_key = $2")
            .bind(seniority.inserted_at)
            .bind(key)
            .execute(self)
            .await?;

        Ok(
            sqlx::query_as(
                r#"
                SELECT co.uuid, h.hex, co.indoor, co.radio_key, h.signal_level, co.coverage_claim_time, co.inserted_at)
                FROM coverage_objects co
                    INNER JOIN hexes h on co.uuid = h.uuid
                WHERE co.radio_key = $1
                    AND co.uuid = $2
                "#,
            )
            .bind(key)
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
        key: KeyType<'_>,
        period_end: DateTime<Utc>,
    ) -> Result<Seniority, sqlx::Error> {
        sqlx::query_as(
            r#"
            SELECT uuid, seniority_ts, last_heartbeat, inserted_at, update_reason FROM seniority
            WHERE
              radio_key = $1 AND
              inserted_at <= $2
            ORDER BY inserted_at DESC
            LIMIT 1
            "#,
        )
        .bind(key)
        .bind(period_end)
        .fetch_one(self)
        .await
    }
}

pub async fn clear_coverage_objects(
    tx: &mut Transaction<'_, Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    // Delete any hex coverage objects that were invalidated before the given timestamp
    sqlx::query(
        r#"
        DELETE FROM hexes WHERE uuid IN (
            SELECT uuid
            FROM coverage_objects
            WHERE invalidated_at < $1
        )
        "#,
    )
    .bind(timestamp)
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM coverage_objects WHERE invalidated_at < $1")
        .bind(timestamp)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

#[derive(Default, Debug)]
pub struct CoveredHexes {
    indoor: HashMap<CellIndex, BTreeMap<SignalLevel, BinaryHeap<IndoorCoverageLevel>>>,
    outdoor: HashMap<CellIndex, BinaryHeap<OutdoorCoverageLevel>>,
}

pub const MAX_INDOOR_RADIOS_PER_RES12_HEX: usize = 5;
pub const MAX_OUTDOOR_RADIOS_PER_RES12_HEX: usize = 3;
pub const OUTDOOR_REWARD_WEIGHTS: [Decimal; 3] = [dec!(1.0), dec!(0.75), dec!(0.25)];

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
            radio_key,
            signal_power,
            ..
        }) = covered_hexes.next().await.transpose()?
        {
            if indoor {
                self.indoor
                    .entry(CellIndex::try_from(hex as u64).unwrap())
                    .or_default()
                    .entry(signal_level)
                    .or_default()
                    .push(IndoorCoverageLevel {
                        radio_key,
                        seniority_timestamp: coverage_claim_time,
                        signal_level,
                        hotspot: hotspot.clone(),
                    });
            } else {
                self.outdoor
                    .entry(CellIndex::try_from(hex as u64).unwrap())
                    .or_default()
                    .push(OutdoorCoverageLevel {
                        radio_key,
                        seniority_timestamp: coverage_claim_time,
                        signal_level,
                        signal_power,
                        hotspot: hotspot.clone(),
                    });
            }
        }

        Ok(())
    }

    /// Returns the radios that should be rewarded for giving coverage.
    pub fn into_coverage_rewards(self) -> impl Iterator<Item = CoverageReward> {
        let outdoor_rewards = self.outdoor.into_values().flat_map(|radios| {
            radios
                .into_sorted_vec()
                .into_iter()
                .take(MAX_OUTDOOR_RADIOS_PER_RES12_HEX)
                .zip(OUTDOOR_REWARD_WEIGHTS)
                .map(|(cl, rank)| CoverageReward {
                    points: cl.coverage_points() * rank,
                    hotspot: cl.hotspot,
                    radio_key: cl.radio_key,
                })
        });
        let indoor_rewards = self
            .indoor
            .into_values()
            .flat_map(|mut radios| {
                radios.pop_last().map(|(_, radios)| {
                    radios
                        .into_sorted_vec()
                        .into_iter()
                        .take(MAX_INDOOR_RADIOS_PER_RES12_HEX)
                        .map(|cl| CoverageReward {
                            points: cl.coverage_points(),
                            hotspot: cl.hotspot,
                            radio_key: cl.radio_key,
                        })
                })
            })
            .flatten();
        outdoor_rewards
            .chain(indoor_rewards)
            .filter(|r| r.points > Decimal::ZERO)
    }
}

type CoverageClaimTimeKey = ((String, HbType), Option<Uuid>);

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

    pub async fn fetch_coverage_claim_time<'a, 'b>(
        &self,
        radio_key: KeyType<'a>,
        coverage_object: &'a Option<Uuid>,
        exec: &mut Transaction<'b, Postgres>,
    ) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
        let key = (radio_key.to_id(), *coverage_object);
        if let Some(coverage_claim_time) = self.cache.get(&key).await {
            Ok(Some(*coverage_claim_time))
        } else {
            let coverage_claim_time: Option<DateTime<Utc>> = sqlx::query_scalar(
                r#"
                SELECT coverage_claim_time FROM coverage_objects WHERE radio_key = $1 AND uuid = $2
                "#,
            )
            .bind(radio_key)
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

pub struct CoverageObjects {
    pool: Pool<Postgres>,
}

impl CoverageObjects {
    pub fn new(pool: &Pool<Postgres>) -> Self {
        Self { pool: pool.clone() }
    }

    pub async fn coverage_info(
        &self,
        uuid: &Uuid,
        key: KeyType<'_>,
    ) -> Result<Option<CoverageInfo>, sqlx::Error> {
        sqlx::query_as(
	    "SELECT inserted_at, indoor FROM coverage_objects WHERE uuid = $1 AND radio_key = $2 AND invalidated_at IS NULL LIMIT 1"
	)
	.bind(uuid)
	.bind(key)
	.fetch_optional(&self.pool)
	.await
    }
}

#[derive(Clone, FromRow)]
pub struct CoverageInfo {
    pub inserted_at: DateTime<Utc>,
    pub indoor: bool,
}

#[derive(Clone)]
pub struct CachedCoverage {
    pub radio_key: OwnedKeyType,
    pub coverage: Vec<CellIndex>,
    pub inserted_at: DateTime<Utc>,
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

    fn default_indoor_hex_coverage(cbsd_id: &str, signal_level: SignalLevel) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: true,
            radio_key: OwnedKeyType::Cbrs(cbsd_id.to_string()),
            signal_level,
            // Signal power is ignored for indoor radios:
            signal_power: 0,
            coverage_claim_time: DateTime::<Utc>::MIN_UTC,
            inserted_at: DateTime::<Utc>::MIN_UTC,
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
                    anyhow::Ok(default_indoor_hex_coverage("1", SignalLevel::None)),
                    anyhow::Ok(default_indoor_hex_coverage("2", SignalLevel::Low)),
                    anyhow::Ok(default_indoor_hex_coverage("3", SignalLevel::High)),
                    anyhow::Ok(default_indoor_hex_coverage("4", SignalLevel::Low)),
                    anyhow::Ok(default_indoor_hex_coverage("5", SignalLevel::None)),
                ]),
            )
            .await
            .unwrap();
        let rewards: Vec<_> = covered_hexes.into_coverage_rewards().collect();
        assert_eq!(
            rewards,
            vec![CoverageReward {
                radio_key: OwnedKeyType::Cbrs("3".to_string()),
                hotspot: owner,
                points: dec!(400)
            }]
        );
    }

    fn date(year: i32, month: u32, day: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    }

    fn indoor_hex_coverage_with_date(
        cbsd_id: &str,
        signal_level: SignalLevel,
        coverage_claim_time: DateTime<Utc>,
    ) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: true,
            radio_key: OwnedKeyType::Cbrs(cbsd_id.to_string()),
            signal_level,
            // Signal power is ignored for indoor radios:
            signal_power: 0,
            coverage_claim_time,
            inserted_at: DateTime::<Utc>::MIN_UTC,
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
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "1",
                        SignalLevel::High,
                        date(1980, 1, 1),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "2",
                        SignalLevel::High,
                        date(1970, 1, 5),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "3",
                        SignalLevel::High,
                        date(1990, 2, 2),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "4",
                        SignalLevel::High,
                        date(1970, 1, 4),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "5",
                        SignalLevel::High,
                        date(1975, 3, 3),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "6",
                        SignalLevel::High,
                        date(1970, 1, 3),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "7",
                        SignalLevel::High,
                        date(1974, 2, 2),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "8",
                        SignalLevel::High,
                        date(1970, 1, 2),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
                        "9",
                        SignalLevel::High,
                        date(1976, 5, 2),
                    )),
                    anyhow::Ok(indoor_hex_coverage_with_date(
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
                    radio_key: OwnedKeyType::Cbrs("10".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(400)
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("8".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(400)
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("6".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(400)
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("4".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(400)
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("2".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(400)
                }
            ]
        );
    }

    fn outdoor_hex_coverage(
        cbsd_id: &str,
        signal_power: i32,
        coverage_claim_time: DateTime<Utc>,
    ) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: false,
            radio_key: OwnedKeyType::Cbrs(cbsd_id.to_string()),
            signal_power,
            signal_level: SignalLevel::High,
            coverage_claim_time,
            inserted_at: DateTime::<Utc>::MIN_UTC,
        }
    }

    #[tokio::test]
    async fn ensure_outdoor_radios_ranked_by_power() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let mut covered_hexes = CoveredHexes::default();
        covered_hexes
            .aggregate_coverage(
                &owner,
                iter(vec![
                    anyhow::Ok(outdoor_hex_coverage("1", -9469, date(2022, 8, 1))),
                    anyhow::Ok(outdoor_hex_coverage("2", -9360, date(2022, 12, 5))),
                    anyhow::Ok(outdoor_hex_coverage("3", -8875, date(2022, 12, 2))),
                    anyhow::Ok(outdoor_hex_coverage("4", -8875, date(2022, 12, 1))),
                    anyhow::Ok(outdoor_hex_coverage("5", -7733, date(2023, 5, 1))),
                ]),
            )
            .await
            .unwrap();
        let rewards: Vec<_> = covered_hexes.into_coverage_rewards().collect();
        assert_eq!(
            rewards,
            vec![
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("5".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(16)
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("4".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(12)
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("3".to_string()),
                    hotspot: owner,
                    points: dec!(4)
                }
            ]
        );
    }
}
