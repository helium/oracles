use crate::{
    boosting_oracles::{
        assignment::footfall_and_urbanization_multiplier, Assignment, DiskTreeLike, FootfallData,
        FootfallLike, UrbanizationData,
    },
    geofence::GeofenceValidator,
    heartbeats::{HbType, KeyType, OwnedKeyType},
    IsAuthorized,
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
    poc_mobile::{
        self as proto, CoverageObjectValidity, OracleBoostingReportV1,
        SignalLevel as SignalLevelProto,
    },
};
use mobile_config::{
    boosted_hex_info::{BoostedHex, BoostedHexes},
    client::AuthorizationClient,
};
use retainer::{entry::CacheReadGuard, Cache};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::{FromRow, PgPool, Pool, Postgres, QueryBuilder, Transaction, Type};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap, HashMap},
    num::NonZeroU32,
    pin::pin,
    sync::Arc,
    time::Instant,
};
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;

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

pub struct CoverageDaemon<DT, GF, FL> {
    pool: Pool<Postgres>,
    auth_client: AuthorizationClient,
    urbanization_data: UrbanizationData<DT, GF>,
    footfall_data: FootfallData<FL>,
    coverage_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
    initial_boosting_reports: Option<Vec<OracleBoostingReportV1>>,
    coverage_obj_sink: FileSinkClient,
    oracle_boosting_sink: FileSinkClient,
}

impl<DT, GF, FL> CoverageDaemon<DT, GF, FL>
where
    DT: DiskTreeLike,
    GF: GeofenceValidator<hextree::Cell>,
    FL: FootfallLike,
{
    pub async fn new(
        pool: PgPool,
        auth_client: AuthorizationClient,
        footfall_data: FootfallData<FL>,
        urbanization_data: UrbanizationData<DT, GF>,
        coverage_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
        coverage_obj_sink: FileSinkClient,
        oracle_boosting_sink: FileSinkClient,
    ) -> anyhow::Result<Self> {
        tracing::info!("Setting initial values for the urbanization column");

        let unassigned_urbanization_hexes = UnassignedHex::fetch(&pool);

        let initial_boosting_reports = Some(
            set_oracle_boosting_assignments(
                unassigned_urbanization_hexes,
                &footfall_data,
                &urbanization_data,
                &pool,
            )
            .await?
            .collect(),
        );

        Ok(Self {
            pool,
            auth_client,
            urbanization_data,
            footfall_data,
            coverage_objs,
            coverage_obj_sink,
            oracle_boosting_sink,
            initial_boosting_reports,
        })
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        let Some(initial_boosting_reports) = std::mem::take(&mut self.initial_boosting_reports)
        else {
            anyhow::bail!("Initial boosting reports is None");
        };
        self.oracle_boosting_sink
            .write_all(initial_boosting_reports)
            .await?;
        self.oracle_boosting_sink.commit().await?;

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
    }

    async fn process_file(
        &self,
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
            coverage_object.write(&self.coverage_obj_sink).await?;
            if coverage_object.is_valid() {
                coverage_object.save(&mut transaction).await?;
            }
        }

        self.coverage_obj_sink.commit().await?;
        transaction.commit().await?;

        // After writing all of the coverage objects, we set their oracle boosting assignments. This is
        // done in two steps to improve the testability of the assignments.
        let unassigned_urbinization_hexes = UnassignedHex::fetch(&self.pool);

        let boosting_reports = set_oracle_boosting_assignments(
            unassigned_urbinization_hexes,
            &self.footfall_data,
            &self.urbanization_data,
            &self.pool,
        )
        .await?;
        self.oracle_boosting_sink
            .write_all(boosting_reports)
            .await?;
        self.oracle_boosting_sink.commit().await?;

        Ok(())
    }
}

#[derive(FromRow)]
pub struct UnassignedHex {
    uuid: Uuid,
    #[sqlx(try_from = "i64")]
    hex: u64,
    signal_level: SignalLevel,
    signal_power: i32,
}

impl UnassignedHex {
    pub fn fetch(pool: &PgPool) -> impl Stream<Item = sqlx::Result<Self>> + '_ {
        sqlx::query_as(
            "SELECT uuid, hex, signal_level, signal_power FROM hexes WHERE urbanized IS NULL OR footfall IS NULL",
        )
        .fetch(pool)
    }

    fn to_location_string(&self) -> String {
        format!("{:x}", self.hex)
    }
}

pub async fn set_oracle_boosting_assignments<'a>(
    unassigned_urbinization_hexes: impl Stream<Item = sqlx::Result<UnassignedHex>>,
    footfall_data: &FootfallData<impl FootfallLike>,
    urbanization_data: &UrbanizationData<impl DiskTreeLike, impl GeofenceValidator<hextree::Cell>>,
    pool: &'a PgPool,
) -> anyhow::Result<impl Iterator<Item = proto::OracleBoostingReportV1>> {
    let now = Utc::now();

    let boost_results = initialize_unassigned_hexes(
        unassigned_urbinization_hexes,
        footfall_data,
        urbanization_data,
        pool,
    )
    .await?;

    Ok(boost_results
        .into_iter()
        .map(
            move |(coverage_object, assignments)| proto::OracleBoostingReportV1 {
                coverage_object: Vec::from(coverage_object.into_bytes()),
                assignments,
                timestamp: now.encode_timestamp(),
            },
        ))
}

async fn initialize_unassigned_hexes(
    unassigned_urbinization_hexes: impl Stream<Item = Result<UnassignedHex, sqlx::Error>>,
    footfall_data: &FootfallData<impl FootfallLike>,
    urbanization_data: &UrbanizationData<impl DiskTreeLike, impl GeofenceValidator<hextree::Cell>>,
    pool: &Pool<Postgres>,
) -> Result<HashMap<Uuid, Vec<proto::OracleBoostingHexAssignment>>, anyhow::Error> {
    const NUMBER_OF_FIELDS_IN_QUERY: u16 = 6;
    const ASSIGNMENTS_MAX_BATCH_ENTRIES: usize = (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

    let mut boost_results = HashMap::<Uuid, Vec<proto::OracleBoostingHexAssignment>>::new();

    let mut unassigned_hexes =
        pin!(unassigned_urbinization_hexes.try_chunks(ASSIGNMENTS_MAX_BATCH_ENTRIES));

    while let Some(hexes) = unassigned_hexes.try_next().await? {
        let hexes: anyhow::Result<Vec<_>> = hexes
            .into_iter()
            .map(|hex| {
                let cell = hextree::Cell::from_raw(hex.hex)?;
                let urbanized = urbanization_data.hex_assignment(cell)?;
                let footfall = footfall_data.hex_assignment(cell)?;
                let location = hex.to_location_string();
                let assignment_multiplier =
                    (footfall_and_urbanization_multiplier(footfall, urbanized) * dec!(1000))
                        .to_u32()
                        .unwrap_or(0);

                boost_results.entry(hex.uuid).or_default().push(
                    proto::OracleBoostingHexAssignment {
                        location,
                        urbanized: urbanized.into(),
                        footfall: footfall.into(),
                        assignment_multiplier,
                    },
                );

                Ok((hex, urbanized, footfall))
            })
            .collect();

        QueryBuilder::new(
            "INSERT INTO hexes (uuid, hex, signal_level, signal_power, urbanized, footfall)",
        )
        .push_values(hexes?, |mut b, (hex, urbanized, footfall)| {
            b.push_bind(hex.uuid)
                .push_bind(hex.hex as i64)
                .push_bind(hex.signal_level)
                .push_bind(hex.signal_power)
                .push_bind(urbanized)
                .push_bind(footfall);
        })
        .push(
            r#"
            ON CONFLICT (uuid, hex) DO UPDATE SET
                urbanized = EXCLUDED.urbanized,
                footfall = EXCLUDED.footfall
            "#,
        )
        .build()
        .execute(pool)
        .await?;
    }

    Ok(boost_results)
}

impl<DT, GF, FL> ManagedTask for CoverageDaemon<DT, GF, FL>
where
    DT: DiskTreeLike,
    GF: GeofenceValidator<hextree::Cell>,
    FL: FootfallLike,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

pub struct CoverageObject {
    pub coverage_object: file_store::coverage::CoverageObject,
    pub validity: CoverageObjectValidity,
}

impl CoverageObject {
    /// Validate a coverage object
    pub async fn validate(
        coverage_object: file_store::coverage::CoverageObject,
        auth_client: &impl IsAuthorized,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            validity: if auth_client
                .is_authorized(&coverage_object.pub_key, NetworkKeyRole::MobilePcs)
                .await?
            {
                CoverageObjectValidity::Valid
            } else {
                CoverageObjectValidity::InvalidPubKey
            },
            coverage_object,
        })
    }

    pub fn validate_coverage_objects<'a>(
        auth_client: &'a impl IsAuthorized,
        coverage_objects: impl Stream<Item = CoverageObjectIngestReport> + 'a,
    ) -> impl Stream<Item = anyhow::Result<Self>> + 'a {
        coverage_objects.then(move |coverage_object_report| async move {
            Self::validate(coverage_object_report.report, auth_client).await
        })
    }

    pub fn is_valid(&self) -> bool {
        matches!(self.validity, CoverageObjectValidity::Valid)
    }

    pub fn key(&self) -> KeyType<'_> {
        match self.coverage_object.key_type {
            coverage::KeyType::CbsdId(ref cbsd) => KeyType::Cbrs(cbsd.as_str()),
            coverage::KeyType::HotspotKey(ref hotspot_key) => KeyType::Wifi(hotspot_key),
        }
    }

    pub async fn write(&self, coverage_objects: &FileSinkClient) -> anyhow::Result<()> {
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
            INSERT INTO coverage_objects (uuid, radio_type, radio_key, indoor, coverage_claim_time, trust_score, inserted_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
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
    pub urbanized: Assignment,
    pub footfall: Assignment,
}

#[derive(Eq, Debug)]
struct IndoorCoverageLevel {
    radio_key: OwnedKeyType,
    seniority_timestamp: DateTime<Utc>,
    hotspot: PublicKeyBinary,
    signal_level: SignalLevel,
    urbanized: Assignment,
    footfall: Assignment,
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
    urbanized: Assignment,
    footfall: Assignment,
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
    pub boosted_hex_info: BoostedHex,
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
                SELECT co.uuid, h.hex, co.indoor, co.radio_key, h.signal_level, h.signal_power, co.coverage_claim_time, co.inserted_at, h.urbanized, h.footfall
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
    indoor_cbrs: HashMap<CellIndex, BTreeMap<SignalLevel, BinaryHeap<IndoorCoverageLevel>>>,
    indoor_wifi: HashMap<CellIndex, BTreeMap<SignalLevel, BinaryHeap<IndoorCoverageLevel>>>,
    outdoor_cbrs: HashMap<CellIndex, BinaryHeap<OutdoorCoverageLevel>>,
    outdoor_wifi: HashMap<CellIndex, BinaryHeap<OutdoorCoverageLevel>>,
}

pub const MAX_INDOOR_RADIOS_PER_RES12_HEX: usize = 1;
pub const MAX_OUTDOOR_RADIOS_PER_RES12_HEX: usize = 3;
pub const OUTDOOR_REWARD_WEIGHTS: [Decimal; 3] = [dec!(1.0), dec!(0.50), dec!(0.25)];

impl CoveredHexes {
    /// Aggregate the coverage. Returns whether or not any of the hexes are boosted
    pub async fn aggregate_coverage<E>(
        &mut self,
        hotspot: &PublicKeyBinary,
        boosted_hexes: &BoostedHexes,
        covered_hexes: impl Stream<Item = Result<HexCoverage, E>>,
    ) -> Result<bool, E> {
        let mut covered_hexes = std::pin::pin!(covered_hexes);
        let mut boosted = false;

        while let Some(hex_coverage) = covered_hexes.next().await.transpose()? {
            boosted |= boosted_hexes.is_boosted(&(hex_coverage.hex as u64));
            match (hex_coverage.indoor, &hex_coverage.radio_key) {
                (true, OwnedKeyType::Cbrs(_)) => {
                    insert_indoor_coverage(&mut self.indoor_cbrs, hotspot, hex_coverage);
                }
                (true, OwnedKeyType::Wifi(_)) => {
                    insert_indoor_coverage(&mut self.indoor_wifi, hotspot, hex_coverage);
                }
                (false, OwnedKeyType::Cbrs(_)) => {
                    insert_outdoor_coverage(&mut self.outdoor_cbrs, hotspot, hex_coverage);
                }
                (false, OwnedKeyType::Wifi(_)) => {
                    insert_outdoor_coverage(&mut self.outdoor_wifi, hotspot, hex_coverage);
                }
            }
        }

        Ok(boosted)
    }

    /// Returns the radios that should be rewarded for giving coverage.
    pub fn into_coverage_rewards(
        self,
        boosted_hexes: &BoostedHexes,
        epoch_start: DateTime<Utc>,
    ) -> impl Iterator<Item = CoverageReward> + '_ {
        let outdoor_cbrs_rewards =
            into_outdoor_rewards(self.outdoor_cbrs, boosted_hexes, epoch_start);

        let outdoor_wifi_rewards =
            into_outdoor_rewards(self.outdoor_wifi, boosted_hexes, epoch_start);

        let indoor_cbrs_rewards = into_indoor_rewards(self.indoor_cbrs, boosted_hexes, epoch_start);
        let indoor_wifi_rewards = into_indoor_rewards(self.indoor_wifi, boosted_hexes, epoch_start);

        outdoor_cbrs_rewards
            .chain(outdoor_wifi_rewards)
            .chain(indoor_cbrs_rewards)
            .chain(indoor_wifi_rewards)
            .filter(|r| r.points > Decimal::ZERO)
    }
}

fn insert_indoor_coverage(
    indoor: &mut HashMap<CellIndex, BTreeMap<SignalLevel, BinaryHeap<IndoorCoverageLevel>>>,
    hotspot: &PublicKeyBinary,
    hex_coverage: HexCoverage,
) {
    indoor
        .entry(CellIndex::try_from(hex_coverage.hex as u64).unwrap())
        .or_default()
        .entry(hex_coverage.signal_level)
        .or_default()
        .push(IndoorCoverageLevel {
            radio_key: hex_coverage.radio_key,
            seniority_timestamp: hex_coverage.coverage_claim_time,
            signal_level: hex_coverage.signal_level,
            hotspot: hotspot.clone(),
            urbanized: hex_coverage.urbanized,
            footfall: hex_coverage.footfall,
        })
}

fn insert_outdoor_coverage(
    outdoor: &mut HashMap<CellIndex, BinaryHeap<OutdoorCoverageLevel>>,
    hotspot: &PublicKeyBinary,
    hex_coverage: HexCoverage,
) {
    outdoor
        .entry(CellIndex::try_from(hex_coverage.hex as u64).unwrap())
        .or_default()
        .push(OutdoorCoverageLevel {
            radio_key: hex_coverage.radio_key,
            seniority_timestamp: hex_coverage.coverage_claim_time,
            signal_level: hex_coverage.signal_level,
            signal_power: hex_coverage.signal_power,
            hotspot: hotspot.clone(),
            urbanized: hex_coverage.urbanized,
            footfall: hex_coverage.footfall,
        });
}

fn into_outdoor_rewards(
    outdoor: HashMap<CellIndex, BinaryHeap<OutdoorCoverageLevel>>,
    boosted_hexes: &BoostedHexes,
    epoch_start: DateTime<Utc>,
) -> impl Iterator<Item = CoverageReward> + '_ {
    outdoor.into_iter().flat_map(move |(hex, radios)| {
        radios
            .into_sorted_vec()
            .into_iter()
            .take(MAX_OUTDOOR_RADIOS_PER_RES12_HEX)
            .zip(OUTDOOR_REWARD_WEIGHTS)
            .map(move |(cl, rank)| {
                let (boost_multiplier, oracle_multiplier) = boosted_hexes
                    .get_current_multiplier(hex.into(), epoch_start)
                    .map_or_else(
                        || {
                            (
                                NonZeroU32::new(1).unwrap(),
                                footfall_and_urbanization_multiplier(cl.footfall, cl.urbanized),
                            )
                        },
                        |multiplier| (multiplier, dec!(1.0)),
                    );

                CoverageReward {
                    points: cl.coverage_points() * oracle_multiplier * rank,
                    hotspot: cl.hotspot,
                    radio_key: cl.radio_key,
                    boosted_hex_info: BoostedHex {
                        location: hex.into(),
                        multiplier: boost_multiplier,
                    },
                }
            })
    })
}

fn into_indoor_rewards(
    indoor: HashMap<CellIndex, BTreeMap<SignalLevel, BinaryHeap<IndoorCoverageLevel>>>,
    boosted_hexes: &BoostedHexes,
    epoch_start: DateTime<Utc>,
) -> impl Iterator<Item = CoverageReward> + '_ {
    indoor
        .into_iter()
        .flat_map(move |(hex, mut radios)| {
            radios.pop_last().map(move |(_, radios)| {
                radios
                    .into_sorted_vec()
                    .into_iter()
                    .take(MAX_INDOOR_RADIOS_PER_RES12_HEX)
                    .map(move |cl| {
                        let (boost_multiplier, oracle_multiplier) = boosted_hexes
                            .get_current_multiplier(hex.into(), epoch_start)
                            .map_or_else(
                                || {
                                    (
                                        NonZeroU32::new(1).unwrap(),
                                        footfall_and_urbanization_multiplier(
                                            cl.footfall,
                                            cl.urbanized,
                                        ),
                                    )
                                },
                                |multiplier| (multiplier, dec!(1.0)),
                            );

                        CoverageReward {
                            points: cl.coverage_points() * oracle_multiplier,
                            hotspot: cl.hotspot,
                            radio_key: cl.radio_key,
                            boosted_hex_info: BoostedHex {
                                location: hex.into(),
                                multiplier: boost_multiplier,
                            },
                        }
                    })
            })
        })
        .flatten()
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

/// A cache for coverage object hex information needed to validate heartbeats
pub struct CoverageObjectCache {
    pool: PgPool,
    /// Covered hexes that have been cached
    hex_coverage: Arc<Cache<uuid::Uuid, Vec<CellIndex>>>,
}

impl CoverageObjectCache {
    pub fn new(pool: &Pool<Postgres>) -> Self {
        let hex_coverage = Arc::new(Cache::new());
        let hex_coverage_clone = hex_coverage.clone();
        tokio::spawn(async move {
            hex_coverage_clone
                .monitor(4, 0.25, std::time::Duration::from_secs(60 * 60 * 3))
                .await
        });
        Self {
            pool: pool.clone(),
            hex_coverage,
        }
    }

    pub async fn fetch_coverage_object(
        &self,
        uuid: &Uuid,
        key: KeyType<'_>,
    ) -> Result<Option<CachedCoverageObject<'_>>, sqlx::Error> {
        let coverage_meta: Option<CoverageObjectMeta> = sqlx::query_as(
	    "SELECT inserted_at, indoor FROM coverage_objects WHERE uuid = $1 AND radio_key = $2 AND invalidated_at IS NULL LIMIT 1"
	)
	.bind(uuid)
	.bind(key)
	.fetch_optional(&self.pool)
	.await?;
        // If we get a None back from the previous query, the coverage object does not exist
        // or has been invalidated
        let Some(coverage_meta) = coverage_meta else {
            return Ok(None);
        };
        // Check if the hexes have already been inserted into the cache:
        let coverage = if let Some(hexes) = self.hex_coverage.get(uuid).await {
            Some(hexes)
        } else {
            // If they haven't, query them from the database:
            let hexes: Vec<i64> = sqlx::query_scalar("SELECT hex FROM hexes WHERE uuid = $1")
                .bind(uuid)
                .fetch_all(&self.pool)
                .await?;
            let hexes = hexes
                .into_iter()
                .map(|x| CellIndex::try_from(x as u64).unwrap())
                .collect();
            self.hex_coverage
                .insert(
                    *uuid,
                    hexes,
                    // Let's say... three days?
                    std::time::Duration::from_secs(60 * 60 * 24 * 3),
                )
                .await;
            self.hex_coverage.get(uuid).await
        };
        Ok(coverage.map(coverage_meta.into_constructor()))
    }
}

#[derive(Clone, FromRow)]
pub struct CoverageObjectMeta {
    pub inserted_at: DateTime<Utc>,
    pub indoor: bool,
}

impl CoverageObjectMeta {
    pub fn into_constructor(
        self,
    ) -> impl FnOnce(CacheReadGuard<'_, Vec<CellIndex>>) -> CachedCoverageObject<'_> {
        move |covered_hexes| CachedCoverageObject {
            meta: self,
            covered_hexes,
        }
    }
}

pub struct CachedCoverageObject<'a> {
    pub meta: CoverageObjectMeta,
    pub covered_hexes: CacheReadGuard<'a, Vec<CellIndex>>,
}

impl CachedCoverageObject<'_> {
    /// Max distance in meters between the hex coverage and the given Lat Long
    pub fn max_distance_m(&self, latlng: LatLng) -> f64 {
        self.covered_hexes.iter().fold(0.0, |curr_max, curr_cov| {
            let cov = LatLng::from(*curr_cov);
            curr_max.max(cov.distance_m(latlng))
        })
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;
    use chrono::NaiveDate;
    use futures::stream::iter;

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
                &BoostedHexes::default(),
                iter(vec![
                    anyhow::Ok(indoor_cbrs_hex_coverage("1", SignalLevel::None, None)),
                    anyhow::Ok(indoor_cbrs_hex_coverage("2", SignalLevel::Low, None)),
                    anyhow::Ok(indoor_cbrs_hex_coverage("3", SignalLevel::High, None)),
                    anyhow::Ok(indoor_cbrs_hex_coverage("4", SignalLevel::Low, None)),
                    anyhow::Ok(indoor_cbrs_hex_coverage("5", SignalLevel::None, None)),
                ]),
            )
            .await
            .unwrap();
        let rewards: Vec<_> = covered_hexes
            .into_coverage_rewards(&BoostedHexes::default(), Utc::now())
            .collect();
        assert_eq!(
            rewards,
            vec![CoverageReward {
                radio_key: OwnedKeyType::Cbrs("3".to_string()),
                hotspot: owner,
                points: dec!(400),
                boosted_hex_info: BoostedHex {
                    location: 0x8a1fb46622dffff_u64,
                    multiplier: NonZeroU32::new(1).unwrap(),
                },
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
            urbanized: Assignment::A,
            footfall: Assignment::C,
        }
    }

    /// Test to ensure that if there are more than five radios with the highest signal
    /// level in a given hex, that the five oldest radios are chosen.
    #[tokio::test]
    async fn ensure_oldest_radio_selected() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let mut covered_hexes = CoveredHexes::default();
        covered_hexes
            .aggregate_coverage(
                &owner,
                &BoostedHexes::default(),
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
        let rewards: Vec<_> = covered_hexes
            .into_coverage_rewards(&BoostedHexes::default(), Utc::now())
            .collect();
        assert_eq!(
            rewards,
            vec![CoverageReward {
                radio_key: OwnedKeyType::Cbrs("10".to_string()),
                hotspot: owner.clone(),
                points: dec!(400),
                boosted_hex_info: BoostedHex {
                    location: 0x8a1fb46622dffff_u64,
                    multiplier: NonZeroU32::new(1).unwrap(),
                },
            }]
        );
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
                &BoostedHexes::default(),
                iter(vec![
                    anyhow::Ok(outdoor_cbrs_hex_coverage("1", -946, date(2022, 8, 1))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("2", -936, date(2022, 12, 5))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("3", -887, date(2022, 12, 2))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("4", -887, date(2022, 12, 1))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("5", -773, date(2023, 5, 1))),
                ]),
            )
            .await
            .unwrap();
        let rewards: Vec<_> = covered_hexes
            .into_coverage_rewards(&BoostedHexes::default(), Utc::now())
            .collect();
        assert_eq!(
            rewards,
            vec![
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("5".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(16),
                    boosted_hex_info: BoostedHex {
                        location: 0x8a1fb46622dffff_u64,
                        multiplier: NonZeroU32::new(1).unwrap(),
                    },
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("4".to_string()),
                    hotspot: owner.clone(),
                    points: dec!(8),
                    boosted_hex_info: BoostedHex {
                        location: 0x8a1fb46622dffff_u64,
                        multiplier: NonZeroU32::new(1).unwrap(),
                    },
                },
                CoverageReward {
                    radio_key: OwnedKeyType::Cbrs("3".to_string()),
                    hotspot: owner,
                    points: dec!(4),
                    boosted_hex_info: BoostedHex {
                        location: 0x8a1fb46622dffff_u64,
                        multiplier: NonZeroU32::new(1).unwrap(),
                    },
                }
            ]
        );
    }

    #[tokio::test]
    async fn hip_105_ensure_all_types_get_rewards() -> anyhow::Result<()> {
        let mut covered_hexes = CoveredHexes::default();
        let boosted_hexes = BoostedHexes::default();

        let outdoor_cbrs_owner1 =
            PublicKeyBinary::from_str("11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL")?;
        covered_hexes
            .aggregate_coverage(
                &outdoor_cbrs_owner1,
                &boosted_hexes,
                iter(vec![
                    anyhow::Ok(outdoor_cbrs_hex_coverage("oco1-1", -700, date(2024, 2, 20))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("oco1-2", -700, date(2024, 2, 21))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("oco1-3", -699, date(2024, 2, 23))),
                    anyhow::Ok(outdoor_cbrs_hex_coverage("oco1-4", -700, date(2024, 2, 19))),
                ]),
            )
            .await?;

        let indoor_cbrs_owner1 =
            PublicKeyBinary::from_str("11PfLUsMAfsozjy2kcERF43UuhNAhicEQM8ioutFM322Eu37D4m")?;
        covered_hexes
            .aggregate_coverage(
                &indoor_cbrs_owner1,
                &boosted_hexes,
                iter(vec![
                    anyhow::Ok(indoor_cbrs_hex_coverage(
                        "ico1-1",
                        SignalLevel::High,
                        Some(date(2024, 2, 20)),
                    )),
                    anyhow::Ok(indoor_cbrs_hex_coverage(
                        "ico1-2",
                        SignalLevel::High,
                        Some(date(2024, 2, 21)),
                    )),
                ]),
            )
            .await?;

        let outdoor_wifi_owner1 =
            PublicKeyBinary::from_str("1trSuseczBVbfpbJjefoFsuPazRSrzJjCXaKJPU9B3HKJvb6sdqTepcbY2zWBq2yMTt7Jsf7NZCm28ez856kDa5MT3Ja1gh8HyZWS8k9LCSFSWiDNG2YmcCpLgnhGrEw9FriCVPLuXaciQ2Fu9ztW7r1U1Pv64i3HvpkC4mmQWE9DSq7tgiNkNhNuWBA3Sf8KbtefMPofTxjCsfVCUKX2ow8MScn82CK6vWUZNUPonpTJydKVLNiMGfvceY1MsfXtHdx6bUCjoFoZNkikAzEpgArczJV9CdhkBjKX3xLVLpehdrBDGu8aBLfRbNJ4RRz9Gj4pHFnBhFq78tRGi1USpnf6Dohp9bA18qr4XdPJc59Qz")?;
        covered_hexes
            .aggregate_coverage(
                &outdoor_wifi_owner1,
                &boosted_hexes,
                iter(vec![anyhow::Ok(outdoor_wifi_hex_coverage(
                    &outdoor_wifi_owner1,
                    -700,
                    date(2024, 2, 20),
                ))]),
            )
            .await?;

        let outdoor_wifi_owner2 =
            PublicKeyBinary::from_str("1trSuseerjmxKaD43hSLPD1oWTt6Y6svqJX7WJecMwKKcRk1355AQp1GSkUNV7fnL9QYGpZSS378XoxmaHte5PCD64NYzJ1x7bBNdq6qBRFRDqTW1PGPMatjX3i18Y39hh8Ngsephg93YCZoVbvfGc5YMmtvxqqP4WXy4UxmiTZ6uuYzPV5U31piAFVxaUhTZtoQLCyLzAZEks8bj2cP6EyEFMecHb9Vq76d4qnXdjARvFim7xACkBKHTnAwEEN8wfWfGEw5QBQMfpvvSLUerL64xR72tT3SrM7qUXk9m7fTbLuwg8XfUKEs2iqhPSfSu2v4DpcKY7L4fvu8BT2WsMChC3xaPWWiibTVatoNLNxTH6")?;
        covered_hexes
            .aggregate_coverage(
                &outdoor_wifi_owner2,
                &boosted_hexes,
                iter(vec![anyhow::Ok(outdoor_wifi_hex_coverage(
                    &outdoor_wifi_owner2,
                    -700,
                    date(2024, 2, 21),
                ))]),
            )
            .await?;

        let outdoor_wifi_owner3 =
            PublicKeyBinary::from_str("1trSusefexd9C3purVPScg433RXrVb5kU9hLTTKjuc2dTju9udy2rsgAYUTjhxARa9ewZAW1PdsjsErGyaHNJKNDkjHfzuHZmPm7vWK3A13sckxRbwSBtBXAMg4nyChmoJ5JgZVeeHBtdYX69emPoDD8niKSx5vkkoBw5g1AYS7S4LfnpGhCtwKA8PzjjzE3ZY6dWQjm2oCut31ScsH9nZfBHriHpkTbNK8KttkFRU3ax3wdJXmN996PPbYsgm1wx8ctU9iU6Q7FvTvVkZTGfHHcH8J38YCuWB9LfizRSueKWNSPbfsrJgQe3otTYtdU7NDWWQzrpv3yATS2NJzyorKpjg8hH5J2krtU3KdByBgZdU")?;
        covered_hexes
            .aggregate_coverage(
                &outdoor_wifi_owner3,
                &boosted_hexes,
                iter(vec![anyhow::Ok(outdoor_wifi_hex_coverage(
                    &outdoor_wifi_owner3,
                    -699,
                    date(2024, 2, 23),
                ))]),
            )
            .await?;

        let outdoor_wifi_owner4 =
            PublicKeyBinary::from_str("1trSusf5rnmrUHqyv28ksYJGBBkHZi821ss7vLkBchUPi6vxDHpHGoscCftuHddxpaHgMacxD7fyHESf8Ht3JpRjebZnTzZMwqs6u6z2v8S7VZzxjv5KkaNZpX2CPYGYNfVRWC2tovSaUwEdc3P6Tyk6S9axAw7WM9pP2sxyEqWiCmyzhCnnd8xhZqaTKtiyoamvVTXqB1iZaUFX2KtSB6pLVGrDCUxGs7x4PrMrgAcPcdDF1jrF6s7EpAR9MjRHv6qxstoSHnGMpTeZaXLJEhySqtnSvyQEJaT218zuDSoHArKRUSQ9ViWE55T8hbwsVDusNDdayS4JG2fRMoDkj8LPYHvhMtVzQUDSg1ufFEFukh")?;
        covered_hexes
            .aggregate_coverage(
                &outdoor_wifi_owner4,
                &boosted_hexes,
                iter(vec![anyhow::Ok(outdoor_wifi_hex_coverage(
                    &outdoor_wifi_owner4,
                    -700,
                    date(2024, 2, 19),
                ))]),
            )
            .await?;

        let indoor_wifi_owner1 =
            PublicKeyBinary::from_str("1trSusf79ALaHuYSxUcvHLQEtHUgTnc25rfjyUUSKDs9AUwvXkJ7CQoGMrY7RhVpXyKYH4HaDnekRLYTUq5pczPk4XqnzXnACrwbY5CzhzdTSQkHRN2LuHvgQeySeh4LvjfhRP3Cru89zTGNNGMDXkpASuz3NkQx3ctqnTcdrjLgcBavQQmASofARxrqSPUz4UFTU1Gp4eRdaJgu1G7ys1f8NsjH5WU6bi5N4U5cWRVQkC7FEJZsGFn1sNferANVwkkSR2NLEpwYvL5qpGTYtk7zcqPrHY5hNC6jkjWhM5S4JPDYzZNcxRW28ekRCp2igJCqErA3APbcwkaZPXUxpqFJGWqu6GJf7aZRKz8R9cNAmd")?;
        covered_hexes
            .aggregate_coverage(
                &indoor_wifi_owner1,
                &boosted_hexes,
                iter(vec![anyhow::Ok(indoor_wifi_hex_coverage(
                    &indoor_wifi_owner1,
                    SignalLevel::High,
                    date(2024, 2, 19),
                ))]),
            )
            .await?;

        let indoor_wifi_owner2 =
            PublicKeyBinary::from_str("1trSusew4P9SVD2q9GjvNYf8e5qEH2NmZRQDnkXodQt1fmpcR9cG28iA3LJD476H31wrNcr6jbfBhoPdyJvfepQonxXH7kUDjpMcVJqfMZGeQyXQvaxxnPh4yzoPonpS5RM6VSmsk4WNczr6nBUa49ak1XM8s5DCGSRZEPqWhXfG9urQ8hgDSYdkd61eBcWmThRKLAfarT5cE1ZaeexFtUgjgRBUGd7ifCtchZTgkWTa9WVsMdpWTjFd8GUkaTekX1RWzFDtFyETGZHbD6wDut729EfoBuSKowJuwFv2LYZr7Cw4qKPmVboDBpem1ZramSq3PmatdrpNHHipXniz4Z1vtM1vfgtJ57o8BrWSQNuD9B")?;
        covered_hexes
            .aggregate_coverage(
                &indoor_wifi_owner2,
                &boosted_hexes,
                iter(vec![anyhow::Ok(indoor_wifi_hex_coverage(
                    &indoor_wifi_owner2,
                    SignalLevel::High,
                    date(2024, 2, 20),
                ))]),
            )
            .await?;

        //Calculate coverage points
        let rewards: Vec<_> = covered_hexes
            .into_coverage_rewards(&boosted_hexes, Utc::now())
            .collect();

        // assert outdoor cbrs radios
        assert_eq!(
            dec!(16),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Cbrs("oco1-3".to_string()))
                .unwrap()
                .points
        );

        assert_eq!(
            dec!(8),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Cbrs("oco1-4".to_string()))
                .unwrap()
                .points
        );

        assert_eq!(
            dec!(4),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Cbrs("oco1-1".to_string()))
                .unwrap()
                .points
        );

        assert_eq!(
            None,
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Cbrs("oco1-2".to_string()))
        );

        // assert indoor cbrs radios
        assert_eq!(
            dec!(400),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Cbrs("ico1-1".to_string()))
                .unwrap()
                .points
        );

        assert_eq!(
            None,
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Cbrs("ico1-2".to_string()))
        );

        //assert outdoor wifi radios
        assert_eq!(
            dec!(16),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Wifi(outdoor_wifi_owner3.clone()))
                .unwrap()
                .points
        );

        assert_eq!(
            dec!(8),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Wifi(outdoor_wifi_owner4.clone()))
                .unwrap()
                .points
        );

        assert_eq!(
            dec!(4),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Wifi(outdoor_wifi_owner1.clone()))
                .unwrap()
                .points
        );

        assert_eq!(
            None,
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Wifi(outdoor_wifi_owner2.clone()))
        );

        //assert indoor wifi radios
        assert_eq!(
            dec!(400),
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Wifi(indoor_wifi_owner1.clone()))
                .unwrap()
                .points
        );

        assert_eq!(
            None,
            rewards
                .iter()
                .find(|r| r.radio_key == OwnedKeyType::Wifi(indoor_wifi_owner2.clone()))
        );

        Ok(())
    }

    fn indoor_cbrs_hex_coverage(
        cbsd_id: &str,
        signal_level: SignalLevel,
        coverage_claim_time: Option<DateTime<Utc>>,
    ) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: true,
            radio_key: OwnedKeyType::Cbrs(cbsd_id.to_string()),
            signal_level,
            // Signal power is ignored for indoor radios:
            signal_power: 0,
            coverage_claim_time: coverage_claim_time.unwrap_or(DateTime::<Utc>::MIN_UTC),
            inserted_at: DateTime::<Utc>::MIN_UTC,
            urbanized: Assignment::A,
            footfall: Assignment::C,
        }
    }

    fn outdoor_cbrs_hex_coverage(
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
            urbanized: Assignment::A,
            footfall: Assignment::C,
        }
    }

    fn outdoor_wifi_hex_coverage(
        hotspot_key: &PublicKeyBinary,
        signal_power: i32,
        coverage_claim_time: DateTime<Utc>,
    ) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: false,
            radio_key: OwnedKeyType::Wifi(hotspot_key.clone()),
            signal_power,
            signal_level: SignalLevel::High,
            coverage_claim_time,
            inserted_at: DateTime::<Utc>::MIN_UTC,
            urbanized: Assignment::A,
            footfall: Assignment::C,
        }
    }

    fn indoor_wifi_hex_coverage(
        hotspot_key: &PublicKeyBinary,
        signal_level: SignalLevel,
        coverage_claim_time: DateTime<Utc>,
    ) -> HexCoverage {
        HexCoverage {
            uuid: Uuid::new_v4(),
            hex: 0x8a1fb46622dffff_u64 as i64,
            indoor: true,
            radio_key: OwnedKeyType::Wifi(hotspot_key.clone()),
            signal_power: 0,
            signal_level,
            coverage_claim_time,
            inserted_at: DateTime::<Utc>::MIN_UTC,
            urbanized: Assignment::A,
            footfall: Assignment::C,
        }
    }
}
