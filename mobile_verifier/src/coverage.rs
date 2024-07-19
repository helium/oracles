use crate::{
    heartbeats::{HbType, KeyType, OwnedKeyType},
    seniority::Seniority,
    IsAuthorized, Settings,
};
use chrono::{DateTime, Utc};
use file_store::{
    coverage::{self, CoverageObjectIngestReport},
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::{self, FileSinkClient},
    file_source,
    file_upload::FileUpload,
    traits::TimestampEncode,
    FileStore, FileType,
};
use futures::{
    stream::{BoxStream, Stream, StreamExt},
    TryFutureExt, TryStreamExt,
};
use h3o::{CellIndex, LatLng};
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{self as proto, CoverageObjectValidity, SignalLevel as SignalLevelProto},
};
use hex_assignments::assignment::HexAssignments;
use hextree::Cell;
use mobile_config::client::AuthorizationClient;
use retainer::{entry::CacheReadGuard, Cache};

use sqlx::{FromRow, PgPool, Pool, Postgres, QueryBuilder, Transaction, Type};
use std::{
    pin::pin,
    sync::Arc,
    time::{Duration, Instant},
};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::{channel, Receiver, Sender};
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

impl From<SignalLevel> for coverage_map::SignalLevel {
    fn from(value: SignalLevel) -> Self {
        match value {
            SignalLevel::None => coverage_map::SignalLevel::None,
            SignalLevel::Low => coverage_map::SignalLevel::Low,
            SignalLevel::Medium => coverage_map::SignalLevel::Medium,
            SignalLevel::High => coverage_map::SignalLevel::High,
        }
    }
}

pub struct CoverageDaemon {
    pool: Pool<Postgres>,
    auth_client: AuthorizationClient,
    coverage_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
    coverage_obj_sink: FileSinkClient<proto::CoverageObjectV1>,
    new_coverage_object_notifier: NewCoverageObjectNotifier,
}

impl CoverageDaemon {
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: FileStore,
        auth_client: AuthorizationClient,
        new_coverage_object_notifier: NewCoverageObjectNotifier,
    ) -> anyhow::Result<impl ManagedTask> {
        let (valid_coverage_objs, valid_coverage_objs_server) = file_sink::FileSinkBuilder::new(
            FileType::CoverageObject,
            settings.store_base_path(),
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_coverage_object"),
        )
        .auto_commit(false)
        .roll_time(Duration::from_secs(15 * 60))
        .create()
        .await?;

        let (coverage_objs, coverage_objs_server) =
            file_source::continuous_source::<CoverageObjectIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::CoverageObjectIngestReport.to_string())
                .create()
                .await?;

        // let hex_boost_data = boosting_oracles::make_hex_boost_data(settings, geofence)?;
        let coverage_daemon = CoverageDaemon::new(
            pool,
            auth_client,
            coverage_objs,
            valid_coverage_objs,
            new_coverage_object_notifier,
        );

        Ok(TaskManager::builder()
            .add_task(valid_coverage_objs_server)
            .add_task(coverage_objs_server)
            .add_task(coverage_daemon)
            .build())
    }

    pub fn new(
        pool: PgPool,
        auth_client: AuthorizationClient,
        coverage_objs: Receiver<FileInfoStream<CoverageObjectIngestReport>>,
        coverage_obj_sink: FileSinkClient<proto::CoverageObjectV1>,
        new_coverage_object_notifier: NewCoverageObjectNotifier,
    ) -> Self {
        Self {
            pool,
            auth_client,
            coverage_objs,
            coverage_obj_sink,
            new_coverage_object_notifier,
        }
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => {
                    tracing::info!("CoverageDaemon shutting down");
                    break;
                }
                Some(file) = self.coverage_objs.recv() => {
                    let start = Instant::now();
                    self.process_file(file).await?;
                    metrics::histogram!("coverage_object_processing_time")
                        .record(start.elapsed());
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

        // Tell the data set manager to update the assignments.
        self.new_coverage_object_notifier.notify();

        Ok(())
    }
}

impl ManagedTask for CoverageDaemon {
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

pub struct NewCoverageObjectNotifier(Sender<()>);

impl NewCoverageObjectNotifier {
    fn notify(&self) {
        let _ = self.0.try_send(());
    }
}

pub struct NewCoverageObjectNotification(Receiver<()>);

impl NewCoverageObjectNotification {
    pub async fn await_new_coverage_object(&mut self) {
        let _ = self.0.recv().await;
    }
}

pub fn new_coverage_object_notification_channel(
) -> (NewCoverageObjectNotifier, NewCoverageObjectNotification) {
    let (tx, rx) = channel(1);
    (
        NewCoverageObjectNotifier(tx),
        NewCoverageObjectNotification(rx),
    )
}

#[derive(Clone)]
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

    pub async fn write(
        &self,
        coverage_objects: &FileSinkClient<proto::CoverageObjectV1>,
    ) -> anyhow::Result<()> {
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

    pub async fn save(&self, transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
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

#[derive(Debug, Clone, FromRow)]
pub struct HexCoverage {
    pub uuid: Uuid,
    #[sqlx(try_from = "i64")]
    pub hex: Cell,
    pub indoor: bool,
    pub radio_key: OwnedKeyType,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
    pub coverage_claim_time: DateTime<Utc>,
    pub inserted_at: DateTime<Utc>,
    #[sqlx(flatten)]
    pub assignments: HexAssignments,
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
                SELECT co.uuid, h.hex, co.indoor, co.radio_key, h.signal_level, h.signal_power, co.coverage_claim_time, co.inserted_at, h.urbanized, h.footfall, h.landtype
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
