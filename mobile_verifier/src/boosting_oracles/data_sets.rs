use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::pin,
};

use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload::FileUpload,
    traits::{TimestampDecode, TimestampEncode},
    FileStore, FileType,
};
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_proto::services::poc_mobile as proto;
use lazy_static::lazy_static;
use regex::Regex;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use sqlx::{FromRow, PgPool, QueryBuilder};
use task_manager::{ManagedTask, TaskManager};
use tokio::{fs::File, io::AsyncWriteExt, time::Instant};

use crate::{
    boosting_oracles::assignment::HexAssignments,
    coverage::{NewCoverageObjectNotification, SignalLevel},
    Settings,
};

use super::{
    footfall::Footfall, landtype::Landtype, urbanization::Urbanization, HexAssignment, HexBoostData,
};

#[async_trait::async_trait]
pub trait DataSet: HexAssignment + Send + Sync + 'static {
    const TYPE: DataSetType;

    fn timestamp(&self) -> Option<DateTime<Utc>>;

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()>;

    fn is_ready(&self) -> bool;

    async fn fetch_first_data_set(
        &mut self,
        pool: &PgPool,
        data_set_directory: &Path,
    ) -> anyhow::Result<()> {
        let Some(first_data_set) = db::fetch_latest_processed_data_set(pool, Self::TYPE).await?
        else {
            return Ok(());
        };
        let path = get_data_set_path(data_set_directory, Self::TYPE, first_data_set.time_to_use);
        self.update(Path::new(&path), first_data_set.time_to_use)?;
        Ok(())
    }

    async fn check_for_available_data_sets(
        &self,
        store: &FileStore,
        pool: &PgPool,
    ) -> anyhow::Result<()> {
        let mut new_data_sets = store.list(Self::TYPE.to_prefix(), self.timestamp(), None);
        while let Some(new_data_set) = new_data_sets.next().await.transpose()? {
            db::insert_new_data_set(pool, &new_data_set.key, Self::TYPE, new_data_set.timestamp)
                .await?;
        }
        Ok(())
    }

    async fn fetch_next_available_data_set(
        &mut self,
        store: &FileStore,
        pool: &PgPool,
        data_set_directory: &Path,
    ) -> anyhow::Result<Option<NewDataSet>> {
        self.check_for_available_data_sets(store, pool).await?;

        tracing::info!("Checking for new {} data sets", Self::TYPE.to_prefix());
        let latest_unprocessed_data_set =
            db::fetch_latest_unprocessed_data_set(pool, Self::TYPE, self.timestamp()).await?;

        let Some(latest_unprocessed_data_set) = latest_unprocessed_data_set else {
            return Ok(None);
        };

        let path = get_data_set_path(
            data_set_directory,
            Self::TYPE,
            latest_unprocessed_data_set.time_to_use,
        );

        if !latest_unprocessed_data_set.status.is_downloaded() {
            download_data_set(store, &latest_unprocessed_data_set.filename, &path).await?;
            latest_unprocessed_data_set.mark_as_downloaded(pool).await?;
            tracing::info!(
                data_set = latest_unprocessed_data_set.filename,
                "Data set download complete"
            );
        }

        self.update(Path::new(&path), latest_unprocessed_data_set.time_to_use)?;

        Ok(Some(latest_unprocessed_data_set))
    }
}

pub struct DataSetDownloaderDaemon<A, B, C> {
    pool: PgPool,
    data_sets: HexBoostData<A, B, C>,
    store: FileStore,
    oracle_boosting_sink: FileSinkClient,
    data_set_directory: PathBuf,
    new_coverage_object_notification: NewCoverageObjectNotification,
}

#[derive(FromRow)]
pub struct NewDataSet {
    filename: String,
    time_to_use: DateTime<Utc>,
    status: DataSetStatus,
}

impl NewDataSet {
    async fn mark_as_downloaded(&self, pool: &PgPool) -> anyhow::Result<()> {
        db::set_data_set_status(pool, &self.filename, DataSetStatus::Downloaded).await?;
        Ok(())
    }

    async fn mark_as_processed(&self, pool: &PgPool) -> anyhow::Result<()> {
        db::set_data_set_status(pool, &self.filename, DataSetStatus::Processed).await?;
        Ok(())
    }
}

#[derive(Copy, Clone, sqlx::Type)]
#[sqlx(type_name = "data_set_status")]
#[sqlx(rename_all = "lowercase")]
pub enum DataSetStatus {
    Pending,
    Downloaded,
    Processed,
}

impl DataSetStatus {
    pub fn is_downloaded(&self) -> bool {
        matches!(self, Self::Downloaded)
    }
}

impl<A, B, C> ManagedTask for DataSetDownloaderDaemon<A, B, C>
where
    A: DataSet,
    B: DataSet,
    C: DataSet,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::prelude::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(async move {
            #[rustfmt::skip]
            tokio::select! {
                biased;
                _ = shutdown.clone() => Ok(()),
                result = self.run() => result,
            }
        });
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl DataSetDownloaderDaemon<Footfall, Landtype, Urbanization> {
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        new_coverage_object_notification: NewCoverageObjectNotification,
    ) -> anyhow::Result<impl ManagedTask> {
        let (oracle_boosting_reports, oracle_boosting_reports_server) =
            file_sink::FileSinkBuilder::new(
                FileType::OracleBoostingReport,
                settings.store_base_path(),
                file_upload.clone(),
                concat!(env!("CARGO_PKG_NAME"), "_oracle_boosting_report"),
            )
            .auto_commit(true)
            .roll_time(Duration::minutes(15))
            .create()
            .await?;

        let urbanization = Urbanization::new();
        let footfall = Footfall::new();
        let landtype = Landtype::new();
        let hex_boost_data = HexBoostData::builder()
            .footfall(footfall)
            .landtype(landtype)
            .urbanization(urbanization)
            .build()?;

        let data_set_downloader = Self::new(
            pool,
            hex_boost_data,
            FileStore::from_settings(&settings.data_sets).await?,
            oracle_boosting_reports,
            settings.data_sets_directory.clone(),
            new_coverage_object_notification,
        );

        Ok(TaskManager::builder()
            .add_task(oracle_boosting_reports_server)
            .add_task(data_set_downloader)
            .build())
    }
}

impl<A, B, C> DataSetDownloaderDaemon<A, B, C>
where
    A: DataSet,
    B: DataSet,
    C: DataSet,
{
    pub fn new(
        pool: PgPool,
        data_sets: HexBoostData<A, B, C>,
        store: FileStore,
        oracle_boosting_sink: FileSinkClient,
        data_set_directory: PathBuf,
        new_coverage_object_notification: NewCoverageObjectNotification,
    ) -> Self {
        Self {
            pool,
            data_sets,
            store,
            oracle_boosting_sink,
            data_set_directory,
            new_coverage_object_notification,
        }
    }

    async fn check_for_new_data_sets(&mut self) -> anyhow::Result<()> {
        let new_urbanized = self
            .data_sets
            .urbanization
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;
        let new_footfall = self
            .data_sets
            .footfall
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;
        let new_landtype = self
            .data_sets
            .landtype
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;

        // If all of the data sets are ready and there is at least one new one, re-process all
        // hex assignments:
        let new_data_set =
            new_urbanized.is_some() || new_footfall.is_some() || new_landtype.is_some();
        if self.data_sets.is_ready() && new_data_set {
            tracing::info!("Processing new data sets");
            set_all_oracle_boosting_assignments(
                &self.pool,
                &self.data_sets,
                &self.oracle_boosting_sink,
            )
            .await?;
        }

        // Mark the new data sets as processed and delete the old ones
        if let Some(new_urbanized) = new_urbanized {
            new_urbanized.mark_as_processed(&self.pool).await?;
            delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::Urbanization,
                new_urbanized.time_to_use,
            )
            .await?;
        }
        if let Some(new_footfall) = new_footfall {
            new_footfall.mark_as_processed(&self.pool).await?;
            delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::Footfall,
                new_footfall.time_to_use,
            )
            .await?;
        }
        if let Some(new_landtype) = new_landtype {
            new_landtype.mark_as_processed(&self.pool).await?;
            delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::Landtype,
                new_landtype.time_to_use,
            )
            .await?;
        }

        Ok(())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let poll_duration = Duration::minutes(1);

        self.data_sets
            .urbanization
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;
        self.data_sets
            .footfall
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;
        self.data_sets
            .landtype
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;

        // Attempt to fill in any unassigned hexes. This is for the edge case in
        // which we shutdown before a coverage object updates.
        if self.data_sets.is_ready() {
            set_unassigned_oracle_boosting_assignments(
                &self.pool,
                &self.data_sets,
                &self.oracle_boosting_sink,
            )
            .await?;
        }

        let mut wakeup = Instant::now() + poll_duration.to_std()?;
        loop {
            #[rustfmt::skip]
            tokio::select! {
                _ = self.new_coverage_object_notification.await_new_coverage_object() => {
                    // If we see a new coverage object, we want to assign only those hexes
                    // that don't have an assignment
                    if self.data_sets.is_ready() {
                        set_unassigned_oracle_boosting_assignments(
                            &self.pool,
                            &self.data_sets,
                            &self.oracle_boosting_sink
                        ).await?;
                    }
                },
                _ = tokio::time::sleep_until(wakeup) => {
                    self.check_for_new_data_sets().await?;
                    wakeup = Instant::now() + poll_duration.to_std()?;
                }
            }
        }
    }
}

fn get_data_set_path(
    data_set_directory: &Path,
    data_set_type: DataSetType,
    time_to_use: DateTime<Utc>,
) -> PathBuf {
    let path = PathBuf::from(format!(
        "{}.{}.res10.h3tree",
        data_set_type.to_prefix(),
        time_to_use.timestamp_millis()
    ));
    let mut dir = data_set_directory.to_path_buf();
    dir.push(path);
    dir
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"([a-z,_]+).(\d+)(.res10.h3tree)?").unwrap();
}

async fn delete_old_data_sets(
    data_set_directory: &Path,
    data_set_type: DataSetType,
    time_to_use: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut data_sets = tokio::fs::read_dir(data_set_directory).await?;
    while let Some(data_set) = data_sets.next_entry().await? {
        let file_name = data_set.file_name();
        let file_name = file_name.to_string_lossy();
        let Some(cap) = RE.captures(&file_name) else {
            tracing::warn!("Could not determine data set file type: {}", file_name);
            continue;
        };
        let prefix = &cap[1];
        let timestamp = cap[2].parse::<u64>()?.to_timestamp_millis()?;
        if prefix == data_set_type.to_prefix() && timestamp < time_to_use {
            tracing::info!(data_set = &*file_name, "Deleting old data set file");
            tokio::fs::remove_file(data_set.path()).await?;
        }
    }
    Ok(())
}

async fn download_data_set(
    store: &FileStore,
    in_file_name: &str,
    out_path: &Path,
) -> anyhow::Result<()> {
    tracing::info!("Downloading new data set: {}", out_path.to_string_lossy());
    let stream = store.get_raw(in_file_name).await?;
    let mut bytes = tokio_util::codec::FramedRead::new(
        async_compression::tokio::bufread::GzipDecoder::new(tokio_util::io::StreamReader::new(
            stream,
        )),
        tokio_util::codec::BytesCodec::new(),
    );
    let mut file = File::create(&out_path).await?;
    while let Some(bytes) = bytes.next().await.transpose()? {
        file.write_all(&bytes).await?;
    }
    Ok(())
}

#[derive(Copy, Clone, sqlx::Type)]
#[sqlx(type_name = "data_set_type")]
#[sqlx(rename_all = "lowercase")]
pub enum DataSetType {
    Urbanization,
    Footfall,
    Landtype,
}

impl DataSetType {
    pub fn to_prefix(self) -> &'static str {
        match self {
            Self::Urbanization => "urbanization",
            Self::Footfall => "footfall",
            Self::Landtype => "landtype",
        }
    }
}

pub mod db {
    use super::*;

    pub async fn fetch_latest_file_date(
        pool: &PgPool,
        data_set_type: DataSetType,
    ) -> sqlx::Result<Option<DateTime<Utc>>> {
        sqlx::query_scalar("SELECT time_to_use FROM data_sets WHERE data_set = $1 ORDER BY time_to_use DESC LIMIT 1")
            .bind(data_set_type)
            .fetch_optional(pool)
            .await
    }

    pub async fn insert_new_data_set(
        pool: &PgPool,
        filename: &str,
        data_set_type: DataSetType,
        time_to_use: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO data_sets (filename, data_set, time_to_use, status)
            VALUES ($1, $2, $3, 'pending')
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(filename)
        .bind(data_set_type)
        .bind(time_to_use)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_latest_unprocessed_data_set(
        pool: &PgPool,
        data_set_type: DataSetType,
        since: Option<DateTime<Utc>>,
    ) -> sqlx::Result<Option<NewDataSet>> {
        sqlx::query_as(
            "SELECT filename, time_to_use, status FROM data_sets WHERE status != 'processed' AND data_set = $1 AND COALESCE(time_to_use > $2, TRUE) AND time_to_use <= $3 ORDER BY time_to_use DESC LIMIT 1"
        )
        .bind(data_set_type)
        .bind(since)
        .bind(Utc::now())
        .fetch_optional(pool)
        .await
    }

    pub async fn fetch_latest_processed_data_set(
        pool: &PgPool,
        data_set_type: DataSetType,
    ) -> sqlx::Result<Option<NewDataSet>> {
        sqlx::query_as(
            "SELECT filename, time_to_use, status FROM data_sets WHERE status = 'processed' AND data_set = $1 ORDER BY time_to_use DESC LIMIT 1"
        )
        .bind(data_set_type)
        .fetch_optional(pool)
        .await
    }

    pub async fn set_data_set_status(
        pool: &PgPool,
        filename: &str,
        status: DataSetStatus,
    ) -> sqlx::Result<()> {
        sqlx::query("UPDATE data_sets SET status = $1 WHERE filename = $2")
            .bind(status)
            .bind(filename)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn fetch_time_of_latest_processed_data_set(
        pool: &PgPool,
        data_set_type: DataSetType,
    ) -> sqlx::Result<Option<DateTime<Utc>>> {
        sqlx::query_scalar(
            "SELECT time_to_use FROM data_sets WHERE status = 'processed' AND data_set = $1 ORDER BY time_to_use DESC LIMIT 1"
        )
        .bind(data_set_type)
        .fetch_optional(pool)
        .await
    }

    /// Check if there are any pending or downloaded files prior to the given reward period
    pub async fn check_for_unprocessed_data_sets(
        pool: &PgPool,
        period_end: DateTime<Utc>,
    ) -> sqlx::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) > 0 FROM data_sets WHERE time_to_use <= $1 AND status != 'processed'",
        )
        .bind(period_end)
        .fetch_one(pool)
        .await?
            || sqlx::query_scalar(
                r#"
                SELECT COUNT(*) > 0 FROM coverage_objects
                WHERE inserted_at < $1 AND uuid IN (
                        SELECT
                           DISTINCT uuid
                        FROM
                           hexes
                        WHERE
                           urbanized IS NULL
                           OR footfall IS NULL
                           OR landtype IS NULL
                )
                "#,
            )
            .bind(period_end)
            .fetch_one(pool)
            .await?)
    }

    pub fn fetch_all_hexes(pool: &PgPool) -> impl Stream<Item = sqlx::Result<UnassignedHex>> + '_ {
        sqlx::query_as("SELECT uuid, hex, signal_level, signal_power FROM hexes").fetch(pool)
    }

    pub fn fetch_hexes_with_null_assignments(
        pool: &PgPool,
    ) -> impl Stream<Item = sqlx::Result<UnassignedHex>> + '_ {
        sqlx::query_as(
            "SELECT
                uuid, hex, signal_level, signal_power
            FROM
                hexes
            WHERE
                urbanized IS NULL
                OR footfall IS NULL
                OR landtype IS NULL",
        )
        .fetch(pool)
    }
}

pub struct AssignedCoverageObjects {
    pub coverage_objs: HashMap<uuid::Uuid, Vec<AssignedHex>>,
}

impl AssignedCoverageObjects {
    pub async fn assign_hex_stream(
        stream: impl Stream<Item = sqlx::Result<UnassignedHex>>,
        data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment, impl HexAssignment>,
    ) -> anyhow::Result<Self> {
        let mut coverage_objs = HashMap::<uuid::Uuid, Vec<AssignedHex>>::new();
        let mut stream = pin!(stream);
        while let Some(hex) = stream.try_next().await? {
            let hex = hex.assign(data_sets)?;
            coverage_objs.entry(hex.uuid).or_default().push(hex);
        }
        Ok(Self { coverage_objs })
    }

    async fn write(&self, boosting_reports: &FileSinkClient) -> file_store::Result {
        let timestamp = Utc::now().encode_timestamp();
        for (uuid, hexes) in self.coverage_objs.iter() {
            let assignments: Vec<_> = hexes
                .iter()
                .map(|hex| {
                    let location = format!("{:x}", hex.hex);
                    let assignment_multiplier = (hex.assignments.boosting_multiplier()
                        * dec!(1000))
                    .to_u32()
                    .unwrap_or(0);
                    proto::OracleBoostingHexAssignment {
                        location,
                        urbanized: hex.assignments.urbanized.into(),
                        footfall: hex.assignments.footfall.into(),
                        landtype: hex.assignments.landtype.into(),
                        assignment_multiplier,
                    }
                })
                .collect();
            boosting_reports
                .write(
                    proto::OracleBoostingReportV1 {
                        coverage_object: Vec::from(uuid.into_bytes()),
                        assignments,
                        timestamp,
                    },
                    &[],
                )
                .await?;
        }

        Ok(())
    }

    pub async fn save(self, pool: &PgPool) -> anyhow::Result<()> {
        const NUMBER_OF_FIELDS_IN_QUERY: u16 = 7;
        const ASSIGNMENTS_MAX_BATCH_ENTRIES: usize =
            (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

        let assigned_hexes: Vec<_> = self.coverage_objs.into_values().flatten().collect();
        for assigned_hexes in assigned_hexes.chunks(ASSIGNMENTS_MAX_BATCH_ENTRIES) {
            QueryBuilder::new(
                "INSERT INTO hexes (uuid, hex, signal_level, signal_power, footfall, landtype, urbanized)",
            )
                .push_values(assigned_hexes, |mut b, hex| {
                    b.push_bind(hex.uuid)
                        .push_bind(hex.hex as i64)
                        .push_bind(hex.signal_level)
                        .push_bind(hex.signal_power)
                        .push_bind(hex.assignments.footfall)
                        .push_bind(hex.assignments.landtype)
                        .push_bind(hex.assignments.urbanized);
                })
                .push(
                    r#"
                    ON CONFLICT (uuid, hex) DO UPDATE SET
                        footfall = EXCLUDED.footfall,
                        landtype = EXCLUDED.landtype,
                        urbanized = EXCLUDED.urbanized
                    "#,
                )
                .build()
                .execute(pool)
                .await?;
        }

        Ok(())
    }
}

#[derive(FromRow)]
pub struct UnassignedHex {
    uuid: uuid::Uuid,
    #[sqlx(try_from = "i64")]
    hex: u64,
    signal_level: SignalLevel,
    signal_power: i32,
}

impl UnassignedHex {
    fn assign(
        self,
        data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment, impl HexAssignment>,
    ) -> anyhow::Result<AssignedHex> {
        let cell = hextree::Cell::try_from(self.hex)?;
        let assignments = HexAssignments::builder(cell)
            .footfall(&data_sets.footfall)
            .landtype(&data_sets.landtype)
            .urbanized(&data_sets.urbanization)
            .build()?;
        Ok(AssignedHex {
            uuid: self.uuid,
            hex: self.hex,
            signal_level: self.signal_level,
            signal_power: self.signal_power,
            assignments,
        })
    }
}

pub struct AssignedHex {
    pub uuid: uuid::Uuid,
    pub hex: u64,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
    pub assignments: HexAssignments,
}

pub async fn set_all_oracle_boosting_assignments(
    pool: &PgPool,
    data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment, impl HexAssignment>,
    file_sink: &FileSinkClient,
) -> anyhow::Result<()> {
    let assigned_coverage_objs =
        AssignedCoverageObjects::assign_hex_stream(db::fetch_all_hexes(pool), data_sets).await?;
    assigned_coverage_objs.write(file_sink).await?;
    assigned_coverage_objs.save(pool).await?;
    Ok(())
}

pub async fn set_unassigned_oracle_boosting_assignments(
    pool: &PgPool,
    data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment, impl HexAssignment>,
    file_sink: &FileSinkClient,
) -> anyhow::Result<()> {
    let assigned_coverage_objs = AssignedCoverageObjects::assign_hex_stream(
        db::fetch_hexes_with_null_assignments(pool),
        data_sets,
    )
    .await?;
    assigned_coverage_objs.write(file_sink).await?;
    assigned_coverage_objs.save(pool).await?;
    Ok(())
}
