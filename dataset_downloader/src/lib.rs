use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use hex_assignments::assignment::HexAssignments;
use hex_assignments::HexBoostDataAssignmentsExt;
use hextree::disktree::DiskTreeMap;
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use sqlx::{PgConnection, Type};
use tokio::{fs::File, io::AsyncWriteExt};

use file_store::{traits::TimestampDecode, FileStore};
use hex_assignments::{
    footfall::Footfall, landtype::Landtype, service_provider_override::ServiceProviderOverride,
    urbanization::Urbanization, HexAssignment, HexBoostData,
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
#[derive(FromRow)]
pub struct UnassignedHex {
    pub uuid: uuid::Uuid,
    #[sqlx(try_from = "i64")]
    pub hex: u64,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
}

pub struct AssignedHex {
    pub uuid: uuid::Uuid,
    pub hex: u64,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
    pub assignments: HexAssignments,
}

impl UnassignedHex {
    pub fn assign(
        self,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<AssignedHex> {
        let cell = hextree::Cell::try_from(self.hex)?;

        Ok(AssignedHex {
            uuid: self.uuid,
            hex: self.hex,
            signal_level: self.signal_level,
            signal_power: self.signal_power,
            assignments: data_sets.assignments(cell)?,
        })
    }
}

#[async_trait::async_trait]
pub trait NewDataSetHandler: Send + Sync + 'static {
    // Calls when new data set arrived but before it marked as processed
    // If this function fails, new data sets will not be marked as processed.
    async fn callback(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        data_sets: &HexBoostData,
    ) -> anyhow::Result<()>;
}

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
        tracing::info!("Checking for new {} data sets", Self::TYPE.to_prefix());
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
            let con = &mut pool.acquire().await?;
            latest_unprocessed_data_set.mark_as_downloaded(con).await?;
            tracing::info!(
                data_set = latest_unprocessed_data_set.filename,
                "Data set download complete"
            );
        }

        self.update(Path::new(&path), latest_unprocessed_data_set.time_to_use)?;

        Ok(Some(latest_unprocessed_data_set))
    }
}

#[async_trait::async_trait]
impl DataSet for Footfall {
    const TYPE: DataSetType = DataSetType::Footfall;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.footfall = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.footfall.is_some()
    }
}

#[async_trait::async_trait]
impl DataSet for Landtype {
    const TYPE: DataSetType = DataSetType::Landtype;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.landtype = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.landtype.is_some()
    }
}

#[async_trait::async_trait]
impl DataSet for Urbanization {
    const TYPE: DataSetType = DataSetType::Urbanization;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.urbanized = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.urbanized.is_some()
    }
}

#[async_trait::async_trait]
impl DataSet for ServiceProviderOverride {
    const TYPE: DataSetType = DataSetType::ServiceProviderOverride;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.service_provider_override = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.service_provider_override.is_some()
    }
}

pub struct DataSetDownloader {
    pool: PgPool,
    store: FileStore,
    data_set_directory: PathBuf,
}

#[derive(FromRow, Debug)]
pub struct NewDataSet {
    filename: String,
    time_to_use: DateTime<Utc>,
    status: DataSetStatus,
}

impl NewDataSet {
    async fn mark_as_downloaded(&self, con: &mut PgConnection) -> anyhow::Result<()> {
        db::set_data_set_status(con, &self.filename, DataSetStatus::Downloaded).await?;
        Ok(())
    }

    async fn mark_as_processed(&self, con: &mut PgConnection) -> anyhow::Result<()> {
        db::set_data_set_status(con, &self.filename, DataSetStatus::Processed).await?;
        Ok(())
    }

    pub fn filename(&self) -> &String {
        &self.filename
    }
}

#[derive(Copy, Clone, sqlx::Type, Debug)]
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

pub fn is_hex_boost_data_ready(data_sets: &HexBoostData) -> bool {
    let h = &data_sets;
    h.urbanization.is_ready()
        && h.footfall.is_ready()
        && h.landtype.is_ready()
        && h.service_provider_override.is_ready()
}

impl DataSetDownloader {
    pub fn new(pool: PgPool, store: FileStore, data_set_directory: PathBuf) -> Self {
        Self {
            pool,
            store,
            data_set_directory,
        }
    }

    pub async fn check_for_new_data_sets(
        &mut self,
        data_set_processor: Option<&dyn NewDataSetHandler>,
        mut data_sets: HexBoostData,
    ) -> anyhow::Result<HexBoostData> {
        let new_urbanized = data_sets
            .urbanization
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;
        let new_footfall = data_sets
            .footfall
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;
        let new_landtype = data_sets
            .landtype
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;
        let new_service_provider_override = data_sets
            .service_provider_override
            .fetch_next_available_data_set(&self.store, &self.pool, &self.data_set_directory)
            .await?;

        let new_data_set = new_urbanized.is_some()
            || new_footfall.is_some()
            || new_landtype.is_some()
            || new_service_provider_override.is_some();

        if !new_data_set {
            return Ok(data_sets);
        }

        let mut txn = self.pool.begin().await?;

        if let Some(dsp) = data_set_processor {
            if is_hex_boost_data_ready(&data_sets) {
                tracing::info!("Processing new data sets");
                dsp.callback(&mut txn, &data_sets).await?;
            }
        }

        // Mark the new data sets as processed and delete the old ones
        if let Some(ref new_urbanized) = new_urbanized {
            new_urbanized.mark_as_processed(&mut txn).await?;
        }

        if let Some(ref new_footfall) = new_footfall {
            new_footfall.mark_as_processed(&mut txn).await?;
        }
        if let Some(ref new_landtype) = new_landtype {
            new_landtype.mark_as_processed(&mut txn).await?;
        }

        if let Some(ref new_service_provider_override) = new_service_provider_override {
            new_service_provider_override
                .mark_as_processed(&mut txn)
                .await?;
        }
        txn.commit().await?;

        // Ignoring tracing error messages can be critical if server out of space
        if let Some(new_urbanized) = new_urbanized {
            if let Err(err) = delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::Urbanization,
                new_urbanized.time_to_use,
            )
            .await
            {
                tracing::error!(
                    error = ?err,
                    data_set_directory = ?self.data_set_directory,
                    time_to_use = ?new_urbanized.time_to_use,
                    "Deleting old urbanized data set file is failed."
                );
            }
        }
        if let Some(new_footfall) = new_footfall {
            if let Err(err) = delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::Footfall,
                new_footfall.time_to_use,
            )
            .await
            {
                tracing::error!(
                    error = ?err,
                    data_set_directory = ?self.data_set_directory,
                    time_to_use = ?new_footfall.time_to_use,
                    "Deleting old fotfall data set file is failed."
                );
            }
        }
        if let Some(new_landtype) = new_landtype {
            if let Err(err) = delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::Landtype,
                new_landtype.time_to_use,
            )
            .await
            {
                tracing::error!(
                    error = ?err,
                    data_set_directory = ?self.data_set_directory,
                    time_to_use = ?new_landtype.time_to_use,
                    "Deleting old landtype data set file is failed."
                );
            }
        }
        if let Some(new_service_provider_override) = new_service_provider_override {
            if let Err(err) = delete_old_data_sets(
                &self.data_set_directory,
                DataSetType::ServiceProviderOverride,
                new_service_provider_override.time_to_use,
            )
            .await
            {
                tracing::error!(
                    error = ?err,
                    data_set_directory = ?self.data_set_directory,
                    time_to_use = ?new_service_provider_override.time_to_use,
                    "Deleting old service_provider_override data set file is failed."
                );
            }
        }

        Ok(data_sets)
    }
    pub async fn fetch_first_datasets(
        &self,
        mut data_sets: HexBoostData,
    ) -> anyhow::Result<HexBoostData> {
        data_sets
            .urbanization
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;
        data_sets
            .footfall
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;
        data_sets
            .landtype
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;
        data_sets
            .service_provider_override
            .fetch_first_data_set(&self.pool, &self.data_set_directory)
            .await?;
        Ok(data_sets)
    }
}

fn get_data_set_path(
    data_set_directory: &Path,
    data_set_type: DataSetType,
    time_to_use: DateTime<Utc>,
) -> PathBuf {
    let path = PathBuf::from(format!(
        "{}.{}.{}.h3tree",
        data_set_type.to_prefix(),
        time_to_use.timestamp_millis(),
        data_set_type.to_hex_res_prefix(),
    ));
    let mut dir = data_set_directory.to_path_buf();
    dir.push(path);
    dir
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"([a-z,_]+).(\d+)(.res[0-9]{1,2}.h3tree)?").unwrap();
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
#[sqlx(rename_all = "snake_case")]
pub enum DataSetType {
    Urbanization,
    Footfall,
    Landtype,
    ServiceProviderOverride,
}

impl DataSetType {
    pub fn to_prefix(self) -> &'static str {
        match self {
            Self::Urbanization => "urbanization",
            Self::Footfall => "footfall",
            Self::Landtype => "landtype",
            Self::ServiceProviderOverride => "service_provider_override",
        }
    }

    pub fn to_hex_res_prefix(self) -> &'static str {
        match self {
            Self::Urbanization => "res10",
            Self::Footfall => "res10",
            Self::Landtype => "res10",
            Self::ServiceProviderOverride => "res12",
        }
    }
}
pub mod db {
    use super::*;

    pub async fn fetch_latest_file_date(
        pool: &PgPool,
        data_set_type: DataSetType,
    ) -> sqlx::Result<Option<DateTime<Utc>>> {
        sqlx::query_scalar("SELECT time_to_use FROM hex_assignment_data_set_status WHERE data_set = $1 ORDER BY time_to_use DESC LIMIT 1")
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
            INSERT INTO hex_assignment_data_set_status (filename, data_set, time_to_use, status)
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
            "SELECT filename, time_to_use, status FROM hex_assignment_data_set_status WHERE status != 'processed' AND data_set = $1 AND COALESCE(time_to_use > $2, TRUE) AND time_to_use <= $3 ORDER BY time_to_use DESC LIMIT 1"
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
            "SELECT filename, time_to_use, status FROM hex_assignment_data_set_status WHERE status = 'processed' AND data_set = $1 ORDER BY time_to_use DESC LIMIT 1"
        )
        .bind(data_set_type)
        .fetch_optional(pool)
        .await
    }

    pub async fn set_data_set_status(
        con: &mut PgConnection,
        filename: &str,
        status: DataSetStatus,
    ) -> sqlx::Result<()> {
        sqlx::query("UPDATE hex_assignment_data_set_status SET status = $1 WHERE filename = $2")
            .bind(status)
            .bind(filename)
            .execute(con)
            .await?;
        Ok(())
    }

    pub async fn fetch_time_of_latest_processed_data_set(
        pool: &PgPool,
        data_set_type: DataSetType,
    ) -> sqlx::Result<Option<DateTime<Utc>>> {
        sqlx::query_scalar(
            "SELECT time_to_use FROM hex_assignment_data_set_status WHERE status = 'processed' AND data_set = $1 ORDER BY time_to_use DESC LIMIT 1"
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
            "SELECT COUNT(*) > 0 FROM hex_assignment_data_set_status WHERE time_to_use <= $1 AND status != 'processed'",
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
                           OR service_provider_override IS NULL
                )
                "#,
            )
            .bind(period_end)
            .fetch_one(pool)
            .await?)
    }

    pub fn fetch_all_hexes(
        con: &mut PgConnection,
    ) -> impl Stream<Item = sqlx::Result<UnassignedHex>> + '_ {
        sqlx::query_as("SELECT uuid, hex, signal_level, signal_power FROM hexes").fetch(con)
    }

    pub fn fetch_hexes_with_null_assignments(
        con: &mut PgConnection,
    ) -> impl Stream<Item = sqlx::Result<UnassignedHex>> + '_ {
        sqlx::query_as(
            "SELECT
                uuid, hex, signal_level, signal_power
            FROM
                hexes
            WHERE
                urbanized IS NULL
                OR footfall IS NULL
                OR landtype IS NULL
                OR service_provider_override IS NULL",
        )
        .fetch(con)
    }
}
