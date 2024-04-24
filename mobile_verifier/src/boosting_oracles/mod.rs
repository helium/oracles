pub mod assignment;
pub mod footfall;
pub mod landtype;
pub mod urbanization;

use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use crate::boosting_oracles::assignment::HexAssignments;
use crate::coverage::SignalLevel;
pub use assignment::Assignment;
use chrono::{DateTime, Duration, Utc};
use file_store::{file_sink::FileSinkClient, traits::TimestampEncode, FileStore};
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use helium_proto::services::poc_mobile as proto;
use hextree::disktree::DiskTreeMap;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use sqlx::{FromRow, PgPool, QueryBuilder};
use task_manager::ManagedTask;
use tokio::{fs::File, io::AsyncWriteExt, sync::Mutex};
pub use urbanization::Urbanization;
use uuid::Uuid;

pub trait DataSet: HexAssignment + Send + Sync + 'static {
    const TYPE: DataSetType;

    fn timestamp(&self) -> Option<DateTime<Utc>>;

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()>;

    fn is_ready(&self) -> bool;
}

pub struct DataSetDownloaderDaemon<T, A, B, C> {
    pool: PgPool,
    data_set: Arc<Mutex<T>>,
    data_sets: HexBoostData<A, B, C>,
    store: FileStore,
    oracle_boosting_sink: FileSinkClient,
    data_set_directory: PathBuf,
    latest_file_date: Option<DateTime<Utc>>,
}

#[derive(FromRow)]
pub struct NewDataSet {
    filename: String,
    time_to_use: DateTime<Utc>,
    status: DataSetStatus,
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

impl<T, A, B, C> ManagedTask for DataSetDownloaderDaemon<T, A, B, C>
where
    T: DataSet,
    A: HexAssignment,
    B: HexAssignment,
    C: HexAssignment,
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

impl<T, A, B, C> DataSetDownloaderDaemon<T, A, B, C>
where
    T: DataSet,
    A: HexAssignment,
    B: HexAssignment,
    C: HexAssignment,
{
    pub async fn new(
        pool: PgPool,
        data_set: Arc<Mutex<T>>,
        data_sets: HexBoostData<A, B, C>,
        store: FileStore,
        oracle_boosting_sink: FileSinkClient,
        data_set_directory: PathBuf,
    ) -> anyhow::Result<Self> {
        let latest_file_date = db::fetch_latest_file_date(&pool, T::TYPE).await?;
        Ok(Self {
            pool,
            data_set,
            data_sets,
            store,
            oracle_boosting_sink,
            data_set_directory,
            latest_file_date,
        })
    }

    fn get_data_set_path(&self, time_to_use: DateTime<Utc>) -> PathBuf {
        let path = PathBuf::from(format!(
            "{}.{}.res10.h3tree",
            T::TYPE.to_prefix(),
            time_to_use.timestamp()
        ));
        let mut dir = self.data_set_directory.clone();
        dir.push(path);
        dir
    }

    pub async fn check_for_available_data_sets(&mut self) -> anyhow::Result<()> {
        let mut new_data_sets = self
            .store
            .list(T::TYPE.to_prefix(), self.latest_file_date, None);
        while let Some(new_data_set) = new_data_sets.next().await.transpose()? {
            tracing::info!(
                "Found new data set: {}, {:#?}",
                new_data_set.key,
                new_data_set
            );
            db::insert_new_data_set(
                &self.pool,
                &new_data_set.key,
                T::TYPE,
                new_data_set.timestamp,
            )
            .await?;
            self.latest_file_date = Some(new_data_set.timestamp);
        }
        Ok(())
    }

    pub async fn process_data_sets(&self) -> anyhow::Result<()> {
        tracing::info!("Checking for new data sets");
        let mut data_set = self.data_set.lock().await;
        let latest_unprocessed_data_set =
            db::fetch_latest_unprocessed_data_set(&self.pool, T::TYPE, data_set.timestamp())
                .await?;

        let Some(latest_unprocessed_data_set) = latest_unprocessed_data_set else {
            return Ok(());
        };

        // If there is an unprocessed data set, download it (if we need to) and process it.

        let path = self.get_data_set_path(latest_unprocessed_data_set.time_to_use);

        // Download the file if it hasn't been downloaded already:
        if !latest_unprocessed_data_set.status.is_downloaded() {
            download_data_set(&self.store, &latest_unprocessed_data_set.filename, &path).await?;
            db::set_data_set_status(
                &self.pool,
                &latest_unprocessed_data_set.filename,
                DataSetStatus::Downloaded,
            )
            .await?;
            tracing::info!(
                data_set = latest_unprocessed_data_set.filename,
                "Data set download complete"
            );
        }

        // Now that we've downloaded the file, load it into the data set
        data_set.update(Path::new(&path), latest_unprocessed_data_set.time_to_use)?;

        // Release the lock as it is no longer needed
        drop(data_set);

        // Update the hexes
        let boosting_reports = set_oracle_boosting_assignments(
            UnassignedHex::fetch_all(&self.pool),
            &self.data_sets,
            &self.pool,
        )
        .await?;

        db::set_data_set_status(
            &self.pool,
            &latest_unprocessed_data_set.filename,
            DataSetStatus::Processed,
        )
        .await?;

        self.oracle_boosting_sink
            .write_all(boosting_reports)
            .await?;
        self.oracle_boosting_sink.commit().await?;
        tracing::info!(
            data_set = latest_unprocessed_data_set.filename,
            "Data set processing complete"
        );

        Ok(())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        // Get the first data set:
        if let Some(time_to_use) =
            db::fetch_time_of_latest_processed_data_set(&self.pool, T::TYPE).await?
        {
            let data_set_path = self.get_data_set_path(time_to_use);
            tracing::info!(
                "Found initial {} data set: {}",
                T::TYPE.to_prefix(),
                data_set_path.to_string_lossy()
            );
            self.data_set
                .lock()
                .await
                .update(&data_set_path, time_to_use)?;
        }

        let poll_duration = Duration::minutes(5);

        loop {
            self.check_for_available_data_sets().await?;
            self.process_data_sets().await?;
            tokio::time::sleep(poll_duration.to_std()?).await;
        }
    }
}

async fn download_data_set(
    store: &FileStore,
    in_file_name: &str,
    out_path: &Path,
) -> anyhow::Result<()> {
    tracing::info!("Downloading new data set: {}", out_path.to_string_lossy());
    // TODO: abstract this out to a function
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
        sqlx::query_scalar(
            "SELECT COUNT(*) > 0 FROM data_sets WHERE time_to_use <= $1 AND status != 'processed'",
        )
        .bind(period_end)
        .fetch_one(pool)
        .await
    }
}

pub trait HexAssignment: Send + Sync + 'static {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
}

impl HexAssignment for HashMap<hextree::Cell, Assignment> {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(*self.get(&cell).unwrap())
    }
}

impl HexAssignment for Assignment {
    fn assignment(&self, _cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(*self)
    }
}

pub struct HexBoostData<Foot, Land, Urban> {
    pub footfall: Arc<Mutex<Foot>>,
    pub landtype: Arc<Mutex<Land>>,
    pub urbanization: Arc<Mutex<Urban>>,
}

impl<F, L, U> Clone for HexBoostData<F, L, U> {
    fn clone(&self) -> Self {
        Self {
            footfall: self.footfall.clone(),
            landtype: self.landtype.clone(),
            urbanization: self.urbanization.clone(),
        }
    }
}

impl<Foot, Land, Urban> HexBoostData<Foot, Land, Urban> {
    pub fn new(footfall: Foot, landtype: Land, urbanization: Urban) -> Self {
        Self {
            urbanization: Arc::new(Mutex::new(urbanization)),
            footfall: Arc::new(Mutex::new(footfall)),
            landtype: Arc::new(Mutex::new(landtype)),
        }
    }
}

impl<Foot, Land, Urban> HexBoostData<Foot, Land, Urban>
where
    Foot: DataSet,
    Land: DataSet,
    Urban: DataSet,
{
    pub async fn is_ready(&self) -> bool {
        self.urbanization.lock().await.is_ready()
            && self.footfall.lock().await.is_ready()
            && self.landtype.lock().await.is_ready()
    }
}

impl<Foot, Land, Urban> HexBoostData<Foot, Land, Urban>
where
    Foot: HexAssignment,
    Land: HexAssignment,
    Urban: HexAssignment,
{
    pub async fn assignments(&self, cell: hextree::Cell) -> anyhow::Result<HexAssignments> {
        let footfall = self.footfall.lock().await;
        let landtype = self.landtype.lock().await;
        let urbanization = self.urbanization.lock().await;
        HexAssignments::from_data_sets(cell, &*footfall, &*landtype, &*urbanization)
    }
}

trait DiskTreeLike: Send + Sync {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>>;
}

impl DiskTreeLike for DiskTreeMap {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        self.get(cell)
    }
}

impl DiskTreeLike for std::collections::HashSet<hextree::Cell> {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(self.contains(&cell).then_some((cell, &[])))
    }
}

pub struct MockDiskTree;

impl DiskTreeLike for MockDiskTree {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(Some((cell, &[])))
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
    pub fn fetch_all(pool: &PgPool) -> impl Stream<Item = sqlx::Result<Self>> + '_ {
        sqlx::query_as("SELECT uuid, hex, signal_level, signal_power FROM hexes").fetch(pool)
    }

    pub fn fetch_unassigned(pool: &PgPool) -> impl Stream<Item = sqlx::Result<Self>> + '_ {
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

pub async fn set_oracle_boosting_assignments<'a>(
    unassigned_hexes: impl Stream<Item = sqlx::Result<UnassignedHex>>,
    data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment, impl HexAssignment>,
    pool: &'a PgPool,
) -> anyhow::Result<impl Iterator<Item = proto::OracleBoostingReportV1>> {
    const NUMBER_OF_FIELDS_IN_QUERY: u16 = 5;
    const ASSIGNMENTS_MAX_BATCH_ENTRIES: usize = (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

    let now = Utc::now();
    let mut boost_results = HashMap::<Uuid, Vec<proto::OracleBoostingHexAssignment>>::new();
    let mut unassigned_hexes = pin!(unassigned_hexes.try_chunks(ASSIGNMENTS_MAX_BATCH_ENTRIES));

    let urbanization = data_sets.urbanization.lock().await;
    let footfall = data_sets.footfall.lock().await;
    let landtype = data_sets.landtype.lock().await;

    while let Some(hexes) = unassigned_hexes.try_next().await? {
        let hexes: anyhow::Result<Vec<_>> = hexes
            .into_iter()
            .map(|hex| {
                let cell = hextree::Cell::try_from(hex.hex)?;
                let assignments =
                    HexAssignments::from_data_sets(cell, &*footfall, &*landtype, &*urbanization)?;
                let location = format!("{:x}", hex.hex);
                let assignment_multiplier = (assignments.boosting_multiplier() * dec!(1000))
                    .to_u32()
                    .unwrap_or(0);

                boost_results.entry(hex.uuid).or_default().push(
                    proto::OracleBoostingHexAssignment {
                        location,
                        urbanized: assignments.urbanized.into(),
                        footfall: assignments.footfall.into(),
                        landtype: assignments.landtype.into(),
                        assignment_multiplier,
                    },
                );

                Ok((
                    hex,
                    assignments.footfall,
                    assignments.landtype,
                    assignments.urbanized,
                ))
            })
            .collect();

        let mut transaction = pool.begin().await?;
        sqlx::query("LOCK TABLE hexes")
            .execute(&mut transaction)
            .await?;
        QueryBuilder::new(
            "INSERT INTO hexes (uuid, hex, signal_level, signal_power, footfall, landtype, urbanized)",
        )
        .push_values(hexes?, |mut b, (hex, footfall, landtype, urbanized)| {
            b.push_bind(hex.uuid)
                .push_bind(hex.hex as i64)
                .push_bind(hex.signal_level)
                .push_bind(hex.signal_power)
                .push_bind(footfall)
                .push_bind(landtype)
                .push_bind(urbanized);
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
        .execute(&mut transaction)
        .await?;
        transaction.commit().await?;
    }

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

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use hextree::{HexTreeMap, HexTreeSet};

    use crate::geofence::Geofence;

    use self::{footfall::Footfall, landtype::Landtype};

    use super::*;

    #[tokio::test]
    async fn test_hex_boost_data() -> anyhow::Result<()> {
        // This test will break if any of the logic deriving Assignments from
        // the underlying DiskTreeMap's changes.

        let unknown_cell = hextree::Cell::from_raw(0x8c2681a3064d9ff)?;

        // Types of Cells
        // yellow - POI ≥ 1 Urbanized
        let poi_built_urbanized = hextree::Cell::from_raw(0x8c2681a3064dbff)?;
        let poi_grass_urbanized = hextree::Cell::from_raw(0x8c2681a3064ddff)?;
        let poi_water_urbanized = hextree::Cell::from_raw(0x8c2681a3064e1ff)?;
        // orange - POI ≥ 1 Not Urbanized
        let poi_built_not_urbanized = hextree::Cell::from_raw(0x8c2681a3064e3ff)?;
        let poi_grass_not_urbanized = hextree::Cell::from_raw(0x8c2681a3064e5ff)?;
        let poi_water_not_urbanized = hextree::Cell::from_raw(0x8c2681a3064e7ff)?;
        // light green - Point of Interest Urbanized
        let poi_no_data_built_urbanized = hextree::Cell::from_raw(0x8c2681a3064e9ff)?;
        let poi_no_data_grass_urbanized = hextree::Cell::from_raw(0x8c2681a3064ebff)?;
        let poi_no_data_water_urbanized = hextree::Cell::from_raw(0x8c2681a3064edff)?;
        // dark green - Point of Interest Not Urbanized
        let poi_no_data_built_not_urbanized = hextree::Cell::from_raw(0x8c2681a306501ff)?;
        let poi_no_data_grass_not_urbanized = hextree::Cell::from_raw(0x8c2681a306503ff)?;
        let poi_no_data_water_not_urbanized = hextree::Cell::from_raw(0x8c2681a306505ff)?;
        // light blue - No POI Urbanized
        let no_poi_built_urbanized = hextree::Cell::from_raw(0x8c2681a306507ff)?;
        let no_poi_grass_urbanized = hextree::Cell::from_raw(0x8c2681a306509ff)?;
        let no_poi_water_urbanized = hextree::Cell::from_raw(0x8c2681a30650bff)?;
        // dark blue - No POI Not Urbanized
        let no_poi_built_not_urbanized = hextree::Cell::from_raw(0x8c2681a30650dff)?;
        let no_poi_grass_not_urbanized = hextree::Cell::from_raw(0x8c2681a306511ff)?;
        let no_poi_water_not_urbanized = hextree::Cell::from_raw(0x8c2681a306513ff)?;
        // gray - Outside of USA
        let poi_built_outside_us = hextree::Cell::from_raw(0x8c2681a306515ff)?;
        let poi_grass_outside_us = hextree::Cell::from_raw(0x8c2681a306517ff)?;
        let poi_water_outside_us = hextree::Cell::from_raw(0x8c2681a306519ff)?;
        let poi_no_data_built_outside_us = hextree::Cell::from_raw(0x8c2681a30651bff)?;
        let poi_no_data_grass_outside_us = hextree::Cell::from_raw(0x8c2681a30651dff)?;
        let poi_no_data_water_outside_us = hextree::Cell::from_raw(0x8c2681a306521ff)?;
        let no_poi_built_outside_us = hextree::Cell::from_raw(0x8c2681a306523ff)?;
        let no_poi_grass_outside_us = hextree::Cell::from_raw(0x8c2681a306525ff)?;
        let no_poi_water_outside_us = hextree::Cell::from_raw(0x8c2681a306527ff)?;

        // Footfall Data
        // POI         - footfalls > 1 for a POI across hexes
        // POI No Data - No footfalls for a POI across any hexes
        // NO POI      - Does not exist
        let mut footfall = HexTreeMap::<u8>::new();
        footfall.insert(poi_built_urbanized, 42);
        footfall.insert(poi_grass_urbanized, 42);
        footfall.insert(poi_water_urbanized, 42);
        footfall.insert(poi_built_not_urbanized, 42);
        footfall.insert(poi_grass_not_urbanized, 42);
        footfall.insert(poi_water_not_urbanized, 42);
        footfall.insert(poi_no_data_built_urbanized, 0);
        footfall.insert(poi_no_data_grass_urbanized, 0);
        footfall.insert(poi_no_data_water_urbanized, 0);
        footfall.insert(poi_no_data_built_not_urbanized, 0);
        footfall.insert(poi_no_data_grass_not_urbanized, 0);
        footfall.insert(poi_no_data_water_not_urbanized, 0);
        footfall.insert(poi_built_outside_us, 42);
        footfall.insert(poi_grass_outside_us, 42);
        footfall.insert(poi_water_outside_us, 42);
        footfall.insert(poi_no_data_built_outside_us, 0);
        footfall.insert(poi_no_data_grass_outside_us, 0);
        footfall.insert(poi_no_data_water_outside_us, 0);

        // Landtype Data
        // Map to enum values for Landtype
        // An unknown cell is considered Assignment::C
        let mut landtype = HexTreeMap::<u8>::new();
        landtype.insert(poi_built_urbanized, 50);
        landtype.insert(poi_grass_urbanized, 30);
        landtype.insert(poi_water_urbanized, 80);
        landtype.insert(poi_built_not_urbanized, 50);
        landtype.insert(poi_grass_not_urbanized, 30);
        landtype.insert(poi_water_not_urbanized, 80);
        landtype.insert(poi_no_data_built_urbanized, 50);
        landtype.insert(poi_no_data_grass_urbanized, 30);
        landtype.insert(poi_no_data_water_urbanized, 80);
        landtype.insert(poi_no_data_built_not_urbanized, 50);
        landtype.insert(poi_no_data_grass_not_urbanized, 30);
        landtype.insert(poi_no_data_water_not_urbanized, 80);
        landtype.insert(no_poi_built_urbanized, 50);
        landtype.insert(no_poi_grass_urbanized, 30);
        landtype.insert(no_poi_water_urbanized, 80);
        landtype.insert(no_poi_built_not_urbanized, 50);
        landtype.insert(no_poi_grass_not_urbanized, 30);
        landtype.insert(no_poi_water_not_urbanized, 80);
        landtype.insert(poi_built_outside_us, 50);
        landtype.insert(poi_grass_outside_us, 30);
        landtype.insert(poi_water_outside_us, 80);
        landtype.insert(poi_no_data_built_outside_us, 50);
        landtype.insert(poi_no_data_grass_outside_us, 30);
        landtype.insert(poi_no_data_water_outside_us, 80);
        landtype.insert(no_poi_built_outside_us, 50);
        landtype.insert(no_poi_grass_outside_us, 30);
        landtype.insert(no_poi_water_outside_us, 80);

        // Urbanized data
        // Urban     - something in the map, and in the geofence
        // Not Urban - nothing in the map, but in the geofence
        // Outside   - not in the geofence, urbanized hex never considered
        let mut urbanized = HexTreeMap::<u8>::new();
        urbanized.insert(poi_built_urbanized, 0);
        urbanized.insert(poi_grass_urbanized, 0);
        urbanized.insert(poi_water_urbanized, 0);
        urbanized.insert(poi_no_data_built_urbanized, 0);
        urbanized.insert(poi_no_data_grass_urbanized, 0);
        urbanized.insert(poi_no_data_water_urbanized, 0);
        urbanized.insert(no_poi_built_urbanized, 0);
        urbanized.insert(no_poi_grass_urbanized, 0);
        urbanized.insert(no_poi_water_urbanized, 0);

        let inside_usa = [
            poi_built_urbanized,
            poi_grass_urbanized,
            poi_water_urbanized,
            poi_built_not_urbanized,
            poi_grass_not_urbanized,
            poi_water_not_urbanized,
            poi_no_data_built_urbanized,
            poi_no_data_grass_urbanized,
            poi_no_data_water_urbanized,
            poi_no_data_built_not_urbanized,
            poi_no_data_grass_not_urbanized,
            poi_no_data_water_not_urbanized,
            no_poi_built_urbanized,
            no_poi_grass_urbanized,
            no_poi_water_urbanized,
            no_poi_built_not_urbanized,
            no_poi_grass_not_urbanized,
            no_poi_water_not_urbanized,
        ];
        let geofence_set: HexTreeSet = inside_usa.iter().collect();
        let usa_geofence = Geofence::new(geofence_set, h3o::Resolution::Twelve);

        // These vectors are a standin for the file system
        let mut urbanized_buf = vec![];
        let mut footfall_buff = vec![];
        let mut landtype_buf = vec![];

        // Turn the HexTrees into DiskTrees
        urbanized.to_disktree(Cursor::new(&mut urbanized_buf), |w, v| w.write_all(&[*v]))?;
        footfall.to_disktree(Cursor::new(&mut footfall_buff), |w, v| w.write_all(&[*v]))?;
        landtype.to_disktree(Cursor::new(&mut landtype_buf), |w, v| w.write_all(&[*v]))?;

        let footfall = Footfall::new_mock(DiskTreeMap::with_buf(footfall_buff)?);
        let landtype = Landtype::new_mock(DiskTreeMap::with_buf(landtype_buf)?);
        let urbanization =
            Urbanization::new_mock(DiskTreeMap::with_buf(urbanized_buf)?, usa_geofence);

        // Let the testing commence
        let data = HexBoostData::new(footfall, landtype, urbanization);

        // NOTE(mj): formatting ignored to make it easier to see the expected change in assignments.
        // NOTE(mj): The semicolon at the end of the block is there to keep rust from
        // complaining about attributes on expression being experimental.
        #[rustfmt::skip]
        {
            use Assignment::*;
            // yellow
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: A }, data.assignments(poi_built_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: A }, data.assignments(poi_grass_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: A }, data.assignments(poi_water_urbanized).await?);
            // orange
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: B }, data.assignments(poi_built_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: B }, data.assignments(poi_grass_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: B }, data.assignments(poi_water_not_urbanized).await?);
            // light green
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: A }, data.assignments(poi_no_data_built_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: A }, data.assignments(poi_no_data_grass_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: A }, data.assignments(poi_no_data_water_urbanized).await?);
            // green
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: B }, data.assignments(poi_no_data_built_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: B }, data.assignments(poi_no_data_grass_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: B }, data.assignments(poi_no_data_water_not_urbanized).await?);
            // light blue
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: A }, data.assignments(no_poi_built_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: A }, data.assignments(no_poi_grass_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: A }, data.assignments(no_poi_water_urbanized).await?);
            // dark blue
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: B }, data.assignments(no_poi_built_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: B }, data.assignments(no_poi_grass_not_urbanized).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: B }, data.assignments(no_poi_water_not_urbanized).await?);
            // gray
            assert_eq!(HexAssignments { footfall: A, landtype: A, urbanized: C }, data.assignments(poi_built_outside_us).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: B, urbanized: C }, data.assignments(poi_grass_outside_us).await?);
            assert_eq!(HexAssignments { footfall: A, landtype: C, urbanized: C }, data.assignments(poi_water_outside_us).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: A, urbanized: C }, data.assignments(poi_no_data_built_outside_us).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: B, urbanized: C }, data.assignments(poi_no_data_grass_outside_us).await?);
            assert_eq!(HexAssignments { footfall: B, landtype: C, urbanized: C }, data.assignments(poi_no_data_water_outside_us).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: A, urbanized: C }, data.assignments(no_poi_built_outside_us).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: B, urbanized: C }, data.assignments(no_poi_grass_outside_us).await?);
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: C }, data.assignments(no_poi_water_outside_us).await?);
            // never inserted
            assert_eq!(HexAssignments { footfall: C, landtype: C, urbanized: C }, data.assignments(unknown_cell).await?);
        };

        Ok(())
    }
}
