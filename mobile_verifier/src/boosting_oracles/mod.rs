//! Module for handling the downloading of new data sets from S3.
//!
//! We've split this task into two parts, the CheckForNewDataSetsDaemon and the DataSetDownloaderDaemon.
//! As their names imply, the CheckForNewDataSetsDaemon is responsible for checking S3 for new files for
//! a given data set and inserting those new data sets into the `data_sets` table as [DataSetStatus::Pending].
//! The DataSetDownloaderDaemon is responsible for checking the `data_sets` table and downloading any new
//! data sets and processing them.
//!
//! It seems unnecessary to split this task into two separate daemons, why not have one daemon that handles
//! both? Well, firstly, it's not two daemons, it's actually four (when all data sets are implemented), since
//! CheckForNewDataSetsDaemon only handles one type of data set. And yes, it is unnecessary, as it would be
//! possible in theory to put everything into one daemon that continuously polls S3 and checks if needs to
//! download new files and if some files need to be processed.
//!
//! But it would be extremely complicated. For one thing, the DataSetDownloaderDaemon needs to update the
//! data sets at a particular time, specified by the data set's timestamp. Keeping tracking of everything
//! in one place would make things more prone to bugs, and the implementation becomes a lot simpler.

pub mod assignment;
pub mod footfall;
pub mod urbanization;

use std::{collections::HashMap, path::PathBuf};
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;

use crate::{
    boosting_oracles::assignment::footfall_and_urbanization_multiplier, coverage::SignalLevel,
};
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

pub struct DataSetDownloaderDaemon<T, A, B> {
    pool: PgPool,
    // Later: footfall and landtype
    data_set: Arc<Mutex<T>>,
    data_sets: HexBoostData<A, B>,
    store: FileStore,
    oracle_boosting_sink: FileSinkClient,
    data_set_directory: PathBuf,
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

impl<T, A, B> ManagedTask for DataSetDownloaderDaemon<T, A, B>
where
    T: DataSet,
    A: HexAssignment,
    B: HexAssignment,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::prelude::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<T, A, B> DataSetDownloaderDaemon<T, A, B>
where
    T: DataSet,
    A: HexAssignment,
    B: HexAssignment,
{
    pub fn new(
        pool: PgPool,
        data_set: Arc<Mutex<T>>,
        data_sets: HexBoostData<A, B>,
        store: FileStore,
        oracle_boosting_sink: FileSinkClient,
        data_set_directory: PathBuf,
    ) -> Self {
        Self {
            pool,
            data_set,
            data_sets,
            store,
            oracle_boosting_sink,
            data_set_directory,
        }
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

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        // Get the first data set:
        if let Some(time_to_use) =
            sqlx::query_scalar(
                "SELECT time_to_use FROM data_sets WHERE status = 'processed' AND data_set = $1 ORDER BY time_to_use DESC LIMIT 1"
            )
            .bind(T::TYPE)
            .fetch_optional(&self.pool)
            .await?
        {
            let data_set_path = self.get_data_set_path(time_to_use);
            tracing::info!("Found initial {} data set: {}", T::TYPE.to_prefix(), data_set_path.to_string_lossy());
            self.data_set.lock().await.update(&data_set_path, time_to_use)?;
        }

        // Another option I considered instead of polling was to use ENOTIFY, but that seemed
        // not as good, as it would hog a pool connection.
        //
        // We set the poll duration to 30 minutes since that seems fine.
        let poll_duration = Duration::minutes(30);

        loop {
            // Find the latest urbanization file
            tracing::info!("Checking for new data sets");
            let mut data_set = self.data_set.lock().await;
            let curr_data_set = data_set.timestamp();
            let latest_data_set: Option<NewDataSet> = sqlx::query_as(
                "SELECT filename, time_to_use, status FROM data_sets WHERE status != 'processed' AND data_set = $1 AND COALESCE(time_to_use > $2, TRUE) AND time_to_use <= $3 ORDER BY time_to_use DESC LIMIT 1"
            )
                .bind(T::TYPE)
                .bind(curr_data_set)
                .bind(Utc::now())
                .fetch_optional(&self.pool)
                .await?;

            if let Some(latest_data_set) = latest_data_set {
                let path = self.get_data_set_path(latest_data_set.time_to_use);

                // Download the file if it hasn't been downloaded already:
                if !latest_data_set.status.is_downloaded() {
                    tracing::info!("Downloading new data set: {}", path.to_string_lossy());
                    // TODO: abstract this out to a function
                    let stream = self.store.get_raw(latest_data_set.filename.clone()).await?;
                    let mut bytes = tokio_util::codec::FramedRead::new(
                        async_compression::tokio::bufread::GzipDecoder::new(
                            tokio_util::io::StreamReader::new(stream)
                        ),
                        tokio_util::codec::BytesCodec::new(),
                    );
                    let mut file = File::create(&path).await?;
                    while let Some(bytes) = bytes.next().await.transpose()? {
                        file.write_all(&bytes).await?;
                    }
                    // Set the status to be downloaded
                    sqlx::query("UPDATE data_sets SET status = 'downloaded' WHERE filename = $1")
                        .bind(&latest_data_set.filename)
                        .execute(&self.pool)
                        .await?;
                }

                // Now that we've downloaded the file, load it into the data set
                data_set.update(Path::new(&path), latest_data_set.time_to_use)?;

                drop(data_set);

                // Update the hexes
                let boosting_reports = set_oracle_boosting_assignments(
                    UnassignedHex::fetch_all(&self.pool),
                    &self.data_sets,
                    &self.pool,
                )
                .await?;

                sqlx::query("UPDATE data_sets SET status = 'processed' WHERE filename = $1")
                    .bind(latest_data_set.filename)
                    .execute(&self.pool)
                    .await?;

                self.oracle_boosting_sink
                    .write_all(boosting_reports)
                    .await?;
                self.oracle_boosting_sink.commit().await?;
                tracing::info!("Data set download complete");
            }

            // We don't want to shut down in the middle of downloading a data set, so we hold off until
            // we are sleeping
            #[rustfmt::skip]
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("DataSetDownloaderDaemon shutting down");
                    break;
                }
                _ = tokio::time::sleep(poll_duration.to_std()?) => {
                    continue;
                }
            }
        }

        Ok(())
    }
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

// Better name welcome
pub struct CheckForNewDataSetDaemon {
    pool: PgPool,
    store: FileStore,
    data_set: DataSetType,
}

impl ManagedTask for CheckForNewDataSetDaemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::prelude::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl CheckForNewDataSetDaemon {
    pub fn new(pool: PgPool, store: FileStore, data_set: DataSetType) -> Self {
        Self {
            pool,
            data_set,
            store,
        }
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        // We should check for new data sets more often than we download them. 15 minutes seems fine.
        let poll_duration = Duration::minutes(15);

        let prefix = self.data_set.to_prefix();
        let mut latest_file_date: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT time_to_use FROM data_sets WHERE data_set = $1 ORDER BY time_to_use DESC LIMIT 1")
                .bind(self.data_set)
            .fetch_optional(&self.pool)
            .await?;

        loop {
            #[rustfmt::skip]
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("CheckForNewDataSetDaemon shutting down");
                    break;
                }
                _ = tokio::time::sleep(poll_duration.to_std()?) => {
                    // tracing::info!("Checking file store for new data sets");
                    let mut new_data_sets = self.store.list(prefix, latest_file_date, None);
                    while let Some(new_data_set) = new_data_sets.next().await.transpose()? {
                        tracing::info!("Found new data set: {}, {:#?}", new_data_set.key, new_data_set);
                        sqlx::query( 
                            r#"
                            INSERT INTO data_sets (filename, data_set, time_to_use, status)
                            VALUES ($1, $2, $3, 'pending')
                            "#,
                        )
                        .bind(new_data_set.key)
                        .bind(self.data_set)
                        .bind(new_data_set.timestamp)
                        .execute(&self.pool)
                        .await?;
                        latest_file_date = Some(new_data_set.timestamp);
                    }
                }
            }
        }

        Ok(())
    }
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

pub trait HexAssignment: Send + Sync + 'static {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment>;
}

pub struct HexBoostData<Urban, Foot> {
    pub urbanization: Arc<Mutex<Urban>>,
    pub footfall: Arc<Mutex<Foot>>,
}

impl<U, F> Clone for HexBoostData<U, F> {
    fn clone(&self) -> Self {
        Self {
            urbanization: self.urbanization.clone(),
            footfall: self.footfall.clone(),
        }
    }
}

impl<Urban, Foot> HexBoostData<Urban, Foot> {
    pub fn new(urbanization: Urban, footfall: Foot) -> Self {
        Self {
            urbanization: Arc::new(Mutex::new(urbanization)),
            footfall: Arc::new(Mutex::new(footfall)),
        }
    }
}

impl<Urban, Foot> HexBoostData<Urban, Foot>
where
    Urban: DataSet,
    Foot: DataSet,
{
    pub async fn is_ready(&self) -> bool {
        self.urbanization.lock().await.is_ready() && self.footfall.lock().await.is_ready()
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
            "SELECT uuid, hex, signal_level, signal_power FROM hexes WHERE urbanized IS NULL",
        )
        .fetch(pool)
    }
}

pub async fn set_oracle_boosting_assignments<'a>(
    unassigned_hexes: impl Stream<Item = sqlx::Result<UnassignedHex>>,
    data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment>,
    pool: &'a PgPool,
) -> anyhow::Result<impl Iterator<Item = proto::OracleBoostingReportV1>> {
    const NUMBER_OF_FIELDS_IN_QUERY: u16 = 5;
    const ASSIGNMENTS_MAX_BATCH_ENTRIES: usize = (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

    let now = Utc::now();
    let mut boost_results = HashMap::<Uuid, Vec<proto::OracleBoostingHexAssignment>>::new();
    let mut unassigned_hexes = pin!(unassigned_hexes.try_chunks(ASSIGNMENTS_MAX_BATCH_ENTRIES));

    let urbanization = data_sets.urbanization.lock().await;
    let footfall = data_sets.footfall.lock().await;

    while let Some(hexes) = unassigned_hexes.try_next().await? {
        let hexes: anyhow::Result<Vec<_>> = hexes
            .into_iter()
            .map(|hex| {
                let cell = hextree::Cell::try_from(hex.hex)?;
                let urbanized = urbanization.assignment(cell)?;
                let footfall = footfall.assignment(cell)?;
                let location = format!("{:x}", hex.hex);
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

impl HexAssignment for Assignment {
    fn assignment(&self, _cell: hextree::Cell) -> anyhow::Result<Assignment> {
        Ok(*self)
    }
}

impl HexAssignment for HashMap<hextree::Cell, bool> {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let assignment = match self.get(&cell) {
            Some(true) => Assignment::A,
            Some(false) => Assignment::B,
            None => Assignment::C,
        };
        Ok(assignment)
    }
}
