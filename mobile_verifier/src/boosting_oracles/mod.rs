pub mod assignment;
pub mod footfall;
pub mod urbanization;

use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

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
    data_set: Arc<Mutex<T>>,
    data_sets: HexBoostData<A, B>,
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

impl<T, A, B> DataSetDownloaderDaemon<T, A, B>
where
    T: DataSet,
    A: HexAssignment,
    B: HexAssignment,
{
    pub async fn new(
        pool: PgPool,
        data_set: Arc<Mutex<T>>,
        data_sets: HexBoostData<A, B>,
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

        let mut transaction = pool.begin().await?;
        sqlx::query("LOCK TABLE hexes")
            .execute(&mut transaction)
            .await?;
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
