use std::{collections::HashMap, path::PathBuf, pin::pin, time::Duration};

use chrono::Utc;
use dataset_downloader::{db, AssignedHex, DataSetDownloader, NewDataSetHandler, UnassignedHex};
use file_store::{
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, TimestampEncode},
    FileStore,
};
use futures_util::{Stream, TryFutureExt, TryStreamExt};
use helium_proto::services::poc_mobile::{self as proto, OracleBoostingReportV1};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::time::Instant;

use crate::{coverage::NewCoverageObjectNotification, Settings};

use hex_assignments::{HexBoostData, HexBoostDataAssignmentsExt};

pub struct DataSetDownloaderDaemon {
    data_set_downloader: DataSetDownloader,
    oracle_boostring_writer: OracleBoostingWriter,
    new_coverage_object_notification: NewCoverageObjectNotification,
    poll_duration: Duration,
}

impl ManagedTask for DataSetDownloaderDaemon {
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
                .and_then(|result| async move { result }),
        )
    }
}

impl DataSetDownloaderDaemon {
    pub async fn create_managed_task(
        pool: PgPool,
        settings: &Settings,
        file_upload: FileUpload,
        new_coverage_object_notification: NewCoverageObjectNotification,
    ) -> anyhow::Result<impl ManagedTask> {
        tracing::info!("Creating data set downloader task");
        let (oracle_boosting_reports, oracle_boosting_reports_server) =
            OracleBoostingReportV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Automatic,
                FileSinkRollTime::Duration(Duration::from_secs(15 * 60)),
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let data_set_downloader = Self::new(
            pool,
            HexBoostData::default(),
            FileStore::from_settings(&settings.data_sets).await?,
            oracle_boosting_reports,
            settings.data_sets_directory.clone(),
            new_coverage_object_notification,
            settings.data_sets_poll_duration,
        );

        Ok(TaskManager::builder()
            .add_task(oracle_boosting_reports_server)
            .add_task(data_set_downloader)
            .build())
    }
}

struct OracleBoostingWriter {
    data_set_processor: FileSinkClient<proto::OracleBoostingReportV1>,
}

#[async_trait::async_trait]
impl NewDataSetHandler for OracleBoostingWriter {
    async fn callback(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        data_sets: &HexBoostData,
    ) -> anyhow::Result<()> {
        let assigned_coverage_objs =
            AssignedCoverageObjects::assign_hex_stream(db::fetch_all_hexes(txn), data_sets).await?;
        assigned_coverage_objs
            .write(&self.data_set_processor)
            .await?;
        assigned_coverage_objs.save(txn).await?;
        Ok(())
    }
}

impl DataSetDownloaderDaemon {
    pub fn new(
        pool: PgPool,
        data_sets: HexBoostData,
        store: FileStore,
        data_set_processor: FileSinkClient<proto::OracleBoostingReportV1>,
        data_set_directory: PathBuf,
        new_coverage_object_notification: NewCoverageObjectNotification,
        poll_duration: Duration,
    ) -> Self {
        let data_set_downloader =
            DataSetDownloader::new(pool, data_sets, store, data_set_directory);
        let oracle_boostring_writer = OracleBoostingWriter { data_set_processor };
        Self {
            oracle_boostring_writer,
            data_set_downloader,
            new_coverage_object_notification,
            poll_duration,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        tracing::info!("Starting data set downloader task");
        self.data_set_downloader.fetch_first_datasets().await?;
        // Attempt to fill in any unassigned hexes. This is for the edge case in
        // which we shutdown before a coverage object updates.
        if self.data_set_downloader.is_hex_boost_data_ready() {
            let mut txn = self.data_set_downloader.pool.begin().await?;

            self.oracle_boostring_writer
                .data_set_processor
                .set_unassigned_oracle_boosting_assignments(
                    &mut txn,
                    &self.data_set_downloader.data_sets,
                )
                .await?;

            txn.commit().await?;
        }

        let mut wakeup = Instant::now() + self.poll_duration;
        loop {
            #[rustfmt::skip]
            tokio::select! {
                _ = self.new_coverage_object_notification.await_new_coverage_object() => {
                    let mut txn = self.data_set_downloader.pool.begin().await?;

                    // If we see a new coverage object, we want to assign only those hexes
                    // that don't have an assignment
                    if self.data_set_downloader.is_hex_boost_data_ready() {
                        self.oracle_boostring_writer.data_set_processor.set_unassigned_oracle_boosting_assignments(
                            &mut txn,
                            &self.data_set_downloader.data_sets,
                        ).await?;
                    }

                   txn.commit().await?;

                },
                _ = tokio::time::sleep_until(wakeup) => {
                    self.data_set_downloader.check_for_new_data_sets(&self.oracle_boostring_writer).await?;
                    wakeup = Instant::now() + self.poll_duration;
                }
            }
        }
    }
}

#[async_trait::async_trait]
pub trait DataSetProcessor<'a>: Send + Sync + 'static {
    async fn set_all_oracle_boosting_assignments(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<()>;

    async fn set_unassigned_oracle_boosting_assignments(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl DataSetProcessor<'_> for FileSinkClient<proto::OracleBoostingReportV1> {
    async fn set_all_oracle_boosting_assignments(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<()> {
        let assigned_coverage_objs =
            AssignedCoverageObjects::assign_hex_stream(db::fetch_all_hexes(txn), data_sets).await?;
        assigned_coverage_objs.write(self).await?;
        assigned_coverage_objs.save(txn).await?;
        Ok(())
    }

    async fn set_unassigned_oracle_boosting_assignments(
        &self,
        txn: &mut Transaction<'_, Postgres>,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<()> {
        let assigned_coverage_objs = AssignedCoverageObjects::assign_hex_stream(
            db::fetch_hexes_with_null_assignments(txn),
            data_sets,
        )
        .await?;
        assigned_coverage_objs.write(self).await?;
        assigned_coverage_objs.save(txn).await?;
        Ok(())
    }
}

// pub struct NopDataSetProcessor;
//
// #[async_trait::async_trait]
// impl DataSetProcessor for NopDataSetProcessor {
//     async fn set_all_oracle_boosting_assignments(
//         &self,
//         _pool: &PgPool,
//         _data_sets: &impl HexBoostDataAssignmentsExt,
//     ) -> anyhow::Result<()> {
//         Ok(())
//     }
//
//     async fn set_unassigned_oracle_boosting_assignments(
//         &self,
//         _pool: &PgPool,
//         _data_sets: &impl HexBoostDataAssignmentsExt,
//     ) -> anyhow::Result<()> {
//         Ok(())
//     }
// }

pub struct AssignedCoverageObjects {
    pub coverage_objs: HashMap<uuid::Uuid, Vec<AssignedHex>>,
}

impl AssignedCoverageObjects {
    pub async fn assign_hex_stream(
        stream: impl Stream<Item = sqlx::Result<UnassignedHex>>,
        data_sets: &impl HexBoostDataAssignmentsExt,
    ) -> anyhow::Result<Self> {
        let mut coverage_objs = HashMap::<uuid::Uuid, Vec<AssignedHex>>::new();
        let mut stream = pin!(stream);
        while let Some(hex) = stream.try_next().await? {
            let hex = hex.assign(data_sets)?;
            coverage_objs.entry(hex.uuid).or_default().push(hex);
        }
        Ok(Self { coverage_objs })
    }

    async fn write(
        &self,
        boosting_reports: &FileSinkClient<proto::OracleBoostingReportV1>,
    ) -> file_store::Result {
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
                        service_provider_override: hex.assignments.service_provider_override.into(),
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

    pub async fn save(self, txn: &mut Transaction<'_, Postgres>) -> anyhow::Result<()> {
        const NUMBER_OF_FIELDS_IN_QUERY: u16 = 8;
        const ASSIGNMENTS_MAX_BATCH_ENTRIES: usize =
            (u16::MAX / NUMBER_OF_FIELDS_IN_QUERY) as usize;

        let assigned_hexes: Vec<_> = self.coverage_objs.into_values().flatten().collect();
        for assigned_hexes in assigned_hexes.chunks(ASSIGNMENTS_MAX_BATCH_ENTRIES) {
            QueryBuilder::new(
                "INSERT INTO hexes (uuid, hex, signal_level, signal_power, footfall, landtype, urbanized, service_provider_override)",
            )
                .push_values(assigned_hexes, |mut b, hex| {
                    b.push_bind(hex.uuid)
                        .push_bind(hex.hex as i64)
                        .push_bind(hex.signal_level)
                        .push_bind(hex.signal_power)
                        .push_bind(hex.assignments.footfall)
                        .push_bind(hex.assignments.landtype)
                        .push_bind(hex.assignments.urbanized)
                        .push_bind(hex.assignments.service_provider_override);
                })
                .push(
                    r#"
                    ON CONFLICT (uuid, hex) DO UPDATE SET
                        footfall = EXCLUDED.footfall,
                        landtype = EXCLUDED.landtype,
                        urbanized = EXCLUDED.urbanized,
                        service_provider_override = EXCLUDED.service_provider_override
                    "#,
                )
                .build()
                .execute(&mut **txn)
                .await?;
        }

        Ok(())
    }
}
