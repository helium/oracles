use anyhow::Context;
use chrono::{DateTime, Utc};
use file_store::{
    file_info::FileInfo, file_info_poller::FileInfoStream, file_source, traits::MsgDecode,
    BucketClient,
};
use file_store_oracles::FileType;
use futures::StreamExt;
use helium_iceberg::{
    BatchedWriter, BatchedWriterConfig, BatchedWriterTask, Catalog, TableDefinition,
};

use sqlx::PgPool;
use std::time::{Duration, Instant};
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub trait IcebergBackfill: Send + 'static {
    type FileRecord: MsgDecode + Clone + Send + Sync + 'static;
    type IcebergRow: serde::Serialize + Send + Sync + 'static;

    const FILE_TYPE: FileType;

    /// Return `Some(row)` to write the record, `None` to skip it.
    fn convert(record: Self::FileRecord) -> Option<Self::IcebergRow>;
}

pub struct BackfillOptions {
    pub process_name: String,
    pub start_after: DateTime<Utc>,
    pub stop_after: DateTime<Utc>,
    pub poll_duration: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}

pub struct BackfillPollerServer(Option<Box<dyn ManagedTask>>);

impl BackfillPollerServer {
    fn noop() -> Self {
        Self(None)
    }

    fn active(server: impl ManagedTask + 'static) -> Self {
        Self(Some(Box::new(server)))
    }
}

impl ManagedTask for BackfillPollerServer {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        match (*self).0 {
            Some(task) => task.start_task(shutdown),
            None => task_manager::spawn(async { anyhow::Ok(()) }),
        }
    }
}

pub struct Backfiller<C: IcebergBackfill> {
    pool: PgPool,
    reports: Receiver<FileInfoStream<C::FileRecord>>,
    writer: Option<BatchedWriter<C::IcebergRow>>,
    done: bool,
}

impl<C: IcebergBackfill> Backfiller<C> {
    pub fn new(
        pool: PgPool,
        reports: Receiver<FileInfoStream<C::FileRecord>>,
        writer: Option<BatchedWriter<C::IcebergRow>>,
    ) -> Self {
        let done = writer.is_none();
        Self {
            pool,
            reports,
            writer,
            done,
        }
    }

    pub async fn recv(&mut self) -> Option<FileInfoStream<C::FileRecord>> {
        if self.done {
            std::future::pending().await
        } else {
            self.reports.recv().await
        }
    }

    pub async fn handle(
        &mut self,
        file: Option<FileInfoStream<C::FileRecord>>,
    ) -> anyhow::Result<()> {
        let Some(stream) = file else {
            tracing::info!("{} backfiller completed", C::FILE_TYPE);
            self.done = true;
            return Ok(());
        };
        tracing::info!(
            file = %stream.file_info,
            age = %file_age(&stream.file_info),
            "backfilling {} file",
            C::FILE_TYPE
        );
        self.handle_file(stream).await
    }

    async fn handle_file(&self, file: FileInfoStream<C::FileRecord>) -> anyhow::Result<()> {
        let Some(ref writer) = self.writer else {
            return Ok(());
        };

        let start = Instant::now();

        let file_info = file.file_info.clone();
        let mut txn = self.pool.begin().await?;

        let all: Vec<_> = file.into_stream(&mut txn).await?.collect().await;
        let total = all.len();

        let rows: Vec<C::IcebergRow> = all.into_iter().filter_map(C::convert).collect();
        let written = rows.len();

        if !rows.is_empty() {
            writer
                .queue_all(rows)
                .await
                .with_context(|| format!("queuing {} backfill", C::FILE_TYPE))?;
        }

        txn.commit().await?;
        tracing::info!(
            file = %file_info,
            written,
            skipped = total - written,
            age = %file_age(&file_info),
            duration = ?start.elapsed(),
            "backfilled {} file",
            C::FILE_TYPE
        );
        Ok(())
    }

    async fn run(mut self, mut shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("{} backfiller starting", C::FILE_TYPE);
        loop {
            if self.done {
                tracing::info!("{} backfiller complete", C::FILE_TYPE);
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::info!("{} backfiller shutting down", C::FILE_TYPE);
                    return Ok(());
                }
                file = self.recv() => {
                    self.handle(file).await?;
                }
            }
        }
    }
}

// `create` needs extra bounds because `file_source::continuous_source()` uses
// `MsgDecodeFileInfoPollerParser`, which requires the error type to be `std::error::Error`
// and the proto message type to implement `prost::Message + Default`.
impl<C: IcebergBackfill> Backfiller<C>
where
    <C::FileRecord as TryFrom<<C::FileRecord as MsgDecode>::Msg>>::Error: std::error::Error,
    <C::FileRecord as MsgDecode>::Msg: prost::Message + Default,
{
    /// Like `create`, but writes go through a `BatchedWriter` (on-disk spool +
    /// size/time-bounded snapshots) instead of per-file `write_idempotent`.
    /// Caller is responsible for registering the `BatchedWriterTask` returned
    /// alongside the writer with the `TaskManager`.
    pub async fn create_batched(
        pool: PgPool,
        bucket_client: BucketClient,
        writer: Option<BatchedWriter<C::IcebergRow>>,
        options: Option<BackfillOptions>,
    ) -> anyhow::Result<(Self, BackfillPollerServer)> {
        let (Some(writer), Some(options)) = (writer, options) else {
            let (_, rx) = tokio::sync::mpsc::channel(1);
            return Ok((
                Backfiller::new(pool, rx, None),
                BackfillPollerServer::noop(),
            ));
        };

        let (reports, reports_server) = file_source::continuous_source()
            .state(pool.clone())
            .bucket_client(bucket_client)
            .prefix(C::FILE_TYPE.to_string())
            .lookback_start_after(options.start_after)
            .stop_after(options.stop_after)
            .process_name(options.process_name)
            .poll_duration_opt(options.poll_duration)
            .idle_timeout_opt(options.idle_timeout)
            .create()
            .await?;

        let backfiller = Backfiller {
            pool,
            reports,
            writer: Some(writer),
            done: false,
        };
        Ok((backfiller, BackfillPollerServer::active(reports_server)))
    }
}

impl<C: IcebergBackfill> ManagedTask for Backfiller<C> {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

fn file_age(file_info: &FileInfo) -> String {
    let age = chrono::Utc::now().signed_duration_since(file_info.timestamp);

    let total_seconds = age.num_seconds();
    if total_seconds < 0 {
        return "in the future".to_string();
    }

    let days = age.num_days();
    let hours = age.num_hours() % 24;
    let minutes = age.num_minutes() % 60;

    match (days, hours, minutes) {
        (0, 0, m) => format!("{m}m ago"),
        (0, h, m) => format!("{h}h {m}m ago"),
        (d, h, _) => format!("{d}d {h}h ago"),
    }
}

#[async_trait::async_trait]
pub trait BatchedWriterExt: Sized {
    async fn batched_writer(
        catalog: Catalog,
        table_def: TableDefinition,
        config: BatchedWriterConfig,
    ) -> anyhow::Result<(BatchedWriter<Self>, BatchedWriterTask<Self>)>;
}

#[async_trait::async_trait]
impl<T> BatchedWriterExt for T
where
    T: serde::Serialize + Send + Sync + 'static,
{
    async fn batched_writer(
        catalog: Catalog,
        table_def: TableDefinition,
        config: BatchedWriterConfig,
    ) -> anyhow::Result<(BatchedWriter<Self>, BatchedWriterTask<Self>)> {
        catalog
            .create_namespace_if_not_exists(table_def.namespace())
            .await?;
        let table = catalog.create_table_if_not_exists(table_def).await?;
        Ok(BatchedWriter::new(table, config))
    }
}
