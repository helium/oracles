//
// consumes entropy reports from S3 and inserts them into the database
// entropy reports are generated and pushed to s3 by the entropy service
// entropy report data is required by the runner in order to validate a POC
//

use crate::entropy::Entropy;
use blake3::hash;
use file_store::file_info_poller::FileInfoStream;
use file_store_oracles::network_common::entropy_report::EntropyReport;
use futures::{StreamExt, TryStreamExt};
use sqlx::PgPool;
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub struct EntropyLoader {
    pub pool: PgPool,
    pub file_receiver: Receiver<FileInfoStream<EntropyReport>>,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl ManagedTask for EntropyLoader {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl EntropyLoader {
    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting entropy_loader");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                msg = self.file_receiver.recv() => if let Some(stream) =  msg {
                    self.handle_report(stream).await?;
                }
            }
        }
        tracing::info!("stopping entropy_loader");
        Ok(())
    }

    async fn handle_report(
        &self,
        file_info_stream: FileInfoStream<EntropyReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, report| async move {
                let id = hash(&report.data).as_bytes().to_vec();
                Entropy::insert_into(
                    &mut transaction,
                    &id,
                    &report.data,
                    &report.timestamp,
                    report.version as i32,
                )
                .await?;
                metrics::counter!("oracles_iot_verifier_loader_entropy").increment(1);
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}
