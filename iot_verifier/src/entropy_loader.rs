use crate::entropy::Entropy;
use blake3::hash;
use file_store::{entropy_report::EntropyReport, file_info_poller::FileInfoStream};
use futures::{future::LocalBoxFuture, StreamExt, TryStreamExt};
use sqlx::PgPool;
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

pub struct EntropyLoader {
    pub pool: PgPool,
    file_receiver: Receiver<FileInfoStream<EntropyReport>>,
}

#[derive(thiserror::Error, Debug)]
pub enum NewLoaderError {
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
    #[error("db_store error: {0}")]
    DbStoreError(#[from] db_store::Error),
}

impl ManagedTask for EntropyLoader {
    fn start_task(
        self: Box<Self>,
        token: CancellationToken,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(token))
    }
}

impl EntropyLoader {
    pub async fn run(mut self, token: CancellationToken) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                msg = self.file_receiver.recv() => if let Some(stream) =  msg {
                    self.handle_report(stream).await?;
                }
            }
        }
        tracing::info!("stopping verifier entropy_loader");
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
                metrics::increment_counter!("oracles_iot_verifier_loader_entropy");
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        Ok(())
    }
}
